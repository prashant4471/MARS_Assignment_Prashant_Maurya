# MARS Order & Inventory Sync Service

## 1. Goals and architecture

Run stateless NestJS adapters, APIs, and autoscaled workers on ECS/Fargate. RDS PostgreSQL is the source of truth for the durable inbox, orders, inventory, ledger, and outbox. Partition inbox consumers by channel. The outbox relay publishes to one ERP queue plus per-channel order-action and stock queues, each with a DLQ. Redis holds token buckets only; if unavailable, pause affected channel stock workers rather than fail open. Scale workers on inbox/queue age. At 8,000 orders/day and a 5-orders/second peak, SQS avoids operating Kafka or EKS.

**Cost model (Mumbai, `ap-south-1`):** calculator baseline: RDS `db.t4g.medium` Multi-AZ, 100 GB gp3/7-day backups; two API Fargate tasks (0.25 vCPU/0.5 GB), two worker tasks scaling to six; one `cache.t4g.micro`, one ALB, two NAT gateways, nine SQS queues plus DLQs, and 30-day logs. This sizing is not a verified ₹50,000 quote. Price NAT traffic, logs, backups, and cross-AZ transfer in the [AWS Pricing Calculator](https://calculator.aws/); alert at ₹40,000/₹50,000. If over cap, document the availability trade-off without silently removing database durability. RDS pricing varies by region, instance, storage, and backup ([RDS](https://aws.amazon.com/rds/postgresql/pricing/)); Fargate by requested CPU/memory ([Fargate](https://aws.amazon.com/fargate/pricing/)); Redis by cache usage ([ElastiCache](https://aws.amazon.com/elasticache/pricing/)); NAT by hours/data processed ([VPC](https://aws.amazon.com/vpc/pricing/)).

![Order and inventory sync architecture](architecture.svg)

Diagram source: [architecture.mmd](architecture.mmd).

## 2. Data model and ingestion

- `channel_event`: channel, non-null `dedupe_key`, source revision, order ID, event time, raw JSON, status. Unique `(channel, dedupe_key)`; use source event ID or derive from `(external_order_id, event_type, source_revision)`. Persist before webhook success; this inbox is the durable work queue.
- `channel_order`: unique `(channel, external_order_id)`, state/revision, rejection reason. `order_item`: order, SKU, positive quantity, unit price.
- `sku_inventory`: SKU, `on_hand_qty`, `reserved_qty`, version, updated time; checks keep quantities nonnegative. `sellable = on_hand - reserved - safety_buffer`.
- `channel_allocation`: SKU, channel, nonnegative allocated listing quantity, version; unique `(sku, channel)`. For each SKU, allocation sum must not exceed sellable stock; enforce it while holding that SKU's inventory row lock.
- `inventory_ledger`: immutable `mutation_id`, SKU, channel, source event, on-hand/reserved deltas, reason/actor, before/after quantities, event time. Unique `(mutation_id, sku)` prevents duplicate movements, including manual changes and returns.
- `outbox`: event type, aggregate/version, payload, attempts, next retry time, status. Unique `(order_id, event_type)` for ERP/rejection actions and `(channel, sku, version)` for stock state.
- `channel_stock_state` / `failed_sync`: desired and last acknowledged quantities/versions, lease/retry state, last error. Ops lists unresolved entries and can retry one by re-publishing its latest desired state.

Adapters authenticate, validate, normalize, and persist raw events; acknowledge only after commit. Flipkart stores its poll cursor with each batch; Blinkit's CSV is for reconciliation. Apply only newer source revisions through a state machine, so an old create cannot undo a cancellation. Retain raw payloads for replay.

## 3. Reservation and lifecycle consistency

The website reserves before confirming checkout; marketplace orders reserve as soon as events arrive. A successful reservation is OMS confirmation. In one transaction, claim the transition idempotently, lock SKU rows in sorted order (`SELECT … FOR UPDATE`), and require both total sellable stock and source-channel allocation for every line. Never borrow another channel's allocation. Reject the whole order if either check fails. Otherwise increase `reserved_qty`, decrement that allocation, append ledger rows, increment versions, and write ERP/stock outbox rows. Sorted locks avoid deadlocks; row locks serialize last-unit sales. Make no downstream calls in this transaction.

On failure, persist `REJECTED_OUT_OF_STOCK` and an `ORDER_REJECTED` outbox action. After commit, a worker cancels at the marketplace using key `channel:external_order_id:reject:dedupe_key`; website checkout fails synchronously. No reservation or stock-ledger delta is written. This assumes channels support rejection/cancellation.

For two last-unit requests, the first transaction reserves it; the second waits and rejects without a decrement. Duplicate transitions cannot reserve twice. At dispatch, decrement on-hand and reserved with a ledger entry. Cancellation releases the reservation; a return becomes sellable after receipt and inspection.

Never publish full warehouse stock to every channel. Seed capped allocations from recent sell-through and leave a safety buffer unlisted. Unallocated stock is `sellable - sum(channel_allocations)` and stays off listings until assigned. Cancellations and inspected returns increase this pool; reallocate only after reconciling source-channel state, then publish new absolute quantities. Do not automatically restore the same channel's allocation.

The database can prevent overselling among orders it has received. It cannot undo a sale made on a marketplace against a stale listing before that marketplace reports it. The allocation cap, local listing decrement, safety buffer, rapid ingestion, and reconciliation reduce that risk; the 0.5% out-of-stock cancellation rate is an SLO to measure and tune, not a hard guarantee under unbounded channel delay.

## 4. Stock publishing and capacity

Stock events carry SKU/channel versions. Workers lease a `(channel, SKU)` state row, coalesce messages, read the latest desired absolute quantity, and serialize writes per key so stale messages cannot restore old values. Skip calls when desired quantity matches the last acknowledgement. Retry the same value on timeout. Reconciliation compares channel snapshots with desired state and enqueues repairs.

Use separate token buckets and queues: website direct write; Nykaa 5 requests/second at one SKU per call; Flipkart 10 requests/second with up to 50 SKUs per call; Blinkit REST with a circuit breaker and retry for its roughly 2% timeout rate. Honor `Retry-After`, use exponential backoff with jitter for 429/5xx/network failures, and keep one channel's backlog from blocking another.

Measure from inventory commit to channel acknowledgement; target p99 ≤60 seconds under normal load with healthy APIs within quota. Nykaa can send 300 distinct SKU updates/minute; Flipkart can batch 30,000 (10 requests/second × 50). Blinkit's ~2% timeout rate is known, but its quota is not; measure with the partner. Coalescing helps repeated changes, not distinct SKUs. Over 300 Nykaa SKU changes/minute cannot meet 60 seconds without a higher quota or reduced sale exposure; more workers cannot bypass quotas.

## 5. Delivery failures and operations

The order transaction writes one ERP create event to the outbox. A worker submits it with idempotency key `channel:external_order_id:create`; ambiguous timeouts retry with the same key. This is at-least-once transport with exactly-once effect if Business Central honors the key or offers lookup by that key. Sunday downtime leaves durable work for recovery. Exactly-once effect requires receiver deduplication.

Retry transient failures with bounded exponential backoff and jitter. Fail fast on invalid SKU/payload errors. After the retry budget, retain the payload/error in a DLQ and show it in the ops console with channel, order/SKU link, attempts, and a per-item retry action. A retry reuses the idempotency key or latest desired stock state; it does not replay unrelated events. Blinkit timeouts trip only its circuit breaker. Poll/webhook ingestion, ERP, and stock queues remain isolated.

Day-one alerts:

1. Inbox age and poll cursor lag by channel; alert on missing/late ingestion.
2. Oldest ERP outbox age, queue depth, and delivery failures; alert before orders age through a Sunday outage.
3. Stock acknowledgement latency and 429/timeout rate per channel; page when the 60-second normal-load target is missed.
4. Out-of-stock rejections as a share of orders (target <0.5%), by channel/SKU.
5. Inventory invariants: negative quantities, allocation sum above sellable stock, or unexplained ledger deltas.

## 6. Assumptions, trade-offs, and scope

**Assumptions:** one warehouse; website checkout calls the reservation endpoint; channel quantities are integer units; listings decrement on sale; stock APIs set absolute quantities and expose stock reads/exports; each source supplies a stable event ID/revision (otherwise derive the non-null dedupe key above); channels support rejecting/cancelling an unreservable order; all lines reserve atomically; cancellation releases stock; returns become sellable only after inspection; Business Central supports a unique idempotency key. Flipkart's two-minute poll delay is an input limitation, so the 60-second push clock starts at internal commit, not at an unpolled marketplace sale.

| Decision | Chosen | Rejected | Why |
|---|---|---|---|
| Reservation | PostgreSQL row locks, sorted SKU order, and a ledger in one transaction | Redis lock or check-then-update | PostgreSQL owns the stock and atomically enforces the last-unit boundary and audit trail. |
| Async delivery | Transactional outbox to SQS; receiver idempotency for ERP | Call ERP/channel APIs in the order transaction | Survives outages and ambiguous timeouts without losing a confirmed order; SQS is managed and small at this volume. |
| Channel stock | Capped per-channel allocations, buffer, and coalesced absolute writes | Publish the same global stock to every listing | Prevents each channel from independently selling the entire warehouse pool and makes retries safe. |

For v1, defer multiple warehouses, partial-line fulfillment, demand forecasting, a custom message bus, and an LLM ops assistant. They add complexity beyond the six-week, three-person scope; first ship correct reservations, reconciliation, retry tooling, and useful alerts.

**AI use:** OpenAI Codex was used to interpret the supplied brief and draft/review this design.
