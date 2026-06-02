# Layered Telemetry Storage Architecture — Design Plan

Status: **proposal / RFC** · Owner: Telemetry · Scope: `telemtry/` ingest + storage + read path

This document proposes a **layered (tiered) database system** behind a **single
data-access abstraction**, so that callers issue the same request regardless of
where the data physically lives. The router decides which tier answers, which
keeps Postgres from being stressed by hot ("tier-1") live-dashboard traffic and
makes the pipeline faster, more reliable, and safer to operate trackside.

---

## 1. Current pipeline (what we have today)

```
 Car (BEVO cand/publishd)
   └─ protobuf over MQTT (Mosquitto local, or AWS MQTT)  topics: data/*, orion/*, angelique/*, nightwatch/*
        │
        ▼
 ingest: telemtry/stack/ingest/mqtt_handler.py   (ONE process, ONE paho thread)
   │  on_message() fans every packet TWO ways:
   │
   ├─(A) REALTIME ─ _send_to_bridge() → bridge_queue → gRPC → Go kafka-bridge
   │        → Kafka topic grafana_data_<car> (flattened JSON)  ─┐
   │        → Kafka topic sensor_data (raw protobuf)            │→ Grafana "real-time" dashboards
   │                                                            │   (custom lhre-kafka-datasource)
   │
   └─(B) DURABLE ─ _proto_ingest() → MessageToDict → split into table rows
            → per-car db_queue (Queue) → _db_worker (batch 1000 jobs / 5s)
            → Postgres  (SET LOCAL synchronous_commit = off)              → Grafana "retrospect"
            → also: CSV log per session (_csv_worker)                        dashboards (postgres DS)
```

### Storage today

- **Postgres 17**, one container, three logical DBs: `telemetry` (Nightwatch),
  `angelique`, `orion`. Schema per car: `packet` (id, time) + child tables
  (`dynamics`, `controls`, `pack`, `diagnostics_high/low`, `thermal`,
  `board_status`), all keyed by `packet_id`.
- **Hand-rolled time partitioning:** `partitions` table + `partition_manager.py`
  + `get_partition_bounds()` SQL function, used by retrospect dashboards to
  bucket/downsample over a time range.
- **Kafka** is used as a **realtime transport + 7-day retention log**, *not* as a
  queryable store. Grafana's Kafka datasource tails `grafana_data_<car>` for live
  panels.
- **`QueryBuilder` + `get_db` + `DBTarget`** (`analysis/sql_utils/`) is the single
  existing DB abstraction. This is the natural seam to layer behind.

### What's good (keep it)

- Proto is the single source of truth; one ingest fan-out to realtime + durable.
- Writes are already **batched + queued off the MQTT thread** (good throughput design).
- `QueryBuilder` already centralizes DB access — we extend it, not replace it.
- Kafka already exists and is genuinely good at being a durable replay bus.

### Deployment reality (drives every sizing decision below)

The whole stack runs on **one machine**: a Dell Precision **tower workstation,
~64 GB RAM, NVMe/SSD**. Everything — ingest, Postgres, Kafka + the Go bridge,
Grafana, processors — is co-located on it. The **only** remote component is the
**MQTT broker** (AWS, via `AWS_MQTT_IP`); the car publishes there and the tower
subscribes.

Consequences:

- The enemy is **resource contention on one box**, not network/multi-node
  scaling. "Load balancing" here means **isolating reads off the write path
  *inside one machine***, not spreading across nodes.
- With **64 GB + NVMe** there is generous headroom: we can run Redis (Tier 0) and
  TimescaleDB (Tier 1/2) **alongside** the existing Kafka bus without starving it.
  64 GB is enough that the OS page cache keeps warm Postgres/Timescale chunks in
  RAM, and NVMe makes hypertable writes + compression cheap.
- **Decision: keep Kafka as the durable bus** (the Go bridge + Grafana Kafka
  datasource stay). The box can afford the JVM, and reusing the wired-up path
  beats a rewrite. We add Redis + Timescale around it rather than replacing it.

### Pain points this plan addresses

| # | Problem (observed in code) | Impact |
|---|---|---|
| P1 | "Tier-1" live reads (Grafana retrospect + live viewer + packet-id handshake) hit the **same Postgres** that's absorbing the write firehose. | Read load competes with ingest; slow panels stress the DB during a session. |
| P2 | `synchronous_commit = off` (`mqtt_handler.py:202`) trades durability for speed. Source of truth is the **car-side CSV**, gaps "backfillable" — but there is no automated replay path. | A crash loses the in-flight window with no automatic recovery. |
| P3 | Bounded queues drop on full (`put_nowait` → `queue.Full` → drop) on both the realtime and DB paths. | Silent data loss under burst/backpressure. |
| P4 | Ingest is a **single process / single paho thread** — no horizontal scaling, single point of failure. | One bad packet / wedged channel stalls everything (history of rc16 disconnects, see comments). |
| P5 | `next_packet_id` handshake does `MAX(packet_id)` on Postgres at connect (`mqtt_handler.py:296,313`). | Car can't start logging if Postgres is slow/down. |
| P6 | Replay is **not idempotent**: child rows are `packet_id` PK with no `ON CONFLICT`; `execute_insert` does plain `INSERT`. Re-ingesting a packet → IntegrityError → whole batch rolls back. | Backfill/replay is unsafe today. |
| P7 | Hand-rolled partitioning + name-keyed schema copies are fragile (see `TELEMETRY_FIELD_PIPELINE.md`). | Silent 0/NULL fields; manual partition bookkeeping. |

---

## 2. Reframe: what "tier 1" means, and the honest data scale

From `mqtt_handler.py` batching comments (~125 packets/commit ≈ 1–2 s), aggregate
packet rate is on the order of **tens to low-hundreds of Hz**, each packet a
handful of small rows. **This is not "big data."** Postgres can absorb the *write*
volume comfortably.

The real problem is **not write throughput — it's read isolation and access
pattern**:

- **Tier 1 = hot path**: "what is the value *right now* / in the last few seconds"
  — live dashboards, the live viewer, last-known-value gauges, the board-status
  panel, the packet-id handshake. These are **high-frequency, latest/last-N,
  latency-sensitive** reads that should *never* touch the same engine doing the
  durable writes.
- **Tier 2 = warm path**: "this session / the last N minutes at reduced
  resolution" — retrospect dashboards during a test day.
- **Tier 3 = cold path**: "a drive day from 3 weeks ago" — offline analysis.

So the fix is **tiering by data age + query shape**, with a router — *not*
replacing Postgres wholesale. Postgres stays excellent for Tier 2/3 SQL.

---

## 3. Target architecture

```
 Car → MQTT ───────────────► ingest (stateless, N replicas)
                                  │  decode + validate proto ONCE
                                  ▼
                         ┌────────────────────┐
                         │   Kafka (the BUS)   │   durable, replayable, partitioned by car
                         │  sensor_data (proto)│   = write-ahead log for the whole system
                         └─────────┬──────────┘
            ┌──────────────────────┼───────────────────────────┐
            ▼                      ▼                             ▼
   Tier 0: HOT (Redis)    Tier 1/2: WARM (TimescaleDB)   realtime fan-out
   last-value cache +     Postgres + hypertables:         grafana_data_<car>
   last-N ring (Streams)  auto time-partition, compression, (Grafana live DS — unchanged)
   ms reads, pub/sub      continuous aggregates (downsample),
                          retention policy
                                   │
                                   ▼
                         Tier 3: COLD (object store / Parquet + DuckDB)
                         compressed session archives for offline analysis
                                   ▲
                                   └── car-side CSV remains an independent backup
```

Everything is fed by **Kafka consumers**, so each sink (hot cache, warm DB, cold
archive) scales and fails **independently**. Ingest's only job becomes "decode +
validate + produce to Kafka," which makes it stateless and horizontally
scalable (fixes P4), and removes the synchronous Postgres write from the hot
path (fixes P2/P5).

### 3.1 The single abstraction — `TelemetryStore`

One façade. Callers (Grafana-backing API, live viewer, processors, analysis
notebooks) **never choose a tier**. The façade routes.

```python
# analysis/sql_utils/telemetry_store.py  (new — wraps existing QueryBuilder)

class TelemetryStore:
    """Single entry point for all telemetry reads/writes.
    Callers express intent (latest / window / range); the router picks the tier.
    Postgres access still goes through QueryBuilder, so existing SQL keeps working."""

    # ---- writes (used by the Kafka→sink consumers, not the hot MQTT thread) ----
    def write(self, car: str, packet: dict) -> None: ...
        # fan-out: Tier0 hot cache (set latest + push ring) AND Tier1 warm upsert

    # ---- reads: same call regardless of where data lives ----
    def latest(self, car: str, fields: list[str]) -> dict: ...
        # Tier 0 only (Redis last-value). Never touches Postgres.

    def last_n(self, car: str, fields: list[str], n: int) -> list[dict]: ...
        # Tier 0 ring (Redis Stream XREVRANGE) for small N; else Tier 1.

    def window(self, car, fields, t0, t1, max_points=5000) -> list[dict]: ...
        # ROUTER decides (see §3.2): hot cache / warm hypertable / warm
        # continuous-aggregate / cold Parquet — caller doesn't know or care.
```

Routing decision (the whole point — "keeps requests the same, handles where to
pull from"):

```python
def _route(self, t0, t1, max_points):
    age   = now() - t1
    span  = t1 - t0
    res   = span / max_points          # requested seconds-per-point
    if age <= HOT_HORIZON and span <= HOT_HORIZON:   # e.g. last 60 s
        return Tier.HOT                 # Redis ring — zero Postgres load
    if res <= RAW_RES_THRESHOLD:                      # needs full-res rows
        return Tier.WARM_RAW            # Timescale hypertable (recent chunks)
    if t1 >= now() - WARM_RETENTION:                  # within retention window
        return Tier.WARM_ROLLUP         # Timescale continuous aggregate (downsampled)
    return Tier.COLD                    # Parquet/DuckDB archive (read replica path)
```

### 3.2 Tier-by-tier

**Tier 0 — HOT (Redis).** Serves every "live" read.
- `HSET car:orion:latest <field> <val>` → O(1) last-value for gauges/banners/board-status.
- `XADD car:orion:ring * ...` capped (`MAXLEN ~ 10k`) → last-N-seconds ring for
  live time-series without paging Postgres.
- Redis **pub/sub** lets *many* dashboard clients subscribe to one stream → read
  fan-out / load-balancing off the DB (addresses P1).
- The packet-id handshake reads a Redis counter instead of `MAX(packet_id)` on
  Postgres (fixes P5).
- Volatile by design; loss is fine — Kafka + Timescale are the durable copies.

**Tier 1/2 — WARM (TimescaleDB).** This is the highest-leverage, lowest-friction
change: **TimescaleDB is a Postgres extension**, so `QueryBuilder`, the existing
SQL, the `get_partition_bounds` style downsampling, and Grafana's Postgres
datasource **all keep working unchanged**. We gain:
- **Hypertables**: automatic time-partitioning → *replaces* the hand-rolled
  `partitions` table + `partition_manager.py` (removes P7's manual bookkeeping).
- **Continuous aggregates**: pre-computed downsampled rollups (1 s / 1 min
  buckets) → retrospect dashboards read a tiny rollup instead of scanning raw
  rows → massive read-stress reduction.
- **Native compression** on older chunks (10×+) and **retention policies** (drop
  raw chunks after N days, keep rollups).
- Run it as the **same Postgres container/image** (`timescale/timescaledb:pg17`)
  — migration is `CREATE EXTENSION` + `create_hypertable()`, not a rewrite.

**Tier 3 — COLD (Parquet + DuckDB / read replica).** Closed sessions get
archived to columnar Parquet (object store or disk); `DuckDB` queries them
directly for offline analysis with zero load on the live DB. The existing
session CSV logs are the natural feedstock. A Postgres **read replica** can also
serve cold SQL so retrospect never competes with ingest.

### 3.3 Kafka's role, clarified

Kafka is **not** the database — it's the **durable, replayable bus / write-ahead
log** for the whole system. Because every sink is a Kafka consumer keyed by
`packet_id`:
- A crashed/slow sink just **replays from its last offset** (fixes P2 — real
  recovery instead of "it's backfillable in theory").
- Backpressure is handled by Kafka retention, not silent `queue.Full` drops
  (fixes P3).
- New sinks (e.g. a second analytics store) attach without touching ingest.

> Alternatives considered for the bus: **Redis Streams** (simpler, already in the
> stack for Tier 0, fine if we want to drop the Kafka/Go bridge) and **NATS
> JetStream** (lightweight, great pub/sub). Kafka is retained because it's already
> wired end-to-end (`stack/kafka`, the Go bridge, the Grafana datasource) — reuse
> over rebuild.

---

## 4. Storage technology trade-offs (the "look into other options" part)

| Option | Role it fits | Pros | Cons / why not (here) |
|---|---|---|---|
| **Postgres (today)** | Tier 2/3 SQL | Already here; `QueryBuilder`; rich SQL; Grafana DS | Hot reads compete w/ writes; manual partitioning |
| **TimescaleDB** ✅ | **Tier 1/2** | *Is* Postgres → zero API change; hypertables, rollups, compression, retention | Adds an extension to operate (small cost) |
| **Redis** ✅ | **Tier 0 hot** | µs/ms reads, last-value + Streams + pub/sub fan-out | Not durable (fine — it's a cache) |
| **QuestDB / ClickHouse** | Tier 1 alt | Extreme write/query speed, columnar | New query dialect → breaks `QueryBuilder`/SQL & Grafana; ops burden; **overkill** at our Hz |
| **InfluxDB** | Tier 1 alt | Purpose-built TSDB | Flux/3.x churn; separate ecosystem; loses our SQL/ORM |
| **Parquet + DuckDB** ✅ | **Tier 3 cold** | Free, columnar, embedded; great for offline | Not for live reads |
| **Kafka (today)** ✅ | **the bus / WAL** | Durable, replay, partitioned, already wired | Operational weight (justified by reuse) |

**Recommendation:** **Redis (Tier 0) + TimescaleDB (Tier 1/2) + Parquet/DuckDB
(Tier 3) + Kafka (bus)**, all behind `TelemetryStore`. This is the combination
that adds tiering **without** breaking the existing SQL/`QueryBuilder`/Grafana
surface — the cheapest path to the biggest reliability + speed win. We explicitly
*reject* swapping Postgres for ClickHouse/QuestDB: our data rate doesn't justify
the rewrite, and it would break every existing dashboard and analysis script.

---

## 5. Reliability & safety hardening (independent of tiering)

These are worth doing regardless and several fall out of the architecture above:

1. **Idempotent writes (fixes P6).** Use `INSERT ... ON CONFLICT (packet_id) DO
   NOTHING` (or `DO UPDATE`) in `execute_insert`/`bulk_insert`. Makes Kafka
   replay and backfill safe. *(Small, high-value change — can land first.)*
2. **No silent drops (fixes P3).** Replace `put_nowait`→drop with bounded-block +
   metric; rely on Kafka retention for true backpressure. Emit a dropped-packet
   counter to Grafana.
3. **Durability dial (P2).** Keep `synchronous_commit=off` on the *warm* sink
   (Kafka is the durable copy now), but make it explicit/configurable, and add a
   replay tool: `kafka sensor_data @offset → TelemetryStore.write`.
4. **Decouple handshake (P5).** Packet-id sequence served from Redis (Tier 0),
   periodically checkpointed to Postgres — car can start logging even if Postgres
   is busy.
5. **Schema contract test (P7).** Unit test that reflects over every leaf field of
   `OrionSensorData` and asserts it appears in `orionToMap` *and* has a column —
   exactly the guardrail recommended in `TELEMETRY_FIELD_PIPELINE.md`. Stops
   silent 0/NULL fields.
6. **Health/observability.** Per-tier liveness in the router so a down tier
   **fails over** (e.g. Redis miss → Timescale) instead of erroring; expose
   lag/queue-depth/commit-rate metrics.

---

## 6. Single-box resource isolation & budgeting (64 GB / NVMe)

This is **one tower**, so "load balancing" = isolating reads from writes *inside
the box* and giving each engine a bounded slice of RAM so nothing starves Kafka
or Postgres. The tiering does the isolation; explicit memory caps keep the peace.

**Read isolation (the actual win):**
- Tier 0 **Redis pub/sub** fans one live stream to every dashboard/viewer client,
  so N live panels = ~1 source, **zero** Postgres hits for live data.
- Retrospect/analysis read **Timescale continuous-aggregate rollups** (tiny) +
  NVMe-backed page cache instead of scanning raw chunks → they stop contending
  with ingest.
- **PgBouncer** in front of Postgres caps connection/backend sprawl from Grafana
  + notebooks + the live viewer.
- A full **streaming read replica is *not* needed at this scale** — it would
  double write I/O and storage on the same disk for little gain. Revisit only if
  rollups + Redis don't fully de-contend; the box has the RAM/NVMe to add one
  later if ever required.

**Rough memory budget (64 GB, leave headroom for OS page cache):**

| Component | Suggested cap | Notes |
|---|---|---|
| Postgres/Timescale `shared_buffers` | ~16 GB | + rely on OS page cache for the rest (NVMe) |
| Postgres `work_mem` / `maintenance_work_mem` | tuned | compression/rollup jobs |
| Redis `maxmemory` | 4–8 GB | `allkeys-lru` or stream `MAXLEN` caps the rings |
| Kafka JVM heap | 4–6 GB | bus only; short retention on `sensor_data` |
| Grafana + bridge + ingest + processors | ~4–6 GB total | mostly light |
| **OS page cache (unallocated)** | **~20 GB+** | keeps warm chunks hot → fast Tier 2 reads |

- **Write scaling** is a non-issue at our Hz on NVMe; we do **not** need multiple
  ingest/consumer replicas. Kafka stays single-broker, partitioned by `car` only
  for clean per-car ordering/replay — not for throughput.
- **Router-level balancing:** `TelemetryStore._route` still sends each query to
  the cheapest capable tier — on one box that's about *which engine's buffer pool
  absorbs the work*, keeping the hot path off Postgres entirely.

---

## 7. Phased migration (incremental, low-risk, each phase independently shippable)

**Phase 0 — Safety quick wins (days, no new infra).**
- `ON CONFLICT` idempotent inserts (P6); drop-counter metrics (P3); schema
  contract test (P7). Pure code changes in `query_builder.py` / tests.

**Phase 1 — Introduce the façade (no behavior change).**
- Add `TelemetryStore` wrapping today's `QueryBuilder`. Migrate the live viewer +
  handshake + one dashboard-backing query to call it. Router has a single tier
  (Postgres). Proves the seam with zero risk.

**Phase 2 — Tier 0 hot cache (Redis).**
- Add a Redis consumer of `sensor_data`; populate last-value + ring. Point
  `latest()`/`last_n()` and the packet-id handshake at Redis. Live panels stop
  hitting Postgres (kills most of P1, P5).

**Phase 3 — Tier 1/2 TimescaleDB.**
- Swap Postgres image for `timescaledb`, `create_hypertable` on `packet`+children,
  add continuous aggregates + retention. Retire `partition_manager.py`. Router
  starts sending raw vs rollup vs cold. Existing SQL/Grafana unchanged.

**Phase 4 — Kafka-fed sinks + replay (decouple ingest).**
- Move the Postgres write out of the MQTT-thread worker into a Kafka consumer;
  ingest becomes produce-only. Add the replay tool. Now crashes self-heal (P2),
  ingest scales (P4).

**Phase 5 — Tier 3 cold + read replica + load balancing.**
- Session archival to Parquet/DuckDB; Postgres read replica + PgBouncer for
  retrospect/analysis.

Each phase is reversible and leaves the system working; we can stop at any phase
and still have banked a concrete win.

---

## 8. Decisions & open questions

**Resolved (team input):**
- **Deployment:** single Precision tower, ~64 GB RAM, NVMe; everything except the
  (remote AWS) MQTT broker runs on it. → Goal is **single-box read isolation**, not
  multi-node scaling. No read replica / consumer fan-out needed at this scale.
- **Kafka stays** as the durable bus (Go bridge + Grafana Kafka datasource kept);
  Redis + Timescale are added around it. The box has headroom for all three.

**Still open (tunables, not blockers):**
- Hot-window horizon (`HOT_HORIZON`, e.g. 30–120 s) → sets Redis stream `MAXLEN`.
- Warm raw-retention before compress/rollup-only (e.g. keep raw 7–14 days on NVMe,
  rollups indefinitely) → Timescale compression + retention policy.
- Continuous-aggregate bucket sizes (1 s / 1 min?) to match the retrospect
  dashboards' typical zoom levels.

---

### TL;DR

Keep Postgres for what it's good at (Tier 2/3 SQL). Put a **`TelemetryStore`
façade** in front of `QueryBuilder` that routes by data age + query shape to
**Redis (hot) → TimescaleDB (warm, still Postgres so nothing breaks) → Parquet
(cold)**, with **Kafka as the durable replay bus** feeding every sink. Land the
idempotency/observability safety fixes first. Net result: live ("tier-1") reads
never touch the write path, crashes self-heal via replay, and the request API the
callers use never changes.
