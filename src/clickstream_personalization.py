# Databricks notebook source

# MAGIC %md
# MAGIC # Clickstream → Real-Time Product Recommendations (Databricks RTM + Redis)
# MAGIC
# MAGIC Companion code for the Databricks + Redis blog. Reads a clickstream from **Kafka / Azure Event Hubs**,
# MAGIC maintains a recency-weighted **per-user product-affinity model** in Spark Structured Streaming
# MAGIC **Real-Time Mode (RTM)**, and writes each user's top-N recommendations to **Redis** for low-latency
# MAGIC serving. This is the pipeline used for the blog's benchmarks.
# MAGIC
# MAGIC ```
# MAGIC Kafka / Event Hubs  →  transformWithState (per-user affinity)  →  foreach(RedisSink)  →  Redis
# MAGIC ```
# MAGIC - **Source**: `spark.readStream.format("kafka")`. Point `eh_topic` / `eh_connection_string` at your bus.
# MAGIC   **Use a Databricks secret scope; do NOT hardcode** (widgets blank on purpose).
# MAGIC - **Scorer** (`ProductAffinityBlob`): each user's whole affinity map as one JSON `ValueState` blob; per
# MAGIC   event: decay → bump the touched product → re-rank → prune → emit top-N. Weights
# MAGIC   `view 1.0 / search 1.5 / cart 4.0 / purchase 8.0`; scores decay with a configurable half-life. A
# MAGIC   processing-time sweep timer re-decays / prunes and re-emits, keeping per-user state bounded.
# MAGIC - **Sink** (`RedisSink`): `recs:{user_id}` ZSET + `recs:{user_id}:meta` HASH, hash-tag co-located, TTL.
# MAGIC   `write_mode=batched` (pipelined batch, default) or `per_row` (`MULTI`/`EXEC` per-user atomic).
# MAGIC - Generate sample traffic with **`clickstream_producer.py`**, or point at your real clickstream.
# MAGIC - **RTM**: `.trigger(realTime=…)` + `outputMode("update")` + `foreach`; latency via
# MAGIC   `query.lastProgress["rtmMetrics"]["e2eLatencyMs"]`.
# COMMAND ----------

# redis-py must be on the cluster: RedisSink imports it on the executors
# (the foreach sink runs there). Notebook-scoped install reaches all nodes.
# MAGIC %pip install redis --quiet
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters (widgets)

# COMMAND ----------

# RTM tuning: (input partitions + shuffle partitions) must be <= total worker vCPUs.
spark.conf.set("spark.sql.shuffle.partitions", 130)

# COMMAND ----------

# Tuning constants. Defaults are fine for the demo; adjust to taste.
TRIGGER_INTERVAL  = "5 minutes"   # RTM batch / checkpoint duration
HALF_LIFE_SECONDS = 120.0         # interest half-life for score decay
REDIS_FLUSH_EVERY = 1000          # batched mode: users buffered per pipeline flush
REDIS_TTL_SECONDS = 30            # TTL on each user's Redis keys

# Source: Kafka / Azure Event Hubs
dbutils.widgets.text("eh_topic", "rtm-blog-clickstream", "Event Hubs / Kafka topic")
dbutils.widgets.text("eh_connection_string", "", "Event Hubs connection string")

# Redis connection. Store credentials in a Databricks secret scope rather than hardcoding.
dbutils.widgets.text("redis_host", "", "Redis host")
dbutils.widgets.text("redis_port", "10000", "Redis port")
dbutils.widgets.text("redis_password", "", "Redis password")
# TLS: false for a plain redis:// endpoint, true for a TLS rediss:// endpoint.
dbutils.widgets.dropdown("redis_ssl", "true", ["false", "true"], "Redis TLS")

# Checkpoint. Use a fresh path whenever you change the stateful logic.
dbutils.widgets.text("checkpoint_location", "/Volumes/<catalog>/<schema>/checkpoints/clickstream_personalization", "Checkpoint (Unity Catalog volume)")

# Redis write strategy:
#   batched = buffer users and send them in one pipeline flush (fewer round trips, higher throughput)
#   per_row = one MULTI/EXEC transaction per user (strict per-user atomicity, more round trips)
dbutils.widgets.dropdown("write_mode", "batched", ["per_row", "batched"], "Redis write mode")

EH_TOPIC            = dbutils.widgets.get("eh_topic").strip()
EH_CS               = dbutils.widgets.get("eh_connection_string").strip()
REDIS_HOST          = dbutils.widgets.get("redis_host").strip()
REDIS_PORT          = dbutils.widgets.get("redis_port").strip()
REDIS_PASSWORD      = dbutils.widgets.get("redis_password").strip()
REDIS_SSL           = dbutils.widgets.get("redis_ssl") == "true"
CHECKPOINT_LOCATION = dbutils.widgets.get("checkpoint_location")
WRITE_MODE          = dbutils.widgets.get("write_mode")

print(f"topic={EH_TOPIC}  write_mode={WRITE_MODE}  tls={REDIS_SSL}\n"
      f"trigger='{TRIGGER_INTERVAL}'  half_life={HALF_LIFE_SECONDS}s  flush_every={REDIS_FLUSH_EVERY}\n"
      f"checkpoint={CHECKPOINT_LOCATION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The product catalog
# MAGIC
# MAGIC A small but real catalog, so recommendations are readable end to end: a Redis lookup returns *"Stride Road Running Shoe"*, not `run-101`. In production this lives in a Delta table or a feature store and is broadcast into the stream; inline here to keep the notebook self-contained.

# COMMAND ----------

# product_id -> (title, category, price)
CATALOG = {
    # footwear / running
    "P1042": ("Stride Road Running Shoe",         "running",     129.99),
    "P1043": ("Velocity Boost Running Shoe",      "running",     189.99),
    "P1044": ("Cloudline Cushion Trainer",        "running",     144.99),
    "P1045": ("Pacer GPS Running Watch",          "running",     449.99),
    "P1046": ("ComfortStep Running Socks 3-Pack", "running",     17.99),
    # electronics / audio & charging
    "P2011": ("Acme Noise-Canceling Headphones",  "electronics", 379.00),
    "P2012": ("VoltEdge Power Bank",              "electronics", 99.00),
    "P2013": ("AeroBuds Pro Wireless Earbuds",    "electronics", 249.00),
    "P2014": ("Precision Wireless Mouse",         "electronics", 99.99),
    "P2015": ("PaperReader E-Reader",             "electronics", 159.99),
    # home / kitchen
    "P3021": ("BrewMaster Espresso Machine",      "home",        699.00),
    "P3022": ("Cyclone Cordless Vacuum",          "home",        749.00),
    "P3023": ("CastIron Dutch Oven 4.5qt",        "home",        380.00),
    "P3024": ("TidyHome Laundry Bin 35L",         "home",        59.99),
    "P3025": ("BrewMaster Coffee Pods 40-Pack",   "home",        32.00),
    # outdoor
    "P4031": ("Summit Rain Jacket 3L",            "outdoor",     179.00),
    "P4032": ("Ridgeline Hiking Backpack 22L",    "outdoor",     140.00),
    "P4033": ("TrailBeam Headlamp 400lm",         "outdoor",     49.95),
    "P4034": ("ThermoKing Insulated Bottle 1.2L", "outdoor",     34.99),
    # fashion
    "P5041": ("Denimworks Slim Jeans",            "fashion",     98.00),
    "P5042": ("NorthLoft Merino Crew Sweater",    "fashion",     49.90),
    "P5043": ("SunStyle Classic Sunglasses",      "fashion",     161.00),
    "P5044": ("Traveler Duffle Bag",              "fashion",     109.99),
}

PRODUCT_IDS = list(CATALOG.keys())

# category -> product ids, used by the generator to keep a session coherent
BY_CATEGORY = {}
for pid, (_title, cat, _price) in CATALOG.items():
    BY_CATEGORY.setdefault(cat, []).append(pid)
CATEGORIES = list(BY_CATEGORY.keys())

print(f"{len(CATALOG)} products across {len(CATEGORIES)} categories: {CATEGORIES}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: read the clickstream from Kafka / Event Hubs
# MAGIC
# MAGIC The pipeline reads clickstream events from Kafka or Azure Event Hubs and parses the JSON
# MAGIC payload into columns for the scorer. Point `eh_topic` and `eh_connection_string` at your bus.
# MAGIC To feed the demo without a live stream, run `clickstream_producer.py`, which writes
# MAGIC session-shaped sample events into the topic.
# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType

# Event Hubs via the Kafka interface (shaded JAAS; bootstrap derived from the conn string)
EH_BOOTSTRAP = EH_CS.split(";")[0].replace("Endpoint=sb://", "").rstrip("/") + ":9093"
JAAS = ('kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        'username="$ConnectionString" password="%s";' % EH_CS)

EVENT_SCHEMA = StructType([
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("category", StringType()),
    StructField("product_id", StringType()),
    StructField("event_ts", StringType()),
])

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", EH_BOOTSTRAP)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", JAAS)
    .option("subscribe", EH_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# parse JSON payload back into clickstream columns for the scorer
events = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")).select("e.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: recency-weighted product affinity (`transformWithState`)
# MAGIC
# MAGIC One keyed stateful operator, one shuffle on the real-time path.
# MAGIC
# MAGIC **State per user:** one JSON `ValueState` blob holding the whole `product_id -> (score, last_seen_ms)` map.
# MAGIC
# MAGIC **On each event:** decay the product's existing score to *now*, then add the event's weight. Decay is exponential with a configurable half-life, so interest fades the way real intent does.
# MAGIC
# MAGIC **On a timer:** re-decay everything, drop products that have fallen below a floor, and re-emit. This is what keeps state bounded and lets a user's recommendations go stale gracefully when they stop browsing.
# MAGIC
# MAGIC Two RTM-specific details worth copying into your own code:
# MAGIC
# MAGIC * `handleInputRows` is called **once per row** in RTM (not once per key per batch), so don't assume the iterator carries a whole batch.
# MAGIC * **Timers must be idempotent.** Because we are invoked per row, blindly calling `registerTimer` on every event would pile up thousands of redundant timers. We keep exactly one pending timer in a `ValueState`, deleting the old one before registering a new one.

# COMMAND ----------

# How much each event type says about intent. A purchase is a far stronger
# signal than a passing view.
EVENT_WEIGHTS = {"view": 1.0, "search": 1.5, "cart": 4.0, "purchase": 8.0}

TOP_N = 10               # recommendations served per user
MAX_TRACKED = 50         # cap on products held in state per user
SCORE_FLOOR = 0.05       # below this a product is forgotten
SWEEP_INTERVAL_MS = 2_000   # timer now drives emit; controls rec freshness

# Output row: the ranked, app-ready recommendation list for one user.
OUTPUT_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("recs", ArrayType(StructType([
        StructField("product_id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("score", DoubleType(), False),
    ])), False),
    StructField("updated_ms", DoubleType(), False),
])

TIMER_SCHEMA = "expiry_ms DOUBLE"

# COMMAND ----------

import json, math
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle


class ProductAffinityBlob(StatefulProcessor):
    """Whole per-user affinity map stored as ONE serialized ValueState blob.

    Per event: 1 read (whole map), in-memory decay/bump/rank, then 1 write
    (whole map), and emit the user's top products.
    """

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.handle = handle
        self.state = handle.getValueState("affinityBlob", "payload STRING")
        self.pending_timer = handle.getValueState("sweepTimer", TIMER_SCHEMA)

    @staticmethod
    def _decay(score, last_seen_ms, now_ms):
        elapsed_s = max(0.0, (now_ms - last_seen_ms) / 1000.0)
        return score * math.pow(0.5, elapsed_s / HALF_LIFE_SECONDS)

    def _load(self):
        v = self.state.get()
        return json.loads(v[0]) if v is not None else {}

    def _save(self, m):
        self.state.update((json.dumps(m),))

    def _ensure_timer(self, now_ms):
        existing = self.pending_timer.get()
        if existing is not None:
            if existing[0] > now_ms:
                return
            self.handle.deleteTimer(int(existing[0]))
        next_ms = now_ms + SWEEP_INTERVAL_MS
        self.handle.registerTimer(int(next_ms))
        self.pending_timer.update((float(next_ms),))

    def _rank(self, m, now_ms):
        survivors = []
        for pid, (score, ts) in m.items():
            d = self._decay(score, ts, now_ms)
            if d >= SCORE_FLOOR:
                survivors.append((pid, d))
        survivors.sort(key=lambda x: x[1], reverse=True)
        return survivors[:MAX_TRACKED]

    def _emit(self, user_id, ranked, now_ms):
        recs = []
        for pid, score in ranked[:TOP_N]:
            title, _cat, price = CATALOG.get(pid, (None, None, None))
            recs.append((pid, title, price, round(float(score), 4)))
        return (user_id, recs, now_ms)

    def handleInputRows(self, key, rows, timerValues):
        user_id = key[0]
        now_ms = float(timerValues.getCurrentProcessingTimeInMs())
        m = self._load()                          # 1 read: whole map
        for row in rows:
            pid = row["product_id"]
            if pid is None:
                continue
            w = EVENT_WEIGHTS.get(row["event_type"], 1.0)
            if pid in m:
                old_score, old_ts = m[pid]
                new_score = self._decay(old_score, old_ts, now_ms) + w
            else:
                new_score = w
            m[pid] = [new_score, now_ms]
        ranked = self._rank(m, now_ms)
        m = {pid: [score, now_ms] for pid, score in ranked}   # decayed + pruned + re-anchored
        self._save(m)                             # 1 write: whole map, atomic
        self._ensure_timer(now_ms)
        yield self._emit(user_id, ranked, now_ms)             # per-event emit

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo):
        user_id = key[0]
        now_ms = float(timerValues.getCurrentProcessingTimeInMs())
        self.pending_timer.clear()
        m = self._load()
        ranked = self._rank(m, now_ms)
        if ranked:
            self._save({pid: [score, now_ms] for pid, score in ranked})
            self._ensure_timer(now_ms)
        else:
            self.state.clear()   # cold user: drop the entry, don't store an empty blob
        yield self._emit(user_id, ranked, now_ms)

    def close(self) -> None:
        pass


recommendations = events.groupBy("user_id").transformWithState(
    statefulProcessor=ProductAffinityBlob(),
    outputStructType=OUTPUT_SCHEMA,
    outputMode="update",
    timeMode="processingTime",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Where a model would plug in
# MAGIC
# MAGIC `ProductAffinityBlob` is a transparent heuristic: recency-weighted affinity over observed events, which is exactly the *"recently viewed"* / *"because you viewed X"* logic behind most storefront carousels. It is also the seam for something smarter: swap `_emit`'s ranking for a model call (an `mlflow.pyfunc` loaded once per partition in `init`, or precomputed item embeddings broadcast into the stream) and nothing else in the pipeline changes.
# MAGIC
# MAGIC Keep the RTM latency budget in mind if you do: a per-row network hop to a serving endpoint would undercut the sub-second story. Load the model in-process, or batch the calls.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: write to the serving layer
# MAGIC
# MAGIC `RedisSink` is a `foreach` sink (`open`, then `process` per row, then `close`) that writes each user's ranked recommendations to Redis. `write_mode` (`per_row` or `batched`) is documented in the class docstring.

# COMMAND ----------

class RedisSink:
    """Materializes each user's ranked recommendations in Redis.

    Layout per user (the braces are a Redis Cluster hash tag, so both keys
    live in one slot and can share a transaction):
      recs:{user_id}       ZSET  product_id -> score
      recs:{user_id}:meta  HASH  product_id -> "title|price"

    batched mode buffers `flush_every` writes and sends them in one pipeline
    without MULTI/EXEC, so a flush is not atomic per user (use per_row for
    strict per-user atomicity).
    """

    def __init__(self, host, port, password, user="default", use_ssl=True,
                 ttl_seconds=1800, write_mode="per_row", flush_every=2000):
        self.host = host
        self.port = port
        self.password = password
        self.user = user
        self.use_ssl = use_ssl
        self.ttl_seconds = ttl_seconds
        self.write_mode = write_mode
        self.flush_every = flush_every

    def open(self, partition_id: int, epoch_id: int) -> bool:
        import redis

        self.client = redis.Redis(
            host=self.host, port=self.port, username=self.user,
            password=self.password, ssl=self.use_ssl,
            socket_connect_timeout=5.0,
            socket_timeout=2.0,
            socket_keepalive=True,
            health_check_interval=30,
            ssl_cert_reqs="required" if self.use_ssl else None,
        )
        self.partition_id = partition_id
        self.count = 0
        self._buffer = []          # list of (user_id, recs); flush every flush_every rows
        return True

    def _queue_user(self, pipe, user_id, recs):
        key = f"recs:{{{user_id}}}"          # {..} = Redis Cluster hash tag
        meta_key = f"{key}:meta"
        pipe.delete(key, meta_key)
        if recs:
            pipe.zadd(key, {r["product_id"]: r["score"] for r in recs})
            pipe.hset(meta_key, mapping={
                r["product_id"]: f"{r['title']}|{r['price']}" for r in recs
            })
            pipe.expire(key, self.ttl_seconds)
            pipe.expire(meta_key, self.ttl_seconds)

    def _flush_batch(self):
        if not self._buffer:
            return
        pipe = self.client.pipeline(transaction=False)
        for user_id, recs in self._buffer:
            self._queue_user(pipe, user_id, recs)
        pipe.execute()
        self._buffer = []

    def process(self, row) -> None:
        if self.write_mode == "per_row":
            with self.client.pipeline(transaction=True) as pipe:
                self._queue_user(pipe, row["user_id"], row["recs"])
                pipe.execute()
        else:  # batched by row count
            self._buffer.append((row["user_id"], list(row["recs"])))
            if len(self._buffer) >= self.flush_every:
                self._flush_batch()
        self.count += 1

    def close(self, error) -> None:
        if self.write_mode == "batched" and getattr(self, "_buffer", None):
            try:
                self._flush_batch()
            except Exception:
                pass
        if getattr(self, "client", None) is not None:
            self.client.close()


assert REDIS_HOST and REDIS_PASSWORD, "Set redis_host / redis_password widgets"
sink = RedisSink(
    host=REDIS_HOST,
    port=int(REDIS_PORT),
    password=REDIS_PASSWORD,
    use_ssl=REDIS_SSL,
    ttl_seconds=REDIS_TTL_SECONDS,
    write_mode=WRITE_MODE,
    flush_every=REDIS_FLUSH_EVERY,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: start the stream
# MAGIC
# MAGIC The `foreach` `RedisSink` writes each user's recommendations to Redis. The notebook ends with `query` so the live progress dashboard renders.

# COMMAND ----------

query = (
    recommendations.writeStream
    .foreach(sink)                       # write each user's recs to Redis
    .outputMode("update")                # RTM requires update mode
    .trigger(realTime=TRIGGER_INTERVAL)  # enables Real-Time Mode
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .queryName("clickstream_personalization_rtm")
    .start()
)
query

# COMMAND ----------

# MAGIC %md
# MAGIC ## How the application reads
# MAGIC
# MAGIC One pipelined round trip per page render: `ZREVRANGE` for the ranking and `HGETALL` for titles and prices. Example client code:

# COMMAND ----------

# import redis
# r = redis.Redis(host=HOST, port=PORT, username=USER, password=PWD, ssl=USE_SSL)
#
# user_id = "user-42"
# key = f"recs:{{{user_id}}}"
# pipe = r.pipeline(transaction=True)
# pipe.zrevrange(key, 0, 9, withscores=True)
# pipe.hgetall(f"{key}:meta")
# ranked, meta = pipe.execute()
#
# for pid, score in ranked:
#     title, price = meta[pid].decode().split("|")
#     print(f"{score:7.3f}  {title}  ${price}")
