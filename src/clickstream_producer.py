# Databricks notebook source

# MAGIC %md
# MAGIC # Mock Clickstream Producer → Kafka / Event Hubs
# MAGIC
# MAGIC Generates a realistic mock clickstream (view / search / cart / purchase across product categories,
# MAGIC each user with a stable "home" category + ~20% wandering) and writes it to a **Kafka / Event Hubs**
# MAGIC topic, so `clickstream_personalization.py` has something to consume.
# MAGIC
# MAGIC **In production you do NOT need this.** Your real clickstream already lands in Kafka. This exists only
# MAGIC to feed the demo / benchmarks with representative traffic.
# MAGIC
# MAGIC - Set `eh_topic` / `eh_connection_string` (use a **secret scope**, not hardcoded; widget left blank).
# MAGIC - `rows_per_second` controls load (we benchmarked at 100k and 10k).
# MAGIC - Writes with RTM trigger + gzip (Event Hubs does not support snappy).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Generates a synthetic, session-shaped clickstream and writes it to Event Hubs via the Kafka interface.
dbutils.widgets.text("rows_per_second", "100000", "Rows/sec")
dbutils.widgets.text("input_partitions", "16", "Generator partitions (match topic partitions)")
dbutils.widgets.text("active_users", "10000", "Concurrent shoppers")
dbutils.widgets.text("eh_topic", "rtm-blog-clickstream", "EH topic")
dbutils.widgets.text("eh_connection_string", "", "EH connection string (namespace or EntityPath)")
dbutils.widgets.text("checkpoint_location", "/Volumes/<catalog>/<schema>/checkpoints/clickstream_producer", "Checkpoint (Unity Catalog volume)")

ROWS_PER_SECOND     = int(dbutils.widgets.get("rows_per_second"))
INPUT_PARTITIONS    = int(dbutils.widgets.get("input_partitions"))
ACTIVE_USERS        = int(dbutils.widgets.get("active_users"))
EH_TOPIC            = dbutils.widgets.get("eh_topic").strip()
EH_CS               = dbutils.widgets.get("eh_connection_string").strip()
CHECKPOINT_LOCATION = dbutils.widgets.get("checkpoint_location")

# derive bootstrap from the connection string (same as the official generator)
EH_BOOTSTRAP = EH_CS.split(";")[0].replace("Endpoint=sb://", "").rstrip("/") + ":9093"
# Databricks' Kafka connector is shaded -> PlainLoginModule under kafkashaded.*
JAAS = ('kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        'username="$ConnectionString" password="%s";' % EH_CS)
print("producing", ROWS_PER_SECOND, "rows/sec ->", EH_BOOTSTRAP, "topic", EH_TOPIC)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog

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
# MAGIC ## Generate a session-shaped clickstream

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, StringType, StructField, StructType,
)
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle

raw = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", ROWS_PER_SECOND)
    .option("numPartitions", INPUT_PARTITIONS)  # match cluster: total vCPUs >= input + shuffle
    .load()
)

# Two independent pseudo-random streams derived from a monotonic counter. Hashing
# (rather than plain modulo) decorrelates user / product / event choice, so a
# given shopper sees varied products over time instead of one frozen pair.
rnd_user = F.pmod(F.hash(F.col("value"), F.lit("u")), F.lit(ACTIVE_USERS))  # non-negative, no overflow
rnd_a = F.pmod(F.hash(F.col("value"), F.lit("a")), F.lit(100))   # funnel roll
rnd_b = F.pmod(F.hash(F.col("value"), F.lit("b")), F.lit(100))   # category jump roll
rnd_c = F.pmod(F.hash(F.col("value"), F.lit("c")), F.lit(1000003))  # product pick, non-negative

# The shopper's "home" category is stable per user; ~20% of events wander off it.
home_category = F.element_at(
    F.array(*[F.lit(c) for c in CATEGORIES]),
    (rnd_user % F.lit(len(CATEGORIES)) + 1).cast("int"),
)
wander_category = F.element_at(
    F.array(*[F.lit(c) for c in CATEGORIES]),
    (rnd_c % F.lit(len(CATEGORIES)) + 1).cast("int"),
)

with_session = raw.select(
    F.concat(F.lit("user-"), rnd_user.cast("string")).alias("user_id"),
    F.col("timestamp").alias("event_ts"),
    # Funnel: view 70 / search 15 / cart 12 / purchase 3
    F.when(rnd_a < F.lit(70), F.lit("view"))
     .when(rnd_a < F.lit(85), F.lit("search"))
     .when(rnd_a < F.lit(97), F.lit("cart"))
     .otherwise(F.lit("purchase")).alias("event_type"),
    F.when(rnd_b < F.lit(80), home_category)
     .otherwise(wander_category).alias("category"),
    rnd_c.alias("_pick"),
)

# Pick a concrete product from within the chosen category. One CASE per
# category keeps this pure Spark SQL (no Python UDF on the real-time path).
product_expr = F.coalesce(
    *[
        F.when(
            F.col("category") == F.lit(cat),
            F.element_at(
                F.array(*[F.lit(p) for p in pids]),
                (F.col("_pick") % F.lit(len(pids)) + 1).cast("int"),
            ),
        )
        for cat, pids in BY_CATEGORY.items()
    ]
)

events = with_session.select(
    "user_id", "event_type", "category", "event_ts",
    product_expr.alias("product_id"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## write to Event Hubs (Kafka sink)

# COMMAND ----------

from pyspark.sql import functions as F

# events (from the shaping cell): user_id, event_type, category, product_id, event_ts
payload = events.select(
    F.col("user_id").cast("string").alias("key"),
    F.to_json(F.struct("user_id", "event_type", "category", "product_id", "event_ts")).alias("value"),
)

query = (
    payload.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", EH_BOOTSTRAP)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", JAAS)
    .option("kafka.compression.type", "gzip")   # Event Hubs supports gzip, NOT snappy
    .option("topic", EH_TOPIC)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .outputMode("update")                          # RTM requires update
    .trigger(realTime="5 minutes")                  # RTM producer (continuous; commits per interval)
    .queryName("eh_producer")
    .start()
)
query
