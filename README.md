# Real-time personalization with Real-Time Mode and Redis

Companion code for the Databricks + Redis blog post. It computes product recommendations
that update as a customer browses. Real-Time Mode (RTM) in Apache Spark Structured
Streaming keeps each user's recommendations current from a clickstream, and Redis serves
them to the application in milliseconds.

## Pipeline

```
Kafka / Azure Event Hubs  →  transformWithState (per-user affinity)  →  foreach(RedisSink)  →  Redis
```

1. Read the clickstream. The pipeline reads events from Kafka or Azure Event Hubs
   (`spark.readStream.format("kafka")`), which is the source used for the benchmarks. If you
   don't have a live feed, `clickstream_producer.py` writes sample events into the topic.
2. Score per user. One keyed `transformWithState` operator keeps a recency-weighted affinity
   score per product, stored as a single JSON `ValueState` blob per user. Each event adds
   weight to the product it touched (`view` 1.0, `search` 1.5, `cart` 4.0, `purchase` 8.0),
   and scores decay exponentially with a configurable half-life so recent interest ranks
   higher. A processing-time timer re-decays, prunes faded products, and re-emits, which keeps
   per-user state bounded.
3. Write to Redis. A `foreach` `RedisSink` writes each user's ranked list. The default
   (`write_mode=batched`) buffers users and sends them in one pipelined flush, which keeps
   round trips down. `write_mode=per_row` wraps each user in a `MULTI`/`EXEC` transaction when
   you need strict per-user atomicity.
4. Read from the application. One pipelined round trip per page render:
   `ZREVRANGE recs:{user_id} 0 9 WITHSCORES` for the ranking and `HGETALL recs:{user_id}:meta`
   for titles and prices. No Spark query, no lakehouse scan.

The catalog is about 23 products across five categories. The generator produces
session-shaped traffic (a shopper mostly stays in one category and occasionally wanders),
following a view 70% / search 15% / cart 12% / purchase 3% funnel.

## Layout

```
redis_blog/
└── src/
    ├── clickstream_personalization.py   # RTM pipeline: Kafka/Event Hubs -> scorer -> Redis
    └── clickstream_producer.py          # writes sample clickstream into the topic
```

## Configure and run

1. Attach `clickstream_personalization.py` to an RTM-compliant cluster.
2. Set the widgets. Use a Databricks secret scope for anything sensitive rather than
   hardcoding: `eh_topic` and `eh_connection_string` for your Kafka or Event Hubs bus,
   `redis_host` and `redis_password`, and a fresh `checkpoint_location`. Set
   `shuffle_partitions` so that (input partitions + shuffle) is at most the total worker
   vCPUs, since RTM runs every stage's tasks at once.
3. Run `clickstream_producer.py` to feed sample events into the topic (skip this if you have
   a live clickstream), then run the pipeline notebook.

## Real-Time Mode notes

In code, RTM means the trigger is `realTime`, the output mode is `update`, and the custom
sink uses `foreach` rather than `foreachBatch`. Timers are processing-time only, and
`handleInputRows` runs once per row, so the processor keeps a single pending timer in a
`ValueState` and deletes the old one before registering the next.

RTM also has cluster requirements: Classic compute, `SINGLE_USER` access mode, fixed size
(no autoscaling), Photon off, on-demand instances, and
`spark.databricks.streaming.realTimeMode.enabled = true`.

## Redis serving layout

| Key | Type | Contents |
|---|---|---|
| `recs:{user_id}` | ZSET | `product_id -> score` (the ranking) |
| `recs:{user_id}:meta` | HASH | `product_id -> "title\|price"` |

The braces are Redis Cluster hash-tag syntax, so both keys land in one slot. Both keys carry
a TTL, so recommendations for a shopper who leaves expire on their own.

## Benchmark

The pipeline sustained 100,000 events/sec across 10,000 active users on a 10-worker cluster
(160 vCPU) against Azure Managed Redis, at roughly 530,000 ops/sec with no evictions. RTM
engine latency (`rtmMetrics.e2eLatencyMs`, from the source to just before the sink) was p50
62, p90 99, p95 119, p99 157 ms at 100k/sec, and p50 56, p90 87, p95 117, p99 139 ms at
10k/sec. The Redis write and the application read add a few milliseconds each.
