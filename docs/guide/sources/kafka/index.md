---
title: Kafka
rank: 6
---

# Kafka Data Source

Sail provides a Kafka batch connector exposed under the `kafka` format name for API parity with vanilla
PySpark. The implementation is based on the Python `confluent-kafka` library, which wraps `librdkafka`.
No JVM or Kafka client JAR is involved.

Every read resolves to a bounded `[start, end)` offset range per partition. The fixed Kafka schema
(`key`, `value`, `topic`, `partition`, `offset`, `timestamp`, `timestampType`) matches Spark, so
downstream code that already works against `spark.read.format("kafka")` runs unchanged.

<!--@include: ../../_common/spark-session.md-->

## Installation

You need to install the `pysail` package with the `kafka` extra to use the Kafka data source.

```bash
pip install pysail[kafka]
```

## Quick Start

Register the datasource once per Spark session.

```python
from pysail.spark.datasource.kafka import KafkaDataSource

spark.dataSource.register(KafkaDataSource)
```

Then read from a Kafka topic using the standard PySpark API.

```python
df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)
```

## Schema

Every read returns the fixed Spark-compatible Kafka schema:

| Column          | Type                       | Description                                         |
| --------------- | -------------------------- | --------------------------------------------------- |
| `key`           | `binary`                   | Message key, or `null` if absent.                   |
| `value`         | `binary`                   | Message value.                                      |
| `topic`         | `string`                   | Source topic.                                       |
| `partition`     | `int`                      | Source partition.                                   |
| `offset`        | `bigint`                   | Message offset within the partition.                |
| `timestamp`     | `timestamp` (microsecond)  | Message timestamp (see `timestampType`).            |
| `timestampType` | `int`                      | `-1` = none, `0` = CreateTime, `1` = LogAppendTime. |
| `headers`       | `array<struct<key,value>>` | Present only when `includeHeaders=true`.            |

Cast `key` and `value` to `string` (or decode via `from_json`, `from_avro`, etc.) at the query level.

Every column is nullable, matching Spark's declared schema.

::: info
When the broker reports no timestamp type for a message, `timestampType` is `-1` (Spark's
`NO_TIMESTAMP_TYPE`) and `timestamp` is the `-1` millisecond Kafka reports, i.e.
`1969-12-31 23:59:59.999` — the same value Spark surfaces. This only arises against very old brokers;
modern ones always set `CreateTime` or `LogAppendTime`, so `timestampType` is `0` or `1` in practice.
:::

## Options

The data source options are consistent with
the [PySpark Kafka documentation](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html).

Option names are case-insensitive, as in Spark.

| Name                                 | Required   | Default    | Description                                                                                                                   |
| ------------------------------------ | ---------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `kafka.bootstrap.servers`            | Yes        |            | Comma-separated broker list.                                                                                                  |
| `assign`                             | One of the |            | JSON of `{"topic": [partition, ...]}`.                                                                                        |
| `subscribe`                          | three      |            | Comma-separated topic names.                                                                                                  |
| `subscribePattern`                   |            |            | Regular expression matched against whole topic names.                                                                         |
| `startingOffsets`                    | No         | `earliest` | `earliest`, or per-partition JSON (see below).                                                                                |
| `endingOffsets`                      | No         | `latest`   | `latest`, or per-partition JSON (see below).                                                                                  |
| `startingTimestamp`                  | No         |            | Global start time as milliseconds since epoch. Applied to every partition.                                                    |
| `endingTimestamp`                    | No         |            | Global end time as milliseconds since epoch. Applied to every partition.                                                      |
| `startingOffsetsByTimestamp`         | No         |            | Per-partition JSON of start timestamps in ms since epoch.                                                                     |
| `endingOffsetsByTimestamp`           | No         |            | Per-partition JSON of end timestamps in ms since epoch.                                                                       |
| `startingOffsetsByTimestampStrategy` | No         | `error`    | `error` or `latest`: what to do when no offset matches a starting timestamp.                                                  |
| `failOnDataLoss`                     | No         | `true`     | Whether to fail when records that were planned for reading are no longer in the log.                                          |
| `includeHeaders`                     | No         | `false`    | Whether to project the `headers` column.                                                                                      |
| `minPartitions`                      | No         |            | Lower bound on the number of input partitions; offset ranges are split to reach it.                                           |
| `maxRecordsPerPartition`             | No         |            | Upper bound on records per input partition; larger ranges are split.                                                          |
| `kafkaConsumer.pollTimeoutMs`        | No         | `120000`   | Per-call `consumer.poll()` timeout in milliseconds. Also accepted as `pollTimeoutMs`; the Spark-standard name wins if both are given. |
| `maxBatchRows`                       | No         | `10000`    | Maximum rows per Arrow `RecordBatch` returned to the executor. Sail-specific.                                                 |
| `stallTimeoutMs`                     | No         | `300000`   | Wall-clock time a read may go without receiving a record before failing (guards against an unreachable broker or dead partition leader). Sail-specific. |
| `adminTimeoutMs`                     | No         | `10000`    | Timeout in milliseconds for the broker metadata, `describe_topics`, and `list_offsets` calls made while planning. Bump this for slow or remote clusters. Sail-specific. |
| `kafka.*`                            | No         |            | Any additional `librdkafka` client property, passed through with the `kafka.` prefix stripped.                                |

::: info
Exactly one of `assign`, `subscribe`, and `subscribePattern` must be specified.

When several range options are given for the same endpoint, Spark's precedence applies rather than an
error: the global timestamp wins, then the per-partition timestamp map, then the offsets. Lower-priority
values are not parsed.
:::

::: warning
The consumer properties this source owns are rejected rather than silently overridden, as in Spark:
`kafka.auto.offset.reset` (use `startingOffsets`/`endingOffsets`), `kafka.enable.auto.commit`, and
`kafka.enable.partition.eof`.
:::

::: info
**Consumer group id.** Spark mints a fresh group id per query because its consumers call `subscribe()`
and so join a real consumer group, where a shared id makes concurrent queries steal partitions from one
another. This source always uses `assign()` — librdkafka's simple-consumer path, with no group
membership — and never commits offsets, so it uses a stable `sail-kafka-*` id instead. Nothing is written
to `__consumer_offsets`, and concurrent queries do not interfere. Override with `kafka.group.id` if your
broker ACLs require a specific one.
:::

## Examples

### Per-Partition Offset Ranges

Use JSON to specify explicit start and end offsets per partition. The end offset is exclusive.

```python
import json

df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", json.dumps({"orders": {"0": 100, "1": 200}}))
    .option("endingOffsets", json.dumps({"orders": {"0": 500, "1": 600}}))
    .load()
)
```

The sentinel values `-2` (earliest) and `-1` (latest) are also accepted in the JSON form for parity with Spark.
As in Spark's batch source, each sentinel is only valid at its own endpoint: `-2` in `startingOffsets` and `-1` in
`endingOffsets`. The reverse (`-1` in `startingOffsets`, `-2` in `endingOffsets`) would always produce an empty
range, so it is rejected rather than silently returning no rows. The same rule applies to the string forms:
`startingOffsets` does not accept `latest`, and `endingOffsets` does not accept `earliest`.

### Including Headers

Kafka message headers are excluded by default. Opt in with `includeHeaders=true` to project an
`array<struct<key: string, value: binary>>` column.

```python
df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("includeHeaders", "true")
    .load()
)
```

### Timestamp-Based Reads

`startingTimestamp` and `endingTimestamp` resolve to concrete offsets via a timestamp `list_offsets` lookup.
Useful for time-window reads.

When a partition holds no message satisfying `ts >= T`, the endpoint decides what happens, as in Spark. An
_ending_ timestamp falls back to the current high watermark. A _starting_ timestamp follows
`startingOffsetsByTimestampStrategy`, which defaults to `error` — so a future or mistaken start time fails
rather than silently returning nothing. Set it to `latest` to bound the read at the end of the log instead.

```python
df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingTimestamp", "1700000000000")
    .load()
)
```

::: info
The timestamp lookup searches the topic's stored timestamps. If the topic is configured with
`message.timestamp.type=CreateTime` (the default) the search runs against producer clocks, so results
depend on producer time skew. Configure the topic with `message.timestamp.type=LogAppendTime` to search
against broker ingest time instead.
:::

### Passing librdkafka Options

Any option prefixed with `kafka.` is forwarded to the underlying `librdkafka` consumer with the prefix
stripped. This is the standard mechanism for authentication, TLS, and tuning.

```python
df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "broker.example.com:9093")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanisms", "PLAIN")
    .option("kafka.sasl.username", "alice")
    .option("kafka.sasl.password", "secret")
    .option("subscribe", "orders")
    .load()
)
```

### Selecting Topics

`subscribe` takes a comma-separated topic list, `subscribePattern` a regular expression matched against
whole topic names, and `assign` a JSON map of explicit partitions.

Topics the broker flags as internal — `__consumer_offsets` and `__transaction_state` — are excluded from
all three strategies, matching Spark's `isInternal` filter. The flag is what counts, not the name: a topic
of your own called `__audit` is read normally.

```python
df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("assign", json.dumps({"orders": [0, 2]}))
    .load()
)
```

### Controlling Parallelism

By default each Kafka partition becomes one input partition. `minPartitions` splits offset ranges — in
proportion to their size — until there are at least that many, and `maxRecordsPerPartition` caps how many
records any single input partition covers. Both leave the rows and their offsets unchanged, and both
reproduce Spark's `KafkaOffsetRangeCalculator` exactly, down to the split boundaries: ranges too small to
be worth splitting are set aside before the `minPartitions` budget is divided among the rest, and within a
range the remainder lands in the last chunk.

```python
df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("minPartitions", "16")
    .load()
)
```

### When Offsets Are Resolved

`earliest` and `latest` — including their JSON `-2`/`-1` forms — are resolved when each task starts
reading, not when the query is planned. This matches Spark, whose `fetchPartitionOffsets` carries the
sentinels through to `KafkaSourceRDD.resolveRange`, and it matters twice:

- `latest` means the end of the log when the task runs, so a read started while a producer is active
  picks up the tail rather than stopping at a watermark captured earlier.
- An `earliest` start stays valid if the log ages on between planning and execution. Freezing it would
  turn ordinary retention into a `failOnDataLoss` error.

Explicit numeric offsets and all timestamp options are resolved during planning, as they are in Spark.
Setting `minPartitions` or `maxRecordsPerPartition` forces the sentinels to be resolved early so the
ranges can be divided — but the outer boundaries are then put back, so the overall start and end stay
late-bound and only the interior split points are fixed.

::: warning
A consequence inherited from Spark: because `latest` binds per task, a retried task can observe a longer
log than its first attempt. Use explicit `endingOffsets` if you need a byte-for-byte reproducible read.
:::

### Data Loss

`failOnDataLoss` defaults to `true`, as in Spark. Data loss is reported when records that existed at
planning time are gone by the time the read runs:

- A start offset that has aged out of the log (retention or truncation) fails the query. With
  `failOnDataLoss=false` the read skips ahead to the earliest offset still available.
- Reaching the end of the log below the planned end offset fails the query when the high watermark no
  longer covers that end — because the log was truncated, or because `endingOffsets` points past it. With
  `failOnDataLoss=false` the records that remain are returned.

Offsets that are simply unreadable within the available range are not data loss and never fail the read:
compacted-away records are skipped, as Spark skips them.

### Transactional Topics

`isolation.level` defaults to `read_uncommitted`, matching Spark. Note that this is *not* the
`librdkafka` default (`read_committed`), so it is set explicitly — otherwise a topic written by a
transactional producer would return a different row set here than under Spark, dropping aborted records
and anything past the last stable offset. Pass `kafka.isolation.level=read_committed` to opt in to
committed-only reads.

## Limitations

- **No writes.** Producing messages via `df.write.format("kafka")` is not yet supported.
- **Batch only.** There is no streaming or continuous mode; every read resolves to a bounded offset range.
- **No filter pushdown.** Predicates are applied above the scan; every offset in the planned range is
  fetched. Restrict the range with `startingOffsets`/`endingOffsets` or the timestamp options instead.
