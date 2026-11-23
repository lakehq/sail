---
title: Kafka
rank: 6
---

# Kafka Data Source

Sail provides a Kafka batch connector exposed under the `kafka` format name for API parity with vanilla
PySpark. The implementation is based on the Python `confluent-kafka` library, which wraps `librdkafka`.
No JVM or Kafka client JAR is involved.

Every read resolves to a bounded `[start, end)` offset range per partition. The schema and batch options
follow Spark's Kafka conventions, while `kafka.*` client properties are for `librdkafka` rather than the
JVM Kafka client.

<!--@include: ../../_common/spark-session.md-->

## Installation

Install `pysail` with the `kafka` extra:

```bash
pip install pysail[kafka]
```

PySpark 4.0+ is required. Install compatible versions of `pysail[kafka]` and PySpark in the client,
coordinator, and worker Python environments, and ensure coordinators and workers have broker connectivity
plus any referenced TLS/SASL files.

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

Every read returns Spark's fixed Kafka schema (all columns nullable):

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

Cast `key` and `value` to `string` (or decode with `from_json`, `from_avro`, etc.) at the query level.
When a broker reports no timestamp type, `timestampType` is `-1` and `timestamp` is `-1` ms
(`1969-12-31 23:59:59.999`), matching Spark.

## Options

Options are case-insensitive and follow the
[PySpark Kafka documentation](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html).

| Name                                 | Required   | Default    | Description                                                                                    |
| ------------------------------------ | ---------- | ---------- | ---------------------------------------------------------------------------------------------- |
| `kafka.bootstrap.servers`            | Yes        |            | Comma-separated broker list.                                                                   |
| `assign`                             | One of the |            | JSON of `{"topic": [partition, ...]}`.                                                         |
| `subscribe`                          | three      |            | Comma-separated topic names.                                                                   |
| `subscribePattern`                   |            |            | Regular expression searched within topic names (unanchored, matching Spark).                   |
| `startingOffsets`                    | No         | `earliest` | `earliest`, or per-partition JSON.                                                             |
| `endingOffsets`                      | No         | `latest`   | `latest`, or per-partition JSON.                                                               |
| `startingTimestamp`                  | No         |            | Global start time in milliseconds since epoch.                                                 |
| `endingTimestamp`                    | No         |            | Global end time in milliseconds since epoch.                                                   |
| `startingOffsetsByTimestamp`         | No         |            | Per-partition JSON of start timestamps in milliseconds since epoch.                            |
| `endingOffsetsByTimestamp`           | No         |            | Per-partition JSON of end timestamps in milliseconds since epoch.                              |
| `startingOffsetsByTimestampStrategy` | No         | `error`    | `error` or `latest`: behavior when no offset matches a starting timestamp.                     |
| `failOnDataLoss`                     | No         | `true`     | Whether to fail when planned records are no longer in the log.                                 |
| `includeHeaders`                     | No         | `false`    | Whether to include the `headers` column.                                                       |
| `minPartitions`                      | No         |            | Approximate target number of input partitions (positive 32-bit integer).                       |
| `maxRecordsPerPartition`             | No         |            | Offset-span splitting hint per partition (positive 64-bit integer).                            |
| `kafkaConsumer.pollTimeoutMs`        | No         | `120000`   | Per-poll timeout in ms (also accepted as `pollTimeoutMs`; `kafkaConsumer.pollTimeoutMs` wins). |
| `maxBatchRows`                       | No         | `10000`    | Maximum rows per Arrow `RecordBatch`. Sail-specific.                                           |
| `maxBatchBytes`                      | No         | `8388608`  | Estimated Arrow payload bytes per batch (positive 32-bit integer). Sail-specific.              |
| `adminTimeoutMs`                     | No         | `10000`    | Per-call timeout in ms (1–3,600,000) for planning and task watermark lookups. Sail-specific.   |
| `kafka.*`                            | No         |            | Additional `librdkafka` property, forwarded with the `kafka.` prefix stripped.                 |

::: info
Exactly one of `assign`, `subscribe`, and `subscribePattern` is required.

When multiple range options are set for the same endpoint, Spark's precedence applies: global timestamp
beats per-partition timestamps, which beat offsets, and lower-priority values are not parsed. Partition
IDs must fit signed 32-bit integers; offsets and timestamps must fit signed 64-bit integers. Present blank
values are rejected; booleans accept only case-insensitive `true`/`false`; timeouts must be positive whole
numbers of milliseconds, as in Spark.
:::

::: warning
The consumer properties owned by this source are rejected rather than overridden:
`kafka.auto.offset.reset` (use `startingOffsets`/`endingOffsets`), `kafka.enable.auto.commit`, and
`kafka.enable.partition.eof`.
:::

::: info
**Consumer group id.** This source defaults to `sail-kafka-reader` rather than a generated per-query id.
Consumers use explicit `assign()` without group membership or offset commits, so concurrent queries do not
coordinate partition ownership or modify committed offsets. Override with `kafka.group.id` if your broker
ACLs require a specific id.
:::

## Examples

### Per-Partition Offset Ranges

Use JSON to specify explicit start and exclusive end offsets per partition:

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

Sentinels `-2` (`earliest`) and `-1` (`latest`) are accepted in JSON at their valid endpoints only:
`-2`/`earliest` in `startingOffsets` and `-1`/`latest` in `endingOffsets`. The reverse is rejected,
matching Spark.

### Including Headers

Opt in with `includeHeaders=true` to project an `array<struct<key: string, value: binary>>` column:

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

`startingTimestamp` and `endingTimestamp` resolve to concrete offsets via broker timestamp lookups. When a
partition has no message with `ts >= T`, ending timestamps fall back to `latest`, while starting timestamps
follow `startingOffsetsByTimestampStrategy` (default `error`, or `latest` to bound at the log end).

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
Timestamp lookups search the topic's stored timestamps: producer clocks under `CreateTime` (the default),
or broker ingest time under `LogAppendTime`.
:::

### Passing librdkafka Options

Options prefixed with `kafka.` are forwarded to `librdkafka` with the prefix stripped. JVM-only settings
such as `kafka.sasl.jaas.config` are not translated; use `librdkafka` SASL/TLS properties instead.

Connection options (including credentials) are serialized to coordinators and workers — use trusted
execution environments, protect the Spark Connect transport, and avoid sharing client debug logs.

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

- `subscribe` accepts a comma-separated topic list.
- `subscribePattern` searches a Python regex anywhere inside each topic name (matching Spark). Anchor
  explicitly (e.g. `^orders$`) for whole-name matching.
- `assign` takes a JSON map of explicit partitions and intersects it with broker metadata — empty maps,
  empty lists, and nonexistent partition IDs contribute no input partitions.
- Broker-flagged internal topics (such as `__consumer_offsets` and `__transaction_state`) are excluded from
  all three strategies; user topics with leading underscores (e.g. `__audit`) are read normally.

```python
df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("assign", json.dumps({"orders": [0, 2]}))
    .load()
)
```

### Controlling Parallelism

By default, each Kafka partition becomes one input partition. `minPartitions` requests an approximate
partition target, and `maxRecordsPerPartition` splits large offset spans (neither is a memory limit, and
compacted/transactional logs may contain fewer visible records than offsets).

Following Spark, splitting is decided on unresolved endpoint values (`earliest`/`latest` is `(-2, -1)`
with size 1), so `maxRecordsPerPartition` alone does not split an `earliest`–`latest` read; use explicit
offsets or `minPartitions` when splitting is needed. Outer endpoints remain late-bound, so log growth or
retention can affect outer chunks — use explicit numeric endpoints for reproducible splits.

```python
df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("minPartitions", "16")
    .load()
)
```

## Runtime Behavior

### Offset Resolution

`earliest` and `latest` (including JSON `-2`/`-1`) bind when each task starts reading rather than during
planning, matching Spark:

- `latest` reads up to the current tail when the task runs.
- `earliest` stays valid if retention advances between planning and execution.

Explicit offsets and timestamps resolve during planning. When splitting triggers, sentinels are resolved
only to place interior split points; outer endpoints remain late-bound.

::: warning
Because `latest` binds per task, a retried task can observe a longer log than its first attempt. Use
explicit `endingOffsets` for a stable upper bound across retries.
:::

### Data Loss

`failOnDataLoss` defaults to `true`, matching Spark:

- **Unavailable start offset:** fails under `failOnDataLoss=true`; with `false`, resumes at the earliest
  offset still inside the planned range (never below it), or returns nothing if there is no overlap.
- **Mid-range gaps:** fails under `failOnDataLoss=true` when the log now starts above the requested offset.
  Unreadable offsets within the available range (compacted records, control markers, aborted transactional
  records under `read_committed`) are skipped, matching Spark.
- **Truncated end:** reaching end-of-log below the planned end fails under `failOnDataLoss=true`; with
  `false`, returns the remaining records.

Strict reads query broker watermarks on each offset gap to detect retention, adding round trips on
compacted or transactional topics. With `failOnDataLoss=false`, ordinary gaps are skipped without a
watermark lookup or warning; out-of-range recovery or truncated EOF logs one partial-read warning per task.

### Resource Limits and Cancellation

- **Batch flushing:** batches flush on the first of `maxBatchRows` or `maxBatchBytes`. Oversized messages
  are emitted intact as singleton batches, never truncated.
- **Memory & prefetch:** `maxBatchBytes` estimates Arrow payload bytes, not total process RSS. Tune client
  prefetch with `kafka.queued.max.messages.kbytes` and related `librdkafka` settings; higher partition counts
  increase concurrent consumers and memory use.
- **Cleanup & cancellation:** consumers close on normal completion, read errors, or explicit generator
  closure. Because early exits such as cancellation or `LIMIT` may not finalize the iterator immediately, a
  task can keep polling until it yields a batch or hits `kafkaConsumer.pollTimeoutMs`. Metadata lookups
  (`adminTimeoutMs`) and `Consumer.close()` are not interruptible by the reader.

### Compatibility & Transactional Topics

- Semantics follow Spark 4.1's batch Kafka source on PySpark 4.0+ (filtering remains above the scan).
- `kafkaConsumer.pollTimeoutMs` defaults to a fixed `120000` ms and does not inherit `spark.network.timeout`.
- `subscribePattern` uses Python's regex engine rather than Java's.
- `isolation.level` defaults to `read_uncommitted` (matching Spark rather than `librdkafka`'s
  `read_committed` default). Pass `kafka.isolation.level=read_committed` to read committed records only;
  planning-time timestamp and split lookups honor the same setting.

## Limitations

- **No writes.** Producing messages via `df.write.format("kafka")` is not yet supported.
- **Batch only.** There is no streaming or continuous mode; every read resolves to a bounded offset range.
- **No filter pushdown.** Predicates are applied above the scan; restrict ranges with
  `startingOffsets`/`endingOffsets` or timestamp options instead.
