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
the supported batch API follows Spark's Kafka conventions. Client configuration is
for librdkafka, not the JVM Kafka client; see the compatibility and resource limits below.

<!--@include: ../../_common/spark-session.md-->

## Installation

You need to install the `pysail` package with the `kafka` extra to use the Kafka data source.

```bash
pip install pysail[kafka]
```

PySpark 4.0+ is required. Install compatible versions of `pysail[kafka]` and PySpark
in the client, coordinator, and worker Python environments. Every coordinator and
worker that contacts Kafka needs broker connectivity and any referenced TLS/SASL files.
Installing the extra only in a remote notebook does not provision worker dependencies.

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

| Name                                 | Required   | Default     | Description                                                                                                                                                                         |
| ------------------------------------ | ---------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `kafka.bootstrap.servers`            | Yes        |             | Comma-separated broker list.                                                                                                                                                        |
| `assign`                             | One of the |             | JSON of `{"topic": [partition, ...]}`.                                                                                                                                              |
| `subscribe`                          | three      |             | Comma-separated topic names.                                                                                                                                                        |
| `subscribePattern`                   |            |             | Regular expression searched within topic names (unanchored, as in Spark).                                                                                                           |
| `startingOffsets`                    | No         | `earliest`  | `earliest`, or per-partition JSON (see below).                                                                                                                                      |
| `endingOffsets`                      | No         | `latest`    | `latest`, or per-partition JSON (see below).                                                                                                                                        |
| `startingTimestamp`                  | No         |             | Global start time as milliseconds since epoch. Applied to every partition.                                                                                                          |
| `endingTimestamp`                    | No         |             | Global end time as milliseconds since epoch. Applied to every partition.                                                                                                            |
| `startingOffsetsByTimestamp`         | No         |             | Per-partition JSON of start timestamps in ms since epoch.                                                                                                                           |
| `endingOffsetsByTimestamp`           | No         |             | Per-partition JSON of end timestamps in ms since epoch.                                                                                                                             |
| `startingOffsetsByTimestampStrategy` | No         | `error`     | `error` or `latest`: what to do when no offset matches a starting timestamp.                                                                                                        |
| `failOnDataLoss`                     | No         | `true`      | Whether to fail when records that were planned for reading are no longer in the log.                                                                                                |
| `includeHeaders`                     | No         | `false`     | Whether to project the `headers` column.                                                                                                                                            |
| `minPartitions`                      | No         |             | Approximate target number of input partitions; empty ranges and proportional rounding can affect the result. Positive signed 32-bit integer.                                        |
| `maxRecordsPerPartition`             | No         |             | Offset-span splitting hint, not a hard bound on records or memory. Positive signed 64-bit integer. See parallelism below.                                                           |
| `kafkaConsumer.pollTimeoutMs`        | No         | `120000`    | Effective idle polling budget in milliseconds. Also accepted as `pollTimeoutMs`; the Spark-standard name wins. Internally the budget is polled in slices of at most 250 ms.         |
| `maxBatchRows`                       | No         | `10000`     | Maximum rows per Arrow `RecordBatch` returned to the executor. Sail-specific.                                                                                                       |
| `maxBatchBytes`                      | No         | `8388608`   | Estimated Arrow payload bytes per batch; positive signed 32-bit integer. A single oversized record is emitted intact in its own batch. Sail-specific.                               |
| `maxBatchWaitMs`                     | No         | `250`       | Target maximum active time to accumulate a nonempty batch. Positive finite milliseconds. Broker metadata calls may extend this. Sail-specific.                                      |
| `stallTimeoutMs`                     | No         | Poll budget | Optional positive finite idle deadline overriding the poll budget. Downstream backpressure is excluded. Recovery attempts without progress also consume this budget. Sail-specific. |
| `adminTimeoutMs`                     | No         | `10000`     | Per-call timeout in milliseconds for planning metadata/offset requests and read-task watermark lookups (late binding, offset gaps, recovery, and EOF checks). Sail-specific.        |
| `kafka.*`                            | No         |             | Any additional `librdkafka` client property, passed through with the `kafka.` prefix stripped.                                                                                      |

::: info
Exactly one of `assign`, `subscribe`, and `subscribePattern` must be specified.

When several range options are given for the same endpoint, Spark's precedence applies rather than an
error: the global timestamp wins, then the per-partition timestamp map, then the offsets. Lower-priority
values are not parsed.

Partition IDs must fit signed 32-bit integers; offsets and timestamp options must fit signed 64-bit integers.
An absent option uses its default, whereas a present empty range option is rejected.
Boolean options accept case-insensitive `true` or `false`, not `0`/`1` or `yes`/`no`.
Timeout options must be positive and finite; `maxBatchRows` must be a positive integer.
:::

::: warning
The consumer properties this source owns are rejected rather than silently overridden:
`kafka.auto.offset.reset` (use `startingOffsets`/`endingOffsets`), `kafka.enable.auto.commit`, and
`kafka.enable.partition.eof`.
:::

::: info
**Consumer group id.** This source uses the stable default `sail-kafka-reader`, unlike Spark's generated
per-query group id. Consumers use explicit `assign()` without group membership and never commit
offsets, so concurrent queries do not coordinate partition ownership or modify committed group offsets.
They still share broker capacity and quotas. Override with `kafka.group.id` if your broker ACLs require
a specific one.
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
JVM-only properties such as `kafka.sasl.jaas.config` are not translated; use librdkafka's SASL/TLS
properties and make any referenced files available in every contacting process.

Connection options, including credentials, are serialized to the coordinator and workers.
Use trusted execution environments and protect the Spark Connect transport and access to query plans.
Partition summaries omit client settings, but client debug logs and external tooling may still expose
sensitive configuration. Do not include credentials in shared logs or notebooks.

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

`subscribe` takes a comma-separated topic list, `subscribePattern` a regular expression, and `assign` a
JSON map of explicit partitions.

The pattern is _searched within_ each topic name rather than anchored to it, matching Spark's
`topicRegex.findFirstIn(name)`: `subscribePattern=orders` selects `orders-eu` as well as `orders`. Anchor
it yourself — `orders$`, or `^orders$` — if you want whole-name matching.

Topics the broker flags as internal — `__consumer_offsets` and `__transaction_state` — are excluded from
all three strategies, matching Spark's `isInternal` filter. The flag is what counts, not the name: a topic
of your own called `__audit` is read normally.

An `assign` map is intersected with the partitions the broker reports, as Spark's `AssignStrategy` does.
An empty map, an empty partition list, and a partition id that does not exist all simply contribute no
input partitions, so an assignment naming a partition that has since been removed still reads the rest.

```python
df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("assign", json.dumps({"orders": [0, 2]}))
    .load()
)
```

### Controlling Parallelism

By default each Kafka partition becomes one input partition. `minPartitions` requests an approximate
parallelism target; `maxRecordsPerPartition` splits sufficiently large offset spans. Compacted and
transactional logs can have fewer visible records than offsets. Neither option is a process-memory limit.

Following Spark's batch planner, the splitting decision uses unresolved endpoint values. The default
`earliest`/`latest` range is represented as `(-2, -1)`, with numeric size 1, so
`maxRecordsPerPartition` alone does not split that range. Use explicit offset endpoints or
`minPartitions` when splitting is needed. Within a split, remainders land in the last chunk.

Outer endpoints remain late-bound. Log growth can therefore make the last chunk larger than the
planning hint; retention can invalidate an interior boundary or invert the first chunk. Splitting is
not guaranteed to preserve the success of an unsplit read when the log changes. Use explicit numeric
endpoints for reproducibility, and choose retention settings that preserve those ranges for the job.

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
When `minPartitions` or `maxRecordsPerPartition` triggers splitting, sentinels are resolved early so the
ranges can be divided — but the outer boundaries are then put back, so the overall start and end stay
late-bound and only the interior split points are fixed.

::: warning
A consequence inherited from Spark: because `latest` binds per task, a retried task can observe a longer
log than its first attempt. Use explicit `endingOffsets` to keep the requested end stable across retries;
retention or compaction can still change which records remain available within the range.
:::

### Data Loss

`failOnDataLoss` defaults to `true`, as in Spark. Data loss is reported when records that existed at
planning time are gone by the time the read runs:

- A start offset that has aged out of the log (retention or truncation) fails the query. With
  `failOnDataLoss=false` the read resumes at the earliest offset that is both still in the log and inside
  the planned range, and returns nothing when the two no longer overlap. It never reaches below the
  planned start.
- Records missing from the middle of a range fail the query when the log now starts above the offset that
  was requested. With `failOnDataLoss=false` they are skipped.
- Reaching the end of the log below the planned end offset fails the query when the high watermark no
  longer covers that end — because the log was truncated, or because `endingOffsets` points past it. With
  `failOnDataLoss=false` the records that remain are returned.

Offsets that are simply unreadable within the available range are not data loss and never fail the read:
compacted records, transaction control markers, and aborted records under `read_committed` are skipped, as
Spark skips them. The two are told apart by the log's start offset, as `KafkaDataConsumer` tells them
apart.

When missing offsets are skipped, the reader logs one partial-read summary per task with the number
and enclosing span of skipped intervals. An offset span is not an exact count of lost records.

Strict reads refresh the broker watermarks for each offset gap to detect retention, which adds
round trips on compacted or transactional topics. With `failOnDataLoss=false`, gaps are classified
once per output batch for the partial-read warning. This observation is made at batch delivery;
it cannot determine whether an interval was compacted before retention passed it.

### Resource Limits and Cancellation

Batches flush on the first of the row threshold, estimated-byte threshold, or nonempty-batch wait
target. The byte estimate includes keys, values, topic names, headers, and structural overhead; it does
not measure Python allocator overhead or total process RSS. An oversized message is returned as a
singleton batch, never truncated or silently dropped. Invalid/nonpositive limits fail during option
validation; blank values are not treated as absent.

The Python accumulator is released before a batch is yielded. Sail's Python data source runtime
buffers up to 16 batches per active partition between the reader and the query. Kafka prefetch,
in-flight conversion, and the number of concurrent partitions add to this budget. Tune client prefetch
through `kafka.queued.max.messages.kbytes` and related librdkafka settings; these settings also have
oversized-fetch exceptions. More input partitions can increase consumers, connections, and memory.

The reader closes its consumer on normal completion, a read error, or explicit generator closure.
The current runtime does not install a cooperative cancellation callback or explicitly finalize
the iterator on every early-exit path. A cancelled query can therefore leave a reader polling until
it yields another batch or reaches its idle timeout. Prompt cleanup after cancellation or `LIMIT`
is not guaranteed. Metadata calls use `adminTimeoutMs` (1–3,600,000 milliseconds); they and `Consumer.close()` cannot be
interrupted by the reader. Shutdown timing also depends on the client.

### Compatibility

The batch contract is based on Spark 4.1's Kafka planner/consumer behavior. The Python DataSource API
is supported from PySpark 4.0; unavailable filter APIs are bypassed and filtering remains above the scan.
Additional Sail options control Arrow batching and extended waiting, and are not JVM Kafka options.
The default poll budget is fixed at 120000 ms; it does not inherit a changed `spark.network.timeout`.
Set `kafkaConsumer.pollTimeoutMs` explicitly when a different budget is required.
Topic patterns use Python's regular-expression engine, so Java-specific pattern syntax is not guaranteed
to work unchanged.

### Transactional Topics

`isolation.level` defaults to `read_uncommitted`, matching Spark. Note that this is _not_ the
`librdkafka` default (`read_committed`), so it is set explicitly — otherwise a topic written by a
transactional producer would return a different row set here than under Spark, dropping aborted records
and anything past the last stable offset. Pass `kafka.isolation.level=read_committed` to opt in to
committed-only reads.

The setting also follows through to planning, as it does in Spark's `KafkaOffsetReaderAdmin`: under
`read_committed` a timestamp or a forced split boundary resolves against the last stable offset rather
than the high watermark, so planning cannot point past what the read will be allowed to see.

## Limitations

- **No writes.** Producing messages via `df.write.format("kafka")` is not yet supported.
- **Batch only.** There is no streaming or continuous mode; every read resolves to a bounded offset range.
- **No filter pushdown.** Predicates are applied above the scan; every offset in the planned range is
  fetched. Restrict the range with `startingOffsets`/`endingOffsets` or the timestamp options instead.
