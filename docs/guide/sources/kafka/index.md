---
title: Kafka
rank: 6
---

# Kafka Data Source

Sail provides a Kafka batch connector exposed under the `kafka` format name for API parity with vanilla
PySpark. The implementation is based on the Python `confluent-kafka` library, which wraps `librdkafka`.
No JVM or Kafka client JAR is involved.

Every read resolves to a bounded `[start, end)` offset range per partition at planning time. The fixed
Kafka schema (`key`, `value`, `topic`, `partition`, `offset`, `timestamp`, `timestampType`) matches Spark,
so downstream code that already works against `spark.read.format("kafka")` runs unchanged.

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

| Column          | Type                       | Description                              |
| --------------- | -------------------------- | ---------------------------------------- |
| `key`           | `binary`                   | Message key, or `null` if absent.        |
| `value`         | `binary`                   | Message value.                           |
| `topic`         | `string`                   | Source topic.                            |
| `partition`     | `int`                      | Source partition.                        |
| `offset`        | `bigint`                   | Message offset within the partition.     |
| `timestamp`     | `timestamp` (microsecond)  | Message timestamp (see `timestampType`). |
| `timestampType` | `int`                      | `0` = CreateTime, `1` = LogAppendTime.   |
| `headers`       | `array<struct<key,value>>` | Present only when `includeHeaders=true`. |

Cast `key` and `value` to `string` (or decode via `from_json`, `from_avro`, etc.) at the query level.

## Options

The data source options are consistent with
the [PySpark Kafka documentation](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html).

| Name                         | Required | Default    | Description                                                                                                                   |
| ---------------------------- | -------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `kafka.bootstrap.servers`    | Yes      |            | Comma-separated broker list.                                                                                                  |
| `subscribe`                  | Yes      |            | Comma-separated topic names.                                                                                                  |
| `startingOffsets`            | No       | `earliest` | `earliest`, `latest`, or per-partition JSON (see below).                                                                      |
| `endingOffsets`              | No       | `latest`   | `latest`, or per-partition JSON (see below).                                                                                  |
| `startingTimestamp`          | No       |            | Global start time as milliseconds since epoch. Applied to every partition.                                                    |
| `endingTimestamp`            | No       |            | Global end time as milliseconds since epoch. Applied to every partition.                                                      |
| `startingOffsetsByTimestamp` | No       |            | Per-partition JSON of start timestamps in ms since epoch.                                                                     |
| `endingOffsetsByTimestamp`   | No       |            | Per-partition JSON of end timestamps in ms since epoch.                                                                       |
| `includeHeaders`             | No       | `false`    | Whether to project the `headers` column.                                                                                      |
| `maxBatchRows`               | No       | `10000`    | Maximum rows per Arrow `RecordBatch` returned to the executor.                                                                |
| `pollTimeoutMs`              | No       | `1000`     | Per-call `consumer.poll()` timeout in milliseconds.                                                                           |
| `maxEmptyPolls`              | No       | `30`       | Consecutive empty polls tolerated before failing (guards against an unreachable broker or dead partition leader).             |
| `adminTimeoutMs`             | No       | `10000`    | Timeout in milliseconds for broker metadata, watermark, and `offsets_for_times` calls. Bump this for slow or remote clusters. |
| `kafka.*`                    | No       |            | Any additional `librdkafka` client property, passed through with the `kafka.` prefix stripped.                                |

::: info
Only one starting spec may be set at a time: `startingOffsets`, `startingOffsetsByTimestamp`, or
`startingTimestamp`. The same rule applies to the three ending options.
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

`startingTimestamp` and `endingTimestamp` resolve to concrete offsets via Kafka's `offsets_for_times` API.
Useful for time-window reads. A partition with no message satisfying `ts >= T` resolves to the current
high watermark, so the read bounds cleanly at whatever's in the log at planning time.

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
`offsets_for_times` searches the topic's stored timestamps. If the topic is configured with
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

## Limitations

- **No `subscribePattern` or `assign`.** Only explicit `subscribe` with a comma-separated topic list is supported.
- **No writes.** Producing messages via `df.write.format("kafka")` is not yet supported.
- **No `failOnDataLoss`.** Missing offset ranges surface as empty partitions rather than errors.
