---
title: Examples
rank: 1
---

# Examples

<!--@include: ../../_common/spark-session.md-->

## Basic Usage

::: code-group

```python [Python]
path = "file:///tmp/sail/users"
df = spark.createDataFrame(
    [(1, "Alice"), (2, "Bob")],
    schema="id INT, name STRING",
)

# This creates a new table or overwrites an existing one.
df.write.format("iceberg").mode("overwrite").save(path)
# This appends data to an existing table.
df.write.format("iceberg").mode("append").save(path)

df = spark.read.format("iceberg").load(path)
df.show()
```

```sql [SQL]
CREATE TABLE users (id INT, name STRING)
USING iceberg
LOCATION 'file:///tmp/sail/users';

INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');

SELECT * FROM users;
```

:::

## Data Partitioning

You can work with partitioned Iceberg tables using the Spark DataFrame API.
Iceberg records partition values and transforms in its metadata so queries can skip files that cannot match a filter.
The examples below use identity partitioning.

::: code-group

```python [Python]
path = "file:///tmp/sail/metrics"
df = spark.createDataFrame(
    [(2024, 1.0), (2025, 2.0)],
    schema="year INT, value FLOAT",
)

df.write.format("iceberg").mode("overwrite").partitionBy("year").save(path)

df = spark.read.format("iceberg").load(path).filter("year > 2024")
df.show()
```

```sql [SQL]
CREATE TABLE metrics (year INT, value FLOAT)
USING iceberg
LOCATION 'file:///tmp/sail/metrics'
PARTITIONED BY (year);

INSERT INTO metrics VALUES (2024, 1.0), (2025, 2.0);

SELECT * FROM metrics WHERE year > 2024;
```

:::

Iceberg also supports hidden partitioning with transforms. For example, partition by day while querying the original timestamp column:

```sql
CREATE TABLE events (id BIGINT, event_time TIMESTAMP, message STRING)
USING iceberg
PARTITIONED BY (days(event_time))
LOCATION 'file:///tmp/sail/events';

INSERT INTO events VALUES (1, TIMESTAMP '2025-01-02 03:04:05', 'created');

SELECT * FROM events
WHERE event_time >= TIMESTAMP '2025-01-02 00:00:00'
  AND event_time < TIMESTAMP '2025-01-03 00:00:00';
```

## Format Version

New Iceberg tables created in Sail default to version 2. Select a different version through `TBLPROPERTIES`:

```sql
CREATE TABLE iceberg_v3_users (id INT, name STRING)
USING iceberg
LOCATION 'file:///tmp/sail/iceberg_v3_users'
TBLPROPERTIES ('format-version' = '3');
```

For a filesystem-backed table, you can upgrade the version with:

```sql
ALTER TABLE users SET TBLPROPERTIES ('format-version' = '3');
```

See [Supported Features](./features#format-versions) for details by format version.

## Schema Evolution

Use `mergeSchema` to add fields or apply supported promotions during an append or overwrite.
For the table created by the basic Python example, append a row with a new `age` column:

```python
path = "file:///tmp/sail/users"
df = spark.createDataFrame([(3, "Carol", 30)], "id INT, name STRING, age INT")
df.write.format("iceberg").mode("append").option("mergeSchema", "true").save(path)
```

Supported promotions include `INT` to `BIGINT`, `FLOAT` to `DOUBLE`, and increasing decimal precision while keeping the scale.
Use `overwriteSchema` with a full overwrite to replace the schema:

```python
df = spark.createDataFrame([(1, "Alice")], "id BIGINT, name STRING")
df.write.format("iceberg").mode("overwrite").option("overwriteSchema", "true").save(path)
```

## Scoped Overwrite

For the identity-partitioned `metrics` table from the SQL example, replace one partition using a predicate:

```python
from pyspark.sql import functions as F

replacement = spark.createDataFrame([(2025, 3.0)], "year INT, value FLOAT")
replacement.writeTo("metrics").overwrite(F.col("year") == 2025)
```

Alternatively, replace the partitions present in the input:

```python
replacement.writeTo("metrics").overwritePartitions()
```

Both operations preserve untouched partitions. Dynamic overwrite with empty input is a no-op.

## DML Operations

In Sail, `DELETE`, `UPDATE`, and `MERGE INTO` use copy-on-write by default and work with format versions 1, 2, and 3.
For the `users` table from the SQL example:

```sql
UPDATE users SET name = 'Robert' WHERE id = 2;

DELETE FROM users WHERE id = 1;

MERGE INTO users AS target
USING (SELECT * FROM VALUES (2, 'Bob'), (3, 'Carol') AS s(id, name)) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET name = source.name
WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name);
```

To use merge-on-read `MERGE`, create a version 2 table with the operation's mode set explicitly:

```sql
CREATE TABLE iceberg_mor_users (id INT, name STRING)
USING iceberg
LOCATION 'file:///tmp/sail/iceberg_mor_users'
TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read');

INSERT INTO iceberg_mor_users VALUES (1, 'Alice');

MERGE INTO iceberg_mor_users AS target
USING (SELECT 1 AS id, 'Alicia' AS name) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET name = source.name;
```

This mode writes position-delete files and replacement data files.
See [DML Operations](./features#dml-operations) for the supported modes.

## Time Travel

Iceberg snapshots can be selected by ID, timestamp, or an existing branch or tag.
A timestamp selects the snapshot that was current at or before that time.

```python
df = spark.read.format("iceberg").option("snapshotId", "123").load(path)
df = spark.read.format("iceberg").option("timestampAsOf", "2025-01-02T03:04:05.678").load(path)
df = spark.read.format("iceberg").option("branch", "main").load(path)
df = spark.read.format("iceberg").option("tag", "release_1").load(path)
```

Replace the snapshot ID, timestamp, or reference with one that exists in the table's history.

For catalog-backed tables, use the catalog table name so that reads and writes follow the catalog's metadata pointer.
See [Catalogs and Maintenance](./features#catalogs-and-maintenance) for catalog integration details.
