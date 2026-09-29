---
title: Examples
rank: 1
---

# Examples

<!--@include: ../../_common/spark-session.md-->

## Basic Usage

The Python example writes a DataFrame to an Iceberg table, appends the same rows, and reads the result. The SQL example creates a table at a path before inserting and querying rows.

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

Iceberg records partition values and transforms in table metadata. Sail uses them to skip files that cannot match a query filter. These examples partition `metrics` by the value of `year` and filter out the 2024 row.

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

With hidden partitioning, a transform determines the partition without adding a column to queries. This table partitions events by day, while the query filters on the original `event_time` column:

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

Sail creates version 2 Iceberg tables by default. Set `format-version` in `TBLPROPERTIES` to create a table in another supported version:

```sql
CREATE TABLE iceberg_v3_users (id INT, name STRING)
USING iceberg
LOCATION 'file:///tmp/sail/iceberg_v3_users'
TBLPROPERTIES ('format-version' = '3');
```

To upgrade a filesystem-backed table, change the same property:

```sql
ALTER TABLE users SET TBLPROPERTIES ('format-version' = '3');
```

The [format version reference](./features#format-versions) describes the available features in each version.

## Schema Evolution

Set `mergeSchema` during an append or overwrite to add fields or apply supported type promotions. For the `users` table from the basic Python example, this append adds an `age` column:

```python
path = "file:///tmp/sail/users"
df = spark.createDataFrame([(3, "Carol", 30)], "id INT, name STRING, age INT")
df.write.format("iceberg").mode("append").option("mergeSchema", "true").save(path)
```

Supported promotions include `INT` to `BIGINT`, `FLOAT` to `DOUBLE`, and greater decimal precision with the same scale. To replace the schema instead, use `overwriteSchema` with a full overwrite:

```python
df = spark.createDataFrame([(1, "Alice")], "id BIGINT, name STRING")
df.write.format("iceberg").mode("overwrite").option("overwriteSchema", "true").save(path)
```

## Scoped Overwrite

For the `metrics` table partitioned by `year`, a predicate overwrite replaces only the 2025 partition:

```python
from pyspark.sql import functions as F

replacement = spark.createDataFrame([(2025, 3.0)], "year INT, value FLOAT")
replacement.writeTo("metrics").overwrite(F.col("year") == 2025)
```

To replace every partition present in the input, use dynamic partition overwrite:

```python
replacement.writeTo("metrics").overwritePartitions()
```

Both operations leave other partitions intact. An empty input makes a dynamic overwrite a no-op.

## DML Operations

By default, Sail uses copy-on-write for `DELETE`, `UPDATE`, and `MERGE INTO` on format versions 1, 2, and 3. These statements change the `users` table from the SQL example:

```sql
UPDATE users SET name = 'Robert' WHERE id = 2;

DELETE FROM users WHERE id = 1;

MERGE INTO users AS target
USING (SELECT * FROM VALUES (2, 'Bob'), (3, 'Carol') AS s(id, name)) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET name = source.name
WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name);
```

For a version 3 table, set each DML operation to merge-on-read through its table property:

```sql
CREATE TABLE iceberg_mor_users (id INT, name STRING)
USING iceberg
LOCATION 'file:///tmp/sail/iceberg_mor_users'
TBLPROPERTIES (
  'format-version' = '3',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode' = 'merge-on-read'
);

INSERT INTO iceberg_mor_users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');

UPDATE iceberg_mor_users SET name = 'Alicia' WHERE id = 1;
DELETE FROM iceberg_mor_users WHERE id = 2;

MERGE INTO iceberg_mor_users AS target
USING (SELECT * FROM VALUES (3, 'Caroline'), (4, 'Dave') AS s(id, name)) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET name = source.name
WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name);

SELECT * FROM iceberg_mor_users ORDER BY id;
```

The final query returns `(1, 'Alicia')`, `(3, 'Caroline')`, and `(4, 'Dave')`. Version 3 merge-on-read records deleted row positions in Puffin deletion vectors and writes replacement data files for updated rows. See [DML operations](./features#dml-operations) for the supported modes.

## Time Travel

Time travel selects an Iceberg snapshot by ID, timestamp, or an existing branch or tag. A timestamp selects the snapshot that was current at or before that time.

```python
df = spark.read.format("iceberg").option("snapshotId", "123").load(path)
df = spark.read.format("iceberg").option("timestampAsOf", "2025-01-02T03:04:05.678").load(path)
df = spark.read.format("iceberg").option("branch", "main").load(path)
df = spark.read.format("iceberg").option("tag", "release_1").load(path)
```

Use a snapshot ID, timestamp, or reference from the table history. For catalog-backed tables, read and write by table name so Sail follows the catalog metadata pointer. See [catalogs and maintenance](./features#catalogs-and-maintenance) for details.
