---
title: Iceberg
rank: 2
---

# Iceberg

You can use the `iceberg` format in Sail to work with [Apache Iceberg](https://iceberg.apache.org/).
You can use the Spark DataFrame API or Spark SQL to read and write Iceberg tables.

::: warning
The Iceberg integration in Sail is under active development.
You can use Sail to read Iceberg tables and write new Iceberg tables.
But it is not recommended to use Sail to overwrite or modify existing Iceberg tables created by other engines.
If you encounter any issues or would like to request advanced Iceberg features, feel free to reach out to us on [GitHub Issues](https://github.com/lakehq/sail/issues)!
:::

## Examples

<!--@include: ../_common/spark-session.md-->

### Basic Usage

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

::: warning

Reading and writing partitioned Iceberg tables is not yet supported.
We are actively working on this feature and plan to release it soon.
Stay tuned!

:::

### More Features

We will continue adding more examples for advanced Iceberg features as they become available in Sail.
In the meantime, feel free to reach out to us on [Slack](https://lakesail.com/slack) or [GitHub Discussions](https://github.com/lakehq/sail/discussions) if you have questions!
