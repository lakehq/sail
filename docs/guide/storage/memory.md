---
title: Memory
rank: 8
---

# Memory

Sail provides an in-memory storage backend that stores data entirely in RAM. This is particularly useful for testing, development, and scenarios where you need temporary storage without persisting to disk.

## URI Format

Memory storage uses the `memory://` URI scheme.

## Examples

<!--@include: ../_common/spark-session.md-->

### Spark DataFrame API

```python
path = "memory:///users"

df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], schema="id INT, name STRING")
df.write.parquet(path)

df = spark.read.parquet(path)
df.show()
```

### Spark SQL

```python
sql = """
CREATE TABLE my_table (id INT, name STRING)
USING parquet
LOCATION 'memory:///users'
"""
spark.sql(sql)
spark.sql("SELECT * FROM my_table").show()

spark.sql("INSERT INTO my_table VALUES (3, 'Charlie'), (4, 'David')")
spark.sql("SELECT * FROM my_table").show()
```
