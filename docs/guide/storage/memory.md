---
title: Memory Storage
rank: 8
---

# Memory Storage

Sail provides an in-memory storage backend that stores data entirely in RAM. This is particularly useful for testing, development, and scenarios where you need temporary storage without persisting to disk.

## URI Format

Memory storage uses the `memory://` URL scheme:

```
memory:///path/to/data
```

## Examples

### DataFrame

```python
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.write.parquet("memory:///users")

users_df = spark.read.parquet("memory:///users")
users_df.show()
```

### SQL

```python
sql = """
CREATE TABLE memory_table
USING parquet
LOCATION 'memory:///users';
"""
spark.sql(sql)
spark.sql("SELECT * FROM memory_table;").show()

spark.sql("INSERT INTO memory_table VALUES (3::int64, 'Charlie'), (4::int64, 'David');")
spark.sql("SELECT * FROM memory_table;").show()
```
