---
title: Memory Storage
rank: 8
---

# Memory Storage

Sail provides an in-memory storage backend that stores data entirely in RAM. This is particularly useful for testing, development, and scenarios where you need temporary storage without persisting to disk.

## URL Format

Memory storage uses the `memory://` URL scheme:

```
memory:///path/to/data
```

The path after `memory://` is virtual and used to organize data within the in-memory store.

## Use Cases

### Testing and Development

Memory storage is ideal for unit tests and development workflows:

```python
test_df = spark.range(100).toDF("id")
test_df.write.mode("overwrite").parquet("memory:///test_data")

result_df = spark.read.parquet("memory:///test_data")
assert result_df.count() == 100

# Test with complex schemas
from pyspark.sql.types import *

schema = StructType([
    StructField("id", LongType(), False),
    StructField("data", ArrayType(StringType()), True),
    StructField("metadata", MapType(StringType(), StringType()), True)
])

test_df = spark.createDataFrame([
    (1, ["a", "b"], {"key": "value"}),
    (2, ["c", "d"], {"foo": "bar"})
], schema)
test_df.write.parquet("memory:///complex_test")

result_df = spark.read.parquet("memory:///complex_test")
result_df.show()
```

### Caching Frequently Used Data

```python
spark.read.parquet("s3://foo/bar.parquet") \
    .write.mode("overwrite").parquet("memory:///cache/reference")

ref_df = spark.read.parquet("memory:///cache/reference")
ref_df.show()
```

## Examples

### Basic Read/Write

```python
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.write.parquet("memory:///users")

users_df = spark.read.parquet("memory:///users")
users_df.show()
```

### SQL Tables

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
