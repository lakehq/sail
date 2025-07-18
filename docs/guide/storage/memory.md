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

## Features

- **Fast access**: No disk I/O overhead
- **Isolated**: Each Sail session has its own memory store
- **Temporary**: Data is lost when the session ends
- **Full operations**: Supports all read and write operations

## Use Cases

### Testing and Development

Memory storage is ideal for unit tests and development workflows:

```python
# Write test data to memory
test_df = spark.range(100).toDF("id")
test_df.write.mode("overwrite").parquet("memory:///test_data")

# Read it back
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
```

### Temporary Processing

Use memory storage for intermediate results in complex pipelines:

```python
# Stage 1: Process raw data
raw_df = spark.read.csv("s3://bucket/raw_data.csv")
processed_df = raw_df.filter(col("status") == "active")
processed_df.write.parquet("memory:///temp/processed")

# Stage 2: Aggregate
temp_df = spark.read.parquet("memory:///temp/processed")
final_df = temp_df.groupBy("category").agg(sum("amount"))
final_df.write.parquet("s3://bucket/final_results")
```

### Caching Frequently Used Data

```python
# Load reference data into memory once
spark.read.csv("s3://bucket/reference_data.csv") \
    .write.mode("overwrite").parquet("memory:///cache/reference")

# Use it multiple times without re-reading from S3
ref_df = spark.read.parquet("memory:///cache/reference")

# Implement cache warming on startup
def warm_cache():
    datasets = [
        ("s3://bucket/dim_users.parquet", "memory:///cache/dim_users"),
        ("s3://bucket/dim_products.parquet", "memory:///cache/dim_products"),
        ("s3://bucket/lookup_table.csv", "memory:///cache/lookup")
    ]

    for source, cache_path in datasets:
        spark.read.parquet(source).write.mode("overwrite").parquet(cache_path)
        print(f"Cached {source} to {cache_path}")

warm_cache()
```

## Examples

### Basic Read/Write

```python
# Create a DataFrame and write to memory
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.write.parquet("memory:///users")

# Read it back
users_df = spark.read.parquet("memory:///users")
users_df.show()
```

### SQL Tables

```sql
-- Create a table backed by memory storage
CREATE TABLE memory_table
USING parquet
LOCATION 'memory:///tables/my_table';

-- Insert data
INSERT INTO memory_table VALUES (1, 'Data in memory');

-- Query the table
SELECT * FROM memory_table;
```
