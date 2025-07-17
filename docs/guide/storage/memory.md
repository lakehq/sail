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

### Working with Multiple Formats

```python
# Write different formats
df.write.json("memory:///data.json")
df.write.csv("memory:///data.csv")
df.write.orc("memory:///data.orc")

# Read them back
json_df = spark.read.json("memory:///data.json")
csv_df = spark.read.csv("memory:///data.csv")
orc_df = spark.read.orc("memory:///data.orc")
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

## Limitations

::: warning
**Memory Constraints**: The in-memory store is limited by available RAM. Large datasets may cause out-of-memory errors.

**Persistence**: Data is not persisted and will be lost when:

- The Spark session ends
- The process terminates
- The system restarts
  :::

## Performance Characteristics

- **Write Speed**: Extremely fast, limited only by memory bandwidth
- **Read Speed**: Instant access with no network or disk latency
- **Capacity**: Limited by available system memory
- **Concurrency**: Thread-safe for concurrent reads and writes

## Best Practices

1. **Use for small to medium datasets**: Ideal for data that fits comfortably in memory
2. **Temporary data only**: Don't use for data that needs to persist
3. **Clean up**: Remove data when no longer needed to free memory
4. **Monitor memory usage**: Keep track of memory consumption in long-running processes

```python
# Clean up memory storage when done
# Note: There's no direct delete API, but overwriting with empty data achieves the same
spark.createDataFrame([], schema).write.mode("overwrite").parquet("memory:///temp_data")
```
