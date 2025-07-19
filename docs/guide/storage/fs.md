---
title: File Systems
rank: 1
---

# File Systems

Sail supports reading and writing data to local and mounted file systems using file paths or `file://` URIs.

## Path Formats

You can reference files in three ways:

```python
# Relative path (relative to Sail server's working directory)
df = spark.read.parquet("data/input.parquet")

# Absolute path
df = spark.read.parquet("/home/user/data/input.parquet")

# File URI
df = spark.read.parquet("file:///home/user/data/input.parquet")
```

::: info
Sail is agnostic whether data is stored on local disk or network drives, as long as it appears in a mounted file system.
:::

## Reading Data

### Single Files

```python
# Read various file formats
df_parquet = spark.read.parquet("/path/to/file.parquet")
df_csv = spark.read.option("header", "true").csv("/path/to/file.csv")
df_json = spark.read.json("/path/to/file.json")
df_orc = spark.read.orc("/path/to/file.orc")
df_avro = spark.read.format("avro").load("/path/to/file.avro")
```

### Directories

When reading a directory, Sail reads all files matching the specified format:

```python
# Read all Parquet files in a directory
df = spark.read.parquet("/path/to/directory/")

# Read with wildcards
df = spark.read.parquet("/path/to/data/*.parquet")
df = spark.read.parquet("/path/to/data/year=202*/month=*/")

# Read multiple paths
df = spark.read.parquet(["/path/to/data1", "/path/to/data2"])
```

### Partitioned Data

Sail automatically discovers partitioned data:

```python
# Directory structure:
# /data/
#   year=2023/
#     month=01/file1.parquet
#     month=02/file2.parquet
#   year=2024/
#     month=01/file3.parquet

df = spark.read.parquet("/data/")
# Partition columns (year, month) are automatically included
```

## Writing Data

### Basic Writing

```python
# Write to different formats
df.write.mode("overwrite").parquet("/path/to/output/")
df.write.mode("overwrite").option("header", "true").csv("/path/to/output/")
df.write.mode("overwrite").json("/path/to/output/")
df.write.mode("overwrite").orc("/path/to/output/")
```

### Write Modes

```python
# Overwrite existing data
df.write.mode("overwrite").parquet("/path/to/output/")

# Append to existing data
df.write.mode("append").parquet("/path/to/output/")

# Fail if data already exists (default)
df.write.mode("error").parquet("/path/to/output/")

# Ignore if data already exists
df.write.mode("ignore").parquet("/path/to/output/")
```

### Partitioned Writing

```python
# Partition by columns
df.write.partitionBy("year", "month").parquet("/path/to/partitioned/")

# Control number of output files
df.coalesce(1).write.parquet("/path/to/single-file/")
df.repartition(10).write.parquet("/path/to/ten-files/")
```

## Working with SQL

```sql
-- Create external table from files
CREATE TABLE IF NOT EXISTS file_table
USING parquet
LOCATION 'file:///path/to/data/';

-- Query the table
SELECT * FROM file_table WHERE year = 2024;

-- Create table and write data
CREATE TABLE output_table
USING parquet
LOCATION '/path/to/output/'
AS SELECT * FROM file_table;
```

## Common Patterns

### Processing Multiple Files

```python
# Process files in a loop
import os

for filename in os.listdir("/data/inputs/"):
    if filename.endswith(".csv"):
        df = spark.read.option("header", "true").csv(f"/data/inputs/{filename}")
        processed = df.filter(df.value > 100)
        output_name = filename.replace(".csv", ".parquet")
        processed.write.parquet(f"/data/outputs/{output_name}")
```

### Handling Schema Evolution

```python
# Read with schema inference
df = spark.read.option("mergeSchema", "true").parquet("/path/to/evolving-data/")

# Provide explicit schema
from pyspark.sql.types import *

schema = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("value", DoubleType(), True)
])

df = spark.read.schema(schema).parquet("/path/to/data/")
```

## Best Practices

1. **Use absolute paths in production** to avoid ambiguity
2. **Partition large datasets** by commonly filtered columns
3. **Coalesce small files** to reduce file system overhead
4. **Use compression** for better storage efficiency:

```python
df.write.option("compression", "snappy").parquet("/path/to/compressed/")
```

## Troubleshooting

### Permission Errors

- Ensure the Sail server process has read/write permissions
- Check file ownership and permissions: `ls -la /path/to/data`

### Path Not Found

- Verify the path exists from the server's perspective
- Use absolute paths to avoid working directory confusion

### Performance Issues

- Avoid too many small files (use coalesce)
- Avoid too few large files (use repartition)
- Consider partitioning for better query performance
