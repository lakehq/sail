---
title: HDFS
rank: 3
---

# HDFS

Sail supports reading and writing data to Hadoop Distributed File System (HDFS) using the `hdfs://` URI scheme.

## URI Format

```
hdfs://namenode:port/path/to/data
```

Components:

- `namenode`: HDFS NameNode hostname or IP address
- `port`: NameNode port (default: 9000 for Hadoop 2.x+, 8020 for older versions)
- `/path/to/data`: Path within HDFS

Examples:

```python
# Default port
df = spark.read.parquet("hdfs://namenode/data/input.parquet")

# Explicit port
df = spark.read.parquet("hdfs://namenode:9000/data/input.parquet")

# High availability cluster
df = spark.read.parquet("hdfs://mycluster/data/input.parquet")
```

## Configuration

### Basic Configuration

Set Hadoop configuration through environment variables:

```bash
# Point to Hadoop configuration directory
export HADOOP_CONF_DIR="/etc/hadoop/conf"

# Or set individual properties
export HADOOP_USER_NAME="hdfs_user"
```

### High Availability Configuration

For HDFS with High Availability (HA), configure the cluster name:

```python
# Use the cluster name instead of specific namenode
df = spark.read.parquet("hdfs://mycluster/path/to/data")
```

Ensure your `hdfs-site.xml` contains the HA configuration:

```xml
<property>
  <name>dfs.nameservices</name>
  <value>mycluster</value>
</property>
<property>
  <name>dfs.ha.namenodes.mycluster</name>
  <value>nn1,nn2</value>
</property>
```

## Reading Data

### Basic Reading

```python
# Read various formats from HDFS
df_parquet = spark.read.parquet("hdfs://namenode:9000/data/input.parquet")
df_csv = spark.read.option("header", "true").csv("hdfs://namenode:9000/data/input.csv")
df_json = spark.read.json("hdfs://namenode:9000/data/input.json")
df_orc = spark.read.orc("hdfs://namenode:9000/data/input.orc")
```

### Reading Directories

```python
# Read all files in a directory
df = spark.read.parquet("hdfs://namenode:9000/data/2024/")

# Use wildcards
df = spark.read.parquet("hdfs://namenode:9000/data/*/part-*.parquet")

# Read multiple paths
df = spark.read.parquet([
    "hdfs://namenode:9000/data/2023/",
    "hdfs://namenode:9000/data/2024/"
])
```

### Partitioned Data

```python
# Read partitioned data
# HDFS structure: /data/year=2024/month=01/part-00000.parquet
df = spark.read.parquet("hdfs://namenode:9000/data/")
# Partition columns are automatically detected
```

## Writing Data

### Basic Writing

```python
# Write to HDFS
df.write.mode("overwrite").parquet("hdfs://namenode:9000/output/data.parquet")

# Write with specific options
df.write \
  .mode("overwrite") \
  .option("compression", "snappy") \
  .parquet("hdfs://namenode:9000/output/compressed.parquet")
```

### Write Modes

```python
# Overwrite existing data
df.write.mode("overwrite").parquet("hdfs://namenode:9000/output/")

# Append to existing data
df.write.mode("append").parquet("hdfs://namenode:9000/output/")

# Fail if data exists
df.write.mode("error").parquet("hdfs://namenode:9000/output/")
```

### Partitioned Writing

```python
# Write partitioned data
df.write \
  .partitionBy("year", "month") \
  .parquet("hdfs://namenode:9000/output/partitioned/")

# Control output files
df.repartition(10).write.parquet("hdfs://namenode:9000/output/")
df.coalesce(1).write.parquet("hdfs://namenode:9000/output/single-file/")
```

## Working with SQL

```sql
-- Create external table on HDFS
CREATE TABLE IF NOT EXISTS hdfs_table
USING parquet
LOCATION 'hdfs://namenode:9000/data/';

-- Query the table
SELECT * FROM hdfs_table WHERE year = 2024;

-- Create and populate table
CREATE TABLE hdfs_output
USING parquet
LOCATION 'hdfs://namenode:9000/output/'
AS SELECT * FROM hdfs_table;
```

## Common Use Cases

### ETL Pipeline

```python
# Read raw data from HDFS
raw_df = spark.read.json("hdfs://namenode:9000/raw/events/")

# Transform data
processed_df = raw_df \
  .filter(raw_df.event_type == "purchase") \
  .groupBy("user_id", "product_id") \
  .agg({"amount": "sum", "quantity": "sum"})

# Write results back to HDFS
processed_df.write \
  .mode("overwrite") \
  .partitionBy("date") \
  .parquet("hdfs://namenode:9000/processed/purchases/")
```

### Temporary Storage in EMR

```python
# Use HDFS for intermediate results in AWS EMR
intermediate = spark.read.parquet("s3://bucket/input/")
intermediate.write.parquet("hdfs:///tmp/intermediate/")

# Process intermediate data
result = spark.read.parquet("hdfs:///tmp/intermediate/").filter(...)
result.write.parquet("s3://bucket/output/")
```

## Performance Optimization

### Data Locality

HDFS provides data locality when Spark executors run on the same nodes as HDFS DataNodes:

```python
# Spark will prefer to process data on nodes where it's stored
df = spark.read.parquet("hdfs://namenode:9000/data/")
```

### Block Size Considerations

```python
# Write with specific block size (128MB)
df.write \
  .option("dfs.blocksize", "134217728") \
  .parquet("hdfs://namenode:9000/output/")
```

### Replication Factor

```python
# Set replication factor for output files
df.write \
  .option("dfs.replication", "3") \
  .parquet("hdfs://namenode:9000/output/")
```

## Security

::: warning
Kerberos authentication for HDFS is not supported yet. We recommend using HDFS with Sail only in trusted environments.
:::

For now, you can:

- Run Sail in a secure network environment
- Use HDFS for temporary/intermediate data only
- Implement network-level security controls

## Troubleshooting

### Connection Issues

```bash
# Verify HDFS is accessible
hdfs dfs -ls hdfs://namenode:9000/

# Check namenode status
hdfs dfsadmin -report
```

### Permission Errors

```bash
# Set HDFS user
export HADOOP_USER_NAME="hdfs"

# Or ensure proper permissions
hdfs dfs -chmod -R 755 /path/to/data
hdfs dfs -chown -R your_user:your_group /path/to/data
```

### Performance Issues

- Check HDFS health: `hdfs dfsadmin -report`
- Monitor DataNode status
- Verify network connectivity between Spark and HDFS nodes
- Consider data locality and replication factors
