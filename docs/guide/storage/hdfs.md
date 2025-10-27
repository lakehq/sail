---
title: HDFS
rank: 3
---

# HDFS

You can use the `hdfs://` URI to read and write data stored in Hadoop Distributed File System (HDFS).

For example, `hdfs://namenode:port/path/to/data` is a path in HDFS.

## Basic Usage

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://localhost:50051") \
    .getOrCreate()

# Write data to HDFS
df = spark.range(1000)
df.write.mode("overwrite").parquet("hdfs://namenode:9000/user/data")
```

## Kerberos Authentication

::: info
Kerberos authentication for HDFS is supported and tested with Sail.
:::

### Prerequisites

- **Kerberos-enabled HDFS cluster** configured with Kerberos authentication
  - See [Apache Hadoop Secure Mode](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SecureMode.html)
- **Valid keytab file** for the principal that will access HDFS
- **krb5.conf** on the Sail server host
  - See [MIT Kerberos Documentation](https://web.mit.edu/kerberos/krb5-latest/doc/admin/conf_files/krb5_conf.html)

### Starting Sail Server

Authenticate with Kerberos before starting the Sail server:

```python
#!/usr/bin/env python3
import subprocess
from pysail.spark import SparkConnectServer

# Authenticate with Kerberos
subprocess.run([
    "kinit", "-kt",
    "/path/to/user.keytab",
    "username@YOUR.REALM"
], check=True)

# Start Sail server
server = SparkConnectServer(ip="0.0.0.0", port=50051)
server.start(background=False)
```

::: tip
The Sail server runs in local mode by default. All executors share the Kerberos ticket from `kinit`, so no additional Spark configuration is needed.

If running Sail in distributed mode (e.g. run on Kubernetes), each worker instance requires its own Kerberos authentication via `kinit`.
:::

### Client Connection

Clients do not need Kerberos authentication. The Sail server handles HDFS authentication:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://localhost:50051") \
    .getOrCreate()

# Write to Kerberos-secured HDFS
df = spark.range(1000)
df.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/user/username/data")
```

## Delta Lake

Delta Lake works with Kerberos-secured HDFS:

```python
# Write Delta table
df.write.format("delta").mode("overwrite") \
    .save("hdfs://namenode:9000/user/username/delta_table")

# Read Delta table
delta_df = spark.read.format("delta") \
    .load("hdfs://namenode:9000/user/username/delta_table/")

# Time travel
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("hdfs://namenode:9000/user/username/delta_table/")
```

## Additional Resources

- [Apache Hadoop Secure Mode](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SecureMode.html)
- [MIT Kerberos Documentation](https://web.mit.edu/kerberos/)
- [Delta Lake Documentation](https://docs.delta.io/)
