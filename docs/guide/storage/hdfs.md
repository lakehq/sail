---
title: HDFS
rank: 3
---

# HDFS

You can use the `hdfs://` URI to read and write data stored in Hadoop Distributed File System (HDFS).

For example, `hdfs://namenode:port/path/to/data` is a path in HDFS.

## Basic Usage

<!--@include: ../_common/spark-session.md-->

```python
df = spark.range(1000)
df.write.mode("overwrite").parquet("hdfs://namenode:9000/user/data")
```

## Kerberos Authentication

::: info
Kerberos authentication for HDFS is supported and tested with Sail.
:::

### Prerequisites

- A **Kerberos-enabled HDFS cluster** configured with Kerberos authentication. See [Apache Hadoop Secure Mode](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SecureMode.html) for details.
- A **valid keytab file** for the principal that will access HDFS.
- A **`krb5.conf`** file on the Sail server host. See [MIT Kerberos Documentation](https://web.mit.edu/kerberos/krb5-latest/doc/admin/conf_files/krb5_conf.html) for details.

### Starting the Sail Server

Authenticate with Kerberos before starting the Sail server.

```python
import subprocess
from pysail.spark import SparkConnectServer

# authenticate with Kerberos
subprocess.run([
    "kinit", "-kt",
    "/path/to/user.keytab",
    "username@YOUR.REALM"
], check=True)

# start the Sail server
server = SparkConnectServer(ip="0.0.0.0", port=50051)
server.start(background=False)
```

::: tip
The Sail server runs in local mode by default. The server process uses a single Kerberos ticket from `kinit`.

If running Sail in cluster mode (e.g. on Kubernetes), each worker instance requires its own Kerberos authentication via `kinit`.
:::

### Client Connection

The client does not need Kerberos authentication. The Sail server handles HDFS authentication.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://localhost:50051") \
    .getOrCreate()

# write to Kerberos-secured HDFS
df = spark.range(1000)
df.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/user/username/data")
```

### Additional Resources

- [Apache Hadoop Secure Mode](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SecureMode.html)
- [MIT Kerberos Documentation](https://web.mit.edu/kerberos/)
