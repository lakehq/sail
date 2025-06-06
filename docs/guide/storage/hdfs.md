---
title: HDFS
rank: 3
---

# HDFS

You can use the `hdfs://` URI to read and write data stored in Hadoop Distributed File System (HDFS).

For example, `hdfs://namenode:port/path/to/data` is a path in HDFS.

::: info
Kerberos authentication for HDFS is not supported yet.
For now, we recommend using HDFS with Sail only in a trusted environment.
For example, when running Sail in single-host mode on the primary node in an AWS EMR cluster, you can use HDFS to store temporary data.
:::
