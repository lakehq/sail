---
title: Troubleshooting
rank: 13
---

# Troubleshooting

## System Time Zone Issue on Amazon Linux

When you run Sail on Amazon Linux, you may encounter the following error when creating a session:

> `failed to get system time zone: No such file or directory (os error 2)`

The reason is that `/etc/localtime` is supposed to be a symlink when retrieving the system time zone, but on Amazon Linux it is a regular file.
There is a [GitHub issue](https://github.com/amazonlinux/amazon-linux-2023/issues/526) for this problem, but it has not been resolved yet.

To work around this issue, you can run the following command on the host:

```bash
sudo timedatectl set-timezone UTC
```

## Local Time Zone Configuration

Spark Connect uses the system local time zone when interpreting certain timestamp values on the client side.
Python `datetime.datetime` objects created without an explicit time zone are converted to Arrow data using the local time zone, before being sent to the server.

On Linux and macOS, you can change the local time zone using the `TZ` environment variable. Changes to the `TZ` environment variable at runtime can be made effective by calling [`time.tzset()`](https://docs.python.org/3/library/time.html#time.tzset) in Python.

```python
import os
import time

os.environ["TZ"] = "America/New_York"
time.tzset()
```

On Windows, `time.tzset()` is not available, so the local time zone cannot be changed at runtime from Python.
However, you can still configure the local time zone by setting the `TZ` environment variable _before_ starting Python.
Note that the `TZ` environment variable may also affect other libraries using the system time zone, so be mindful of any unintended side effects.

## JVM-Only `sparkContext` Patterns Under Spark Connect

Spark Connect does not support `SparkContext` or RDD operations, since there is no JVM in the client process. Code that accesses `spark.sparkContext` will raise an error under Spark Connect (and therefore under Sail).

A common example is creating an empty DataFrame using the legacy pattern:

```python
# ❌ Fails under Spark Connect
spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
```

Replace it with the equivalent Spark Connect-compatible call:

```python
# ✅ Works under Spark Connect
spark.createDataFrame([], schema)
```

The same applies to other `sparkContext` methods that construct RDDs. Migrate these by using the corresponding `SparkSession` or DataFrame API equivalents instead.

## Protobuf Version Mismatch

When you run PySpark 4.0 in Spark Connect mode, you may see a lot of warnings like this:

> `UserWarning: Protobuf gencode version 5.28.3 is exactly one major version older than the runtime version 6.31.1 at spark/connect/base.proto. Please update the gencode to avoid compatibility violations in the next runtime release.`

This is possibly related to upstream issues such as [this](https://github.com/protocolbuffers/protobuf/issues/18096) and [this](https://github.com/grpc/grpc/issues/37609).
You can either ignore these warnings, or install specific versions of Protobuf and gRPC libraries along with PySpark:

```bash
pip install "pyspark[connect]==4.0.0" \
  "protobuf==5.28.3" \
  "grpcio==1.71.2" \
  "grpcio-status==1.71.2"
```

The `pyspark[connect]` package can also be replaced with
its equivalent package `pyspark-connect`, or the lightweight client package `pyspark-client`, depending on your use case.
