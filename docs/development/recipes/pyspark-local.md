---
title: Local PySpark Session
rank: 30
---

# Starting a Local PySpark Session

You can start a local PySpark session for development purposes.

Use the following command to run the PySpark shell with the Java implementation.

```bash
hatch run pyspark
```

If you encounter the Spark driver address binding error (`java.net.BindException`),
try setting the `SPARK_LOCAL_IP` environment variable explicitly.

```bash
env SPARK_LOCAL_IP=127.0.0.1 hatch run pyspark
```

Use the following command to run the PySpark shell with the Spark Connect implementation.

```bash
env SPARK_CONNECT_MODE_ENABLED=1 SPARK_REMOTE="sc://localhost:50051" hatch run pyspark
```
