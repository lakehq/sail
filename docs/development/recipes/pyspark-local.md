---
title: Local PySpark Session
rank: 30
---

# Starting a Local PySpark Session

You can use the following commands to start a local PySpark session.

```bash
# Run the PySpark shell using the original Java implementation.
hatch run pyspark

# Run the PySpark shell using the Spark Connect implementation.
env SPARK_CONNECT_MODE_ENABLED=1 SPARK_REMOTE="sc://localhost:50051" hatch run pyspark
```
