import os

import pyspark

# Snapshot the engine at import time, which happens during collection. Stopping a local Spark
# Connect session DELETES `SPARK_REMOTE` from the environment (PySpark 4.2
# `sql/connect/session.py:982`), so a call made after the first session teardown would report Sail
# in the middle of a JVM run -- silently taking the Sail branch of every runtime `is_jvm_spark()`.
_IS_JVM_SPARK = os.environ.get("SPARK_REMOTE", "").startswith("local")


def pyspark_version() -> tuple[int, ...]:
    return tuple(int(x) for x in pyspark.__version__.split("."))


def is_jvm_spark():
    return _IS_JVM_SPARK
