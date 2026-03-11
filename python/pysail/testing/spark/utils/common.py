import doctest
import os

import pyspark

# This doctest option flag is used to annotate tests involving
# extended Spark features supported by Sail.
# The test will be skipped when running on JVM Spark.
SAIL_ONLY = doctest.register_optionflag("SAIL_ONLY")


def pyspark_version() -> tuple[int, ...]:
    return tuple(int(x) for x in pyspark.__version__.split("."))


def is_jvm_spark():
    return os.environ.get("SPARK_REMOTE", "").startswith("local")
