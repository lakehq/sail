import os

import pyspark


def pyspark_version() -> tuple[int, ...]:
    return tuple(int(x) for x in pyspark.__version__.split("."))


def is_jvm_spark():
    return os.environ.get("SPARK_REMOTE", "").startswith("local")


def shares_deterministic_ctes() -> bool:
    """Whether the Sail server shares the CTEs that are not volatile, which it does not
    under a memory limit with temporary files disabled."""
    limited = os.environ.get("SAIL_RUNTIME__MEMORY_POOL__TYPE", "unbounded").lower() != "unbounded"
    no_temporary_files = os.environ.get("SAIL_RUNTIME__TEMPORARY_FILES__MAX_SIZE") == "0"
    return not (limited and no_temporary_files)
