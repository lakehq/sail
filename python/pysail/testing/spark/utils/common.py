from __future__ import annotations

import os

import pyspark


def pyspark_version() -> tuple[int, ...]:
    return tuple(int(x) for x in pyspark.__version__.split("."))


def is_jvm_spark():
    return os.environ.get("SPARK_REMOTE", "").startswith("local")


def shares_deterministic_ctes(envs: dict[str, str] | None = None) -> bool:
    """Whether the Sail server shares the CTEs that are not volatile, which it does not
    under a memory limit with temporary files disabled or in cluster mode. The server
    runs with the environment variables of the tests and the given ones."""
    envs = {**os.environ, **(envs or {})}
    limited = envs.get("SAIL_RUNTIME__MEMORY_POOL__TYPE", "unbounded").lower() != "unbounded"
    no_temporary_files = envs.get("SAIL_RUNTIME__TEMPORARY_FILES__MAX_SIZE") == "0"
    cluster = envs.get("SAIL_MODE") in ("local-cluster", "kubernetes-cluster")
    return not (limited and (no_temporary_files or cluster))
