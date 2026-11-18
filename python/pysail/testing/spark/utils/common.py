# SPDX-License-Identifier: Apache-2.0

import os

import pyspark


def pyspark_version() -> tuple[int, ...]:
    return tuple(int(x) for x in pyspark.__version__.split("."))


def is_jvm_spark():
    return os.environ.get("SPARK_REMOTE", "").startswith("local")
