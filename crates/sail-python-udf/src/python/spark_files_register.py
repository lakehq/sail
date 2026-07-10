# ruff: noqa: I001

import sys
import types

import pyspark


files = sys.modules["pyspark.core.files"]
core = sys.modules.get("pyspark.core")
if core is None:
    core = types.ModuleType("pyspark.core")
    sys.modules["pyspark.core"] = core
    pyspark.core = core

core.files = files
sys.modules["pyspark.files"] = files
pyspark.files = files
pyspark.SparkFiles = files.SparkFiles
