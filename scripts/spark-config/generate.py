#!/usr/bin/env python
import argparse
import contextlib
import json
import os

import pyspark
from py4j.protocol import Py4JError

KEYS_WITH_DYNAMIC_DEFAULT = [
    "spark.driver.host",
    "spark.sql.session.timeZone",
    "spark.sql.warehouse.dir",
]


def _build_config_entry(spark, entry, *, is_static, deprecated):
    seq_to_list = spark._jvm.scala.collection.JavaConverters.seqAsJavaList

    output = {"key": entry.key(), "doc": entry.doc()}

    if not entry.defaultValue().isEmpty():
        if entry.key() in KEYS_WITH_DYNAMIC_DEFAULT:
            output["defaultValue"] = None
        else:
            output["defaultValue"] = entry.defaultValueString()

    if not entry.alternatives().isEmpty():
        output["alternatives"] = list(seq_to_list(entry.alternatives()))

    if is_static:
        output["isStatic"] = True

    if deprecated is not None:
        output["deprecated"] = deprecated

    with contextlib.suppress(Py4JError):
        output["fallback"] = entry.fallback().key()

    return output


def _build_removed_config_entry(entry):
    return {
        "key": entry.key(),
        "doc": "(Removed)",
        "defaultValue": entry.defaultValue(),
        "removed": {
            "version": entry.version(),
            "comment": entry.comment(),
        },
    }


def collect_spark_config(spark):
    to_java_map = spark._jvm.scala.collection.JavaConverters.mapAsJavaMap

    clazz = spark._jvm.java.lang.Class.forName("org.apache.spark.sql.internal.SQLConf$")
    obj = clazz.getDeclaredField("MODULE$").get(None)

    # Initialize the Scala `Connect$` object so its options are registered in `SQLConf`.
    # In Spark 3.x this class comes from the `spark-connect` package, which is
    # available through Spark's class loader rather than the JVM's default class loader.
    spark._jvm.org.apache.spark.util.Utils.classForName("org.apache.spark.sql.connect.config.Connect$", True, False)

    deprecation_map = {
        x.key(): {"version": x.version(), "comment": x.comment()}
        for x in to_java_map(obj.deprecatedSQLConfigs()).values()
    }
    entries = [
        _build_config_entry(
            spark,
            entry,
            is_static=obj.isStaticConfigKey(entry.key()),
            deprecated=deprecation_map.get(entry.key()),
        )
        for entry in obj.getConfigEntries().toArray()
    ]
    removed_entries = [_build_removed_config_entry(entry) for entry in to_java_map(obj.removedSQLConfigs()).values()]

    return {
        "sparkVersion": pyspark.__version__,
        "comment": "This is a generated file. Do not edit it directly.",
        "entries": sorted(
            entries + removed_entries,
            key=lambda x: x["key"],
        ),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args()

    # PySpark 3.x does not load the Spark Connect Scala package by default.
    # When collecting the Spark 3.x configuration, we set `PYSPARK_SUBMIT_ARGS`
    # so that package is available and its options can be registered.
    if pyspark.__version__.startswith("3."):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--packages org.apache.spark:spark-connect_2.12:{pyspark.__version__} pyspark-shell"
        )

    spark = pyspark.sql.SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

    with open(args.output, "w") as f:
        json.dump(collect_spark_config(spark), f, indent=2)
        f.write("\n")


if __name__ == "__main__":
    main()
