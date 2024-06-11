#!/usr/bin/env python
import argparse
import json

import pyspark
from py4j.protocol import Py4JError

KEYS_WITH_DYNAMIC_DEFAULT = [
    "spark.driver.host",
    "spark.sql.warehouse.dir",
]


def _convert_spark_config_entry(spark, entry):
    seq_to_list = spark._jvm.scala.collection.JavaConverters.seqAsJavaList

    output = {"key": entry.key(), "doc": entry.doc()}

    if not entry.defaultValue().isEmpty():
        if entry.key() in KEYS_WITH_DYNAMIC_DEFAULT:
            output["defaultValue"] = None
        else:
            output["defaultValue"] = entry.defaultValueString()

    if not entry.alternatives().isEmpty():
        output["alternatives"] = list(seq_to_list(entry.alternatives()))

    try:
        output["fallback"] = entry.fallback().key()
    except Py4JError:
        pass

    return output


def collect_spark_config(spark):
    clazz = spark._jvm.java.lang.Class.forName("org.apache.spark.sql.internal.SQLConf$")
    obj = clazz.getDeclaredField("MODULE$").get(None)
    entries = obj.getConfigEntries().toArray()

    return {
        "sparkVersion": pyspark.__version__,
        "comment": "This is a generated file. Do not edit it directly.",
        "entries": sorted(
            [_convert_spark_config_entry(spark, entry) for entry in entries],
            key=lambda x: x["key"],
        ),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args()

    spark = (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .appName("Test")
        .getOrCreate()
    )

    with open(args.output, "w") as f:
        json.dump(collect_spark_config(spark), f, indent=2)
        f.write("\n")


if __name__ == "__main__":
    main()
