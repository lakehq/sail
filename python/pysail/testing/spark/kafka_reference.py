"""Native-Spark Kafka reference; JSON read-option maps are supplied on stdin.

Run with ``python -m pysail.testing.spark.kafka_reference``. Requires full
PySpark, Java, and access to the matching Kafka connector artifact. Never
register Sail's Python source in this process.
"""

import datetime
import json
import sys


def canonical_rows(rows):
    def normalize(value):
        if hasattr(value, "asDict"):
            return normalize(value.asDict())
        if isinstance(value, dict):
            return {k: normalize(v) for k, v in value.items()}
        if isinstance(value, list):
            return [normalize(v) for v in value]
        if isinstance(value, (bytes, bytearray)):
            return {"binary_hex": bytes(value).hex()}
        if isinstance(value, datetime.datetime):
            # Collected Spark timestamps are naive Python-local times. Convert
            # both those and aware timestamps to UTC without relabeling an instant.
            return value.astimezone(datetime.timezone.utc).isoformat()
        return value

    return [normalize(row) for row in sorted(rows, key=lambda r: (r.topic, r.partition, r.offset))]


def main():
    import pyspark  # noqa: PLC0415
    from pyspark.sql import SparkSession  # noqa: PLC0415

    cases = json.load(sys.stdin)
    spark = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}")
        .getOrCreate()
    )
    try:
        results = []
        for options in cases:
            frame = spark.read.format("kafka").options(**options).load()
            results.append({"schema": frame.schema.jsonValue(), "rows": canonical_rows(frame.collect())})
        sys.stdout.write(
            "KAFKA_REFERENCE=" + json.dumps({"spark_version": pyspark.__version__, "results": results}) + "\n"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
