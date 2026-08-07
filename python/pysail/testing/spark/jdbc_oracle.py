"""Opt-in native Spark 4.1.2 JDBC writer oracle for differential tests."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

_DRIVER_PACKAGES = {
    "postgresql": "org.postgresql:postgresql:42.7.7",
    "mysql": "com.mysql:mysql-connector-j:9.4.0",
    "sqlserver": "com.microsoft.sqlserver:mssql-jdbc:12.10.2.jre11",
}

_DRIVER_CLASSES = {
    "postgresql": "org.postgresql.Driver",
    "mysql": "com.mysql.cj.jdbc.Driver",
    "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# Every Spark type the write path can auto-create on all three backends. Evaluated
# with selectExpr by both engines so create-table type mappings can be compared.
SPARK_TYPE_MATRIX_SELECT_EXPRS = [
    "CAST(true AS BOOLEAN) AS c_bool",
    "CAST(1 AS TINYINT) AS c_byte",
    "CAST(2 AS SMALLINT) AS c_short",
    "CAST(3 AS INT) AS c_int",
    "CAST(4 AS BIGINT) AS c_long",
    "CAST(1.5 AS FLOAT) AS c_float",
    "CAST(2.5 AS DOUBLE) AS c_double",
    "CAST(3.14 AS DECIMAL(10,2)) AS c_decimal",
    "'text' AS c_string",
    "X'0102' AS c_binary",
    "DATE'2026-01-02' AS c_date",
    "TIMESTAMP'2026-01-02 03:04:05' AS c_ts",
    # Typed literal, not CAST(string): Spark marks failable casts force-nullable
    # while Sail keeps the literal's non-nullability, so a cast here would make
    # the created columns' nullability legitimately differ between engines.
    "TIMESTAMP_NTZ'2026-01-02 03:04:05' AS c_ts_ntz",
]


def native_spark_4_1_2_python() -> Path | None:
    """Return the configured isolated Spark 4.1.2 interpreter, if available."""
    value = os.environ.get("SAIL_SPARK_4_1_2_PYTHON")
    return Path(value) if value else None


def run_native_jdbc_write(
    *,
    dialect: str,
    jdbc_url: str,
    dbtable: str,
    user: str,
    password: str,
    schema_json: dict | None,
    rows: list[list[object]],
    mode: str | None,
    options: dict[str, str] | None = None,
    select_exprs: list[str] | None = None,
) -> None:
    """Run one JDBC write in an isolated native Spark 4.1.2 process."""
    python = native_spark_4_1_2_python()
    if python is None:
        msg = "SAIL_SPARK_4_1_2_PYTHON is required for native Spark differential tests"
        raise RuntimeError(msg)
    payload = {
        "jdbc_url": jdbc_url,
        "dbtable": dbtable,
        "user": user,
        "password": password,
        "schema": schema_json,
        "rows": rows,
        "mode": mode,
        "options": options or {},
        "select_exprs": select_exprs,
        "package": _DRIVER_PACKAGES[dialect],
        "driver": _DRIVER_CLASSES[dialect],
    }
    code = """
import json, os, sys
payload = json.loads(sys.stdin.read())
os.environ.pop("SPARK_REMOTE", None)
os.environ.pop("SPARK_CONNECT_MODE_ENABLED", None)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
spark = (SparkSession.builder.master("local[2]")
         .appName("sail-jdbc-spark-oracle")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.jars.packages", payload["package"])
         .getOrCreate())
try:
    if spark.version != "4.1.2":
        raise RuntimeError(f"Expected Spark 4.1.2, got {spark.version}")
    if payload["select_exprs"] is None:
        df = spark.createDataFrame(payload["rows"], StructType.fromJson(payload["schema"]))
    else:
        df = spark.range(1).selectExpr(*payload["select_exprs"])
    writer = (df.write.format("jdbc")
              .option("url", payload["jdbc_url"])
              .option("dbtable", payload["dbtable"])
              .option("user", payload["user"])
              .option("password", payload["password"])
              .option("driver", payload["driver"])
              .options(**payload["options"]))
    if payload["mode"] is not None:
        writer = writer.mode(payload["mode"])
    writer.save()
finally:
    spark.stop()
"""
    env = os.environ.copy()
    env["TZ"] = "UTC"
    env["PYSPARK_PYTHON"] = str(python)
    env["PYSPARK_DRIVER_PYTHON"] = str(python)
    subprocess.run(
        [str(python), "-c", code],
        input=json.dumps(payload),
        text=True,
        check=True,
        env=env,
    )
