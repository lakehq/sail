from __future__ import annotations

import os
import importlib
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureDef, SubRequest

# pytest markers defined in `pyproject.toml` of the Ibis project
IBIS_MARKERS = [
"athena",
"databricks",
    "backend: tests specific to a backend",
    "benchmark: benchmarks",
    "core: tests that do not required a backend",
    "examples: tests that exercise examples",
    "geospatial: tests for geospatial functionality",
    "xfail_version: backend tests that for a specific version of a dependency",
    "notimpl: functionality that isn't implemented in ibis",
    "notyet: for functionality that isn't implemented in a backend",
    "never: tests for functionality that a backend is likely to never implement",
    "broken: test has exposed existing broken functionality",
    "bigquery: BigQuery tests",
    "clickhouse: ClickHouse tests",
    "dask: Dask tests",
    "datafusion: Apache Datafusion tests",
    "druid: Apache Druid tests",
    "duckdb: DuckDB tests",
    "exasol: ExasolDB tests",
    "flink: Flink tests",
    "impala: Apache Impala tests",
    "mysql: MySQL tests",
    "mssql: MS SQL Server tests",
    "oracle: Oracle tests",
    "pandas: Pandas tests",
    "polars: Polars tests",
    "postgres: PostgreSQL tests",
    "risingwave: Risingwave tests",
    "pyspark: PySpark tests",
    "snowflake: Snowflake tests",
    "sqlite: SQLite tests",
    "trino: Trino tests",
    "tpch: TPC-H tests",
    "tpcds: TPC-DS tests",
]


@pytest.fixture(scope="session")
def patch_ibis_spark_session():
    from ibis.backends.pyspark.tests.conftest import TestConf, TestConfForStreaming

    def connect(*, tmpdir, worker_id, **kw):  # noqa: ARG001
        import ibis
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        return ibis.pyspark.connect(spark, **kw)

    TestConf.connect = staticmethod(connect)
    TestConfForStreaming.connect = staticmethod(connect)


def _resolve_data_volume() -> str:
    dv = os.environ.get("IBIS_DATA_VOLUME")
    if dv:
        return dv
    root = os.environ.get("IBIS_TESTING_DATA_DIR")
    if root:
        return str(Path(root) / "parquet")
    return "/data"

def pytest_configure(config):
    import importlib
    data_volume = _resolve_data_volume()
    mod = importlib.import_module("ibis.backends.pyspark.tests.conftest")
    TestConf = getattr(mod, "TestConf")
    TestConf.data_volume = data_volume
    TestConf.parquet_dir = property(lambda self: data_volume)
    for marker in IBIS_MARKERS:
        config.addinivalue_line("markers", marker)

