from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureDef, SubRequest

# pytest markers defined in `pyproject.toml` of the Ibis project
IBIS_MARKERS = [
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


@pytest.fixture(scope="session", autouse=True)
def patch_ibis_spark_session():
    from ibis.backends.pyspark.tests.conftest import TestConf, TestConfForStreaming

    def connect(*, tmpdir, worker_id, **kw):  # noqa: ARG001
        import ibis
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("ibis").remote("local").getOrCreate()
        return ibis.pyspark.connect(spark, **kw)

    TestConf.connect = staticmethod(connect)
    TestConfForStreaming.connect = staticmethod(connect)


def pytest_configure(config):
    for marker in IBIS_MARKERS:
        config.addinivalue_line("markers", marker)


def pytest_fixture_setup(
    fixturedef: FixtureDef[Any],
    request: SubRequest,  # noqa: ARG001
) -> object | None:
    from _pytest.nodes import SEP

    env_var = "IBIS_TESTING_DATA_DIR"
    data_dir = os.environ.get(env_var)
    if not data_dir:
        msg = f"missing environment variable '{env_var}'"
        raise RuntimeError(msg)

    data_dir = Path(data_dir)
    if fixturedef.argname == "data_dir" and fixturedef.baseid.endswith(f"{SEP}ibis{SEP}backends"):
        # override the result of the `data_dir` fixture
        fixturedef.cached_result = (data_dir, None, None)
        return data_dir

    return None
