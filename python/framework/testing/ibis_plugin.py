from pathlib import Path
from typing import Any, Optional

import pytest
from _pytest.fixtures import FixtureDef, SubRequest

IBIS_TESTING_DATA_DIR = Path(__file__).absolute().parents[3] / "opt" / "ibis-testing-data"

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
    from ibis.backends.pyspark.tests.conftest import TestConf

    def connect(*, tmpdir, worker_id, **kw):
        import ibis
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("ibis").remote("local").getOrCreate()
        return ibis.pyspark.connect(spark, **kw)

    TestConf.connect = staticmethod(connect)


def pytest_configure(config):
    for marker in IBIS_MARKERS:
        config.addinivalue_line("markers", marker)


def pytest_fixture_setup(
        fixturedef: "FixtureDef[Any]", request: "SubRequest"
) -> Optional[object]:
    from _pytest.nodes import SEP

    if fixturedef.argname == "data_dir" and fixturedef.baseid.endswith(f"{SEP}ibis{SEP}backends"):
        # override the result of the `data_dir` fixture
        fixturedef.cached_result = (IBIS_TESTING_DATA_DIR, None, None)
        return IBIS_TESTING_DATA_DIR
