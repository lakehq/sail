from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from pathlib import Path

import pytest

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


def _is_ibis_testing():
    return os.environ.get("IBIS_TESTING") == "1"


def _resolve_data_volume() -> str:
    env_var = "IBIS_TESTING_DATA_DIR"
    data_dir = os.environ.get(env_var)
    if not data_dir:
        msg = f"missing environment variable '{env_var}'"
        raise RuntimeError(msg)
    return str(Path(data_dir) / "parquet")


def pytest_configure(config):
    data_volume = _resolve_data_volume()
    mod = importlib.import_module("ibis.backends.pyspark.tests.conftest")
    TestConf = getattr(mod, "TestConf")  # noqa: N806 B009
    TestConf.data_volume = data_volume
    TestConf.parquet_dir = property(lambda _: data_volume)
    for marker in IBIS_MARKERS:
        config.addinivalue_line("markers", marker)


@dataclass
class TestMarker:
    keywords: list[str]
    reason: str


SKIPPED_IBIS_TESTS = [
    TestMarker(
        keywords=["test_table_info_large[pyspark]"],
        reason="Complex SQL statements causing timeout",
    ),
    TestMarker(
        keywords=["test_table_describe_large[pyspark]"],
        reason="Complex SQL statements causing timeout",
    ),
]

# Tests that need spark.sql.ansi.enabled=false because they expect non-ANSI behavior.
# With ANSI mode enabled (the default since Spark 4.x), division by zero raises an error.
# These tests expect non-ANSI results (NULL or Infinity), so they correctly fail (XFAIL)
# when ANSI mode is disabled and the results don't match the expected Ibis output.
ANSI_DISABLED_IBIS_TESTS = [
    TestMarker(
        keywords=["test_divide_by_zero", "pyspark"],
        reason="Ibis expects non-ANSI division by zero behavior",
    ),
]


def add_ibis_test_markers(items: list[pytest.Item]):
    for item in items:
        for test in SKIPPED_IBIS_TESTS:
            if all(k in item.keywords for k in test.keywords):
                item.add_marker(pytest.mark.skip(reason=test.reason))


def _needs_ansi_disabled(item: pytest.Item) -> bool:
    return any(all(any(k in kw for kw in item.keywords) for k in test.keywords) for test in ANSI_DISABLED_IBIS_TESTS)


@pytest.fixture(autouse=True)
def _manage_ansi_mode(request):
    """Toggle ANSI mode off for specific Ibis tests that expect non-ANSI behavior."""
    if not _is_ibis_testing() or not _needs_ansi_disabled(request.node):
        yield
        return

    try:
        con = request.getfixturevalue("con")
    except pytest.FixtureLookupError:
        yield
        return

    session = getattr(con, "_session", None)
    if session is None:
        yield
        return

    try:
        original = session.conf.get("spark.sql.ansi.enabled")
    except (ValueError, RuntimeError):
        original = "false"
    session.conf.set("spark.sql.ansi.enabled", "false")
    yield
    session.conf.set("spark.sql.ansi.enabled", original)


def pytest_collection_modifyitems(session: pytest.Session, config: pytest.Config, items: list[pytest.Item]) -> None:  # noqa: ARG001
    if _is_ibis_testing():
        add_ibis_test_markers(items)
