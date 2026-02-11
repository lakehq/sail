from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from pathlib import Path

import pytest

# pytest markers defined in `pyproject.toml` of the Ibis project
IBIS_MARKERS = [
    "athena: Amazon Athena tests",
    "backend: tests specific to a backend",
    "benchmark: benchmarks",
    "bigquery: BigQuery tests",
    "clickhouse: ClickHouse tests",
    "core: tests that do not required a backend",
    "databricks: Databricks SQL tests",
    "datafusion: Apache Datafusion tests",
    "druid: Apache Druid tests",
    "duckdb: DuckDB tests",
    "exasol: ExasolDB tests",
    "examples: tests that exercise examples",
    "flink: Flink tests",
    "geospatial: tests for geospatial functionality",
    "impala: Apache Impala tests",
    "materialize: Materialize tests",
    "mssql: MS SQL Server tests",
    "mysql: MySQL tests",
    "never: Backend will never support this / pass this test",
    "notimpl: Could be implemented/fixed in ibis, but hasn't yet",
    "notyet: Requires upstream to implement/fix something",
    "oracle: Oracle tests",
    "polars: Polars tests",
    "postgres: PostgreSQL tests",
    "pyspark: PySpark tests",
    "risingwave: RisingWave tests",
    "singlestoredb: SingleStoreDB tests",
    "singlestoredb_http: SingleStoreDB HTTP protocol tests",
    "singlestoredb_mysql: SingleStoreDB MySQL protocol tests",
    "snowflake: Snowflake tests",
    "sqlite: SQLite tests",
    "trino: Trino tests",
    "tpch: TPC-H tests",
    "tpcds: TPC-DS tests",
    "xfail_version: backend tests that for a specific version of a dependency",
]


def _is_ibis_testing():
    return os.environ.get("IBIS_TESTING") == "1"


def _resolve_data_dir() -> Path:
    env_var = "IBIS_TESTING_DATA_DIR"
    data_dir = os.environ.get(env_var)
    if not data_dir:
        msg = f"missing environment variable '{env_var}'"
        raise RuntimeError(msg)
    return Path(data_dir)


@pytest.fixture(scope="session")
def data_dir() -> Path:
    """Override Ibis data_dir fixture to use our test data location."""
    return _resolve_data_dir()


def pytest_configure(config):
    resolved = _resolve_data_dir()
    data_volume = str(resolved / "parquet")
    mod = importlib.import_module("ibis.backends.pyspark.tests.conftest")
    TestConf = getattr(mod, "TestConf")  # noqa: N806 B009
    TestConf.data_volume = data_volume
    TestConf.parquet_dir = property(lambda _: data_volume)
    # Skip the ``docker compose cp`` step in ``ServiceBackendTest.preload``.
    # Sail runs locally, so test data is already on the filesystem.
    TestConf.preload = lambda _self: None
    # Override the data_dir fixture in ibis.conftest so that tests
    # resolve test data from our location instead of the default
    # ``<ibis-package>/../ci/ibis-testing-data`` path.
    ibis_conftest = importlib.import_module("ibis.conftest")
    ibis_conftest.data_dir = data_dir
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
