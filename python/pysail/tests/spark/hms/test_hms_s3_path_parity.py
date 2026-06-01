"""S3 (MinIO) path qualification parity tests between Sail and reference Spark."""

from __future__ import annotations

# ruff: noqa: SLF001
import contextlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from pyspark import SparkContext
from pyspark.sql import SparkSession

from pysail.tests.spark.conftest import (
    configure_spark_session,
    patch_spark_connect_session,
)
from pysail.tests.spark.hms.conftest import (
    _HMS_S3_BUCKET,
    _classic_spark_mode,
    _describe_extended_properties,
    _run_sail_hms_server,
)

pytestmark = pytest.mark.catalog_integration

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path


@dataclass(frozen=True)
class WarehouseCase:
    name: str


@dataclass(frozen=True)
class PathCase:
    database_location: str
    table_location: str

    @property
    def case_id(self) -> str:
        return f"db_{self.database_location}__table_{self.table_location}"


WAREHOUSE_CASES = (
    WarehouseCase("default"),
    WarehouseCase("explicit"),
)

PATH_CASES = tuple(
    PathCase(database_location=database_location, table_location=table_location)
    for database_location in ("none", "explicit")
    for table_location in ("none", "explicit")
)


def _s3_prefix(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts)


def _describe_database_properties(spark: SparkSession, database: str) -> dict[str, str]:
    rows = spark.sql(f"DESCRIBE DATABASE EXTENDED {database}").collect()
    props: dict[str, str] = {}
    for row in rows:
        key = (row[0] or "").strip()
        value = (row[1] or "").strip()
        if key:
            props[key] = value
    return props


def _database_location(spark: SparkSession, database: str) -> str:
    return _describe_database_properties(spark, database).get("Location", "")


def _table_location(spark: SparkSession, table_fqn: str) -> str:
    return _describe_extended_properties(spark, table_fqn).get("Location", "")


def _warehouse_dir(
    s3_base: str,
    warehouse_case: WarehouseCase,
) -> str:
    if warehouse_case.name == "default":
        return f"{s3_base}/warehouse_default"
    if warehouse_case.name == "explicit":
        return f"{s3_base}/warehouse_explicit"
    msg = f"unknown warehouse case: {warehouse_case.name}"
    raise AssertionError(msg)


def _database_dir(
    s3_base: str,
    database: str,
    location_kind: str,
) -> str | None:
    if location_kind == "none":
        return None
    if location_kind == "explicit":
        return f"{s3_base}/db_explicit/{database}"
    msg = f"unknown database location kind: {location_kind}"
    raise AssertionError(msg)


def _table_dir(
    s3_base: str,
    database: str,
    table: str,
    location_kind: str,
) -> str | None:
    if location_kind == "none":
        return None
    if location_kind == "explicit":
        return f"{s3_base}/tbl_explicit/{database}/{table}"
    msg = f"unknown table location kind: {location_kind}"
    raise AssertionError(msg)


def _drop_database(*sessions: SparkSession, database: str) -> None:
    for session in sessions:
        with contextlib.suppress(Exception):
            session.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")


def _create_database(spark: SparkSession, database: str, location: str | None) -> None:
    if location is None:
        spark.sql(f"CREATE DATABASE {database}")
    else:
        spark.sql(f"CREATE DATABASE {database} LOCATION '{location}'")


def _create_table(spark: SparkSession, table_fqn: str, location: str | None) -> None:
    if location is None:
        spark.sql(f"CREATE TABLE {table_fqn} (id INT, note STRING) USING PARQUET")
    else:
        spark.sql(f"CREATE TABLE {table_fqn} (id INT, note STRING) USING PARQUET LOCATION '{location}'")


def _assert_s3_location_prefix(location: str, expected_prefix: str) -> None:
    assert location.startswith(f"s3://{_HMS_S3_BUCKET}/{expected_prefix}"), (
        f"expected s3://{_HMS_S3_BUCKET}/{expected_prefix} prefix, got: {location}"
    )


def _reference_s3_options(endpoint: str) -> dict[str, str]:
    from pysail.tests.spark.hms.conftest import _spark_s3_options

    options = _spark_s3_options(endpoint)
    options["spark.jars.packages"] = "org.apache.hadoop:hadoop-aws:3.3.4"
    return options


def _reset_classic_spark_state() -> None:
    # PySpark does not expose a public helper for fully resetting classic-session globals.
    active_context = SparkContext._active_spark_context
    if active_context is not None:
        active_context.stop()
    SparkContext._active_spark_context = None
    SparkContext._gateway = None
    SparkContext._jvm = None
    SparkSession._instantiatedSession = None
    SparkSession._activeSession = None


@contextlib.contextmanager
def _s3_path_parity_sessions(
    hms_s3_metastore_endpoint: str,
    hms_s3_env: dict[str, str],
    hms_warehouse_dir: Path,
    hms_s3_endpoint: str,
    warehouse_case: WarehouseCase,
) -> Generator[tuple[SparkSession, SparkSession, str], None, None]:
    s3_base = f"s3://{_HMS_S3_BUCKET}/s3_parity/{warehouse_case.name}"
    warehouse_value = _warehouse_dir(s3_base, warehouse_case)
    warehouse_dir_local = hms_warehouse_dir / "s3_parity" / warehouse_case.name
    warehouse_dir_local.mkdir(parents=True, exist_ok=True)

    with (
        contextlib.contextmanager(_run_sail_hms_server)(hms_s3_metastore_endpoint, hms_s3_env) as remote,
    ):
        sail = (
            SparkSession.builder.remote(remote)
            .appName(f"s3_parity_sail_{warehouse_case.name}")
            .config("spark.sql.warehouse.dir", warehouse_value)
            .create()
        )
        configure_spark_session(sail)
        patch_spark_connect_session(sail)

        reference_spark = None
        try:
            with _classic_spark_mode():
                _reset_classic_spark_state()

                builder = (
                    SparkSession.builder.master("local[1]")
                    .appName(f"s3_parity_reference_{warehouse_case.name}")
                    .config("spark.sql.catalogImplementation", "hive")
                    .config(
                        "spark.hadoop.hive.metastore.uris",
                        f"thrift://{hms_s3_metastore_endpoint}",
                    )
                    .config(
                        "spark.hadoop.javax.jdo.option.ConnectionURL",
                        f"jdbc:derby:;databaseName={warehouse_dir_local}/metastore_s3_parity;create=true",
                    )
                    .config("spark.sql.warehouse.dir", warehouse_value)
                )
                for key, value in _reference_s3_options(hms_s3_endpoint).items():
                    builder = builder.config(key, value)
                reference_spark = builder.enableHiveSupport().getOrCreate()
                reference_spark.conf.set("spark.sql.session.timeZone", "UTC")

            configure_spark_session(reference_spark)
            yield sail, reference_spark, s3_base
        finally:
            if reference_spark is not None:
                reference_spark.stop()
                _reset_classic_spark_state()
            sail.stop()


def _run_s3_path_case(
    creator: SparkSession,
    observer: SparkSession,
    s3_base: str,
    warehouse_case: WarehouseCase,
    path_case: PathCase,
    creator_name: str,
) -> dict[str, str]:
    database = f"s3p_{creator_name}_{warehouse_case.name}_{path_case.database_location}_{path_case.table_location}"
    table = f"tbl_{path_case.database_location}_{path_case.table_location}"
    table_fqn = f"{database}.{table}"

    database_location = _database_dir(s3_base, database, path_case.database_location)
    table_location = _table_dir(s3_base, database, table, path_case.table_location)

    _drop_database(creator, observer, database=database)
    try:
        _create_database(creator, database, database_location)
        _create_table(creator, table_fqn, table_location)

        creator_db_loc = _database_location(creator, database)
        observer_db_loc = _database_location(observer, database)
        assert creator_db_loc == observer_db_loc, (
            f"database location mismatch: creator={creator_db_loc} observer={observer_db_loc}"
        )

        creator_tbl_loc = _table_location(creator, table_fqn)
        observer_tbl_loc = _table_location(observer, table_fqn)
        assert creator_tbl_loc == observer_tbl_loc, (
            f"table location mismatch: creator={creator_tbl_loc} observer={observer_tbl_loc}"
        )

        return {
            "database": database,
            "table": table,
            "database_location": creator_db_loc,
            "table_location": creator_tbl_loc,
        }
    finally:
        _drop_database(observer, creator, database=database)


def _canonicalize_s3_path(location: str, database: str, table: str | None = None) -> str:
    prefix = f"s3://{_HMS_S3_BUCKET}/"
    assert location.startswith(prefix), f"expected s3:// prefix, got: {location}"
    path = location[len(prefix) :]
    path = path.replace(f"{database}.db", "{database}.db")
    path = path.replace(database, "{database}")
    if table is not None:
        path = path.replace(table, "{table}")
    parts = path.split("/")
    anchor = parts.index("s3_parity") if "s3_parity" in parts else 0
    return "/".join(["{s3_parity_root}", *parts[anchor + 2 :]])


def _assert_matching_s3_path_shape(left: dict[str, str], right: dict[str, str]) -> None:
    left_db = _canonicalize_s3_path(left["database_location"], left["database"])
    right_db = _canonicalize_s3_path(right["database_location"], right["database"])
    assert left_db == right_db, f"database path shape mismatch: {left_db} != {right_db}"

    left_tbl = _canonicalize_s3_path(left["table_location"], left["database"], left["table"])
    right_tbl = _canonicalize_s3_path(right["table_location"], right["database"], right["table"])
    assert left_tbl == right_tbl, f"table path shape mismatch: {left_tbl} != {right_tbl}"


@pytest.mark.parametrize("warehouse_case", WAREHOUSE_CASES, ids=lambda case: case.name)
def test_sail_and_reference_spark_store_matching_s3_path_matrix(
    warehouse_case: WarehouseCase,
    hms_s3_metastore_endpoint: str,
    hms_s3_env: dict[str, str],
    hms_warehouse_dir: Path,
    hms_s3_endpoint: str,
) -> None:
    with _s3_path_parity_sessions(
        hms_s3_metastore_endpoint,
        hms_s3_env,
        hms_warehouse_dir,
        hms_s3_endpoint,
        warehouse_case,
    ) as (sail, reference_spark, s3_base):
        for path_case in PATH_CASES:
            sail_paths = _run_s3_path_case(sail, reference_spark, s3_base, warehouse_case, path_case, "sail")
            spark_paths = _run_s3_path_case(reference_spark, sail, s3_base, warehouse_case, path_case, "spark")
            _assert_matching_s3_path_shape(sail_paths, spark_paths)
