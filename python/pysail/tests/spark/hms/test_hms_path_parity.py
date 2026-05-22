# ruff: noqa: S608
"""HMS path qualification parity tests between Sail and reference Spark."""

from __future__ import annotations

import contextlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlparse

import pytest
from pyspark.sql import SparkSession

from pysail.tests.spark.conftest import (
    configure_spark_session,
    patch_spark_connect_session,
)
from pysail.tests.spark.hms.conftest import (
    _classic_spark_mode,
    _describe_extended_properties,
    _run_sail_hms_server,
)

pytestmark = pytest.mark.catalog_integration

if TYPE_CHECKING:
    from collections.abc import Generator


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


@dataclass(frozen=True)
class CreatedPathCase:
    database: str
    table: str
    database_location: Path
    table_location: Path


WAREHOUSE_CASES = (
    WarehouseCase("default"),
    WarehouseCase("absolute"),
    WarehouseCase("relative"),
)

PATH_CASES = tuple(
    PathCase(database_location=database_location, table_location=table_location)
    for database_location in ("none", "absolute", "relative")
    for table_location in ("none", "absolute", "relative")
)


@contextlib.contextmanager
def _working_directory(path: Path) -> Generator[None, None, None]:
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def _normalized_path_from_location(location: str) -> Path:
    assert location, "expected a non-empty location"
    assert "~" not in location, f"location should not contain '~': {location}"

    parsed = urlparse(location)
    assert parsed.scheme in {"", "file"}, f"expected a qualified local path, got: {location}"

    raw_path = unquote(parsed.path) if parsed.scheme == "file" else location
    assert ".." not in Path(raw_path).parts, f"location should not contain '..': {location}"
    path = Path(raw_path).resolve(strict=False)
    assert path.is_absolute(), f"location should be absolute: {location}"
    return path


def _assert_path_invariants(path: Path, expected_suffix: tuple[str, ...]) -> None:
    assert path.is_absolute(), f"path should be absolute: {path}"
    assert "~" not in path.as_posix(), f"path should not contain '~': {path}"
    assert ".." not in path.parts, f"path should not contain '..': {path}"
    assert path.parts[-len(expected_suffix) :] == expected_suffix, f"path should end with {expected_suffix}, got {path}"


def _canonicalize_path_parts(path: Path, *, database: str, table: str | None = None) -> tuple[str, ...]:
    replacements = {
        f"{database}.db": "{database}.db",
        database: "{database}",
    }
    if table is not None:
        replacements[table] = "{table}"
    parts = tuple(replacements.get(part, part) for part in path.parts)
    if "path_parity" in parts:
        anchor = parts.index("path_parity")
        if anchor + 1 < len(parts):
            return ("{path_parity_root}", *parts[anchor + 2 :])
    return parts


def _assert_matching_path_shape(left: CreatedPathCase, right: CreatedPathCase) -> None:
    assert _canonicalize_path_parts(left.database_location, database=left.database) == _canonicalize_path_parts(
        right.database_location,
        database=right.database,
    )
    assert _canonicalize_path_parts(
        left.table_location,
        database=left.database,
        table=left.table,
    ) == _canonicalize_path_parts(
        right.table_location,
        database=right.database,
        table=right.table,
    )


def _describe_database_properties(spark: SparkSession, database: str) -> dict[str, str]:
    rows = spark.sql(f"DESCRIBE DATABASE EXTENDED {database}").collect()
    props: dict[str, str] = {}
    for row in rows:
        key = (row[0] or "").strip()
        value = (row[1] or "").strip()
        if key:
            props[key] = value
    return props


def _database_location_path(spark: SparkSession, database: str) -> Path:
    location = _describe_database_properties(spark, database).get("Location", "")
    return _normalized_path_from_location(location)


def _table_location_path(spark: SparkSession, table_fqn: str) -> Path:
    location = _describe_extended_properties(spark, table_fqn).get("Location", "")
    return _normalized_path_from_location(location)


def _warehouse_config_value(shared_root: Path, warehouse_case: WarehouseCase) -> str | None:
    if warehouse_case.name == "default":
        return None
    if warehouse_case.name == "absolute":
        return str((shared_root / "warehouse_roots" / "absolute").resolve(strict=False))
    if warehouse_case.name == "relative":
        return "warehouse_roots/relative"
    msg = f"unknown warehouse case: {warehouse_case.name}"
    raise AssertionError(msg)


def _expected_warehouse_root(shared_root: Path, warehouse_case: WarehouseCase) -> Path:
    if warehouse_case.name == "default":
        return (shared_root / "spark-warehouse").resolve(strict=False)
    if warehouse_case.name == "absolute":
        return (shared_root / "warehouse_roots" / "absolute").resolve(strict=False)
    if warehouse_case.name == "relative":
        return (shared_root / "warehouse_roots" / "relative").resolve(strict=False)
    msg = f"unknown warehouse case: {warehouse_case.name}"
    raise AssertionError(msg)


def _assert_warehouse_invariants(warehouse_root: Path, warehouse_case: WarehouseCase) -> None:
    if warehouse_case.name == "default":
        _assert_path_invariants(warehouse_root, ("spark-warehouse",))
    else:
        _assert_path_invariants(warehouse_root, ("warehouse_roots", warehouse_case.name))


def _database_location_value(shared_root: Path, database: str, location_kind: str) -> str | None:
    if location_kind == "none":
        return None
    if location_kind == "absolute":
        return str((shared_root / "database_roots" / "absolute" / database).resolve(strict=False))
    if location_kind == "relative":
        return f"database_roots/relative/{database}"
    msg = f"unknown database location kind: {location_kind}"
    raise AssertionError(msg)


def _expected_database_root(
    shared_root: Path,
    warehouse_root: Path,
    database: str,
    location_kind: str,
) -> Path:
    if location_kind == "none":
        return (warehouse_root / f"{database}.db").resolve(strict=False)
    if location_kind == "absolute":
        return (shared_root / "database_roots" / "absolute" / database).resolve(strict=False)
    if location_kind == "relative":
        return (warehouse_root / "database_roots" / "relative" / database).resolve(strict=False)
    msg = f"unknown database location kind: {location_kind}"
    raise AssertionError(msg)


def _table_location_value(shared_root: Path, database: str, table: str, location_kind: str) -> str | None:
    if location_kind == "none":
        return None
    if location_kind == "absolute":
        return str((shared_root / "table_roots" / "absolute" / database / table).resolve(strict=False))
    if location_kind == "relative":
        return f"table_roots/relative/{table}"
    msg = f"unknown table location kind: {location_kind}"
    raise AssertionError(msg)


def _expected_table_root(
    shared_root: Path,
    database_root: Path,
    database: str,
    table: str,
    location_kind: str,
) -> Path:
    if location_kind == "none":
        return (database_root / table).resolve(strict=False)
    if location_kind == "absolute":
        return (shared_root / "table_roots" / "absolute" / database / table).resolve(strict=False)
    if location_kind == "relative":
        return (database_root / "table_roots" / "relative" / table).resolve(strict=False)
    msg = f"unknown table location kind: {location_kind}"
    raise AssertionError(msg)


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


def _drop_database(*sessions: SparkSession, database: str) -> None:
    for session in sessions:
        with contextlib.suppress(Exception):
            session.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")


@contextlib.contextmanager
def _path_parity_sessions(
    hms_s3_metastore_endpoint: str,
    hms_s3_env: dict[str, str],
    hms_warehouse_dir: Path,
    warehouse_case: WarehouseCase,
) -> Generator[tuple[SparkSession, SparkSession, Path], None, None]:
    shared_root = (hms_warehouse_dir / "path_parity" / warehouse_case.name).resolve(strict=False)
    shared_root.mkdir(parents=True, exist_ok=True)
    warehouse_value = _warehouse_config_value(shared_root, warehouse_case)

    with (
        _working_directory(shared_root),
        contextlib.contextmanager(_run_sail_hms_server)(hms_s3_metastore_endpoint, hms_s3_env) as remote,
    ):
        sail_builder = SparkSession.builder.remote(remote).appName(f"hms_path_parity_sail_{warehouse_case.name}")
        if warehouse_value is not None:
            sail_builder = sail_builder.config("spark.sql.warehouse.dir", warehouse_value)
        sail = sail_builder.create()
        configure_spark_session(sail)
        patch_spark_connect_session(sail)

        reference_spark = None
        try:
            with _classic_spark_mode():
                builder = (
                    SparkSession.builder.master("local[1]")
                    .appName(f"hms_path_parity_reference_{warehouse_case.name}")
                    .config("spark.sql.catalogImplementation", "hive")
                    .config(
                        "spark.hadoop.hive.metastore.uris",
                        f"thrift://{hms_s3_metastore_endpoint}",
                    )
                    .config(
                        "spark.hadoop.javax.jdo.option.ConnectionURL",
                        f"jdbc:derby:;databaseName={shared_root}/metastore_{warehouse_case.name};create=true",
                    )
                )
                if warehouse_value is not None:
                    builder = builder.config("spark.sql.warehouse.dir", warehouse_value)
                reference_spark = builder.enableHiveSupport().getOrCreate()

            configure_spark_session(reference_spark)
            yield sail, reference_spark, shared_root
        finally:
            if reference_spark is not None:
                reference_spark.stop()
            sail.stop()


def _run_path_case(
    creator: SparkSession,
    observer: SparkSession,
    shared_root: Path,
    warehouse_case: WarehouseCase,
    path_case: PathCase,
    creator_name: str,
) -> CreatedPathCase:
    database = f"hms_path_{creator_name}_{warehouse_case.name}_{path_case.database_location}_{path_case.table_location}"
    table = f"tbl_{path_case.database_location}_{path_case.table_location}"
    table_fqn = f"{database}.{table}"
    payload = f"{creator_name}:{warehouse_case.name}:{path_case.case_id}"

    database_location = _database_location_value(shared_root, database, path_case.database_location)
    table_location = _table_location_value(shared_root, database, table, path_case.table_location)

    _drop_database(creator, observer, database=database)
    try:
        _create_database(creator, database, database_location)
        _create_table(creator, table_fqn, table_location)
        creator.sql(f"INSERT INTO {table_fqn} VALUES (1, '{payload}')")

        database_location_creator = _database_location_path(creator, database)
        database_location_observer = _database_location_path(observer, database)
        assert database_location_creator == database_location_observer

        if path_case.database_location == "absolute":
            assert database_location is not None
            expected_database_root = Path(database_location).resolve(strict=False)
            assert database_location_creator == expected_database_root
            _assert_path_invariants(
                database_location_creator,
                ("database_roots", "absolute", database),
            )
        elif path_case.database_location == "relative":
            _assert_path_invariants(
                database_location_creator,
                ("database_roots", "relative", database),
            )
        else:
            _assert_path_invariants(database_location_creator, (f"{database}.db",))

        table_location_creator = _table_location_path(creator, table_fqn)
        table_location_observer = _table_location_path(observer, table_fqn)
        assert table_location_creator == table_location_observer

        if path_case.table_location == "absolute":
            assert table_location is not None
            expected_table_root = Path(table_location).resolve(strict=False)
            assert table_location_creator == expected_table_root
            _assert_path_invariants(
                table_location_creator,
                ("table_roots", "absolute", database, table),
            )
        elif path_case.table_location == "relative":
            assert table_location_creator.is_relative_to(database_location_creator)
            _assert_path_invariants(table_location_creator, ("table_roots", "relative", table))
        else:
            assert table_location_creator.parent == database_location_creator
            _assert_path_invariants(table_location_creator, (table,))

        creator_rows = creator.sql(f"SELECT id, note FROM {table_fqn} ORDER BY id").collect()
        assert [(row.id, row.note) for row in creator_rows] == [(1, payload)]
        return CreatedPathCase(
            database=database,
            table=table,
            database_location=database_location_creator,
            table_location=table_location_creator,
        )
    finally:
        _drop_database(observer, creator, database=database)


@pytest.mark.parametrize("warehouse_case", WAREHOUSE_CASES, ids=lambda case: case.name)
def test_sail_and_reference_spark_store_matching_path_matrix(
    warehouse_case: WarehouseCase,
    hms_s3_metastore_endpoint: str,
    hms_s3_env: dict[str, str],
    hms_warehouse_dir: Path,
) -> None:
    with _path_parity_sessions(
        hms_s3_metastore_endpoint,
        hms_s3_env,
        hms_warehouse_dir,
        warehouse_case,
    ) as (sail, reference_spark, shared_root):
        for path_case in PATH_CASES:
            sail_paths = _run_path_case(sail, reference_spark, shared_root, warehouse_case, path_case, "sail")
            spark_paths = _run_path_case(reference_spark, sail, shared_root, warehouse_case, path_case, "spark")
            _assert_matching_path_shape(sail_paths, spark_paths)


def test_ctas_table_location_matches_database(
    hms_s3_metastore_endpoint: str,
    hms_s3_env: dict[str, str],
    hms_warehouse_dir: Path,
) -> None:
    """CTAS tables should be placed under the database location, matching Spark."""
    warehouse_case = WarehouseCase("default")
    with _path_parity_sessions(
        hms_s3_metastore_endpoint,
        hms_s3_env,
        hms_warehouse_dir,
        warehouse_case,
    ) as (sail, reference_spark, shared_root):
        for creator, observer, creator_name in [
            (sail, reference_spark, "sail"),
            (reference_spark, sail, "spark"),
        ]:
            database = f"hms_ctas_{creator_name}"
            table = "ctas_tbl"
            table_fqn = f"{database}.{table}"
            _drop_database(creator, observer, database=database)
            try:
                _create_database(creator, database, None)
                creator.sql(f"CREATE TABLE {table_fqn} USING PARQUET AS SELECT 1 AS id, 'hello' AS note")

                db_loc = _database_location_path(creator, database)
                tbl_loc = _table_location_path(creator, table_fqn)
                assert tbl_loc.is_relative_to(db_loc), (
                    f"CTAS table {tbl_loc} should be under database {db_loc}"
                )
                assert tbl_loc.name == table or tbl_loc.parts[-1] == table, (
                    f"CTAS table path should end with {table}, got {tbl_loc}"
                )
            finally:
                _drop_database(observer, creator, database=database)


def test_partitioned_table_location_matches_database(
    hms_s3_metastore_endpoint: str,
    hms_s3_env: dict[str, str],
    hms_warehouse_dir: Path,
) -> None:
    """Partitioned tables should be placed under the database location, matching Spark."""
    warehouse_case = WarehouseCase("default")
    with _path_parity_sessions(
        hms_s3_metastore_endpoint,
        hms_s3_env,
        hms_warehouse_dir,
        warehouse_case,
    ) as (sail, reference_spark, shared_root):
        for creator, observer, creator_name in [
            (sail, reference_spark, "sail"),
            (reference_spark, sail, "spark"),
        ]:
            database = f"hms_part_{creator_name}"
            table = "part_tbl"
            table_fqn = f"{database}.{table}"
            _drop_database(creator, observer, database=database)
            try:
                _create_database(creator, database, None)
                creator.sql(
                    f"CREATE TABLE {table_fqn} (id INT, note STRING) "
                    f"USING PARQUET PARTITIONED BY (note)"
                )

                db_loc = _database_location_path(creator, database)
                tbl_loc = _table_location_path(creator, table_fqn)
                assert tbl_loc.is_relative_to(db_loc), (
                    f"Partitioned table {tbl_loc} should be under database {db_loc}"
                )

                creator.sql(f"INSERT INTO {table_fqn} VALUES (1, 'a')")
                creator.sql(f"INSERT INTO {table_fqn} VALUES (2, 'b')")

                rows = creator.sql(f"SELECT id, note FROM {table_fqn} ORDER BY id").collect()
                assert [(row.id, row.note) for row in rows] == [(1, "a"), (2, "b")]
            finally:
                _drop_database(observer, creator, database=database)
