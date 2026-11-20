# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608
"""CREATE TABLE AS SELECT and `saveAsTable` that create Unity Catalog managed Delta tables."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from urllib.parse import quote

import pytest
import requests

from pysail.tests._dev.spark.catalog.unity.conftest import (
    _create_unity_managed_delta_table_with_delta_api,
    _delta_commit_actions,
    _location_to_path,
    _qualified_table_name,
    _table_location,
    _unique_database,
    _unity_column,
    _unity_table_info,
)

if TYPE_CHECKING:
    from pathlib import Path

    from pyspark.sql import DataFrame, SparkSession

FAILING_QUERY = "SELECT id, CASE WHEN id = 3 THEN raise_error('ctas failure') ELSE 'x' END AS name FROM range(5)"


@pytest.fixture
def database(spark: SparkSession):
    name = _unique_database("unity_ctas")
    spark.sql(f"CREATE SCHEMA {name}")
    try:
        yield name
    finally:
        spark.sql(f"DROP SCHEMA {name} CASCADE")


@pytest.fixture(autouse=True)
def _require_delta_api(unity_uses_delta_api: bool) -> None:  # noqa: FBT001
    if not unity_uses_delta_api:
        pytest.skip("only a server with the Delta REST API registers a managed table after its data is written")


def _delta_api_table(unity_rest_url: str, table: str) -> dict:
    catalog_name, schema_name, name = _qualified_table_name(table).split(".", 2)
    response = requests.get(
        f"{unity_rest_url}/delta/v1/catalogs/{quote(catalog_name, safe='')}"
        f"/schemas/{quote(schema_name, safe='')}/tables/{quote(name, safe='')}",
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def _published_versions(location: Path) -> list[int]:
    paths = (location / "_delta_log").glob("[0-9]*.json")
    return sorted(int(path.stem) for path in paths if path.stem.isdigit())


def _actions(actions: list[dict], kind: str) -> list[dict]:
    return [action[kind] for action in actions if kind in action]


def _assert_created_with_data(spark: SparkSession, unity_rest_url: str, table: str, rows: int) -> list[dict]:
    """Asserts that version 0 of the table has the data, and that the catalog registered the
    table at version 0 without any commit made through the catalog."""
    location = _location_to_path(_table_location(spark, table))
    assert _published_versions(location) == [0]
    assert not list((location / "_delta_log" / "_staged_commits").glob("*.json"))
    actions = _delta_commit_actions(location, 0)
    (protocol,) = _actions(actions, "protocol")
    assert "catalogManaged" in protocol["writerFeatures"]
    (metadata,) = _actions(actions, "metaData")
    adds = _actions(actions, "add")
    assert bool(adds) == (rows > 0)
    assert sum(json.loads(add["stats"])["numRecords"] for add in adds) == rows
    (commit_info,) = _actions(actions, "commitInfo")
    assert isinstance(commit_info["inCommitTimestamp"], int)

    info = _unity_table_info(unity_rest_url, table)
    assert info["table_type"] == "MANAGED"
    assert info["table_id"] == metadata["configuration"]["io.unitycatalog.tableId"]
    delta_table = _delta_api_table(unity_rest_url, table)
    assert delta_table["latest-table-version"] == 0, delta_table
    assert not delta_table.get("commits"), delta_table
    return actions


def _assert_not_registered(spark: SparkSession, unity_rest_url: str, table: str) -> None:
    assert not spark.catalog.tableExists(table)
    full_name = _qualified_table_name(table)
    response = requests.get(f"{unity_rest_url}/tables/{quote(full_name, safe='')}", timeout=10)
    assert response.status_code == 404, response.text  # noqa: PLR2004


def test_ctas_creates_managed_table_with_data_in_version_zero(
    spark: SparkSession, unity_rest_url: str, database: str
) -> None:
    table = f"{database}.t"
    spark.sql(
        f"CREATE TABLE {table} USING delta COMMENT 'events' AS SELECT * FROM VALUES (1, 'one'), (2, 'two') AS t(id, name)"
    )
    assert [tuple(row) for row in spark.sql(f"SELECT id, name FROM {table} ORDER BY id").collect()] == [
        (1, "one"),
        (2, "two"),
    ]
    actions = _assert_created_with_data(spark, unity_rest_url, table, 2)
    (metadata,) = _actions(actions, "metaData")
    assert metadata["description"] == "events"
    assert _unity_table_info(unity_rest_url, table)["comment"] == "events"

    # The table takes commits through the catalog after it is created.
    spark.sql(f"INSERT INTO {table} VALUES (3, 'three')")
    assert _delta_api_table(unity_rest_url, table)["latest-table-version"] == 1
    assert spark.sql(f"SELECT count(*) FROM {table}").collect()[0][0] == 3  # noqa: PLR2004


def test_ctas_with_row_tracking_registers_the_row_id_high_water_mark(
    spark: SparkSession, unity_rest_url: str, database: str
) -> None:
    table = f"{database}.t"
    spark.sql(
        f"CREATE TABLE {table} USING delta TBLPROPERTIES ('delta.enableRowTracking' = 'true') "
        "AS SELECT * FROM VALUES (1), (2), (3) AS t(id)"
    )
    actions = _assert_created_with_data(spark, unity_rest_url, table, 3)
    domains = {domain["domain"]: domain for domain in _actions(actions, "domainMetadata")}
    high_water_mark = json.loads(domains["delta.rowTracking"]["configuration"])["rowIdHighWaterMark"]
    assert high_water_mark == 2  # noqa: PLR2004
    assert sorted(add["baseRowId"] for add in _actions(actions, "add"))[0] == 0
    properties = _unity_table_info(unity_rest_url, table)["properties"]
    assert properties["delta.rowTracking.rowIdHighWaterMark"] == str(high_water_mark)


def test_ctas_without_rows_creates_the_table(spark: SparkSession, unity_rest_url: str, database: str) -> None:
    table = f"{database}.t"
    spark.sql(f"CREATE TABLE {table} USING delta AS SELECT id, 'x' AS name FROM range(0)")
    assert spark.table(table).collect() == []
    assert spark.table(table).columns == ["id", "name"]
    _assert_created_with_data(spark, unity_rest_url, table, 0)


def test_failed_ctas_does_not_register_the_table(spark: SparkSession, unity_rest_url: str, database: str) -> None:
    table = f"{database}.t"
    with pytest.raises(Exception, match="ctas failure"):
        spark.sql(f"CREATE TABLE {table} USING delta AS {FAILING_QUERY}")
    _assert_not_registered(spark, unity_rest_url, table)

    # The statement can be run again, since no table is left behind.
    spark.sql(f"CREATE TABLE {table} USING delta AS SELECT * FROM VALUES (1, 'one') AS t(id, name)")
    assert [tuple(row) for row in spark.sql(f"SELECT id, name FROM {table}").collect()] == [(1, "one")]
    _assert_created_with_data(spark, unity_rest_url, table, 1)


@pytest.mark.parametrize("mode", ["error", "append", "overwrite"])
def test_save_as_table_creates_managed_table_with_data_in_version_zero(
    spark: SparkSession, unity_rest_url: str, database: str, mode: str
) -> None:
    table = f"{database}.t"
    frame = spark.createDataFrame([(1, "one"), (2, "two")], schema="id INT, name STRING")
    frame.write.format("delta").mode(mode).saveAsTable(table)
    assert [tuple(row) for row in spark.table(table).orderBy("id").collect()] == [(1, "one"), (2, "two")]
    _assert_created_with_data(spark, unity_rest_url, table, 2)


def test_failed_save_as_table_does_not_register_the_table(
    spark: SparkSession, unity_rest_url: str, database: str
) -> None:
    table = f"{database}.t"
    frame: DataFrame = spark.sql(FAILING_QUERY)
    with pytest.raises(Exception, match="ctas failure"):
        frame.write.format("delta").saveAsTable(table)
    _assert_not_registered(spark, unity_rest_url, table)

    spark.createDataFrame([(1, "one")], schema="id INT, name STRING").write.format("delta").saveAsTable(table)
    _assert_created_with_data(spark, unity_rest_url, table, 1)


def _register_create_table_during_query(spark: SparkSession, unity_rest_url: str, table: str) -> str:
    """Registers a function that creates the table in the catalog while a query that
    calls it writes the data of the statement, and returns the name of the function."""
    full_name = quote(_qualified_table_name(table), safe="")

    def create_table(value: int) -> int:
        response = requests.get(f"{unity_rest_url}/tables/{full_name}", timeout=10)
        if response.status_code == 404:  # noqa: PLR2004
            _create_unity_managed_delta_table_with_delta_api(table, unity_rest_url, [_unity_column("id", "LONG", 0)])
        return value

    spark.udf.register("create_table_during_query", create_table, "bigint")
    return "create_table_during_query"


def _assert_concurrently_created(spark: SparkSession, unity_rest_url: str, table: str) -> None:
    """Asserts that the table is the one created during the query, without the data of the statement."""
    assert spark.table(table).collect() == []
    location = _location_to_path(_table_location(spark, table))
    assert _published_versions(location) == [0]
    assert _unity_table_info(unity_rest_url, table)["table_type"] == "MANAGED"


def test_ctas_if_not_exists_does_nothing_when_the_table_is_created_during_the_write(
    spark: SparkSession, unity_rest_url: str, database: str
) -> None:
    table = f"{database}.t"
    function = _register_create_table_during_query(spark, unity_rest_url, table)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} USING delta AS SELECT {function}(id) AS id FROM range(3)")
    _assert_concurrently_created(spark, unity_rest_url, table)


def test_ctas_fails_when_the_table_is_created_during_the_write(
    spark: SparkSession, unity_rest_url: str, database: str
) -> None:
    table = f"{database}.t"
    function = _register_create_table_during_query(spark, unity_rest_url, table)
    # The data is written before the catalog refuses to register the table.
    with pytest.raises(Exception, match="(?i)already exists"):
        spark.sql(f"CREATE TABLE {table} USING delta AS SELECT {function}(id) AS id FROM range(3)")
    _assert_concurrently_created(spark, unity_rest_url, table)


@pytest.mark.parametrize(("mode", "error"), [("ignore", None), ("error", "(?i)already exists")])
def test_save_as_table_when_the_table_is_created_during_the_write(
    spark: SparkSession, unity_rest_url: str, database: str, mode: str, error: str | None
) -> None:
    table = f"{database}.t"
    function = _register_create_table_during_query(spark, unity_rest_url, table)
    writer = spark.range(3).selectExpr(f"{function}(id) AS id").write.format("delta").mode(mode)
    if error is None:
        writer.saveAsTable(table)
    else:
        with pytest.raises(Exception, match=error):
            writer.saveAsTable(table)
    _assert_concurrently_created(spark, unity_rest_url, table)
