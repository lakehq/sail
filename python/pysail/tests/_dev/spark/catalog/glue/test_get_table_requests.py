# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608
"""Reading a Glue table gets the table once per statement."""

from __future__ import annotations

import base64
import json
from collections import Counter
from contextlib import contextmanager
from typing import TYPE_CHECKING

import pytest
import requests

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator

    from pyspark.sql import SparkSession

DATABASE = "glue_get_table_requests_db"


@contextmanager
def _get_table_calls(moto_endpoint: str) -> Iterator[Counter[str]]:
    """Counts the Glue `GetTable` calls for each table name made inside the block."""
    recorder = f"{moto_endpoint}/moto-api/recorder"
    requests.post(f"{recorder}/reset-recording", timeout=30).raise_for_status()
    requests.post(f"{recorder}/start-recording", timeout=30).raise_for_status()
    calls = Counter()
    try:
        yield calls
    finally:
        requests.post(f"{recorder}/stop-recording", timeout=30).raise_for_status()
        response = requests.get(f"{recorder}/download-recording", timeout=30)
        response.raise_for_status()
        for line in response.text.splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            headers = {key.lower(): value for key, value in record["headers"].items()}
            if headers.get("x-amz-target") != "AWSGlue.GetTable":
                continue
            body = record["body"]
            if record.get("body_encoded"):
                body = base64.b64decode(body)
            calls[json.loads(body)["Name"]] += 1


@pytest.fixture(scope="module")
def tables(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> Generator[None, None, None]:
    path = tmp_path_factory.mktemp("glue_get_table_requests")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    try:
        for name in ("parquet", "iceberg"):
            spark.sql(f"CREATE TABLE {DATABASE}.t_{name} (id INT) USING {name} LOCATION '{(path / name).as_uri()}'")
            spark.sql(f"INSERT INTO {DATABASE}.t_{name} VALUES (1), (2)")
        spark.sql(f"CREATE VIEW {DATABASE}.v AS SELECT id FROM {DATABASE}.t_parquet")
        yield
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {DATABASE} CASCADE")


@pytest.mark.usefixtures("tables")
@pytest.mark.parametrize("table_format", ["parquet", "iceberg"])
def test_select_gets_table_once(spark, moto_endpoint, table_format):
    with _get_table_calls(moto_endpoint) as calls:
        spark.sql(f"SELECT * FROM {DATABASE}.t_{table_format}").collect()
    assert calls == {f"t_{table_format}": 1}


@pytest.mark.usefixtures("tables")
@pytest.mark.parametrize("table_format", ["parquet", "iceberg"])
def test_repeated_reference_gets_table_once(spark, moto_endpoint, table_format):
    name = f"{DATABASE}.t_{table_format}"
    with _get_table_calls(moto_endpoint) as calls:
        spark.sql(f"SELECT count(*) FROM {name} a JOIN {name} b ON a.id = b.id").collect()
    assert calls == {f"t_{table_format}": 1}


@pytest.mark.usefixtures("tables")
def test_view_gets_view_and_table(spark, moto_endpoint):
    # The view is looked up as a table first, and then as a view.
    with _get_table_calls(moto_endpoint) as calls:
        spark.sql(f"SELECT * FROM {DATABASE}.v").collect()
    assert calls == {"v": 2, "t_parquet": 1}


@pytest.mark.usefixtures("tables")
def test_missing_table_gets_table_and_view(spark, moto_endpoint):
    with _get_table_calls(moto_endpoint) as calls, pytest.raises(Exception, match="TABLE_OR_VIEW_NOT_FOUND"):
        spark.sql(f"SELECT * FROM {DATABASE}.missing").collect()
    # Each name is looked up as a table and as a view, and the database name is
    # looked up as the base table of a metadata table.
    assert calls == {"missing": 2, DATABASE: 2}


@pytest.mark.usefixtures("tables")
def test_insert_gets_target_table_once(spark, moto_endpoint):
    name = f"{DATABASE}.t_parquet"
    # The target and the source of the second statement are the same table under two names.
    for sql in [f"INSERT INTO {name} VALUES (3)", f"INSERT INTO {name} SELECT id + 10 FROM sail.{name}"]:
        with _get_table_calls(moto_endpoint) as calls:
            spark.sql(sql)
        assert calls == {"t_parquet": 1}, sql
