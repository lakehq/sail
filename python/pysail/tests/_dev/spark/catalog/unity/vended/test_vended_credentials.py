# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608
"""Managed Delta tables on storage that Sail reaches only with the credentials that Unity Catalog issues."""

from __future__ import annotations

import time
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
import requests

from pysail.testing._dev.containers.unity_defaults import DEFAULT_CATALOG

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from pysail.tests._dev.spark.catalog.unity.vended.conftest import VendedStorage

pytestmark = pytest.mark.timeout(600)

READ = frozenset({"s3:GetObject", "s3:ListBucket"})


def _table() -> str:
    return f"default.vended_{uuid.uuid4().hex[:8]}"


def _prefix(unity_url: str, table: str) -> str:
    response = requests.get(f"{unity_url}/tables/{DEFAULT_CATALOG}.{table}", timeout=30)
    response.raise_for_status()
    # s3://<bucket>/<prefix>
    return response.json()["storage_location"].split("/", 3)[3].rstrip("/") + "/"


def _usage(storage: VendedStorage, prefix: str, since: tuple[int, int] = (0, 0)):
    """Returns the credentials issued for the table and the storage requests for its objects, in order."""
    issued, sent = storage.settled()
    issued = [x for x in issued[since[0] :] if any(prefix in r for r in x[2])]
    return issued, [x for x in sent[since[1] :] if x[1].startswith(prefix)]


def _mark(storage: VendedStorage) -> tuple[int, int]:
    issued, sent = storage.settled()
    return len(issued), len(sent)


def test_tables_are_read_and_written_with_vended_credentials(
    spark: SparkSession, new_session: SparkSession, vended_storage: VendedStorage, vended_unity_url: str
):
    table, copy = _table(), _table()
    # The environment has no credentials for the storage, so an object that exists cannot be read.
    control = vended_storage.put("control/data.json", b'{"id": 1}\n')
    with pytest.raises(Exception):  # noqa: B017, PT011
        spark.read.json(control).collect()

    spark.sql(f"CREATE TABLE {table} (id INT, name STRING) USING DELTA")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
    assert spark.sql(f"SELECT * FROM {table} ORDER BY id").collect() == [(1, "a"), (2, "b")]
    spark.sql(f"CREATE TABLE {copy} USING DELTA AS SELECT * FROM {table}")
    spark.sql(f"INSERT OVERWRITE {table} VALUES (3, 'c')")
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('k' = 'v')")
    spark.sql(f"INSERT INTO {table} VALUES (4, 'd')")
    assert spark.sql(f"SELECT * FROM {table} ORDER BY id").collect() == [(3, "c"), (4, "d")]
    assert spark.sql(f"SELECT * FROM {copy} ORDER BY id").collect() == [(1, "a"), (2, "b")]

    prefix = _prefix(vended_unity_url, table)
    issued, sent = _usage(vended_storage, prefix)
    keys = {key: actions for key, actions, _ in issued}
    for operation, path, status, key in sent:
        # Each request for the table used a credential that was issued for the table only.
        assert key in keys, (operation, path, status)
        assert status != requests.codes.forbidden, (operation, path)
    assert {x[0] for x in sent} >= {"PutObject", "GetObject"}

    # Another session gets one credential for reading, and one for writing when it writes.
    mark = _mark(vended_storage)
    assert new_session.sql(f"SELECT count(*) FROM {table}").collect() == [(2,)]
    assert new_session.sql(f"SELECT max(id) FROM {table}").collect() == [(4,)]
    issued, sent = _usage(vended_storage, prefix, mark)
    assert [actions for _, actions, _ in issued] == [READ]
    assert {x[3] for x in sent} == {issued[0][0]}
    mark = _mark(vended_storage)
    new_session.sql(f"INSERT INTO {table} VALUES (5, 'e')")
    new_session.sql(f"INSERT INTO {table} VALUES (6, 'f')")
    issued, sent = _usage(vended_storage, prefix, mark)
    assert len(issued) == 1
    assert "s3:PutObject" in issued[0][1]
    assert {x[3] for x in sent if x[0] == "PutObject"} == {issued[0][0]}
    assert spark.sql(f"SELECT count(*) FROM {table}").collect() == [(4,)]


def test_table_is_created_with_the_credential_of_the_staging_table(
    spark: SparkSession, vended_storage: VendedStorage, vended_unity_url: str
):
    # The catalog issues a credential for writing when it stages the table. The table is
    # created with that credential, without asking the catalog for another one.
    for statement in ["CREATE TABLE {} (id INT) USING DELTA", "CREATE TABLE {} USING DELTA AS SELECT 1 AS id"]:
        table = _table()
        mark = _mark(vended_storage)
        spark.sql(statement.format(table))
        issued, sent = _usage(vended_storage, _prefix(vended_unity_url, table), mark)
        assert len(issued) == 1, statement
        assert "s3:PutObject" in issued[0][1]
        assert {x[3] for x in sent} == {issued[0][0]}


def test_vended_credentials_are_replaced_before_they_expire(
    new_session: SparkSession, spark: SparkSession, vended_storage: VendedStorage, vended_unity_url: str
):
    table = _table()
    spark.sql(f"CREATE TABLE {table} USING DELTA AS SELECT 1 AS id")
    prefix = _prefix(vended_unity_url, table)
    # Sail replaces a credential that expires within 5 minutes, at most once in 30 seconds.
    vended_storage.reported_lifetime = timedelta(seconds=310)
    try:
        mark = _mark(vended_storage)
        assert new_session.sql(f"SELECT * FROM {table}").collect() == [(1,)]
        first, _ = _usage(vended_storage, prefix, mark)
        assert len(first) == 1
        time.sleep(31)
        deadline = time.monotonic() + 30
        while True:
            mark = _mark(vended_storage)
            assert new_session.sql(f"SELECT * FROM {table}").collect() == [(1,)]
            _, sent = _usage(vended_storage, prefix, mark)
            keys = {x[3] for x in sent}
            # The session keeps the credential that it has until the new one arrives.
            if first[0][0] not in keys or time.monotonic() > deadline:
                break
            time.sleep(1)
        assert keys
        assert first[0][0] not in keys
        assert keys <= {key for key, _, _ in _usage(vended_storage, prefix)[0]}
    finally:
        vended_storage.reported_lifetime = timedelta(seconds=900)


def test_write_fails_when_the_vended_credential_allows_reading_only(
    new_session: SparkSession, spark: SparkSession, vended_storage: VendedStorage
):
    table = _table()
    spark.sql(f"CREATE TABLE {table} USING DELTA AS SELECT 1 AS id")
    vended_storage.deny_writes = True
    try:
        with pytest.raises(Exception, match=r"privileges|AccessDenied|Forbidden"):
            new_session.sql(f"INSERT INTO {table} VALUES (2)")
    finally:
        vended_storage.deny_writes = False
    assert spark.sql(f"SELECT * FROM {table}").collect() == [(1,)]
