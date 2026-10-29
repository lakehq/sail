# ruff: noqa: S608, F811
"""Snapshot reference DDL publishes through the catalog and retries conflicts."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from pysail.testing.spark.session import spark_session_factory
from pysail.tests.spark.catalog.iceberg_rest.test_commit import (
    HTTP_CONFLICT,
    commit_gate_proxy,  # noqa: F401
    gated_remote,  # noqa: F401
)
from pysail.tests.spark.catalog.iceberg_rest.test_lakehouse_ddl import reference_catalog  # noqa: F401


@pytest.mark.parametrize("version", [1, 2, 3])
def test_reference_lifecycle(spark, reference_catalog, version):
    namespace = f"reference_ddl_v{version}"
    table = f"{namespace}.t"
    spark.sql(f"CREATE NAMESPACE {namespace}")
    try:
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg TBLPROPERTIES ('format-version'='{version}')")
        spark.sql(f"ALTER TABLE {table} CREATE BRANCH empty_branch")
        empty = reference_catalog.load_table(table)
        empty_id = empty.refs()["empty_branch"].snapshot_id
        assert empty.current_snapshot() is None
        assert empty.scan(snapshot_id=empty_id).to_arrow().num_rows == 0
        spark.sql(f"INSERT INTO {table} VALUES (1)")
        first = reference_catalog.load_table(table).current_snapshot().snapshot_id
        spark.sql(f"ALTER TABLE {table} CREATE TAG release RETAIN 2 DAYS")
        spark.sql(f"ALTER TABLE {table} REPLACE BRANCH empty_branch AS OF VERSION {first}")
        reference = reference_catalog.load_table(table)
        assert reference.refs()["empty_branch"].snapshot_id == first
        assert reference.refs()["release"].max_ref_age_ms == 172800000  # noqa: PLR2004
        before = reference.metadata_location
        with pytest.raises(Exception, match="different type"):
            spark.sql(f"ALTER TABLE {table} REPLACE TAG empty_branch")
        assert reference_catalog.load_table(table).metadata_location == before
        with reference.manage_snapshots() as update:
            update.create_branch(first, "external")
        spark.sql(f"ALTER TABLE {table} REPLACE BRANCH external WITH SNAPSHOT RETENTION 3 SNAPSHOTS")
        assert reference_catalog.load_table(table).refs()["external"].min_snapshots_to_keep == 3  # noqa: PLR2004
        spark.sql(f"CALL system.set_current_snapshot(table => '{table}', ref => 'release')").collect()
        spark.sql(f"ALTER TABLE {table} DROP TAG release")
        spark.sql(f"ALTER TABLE {table} DROP BRANCH empty_branch")
        reference = reference_catalog.load_table(table)
        assert set(reference.refs()) == {"main", "external"}
        assert reference.scan().to_arrow().to_pylist() == [{"id": 1}]
    finally:
        spark.sql(f"DROP NAMESPACE {namespace} CASCADE")


@pytest.mark.parametrize("concurrent_operation", ["append", "create_branch"])
def test_empty_branch_conflict_discards_uncommitted_manifest(
    spark, reference_catalog, gated_remote, commit_gate_proxy, concurrent_operation
):
    namespace = "reference_conflict"
    table = f"{namespace}.t"
    spark.sql(f"CREATE NAMESPACE {namespace}")
    spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg")
    _, gate = commit_gate_proxy
    try:
        with spark_session_factory(gated_remote) as sessions:
            slow = sessions.create()
            gate.arm(namespace, "t")
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(slow.sql, f"ALTER TABLE {table} CREATE BRANCH IF NOT EXISTS audit")
                try:
                    if not gate.arrived.wait(timeout=60):
                        outcome = future.exception(timeout=1) if future.done() else "still running"
                        pytest.fail(f"Reference DDL did not reach the commit gate: {outcome}")
                    if concurrent_operation == "append":
                        spark.sql(f"INSERT INTO {table} VALUES (1)")
                    else:
                        spark.sql(f"ALTER TABLE {table} CREATE BRANCH audit")
                finally:
                    gate.release.set()
                future.result(timeout=60)
        assert gate.response_status == HTTP_CONFLICT
        reference = reference_catalog.load_table(table)
        assert len(reference.metadata.snapshots) == 1
        assert reference.refs()["audit"].snapshot_id == reference.metadata.snapshots[0].snapshot_id
        abandoned = next(
            update["snapshot"]["manifest-list"]
            for update in gate.request["updates"]
            if update["action"] == "add-snapshot"
        )
        assert not reference.io.new_input(abandoned).exists()
        assert spark.sql(f"SELECT * FROM {table} VERSION AS OF 'audit'").count() == (concurrent_operation == "append")
    finally:
        spark.sql(f"DROP NAMESPACE {namespace} CASCADE")
