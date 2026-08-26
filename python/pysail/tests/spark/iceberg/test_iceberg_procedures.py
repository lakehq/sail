import json
from datetime import datetime, timedelta, timezone

import pytest

from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.sql import escape_sql_string_literal


def _latest_metadata_file(table_path):
    files = sorted((table_path / "metadata").glob("*.metadata.json"))
    assert files
    return files[-1]


def _edit_latest_metadata(table_path, edit):
    metadata_file = _latest_metadata_file(table_path)
    metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
    edit(metadata)
    metadata_file.write_text(json.dumps(metadata, separators=(",", ":")), encoding="utf-8")


def test_procedure_catalog_and_name_are_resolved_before_arguments(spark):
    with pytest.raises(Exception, match=r"Procedure not found: sail\.system\.not_a_procedure"):
        spark.sql("CALL sail.system.not_a_procedure()").collect()

    with pytest.raises(Exception, match=r"Catalog not found: missing_catalog"):
        spark.sql("CALL missing_catalog.system.ancestors_of()").collect()


@pytest.fixture
def multi_catalog_spark():
    catalogs = (
        '[{name="first", type="memory", initial_database=["default"]}, '
        '{name="second", type="memory", initial_database=["default"]}]'
    )
    with (
        spark_connect_server(
            envs={
                "SAIL_CATALOG__DEFAULT_CATALOG": "first",
                "SAIL_CATALOG__DEFAULT_DATABASE": '["default"]',
                "SAIL_CATALOG__LIST": catalogs,
            }
        ) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        yield sessions.create()


def test_explicit_procedure_catalog_owns_unqualified_target(multi_catalog_spark, tmp_path):
    spark = multi_catalog_spark
    table_name = "procedure_catalog_target"
    for catalog, inserts in [("first", 1), ("second", 2)]:
        location = (tmp_path / catalog / table_name).as_uri()
        spark.sql(
            f"""
            CREATE TABLE {catalog}.default.{table_name} (id BIGINT)
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(location)}'
            """
        )
        for identifier in range(inserts):
            spark.sql(f"INSERT INTO {catalog}.default.{table_name} VALUES ({identifier})")  # noqa: S608

    ancestors = spark.sql(f"CALL second.system.ancestors_of(table => 'default.{table_name}')").collect()
    assert len(ancestors) == 2  # noqa: PLR2004

    with pytest.raises(
        Exception,
        match=r"Cannot run procedure from catalog 'second'.*catalog 'first'",
    ):
        spark.sql(f"CALL second.system.ancestors_of(table => 'first.default.{table_name}')").collect()

    with pytest.raises(
        Exception,
        match=r"Cannot run procedure from catalog 'second'.*catalog 'first'",
    ):
        spark.sql("CALL second.system.ancestors_of(table => 'first.default.missing_table')").collect()


def test_iceberg_snapshot_procedures(spark, tmp_path):
    table_name = "iceberg_snapshot_procedures_test"
    table_path = tmp_path / table_name
    table_location = table_path.as_uri()

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id BIGINT, value STRING)
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            """
        )
        for identifier in range(1, 4):
            spark.sql(f"INSERT INTO {table_name} VALUES ({identifier}, 'v{identifier}')")  # noqa: S608

        snapshots = spark.sql(
            f"SELECT snapshot_id FROM {table_name}.snapshots ORDER BY committed_at, snapshot_id"  # noqa: S608
        ).collect()
        snapshot_ids = [row.snapshot_id for row in snapshots]
        assert len(snapshot_ids) == 3  # noqa: PLR2004

        ancestors = spark.sql(f"CALL SyStEm.AnCeStOrS_Of(table => '{table_name}')").collect()
        assert [row.snapshot_id for row in ancestors] == list(reversed(snapshot_ids))

        ancestors_from_middle = spark.sql(
            f"CALL sail.system.ancestors_of('{table_name}', snapshot_id => {snapshot_ids[1]})"
        ).collect()
        assert [row.snapshot_id for row in ancestors_from_middle] == list(reversed(snapshot_ids[:2]))

        rollback = spark.sql(f"CALL system.rollback_to_snapshot('{table_name}', {snapshot_ids[0]})").first()
        assert rollback.previous_snapshot_id == snapshot_ids[2]
        assert rollback.current_snapshot_id == snapshot_ids[0]
        assert [row.id for row in spark.table(table_name).orderBy("id").collect()] == [1]

        restored = spark.sql(
            f"CALL system.set_current_snapshot(table => '{table_name}', snapshot_id => {snapshot_ids[2]})"
        ).first()
        assert restored.previous_snapshot_id == snapshot_ids[0]
        assert restored.current_snapshot_id == snapshot_ids[2]

        base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)

        def set_snapshot_times(metadata):
            timestamp_by_id = {
                snapshot_id: int((base_time + timedelta(seconds=index)).timestamp() * 1000)
                for index, snapshot_id in enumerate(snapshot_ids)
            }
            for snapshot in metadata["snapshots"]:
                snapshot["timestamp-ms"] = timestamp_by_id[snapshot["snapshot-id"]]

        _edit_latest_metadata(table_path, set_snapshot_times)
        cutoff = (base_time + timedelta(seconds=1, milliseconds=500)).strftime("%Y-%m-%d %H:%M:%S.%f")
        rollback_by_time = spark.sql(
            f"CALL system.rollback_to_timestamp(table => '{table_name}', timestamp => TIMESTAMP '{cutoff}')"
        ).first()
        assert rollback_by_time.previous_snapshot_id == snapshot_ids[2]
        assert rollback_by_time.current_snapshot_id == snapshot_ids[1]
        assert [row.id for row in spark.table(table_name).orderBy("id").collect()] == [1, 2]

        def add_branches(metadata):
            refs = metadata.setdefault("refs", {})
            refs["audit"] = {"snapshot-id": snapshot_ids[0], "type": "branch"}
            refs["tip"] = {"snapshot-id": snapshot_ids[2], "type": "branch"}

        _edit_latest_metadata(table_path, add_branches)
        forwarded = spark.sql(
            f"CALL system.fast_forward(table => '{table_name}', branch => 'audit', to => 'tip')"
        ).first()
        assert forwarded.branch_updated == "audit"
        assert forwarded.previous_ref == snapshot_ids[0]
        assert forwarded.updated_ref == snapshot_ids[2]

        current_from_ref = spark.sql(
            f"CALL system.set_current_snapshot(table => '{table_name}', ref => 'audit')"
        ).first()
        assert current_from_ref.previous_snapshot_id == snapshot_ids[1]
        assert current_from_ref.current_snapshot_id == snapshot_ids[2]
        assert [row.id for row in spark.table(table_name).orderBy("id").collect()] == [1, 2, 3]

        with pytest.raises(
            Exception,
            match="Iceberg system procedure 'expire_snapshots' is recognized but not implemented",
        ):
            spark.sql(f"CALL system.expire_snapshots('{table_name}')").collect()

        with pytest.raises(Exception, match="Exactly one of snapshot_id or ref"):
            spark.sql(
                f"CALL system.set_current_snapshot(table => '{table_name}', "
                f"snapshot_id => {snapshot_ids[0]}, ref => 'main')"
            ).collect()
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
