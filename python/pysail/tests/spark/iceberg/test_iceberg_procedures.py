import json
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField

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
def local_cluster_spark():
    with (
        spark_connect_server(envs={"SAIL_MODE": "local-cluster"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        yield sessions.create()


def test_metadata_read_procedure_runs_on_a_worker(local_cluster_spark, tmp_path):
    spark = local_cluster_spark
    table_name = "distributed_iceberg_procedure"
    location = (tmp_path / table_name).as_uri()
    spark.sql(
        f"""
        CREATE TABLE {table_name} (id BIGINT)
        USING ICEBERG
        LOCATION '{escape_sql_string_literal(location)}'
        """
    )
    try:
        spark.sql(f"INSERT INTO {table_name} VALUES (1)")  # noqa: S608

        ancestors = spark.sql(f"CALL system.ancestors_of('{table_name}')").collect()

        assert len(ancestors) == 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.parametrize("option", ["max-file-group-size-bytes", "max-concurrent-file-group-rewrites"])
def test_rewrite_rejects_zero_group_limits(spark, tmp_path, option):
    table_name = "rewrite_invalid_group_limit"
    location = (tmp_path / table_name).as_uri()
    spark.sql(f"CREATE TABLE {table_name} (id BIGINT) USING ICEBERG LOCATION '{escape_sql_string_literal(location)}'")
    try:
        with pytest.raises(Exception, match="must be positive"):
            spark.sql(
                f"CALL system.rewrite_data_files(table => '{table_name}', options => map('{option}', '0'))"
            ).collect()
        assert spark.table(f"{table_name}.snapshots").count() == 0
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.parametrize(("files_per_group", "concurrent_groups"), [(8, 1), (2, 1), (2, 2)])
def test_rewrite_data_files_uses_workers_and_coordinator_commit(
    local_cluster_spark, tmp_path, files_per_group, concurrent_groups
):
    spark = local_cluster_spark
    table_name = "distributed_rewrite_data_files"
    location = (tmp_path / table_name).as_uri()
    spark.sql(
        f"""
        CREATE TABLE {table_name} (id BIGINT, value STRING)
        USING ICEBERG
        LOCATION '{escape_sql_string_literal(location)}'
        """
    )
    try:
        for identifier in range(8):
            spark.sql(f"INSERT INTO {table_name} VALUES ({identifier}, 'v{identifier}')")  # noqa: S608

        before = spark.sql(
            f"SELECT file_path, file_size_in_bytes FROM {table_name}.files ORDER BY file_path"  # noqa: S608
        ).collect()
        assert len(before) == 8  # noqa: PLR2004
        target_file_size = sum(row.file_size_in_bytes for row in before)
        group_size = files_per_group * max(row.file_size_in_bytes for row in before)
        assert group_size < (files_per_group + 1) * min(row.file_size_in_bytes for row in before)

        result = spark.sql(
            f"""
            CALL system.rewrite_data_files(
              table => '{table_name}',
              options => map(
                'rewrite-all', 'true',
                'target-file-size-bytes', '{target_file_size}',
                'max-file-group-size-bytes', '{group_size}',
                'max-concurrent-file-group-rewrites', '{concurrent_groups}'))
            """
        ).first()

        assert result.rewritten_data_files_count == len(before)
        assert result.added_data_files_count == len(before) // files_per_group
        assert result.rewritten_bytes_count == sum(row.file_size_in_bytes for row in before)
        assert result.failed_data_files_count == 0
        assert result.removed_delete_files_count == 0

        after = spark.sql(f"SELECT file_path FROM {table_name}.files ORDER BY file_path").collect()  # noqa: S608
        assert len(after) == result.added_data_files_count
        assert {row.file_path for row in after}.isdisjoint({row.file_path for row in before})
        assert [row.id for row in spark.table(table_name).orderBy("id").collect()] == list(range(8))
        assert (
            spark.sql(
                f"SELECT operation FROM {table_name}.snapshots ORDER BY committed_at DESC LIMIT 1"  # noqa: S608
            )
            .first()
            .operation
            == "replace"
        )

        snapshot_count = (
            spark.sql(
                f"SELECT count(*) AS snapshot_count FROM {table_name}.snapshots"  # noqa: S608
            )
            .first()
            .snapshot_count
        )
        no_op = spark.sql(f"CALL system.rewrite_data_files('{table_name}')").first()
        assert tuple(no_op) == (0, 0, 0, 0, 0)
        assert (
            spark.sql(
                f"SELECT count(*) AS snapshot_count FROM {table_name}.snapshots"  # noqa: S608
            )
            .first()
            .snapshot_count
            == snapshot_count
        )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_rewrite_groups_preserve_transformed_partitions(local_cluster_spark, tmp_path):
    spark = local_cluster_spark
    table_name = "distributed_partition_rewrite"
    location = (tmp_path / table_name).as_uri()
    spark.sql(
        f"""
        CREATE TABLE {table_name} (id BIGINT, ts TIMESTAMP)
        USING ICEBERG
        PARTITIONED BY (days(ts))
        LOCATION '{escape_sql_string_literal(location)}'
        """
    )
    partition_count = 6
    try:
        for hour in [10, 11]:
            values = ", ".join(
                f"({day}, TIMESTAMP '2024-01-{day:02} {hour}:00:00')" for day in range(1, partition_count + 1)
            )
            spark.sql(f"INSERT INTO {table_name} VALUES {values}")  # noqa: S608
        before = spark.table(f"{table_name}.files").collect()
        assert len(before) == partition_count * 2
        expected = spark.table(table_name).orderBy("id", "ts").collect()

        result = spark.sql(
            f"""
            CALL system.rewrite_data_files(
              table => '{table_name}',
              options => map('rewrite-all', 'true', 'target-file-size-bytes', '1048576',
                            'max-concurrent-file-group-rewrites', '2'))
            """
        ).first()

        assert result.rewritten_data_files_count == len(before)
        assert result.added_data_files_count == partition_count
        assert result.rewritten_bytes_count == sum(row.file_size_in_bytes for row in before)
        assert spark.table(table_name).orderBy("id", "ts").collect() == expected
        after = spark.table(f"{table_name}.files").collect()
        assert len(after) == partition_count
        assert len({tuple(row.partition) for row in after}) == partition_count
        assert all(row.record_count == 2 for row in after)  # noqa: PLR2004
        assert spark.table(f"{table_name}.files").select().count() == partition_count
        assert len(spark.table(f"{table_name}.files").where("record_count = 2").limit(1).collect()) == 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


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


def _read_latest_metadata(table_path):
    return json.loads(_latest_metadata_file(table_path).read_text(encoding="utf-8"))


@pytest.fixture(params=["default", "home"])
def procedure_catalog_namespace(request):
    return request.param


@pytest.fixture
def catalog_resolution_spark(procedure_catalog_namespace):
    catalogs = (
        '[{name="first", type="memory", initial_database=["default"]}, '
        f'{{name="second", type="memory", initial_database=["{procedure_catalog_namespace}"]}}]'
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


@pytest.mark.parametrize("procedure", ["ancestors_of", "rewrite_data_files"])
def test_explicit_procedure_catalog_uses_its_default_namespace(
    catalog_resolution_spark, procedure_catalog_namespace, tmp_path, procedure
):
    spark = catalog_resolution_spark
    table_name = "procedure_namespace_target"
    spark.sql("CREATE DATABASE first.custom")
    spark.sql("CREATE DATABASE second.custom")
    tables = []
    try:
        for database, rows in [(procedure_catalog_namespace, [1, 2, 3]), ("custom", [10])]:
            table = f"second.{database}.{table_name}"
            location = tmp_path / database
            spark.sql(
                f"CREATE TABLE {table} (id BIGINT) USING iceberg "
                f"LOCATION '{escape_sql_string_literal(location.as_uri())}'"
            )
            tables.append(table)
            for value in rows:
                spark.sql(f"INSERT INTO {table} VALUES ({value})")  # noqa: S608

        expected_snapshots = _read_latest_metadata(tmp_path / procedure_catalog_namespace)["snapshots"]
        unrelated_snapshot = _read_latest_metadata(tmp_path / "custom")["current-snapshot-id"]
        spark.sql("USE DATABASE custom")

        if procedure == "ancestors_of":
            result = spark.sql(f"CALL second.system.ancestors_of('{table_name}')").collect()
            assert {row.snapshot_id for row in result} == {snapshot["snapshot-id"] for snapshot in expected_snapshots}
        else:
            result = spark.sql(
                f"CALL second.system.rewrite_data_files(table => '{table_name}', options => map('rewrite-all', 'true'))"
            ).first()
            assert _read_latest_metadata(tmp_path / "custom")["current-snapshot-id"] == unrelated_snapshot, (
                "The procedure in second must not mutate second.custom when the current catalog is first"
            )
            assert result.rewritten_data_files_count == len(expected_snapshots)
            assert len(spark.table(f"second.{procedure_catalog_namespace}.{table_name}.files").collect()) == 1
    finally:
        for table in tables:
            spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("timezone", ["UTC", "America/Los_Angeles"])
@pytest.mark.parametrize("argument", ["DATE '2024-01-01'", "'2024-01-01 00:00:00'", "TIMESTAMP '2024-01-01 00:00:00'"])
def test_rollback_timestamp_coercion_uses_session_timezone(spark, tmp_path, timezone, argument):
    table = "procedure_timestamp_coercion"
    location = tmp_path / table
    original_timezone = spark.conf.get("spark.sql.session.timeZone")
    spark.sql(
        f"CREATE TABLE {table} (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(location.as_uri())}'"
    )
    try:
        for value in range(3):
            spark.sql(f"INSERT INTO {table} VALUES ({value})")  # noqa: S608
        metadata_file = sorted((location / "metadata").glob("*.metadata.json"))[-1]
        metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
        instants = ["2023-12-31T22:00:00+00:00", "2024-01-01T04:00:00+00:00", "2024-01-01T12:00:00+00:00"]
        timestamp_by_id = {
            snapshot["snapshot-id"]: int(datetime.fromisoformat(instant).timestamp() * 1000)
            for snapshot, instant in zip(metadata["snapshots"], instants, strict=True)
        }
        for snapshot in metadata["snapshots"]:
            snapshot["timestamp-ms"] = timestamp_by_id[snapshot["snapshot-id"]]
        for entry in metadata["snapshot-log"]:
            entry["timestamp-ms"] = timestamp_by_id[entry["snapshot-id"]]
        metadata_file.write_text(json.dumps(metadata), encoding="utf-8")

        spark.conf.set("spark.sql.session.timeZone", timezone)
        result = spark.sql(f"CALL system.rollback_to_timestamp(table => '{table}', timestamp => {argument})").first()

        # Midnight in Los Angeles is 08:00 UTC, between the second and third snapshots.
        expected_index = 0 if timezone == "UTC" else 1
        assert result.current_snapshot_id == metadata["snapshots"][expected_index]["snapshot-id"]
        assert [row.id for row in spark.table(table).orderBy("id").collect()] == list(range(expected_index + 1))
    finally:
        spark.conf.set("spark.sql.session.timeZone", original_timezone)
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("options", ["map()", "map('min-input-files', 2)", "map('rewrite-all', true)"])
def test_procedure_coerces_options_to_string_map(spark, tmp_path, options):
    table = "procedure_options_coercion"
    location = (tmp_path / table).as_uri()
    spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(location)}'")
    try:
        result = spark.sql(f"CALL system.rewrite_data_files(table => '{table}', options => {options})").first()

        assert tuple(result) == (0, 0, 0, 0, 0)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("procedure", ["ancestors_of", "rollback_to_snapshot", "rollback_to_timestamp", "fast_forward"])
def test_snapshot_procedures_stop_at_expired_ancestor(spark, sql_catalog, procedure):
    table_name = "snapshot_expired_ancestor"
    table = sql_catalog.create_table(f"default.{table_name}", Schema(NestedField(1, "id", LongType(), required=False)))
    for identifier in range(3):
        table.append(pa.table({"id": [identifier]}))
    snapshots = table.snapshots()
    first, second, current = snapshots
    table.manage_snapshots().create_branch(second.snapshot_id, "audit").commit()
    table.maintenance.expire_snapshots().by_id(first.snapshot_id).commit()
    assert table.snapshot_by_id(first.snapshot_id) is None
    # Spark preserves the parent ID when its ancestor expires; PyIceberg clears it.
    with table.io.new_input(table.metadata_location).open() as source:
        metadata = json.load(source)
    for snapshot in metadata["snapshots"]:
        if snapshot["snapshot-id"] == second.snapshot_id:
            snapshot["parent-snapshot-id"] = first.snapshot_id
    with table.io.new_output(table.metadata_location).create(overwrite=True) as output:
        output.write(json.dumps(metadata).encode())

    location = escape_sql_string_literal(table.location())
    spark.sql(f"CREATE TABLE {table_name} USING iceberg LOCATION '{location}'")
    try:
        assert [row.id for row in spark.table(table_name).orderBy("id").collect()] == list(range(3))
        if procedure == "ancestors_of":
            rows = spark.sql(f"CALL system.ancestors_of('{table_name}')").collect()
            assert [row.snapshot_id for row in rows] == [current.snapshot_id, second.snapshot_id]
        elif procedure == "rollback_to_snapshot":
            row = spark.sql(f"CALL system.rollback_to_snapshot('{table_name}', {second.snapshot_id})").first()
            assert row.current_snapshot_id == second.snapshot_id
            assert [row.id for row in spark.table(table_name).orderBy("id").collect()] == [0, 1]
        elif procedure == "rollback_to_timestamp":
            cutoff = datetime.fromtimestamp(current.timestamp_ms / 1000 + 1, timezone.utc)
            row = spark.sql(
                f"CALL system.rollback_to_timestamp('{table_name}', TIMESTAMP '{cutoff.isoformat()}')"
            ).first()
            assert row.current_snapshot_id == current.snapshot_id
        else:
            row = spark.sql(f"CALL system.fast_forward('{table_name}', 'audit', 'main')").first()
            assert row.previous_ref == second.snapshot_id
            assert row.updated_ref == current.snapshot_id
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.parametrize("table_name", ["same_name", "name.with.dots"])
def test_metadata_qualifiers_distinguish_catalogs(catalog_resolution_spark, tmp_path, table_name):
    spark = catalog_resolution_spark
    spark.sql("CREATE DATABASE IF NOT EXISTS second.default")
    tables = [f"{catalog}.default.`{table_name}`" for catalog in ["first", "second"]]
    try:
        snapshots = []
        for index, table in enumerate(tables):
            location = tmp_path / str(index)
            spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg LOCATION '{location.as_uri()}'")
            spark.sql(f"INSERT INTO {table} VALUES ({index})")  # noqa: S608
            snapshots.append(_read_latest_metadata(location)["current-snapshot-id"])
        left, right = [f"{table}.snapshots" for table in tables]
        row = spark.sql(
            f"SELECT {left}.snapshot_id AS l, {right}.snapshot_id AS r FROM {left} CROSS JOIN {right}"  # noqa: S608
        ).first()
        assert tuple(row) == tuple(snapshots)
        rows = spark.sql(f"SELECT {left}.*, {right}.* FROM {left} CROSS JOIN {right}").collect()  # noqa: S608
        assert len(rows) == 1
        with pytest.raises(Exception, match=r"(?i)ambiguous"):
            spark.sql(f"SELECT `{table_name}`.snapshots.snapshot_id FROM {left} CROSS JOIN {right}").collect()  # noqa: S608
    finally:
        for table in tables:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
