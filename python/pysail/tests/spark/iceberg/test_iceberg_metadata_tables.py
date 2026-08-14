import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal


def test_static_iceberg_metadata_tables(spark, tmp_path):
    table_name = "iceberg_metadata_tables_test"
    table_location = tmp_path.joinpath(table_name).as_uri()

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"DROP DATABASE IF EXISTS {table_name} CASCADE")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id BIGINT, value STRING)
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            """
        )

        assert spark.sql(f"SELECT * FROM {table_name}.snapshots").collect() == []  # noqa: S608
        assert spark.sql(f"SELECT * FROM {table_name}.history").collect() == []  # noqa: S608
        assert spark.sql(f"SELECT * FROM {table_name}.refs").collect() == []  # noqa: S608
        initial_metadata_log = spark.sql(
            f"SELECT latest_snapshot_id FROM {table_name}.metadata_log_entries"  # noqa: S608
        ).collect()
        assert len(initial_metadata_log) == 1
        assert initial_metadata_log[0].latest_snapshot_id is None

        spark.sql(f"INSERT INTO {table_name} VALUES (1, 'one')")  # noqa: S608
        spark.sql(f"INSERT INTO {table_name} VALUES (2, 'two')")  # noqa: S608

        snapshots = spark.sql(
            f"""
            SELECT committed_at, snapshot_id, parent_id, operation, manifest_list, summary
            FROM {table_name}.snapshots
            ORDER BY committed_at, snapshot_id
            """  # noqa: S608
        ).collect()
        assert len(snapshots) == 2  # noqa: PLR2004
        assert [row.operation for row in snapshots] == ["append", "append"]
        assert snapshots[0].parent_id is None
        assert snapshots[1].parent_id == snapshots[0].snapshot_id
        assert all(row.manifest_list.endswith(".avro") for row in snapshots)
        assert all(isinstance(row.summary, dict) for row in snapshots)

        history = spark.sql(
            f"""
            SELECT snapshot_id, parent_id, is_current_ancestor
            FROM {table_name}.history
            ORDER BY made_current_at, snapshot_id
            """  # noqa: S608
        ).collect()
        assert [row.snapshot_id for row in history] == [row.snapshot_id for row in snapshots]
        assert [row.parent_id for row in history] == [None, snapshots[0].snapshot_id]
        assert all(row.is_current_ancestor for row in history)

        refs = spark.sql(
            f"""
            SELECT name, type, snapshot_id,
                   max_reference_age_in_ms, min_snapshots_to_keep, max_snapshot_age_in_ms
            FROM {table_name}.refs
            """  # noqa: S608
        ).collect()
        assert len(refs) == 1
        assert refs[0].name == "main"
        assert refs[0].type == "BRANCH"
        assert refs[0].snapshot_id == snapshots[-1].snapshot_id
        assert refs[0].max_reference_age_in_ms is None
        assert refs[0].min_snapshots_to_keep is None
        assert refs[0].max_snapshot_age_in_ms is None

        metadata_log = spark.sql(
            f"""
            SELECT file, latest_snapshot_id, latest_schema_id, latest_sequence_number
            FROM {table_name}.metadata_log_entries
            ORDER BY timestamp, file
            """  # noqa: S608
        ).collect()
        assert len(metadata_log) == 3  # noqa: PLR2004
        assert metadata_log[0].latest_snapshot_id is None
        assert [row.latest_snapshot_id for row in metadata_log[1:]] == [
            snapshots[0].snapshot_id,
            snapshots[1].snapshot_id,
        ]
        assert [row.latest_sequence_number for row in metadata_log[1:]] == [1, 2]
        assert all(row.latest_schema_id is not None for row in metadata_log[1:])
        assert all(row.file.endswith(".metadata.json") for row in metadata_log)

        projected = spark.sql(
            f"""
            SELECT snapshot_id
            FROM {table_name}.snapshots
            WHERE operation = 'append'
            ORDER BY committed_at, snapshot_id
            """  # noqa: S608
        ).collect()
        assert [row.snapshot_id for row in projected] == [row.snapshot_id for row in snapshots]

        fully_qualified = spark.sql(
            f"SELECT snapshot_id FROM sail.default.{table_name}.snapshots ORDER BY committed_at, snapshot_id"  # noqa: S608
        ).collect()
        assert [row.snapshot_id for row in fully_qualified] == [row.snapshot_id for row in snapshots]

        unsupported_metadata_tables = [
            "entries",
            "files",
            "data_files",
            "delete_files",
            "manifests",
            "partitions",
            "all_data_files",
            "all_delete_files",
            "all_files",
            "all_manifests",
            "all_entries",
            "position_deletes",
        ]
        for metadata_table in unsupported_metadata_tables:
            with pytest.raises(
                Exception,
                match=rf"Iceberg metadata table '{metadata_table}' is recognized but not implemented",
            ):
                spark.sql(f"SELECT * FROM {table_name}.{metadata_table}").collect()  # noqa: S608

        with pytest.raises(Exception, match=r"(?i)(not found|table_or_view_not_found)"):
            spark.sql(f"SELECT * FROM {table_name}.unknown_relation").collect()  # noqa: S608

        with pytest.raises(
            Exception,
            match="time travel is not supported for lake relation 'snapshots'",
        ):
            spark.sql(
                f"SELECT * FROM {table_name}.snapshots VERSION AS OF 1"  # noqa: S608
            ).collect()
        with pytest.raises(
            Exception,
            match="time travel is not supported for lake relation 'history'",
        ):
            spark.sql(
                f"SELECT * FROM {table_name}.history TIMESTAMP AS OF '2026-01-01 00:00:00'"  # noqa: S608
            ).collect()

        spark.sql(f"CREATE DATABASE {table_name}")
        spark.sql(f"CREATE TABLE {table_name}.snapshots (marker BIGINT) USING parquet")
        spark.sql(f"INSERT INTO {table_name}.snapshots VALUES (42)")  # noqa: S608
        exact_table = spark.sql(f"SELECT marker FROM {table_name}.snapshots").collect()  # noqa: S608
        assert [row.marker for row in exact_table] == [42]
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {table_name} CASCADE")
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_static_iceberg_metadata_table_schemas_and_case_insensitive_names(spark, tmp_path):
    table_name = "iceberg_metadata_table_schema_test"
    table_location = tmp_path.joinpath(table_name).as_uri()

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id BIGINT)
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            """
        )

        expected_schemas = {
            "SNAPSHOTS": [
                ("committed_at", "timestamp", False),
                ("snapshot_id", "bigint", False),
                ("parent_id", "bigint", True),
                ("operation", "string", True),
                ("manifest_list", "string", True),
                ("summary", "map<string,string>", True),
            ],
            "HiStOrY": [
                ("made_current_at", "timestamp", False),
                ("snapshot_id", "bigint", False),
                ("parent_id", "bigint", True),
                ("is_current_ancestor", "boolean", False),
            ],
            "REFS": [
                ("name", "string", False),
                ("type", "string", False),
                ("snapshot_id", "bigint", False),
                ("max_reference_age_in_ms", "bigint", True),
                ("min_snapshots_to_keep", "int", True),
                ("max_snapshot_age_in_ms", "bigint", True),
            ],
            "Metadata_Log_Entries": [
                ("timestamp", "timestamp", False),
                ("file", "string", False),
                ("latest_snapshot_id", "bigint", True),
                ("latest_schema_id", "int", True),
                ("latest_sequence_number", "bigint", True),
            ],
        }

        for relation_name, expected_schema in expected_schemas.items():
            dataframe = spark.table(f"{table_name}.{relation_name}")
            assert [
                (field.name, field.dataType.simpleString(), field.nullable) for field in dataframe.schema.fields
            ] == expected_schema
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_metadata_tables_track_schema_evolution(spark, tmp_path):
    table_name = "iceberg_metadata_schema_evolution_test"
    table_location = tmp_path.joinpath(table_name).as_uri()

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id BIGINT)
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1)")  # noqa: S608
        evolved = spark.createDataFrame([(2, "two")], schema="id BIGINT, value STRING")
        (evolved.write.format("iceberg").mode("append").option("mergeSchema", "true").save(table_location))

        snapshots = spark.sql(
            f"SELECT snapshot_id FROM {table_name}.snapshots ORDER BY committed_at, snapshot_id"  # noqa: S608
        ).collect()
        assert len(snapshots) == 2  # noqa: PLR2004

        metadata_log = spark.sql(
            f"""
            SELECT latest_snapshot_id, latest_schema_id, latest_sequence_number
            FROM {table_name}.metadata_log_entries
            ORDER BY timestamp, file
            """  # noqa: S608
        ).collect()
        assert len(metadata_log) == 3  # noqa: PLR2004
        assert [row.latest_snapshot_id for row in metadata_log] == [
            None,
            snapshots[0].snapshot_id,
            snapshots[1].snapshot_id,
        ]
        assert metadata_log[0].latest_schema_id is None
        assert metadata_log[1].latest_schema_id is not None
        assert metadata_log[2].latest_schema_id != metadata_log[1].latest_schema_id
        assert [row.latest_sequence_number for row in metadata_log] == [None, 1, 2]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_metadata_tables_load_gzip_metadata(spark, tmp_path):
    table_name = "iceberg_gzip_metadata_table_test"
    table_location = tmp_path.joinpath(table_name).as_uri()

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id BIGINT)
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            TBLPROPERTIES ('write.metadata.compression-codec' = 'gzip')
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1)")  # noqa: S608

        metadata_files = spark.sql(
            f"SELECT file FROM {table_name}.metadata_log_entries ORDER BY timestamp, file"  # noqa: S608
        ).collect()
        assert len(metadata_files) == 2  # noqa: PLR2004
        assert all(row.file.endswith(".gz.metadata.json") for row in metadata_files)
        assert spark.sql(f"SELECT count(*) FROM {table_name}.snapshots").first()[0] == 1  # noqa: S608
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_iceberg_metadata_relations_support_joins_and_aggregations(spark, tmp_path):
    table_name = "iceberg_metadata_relation_join_test"
    table_location = tmp_path.joinpath(table_name).as_uri()

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_name} (id BIGINT)
            USING ICEBERG
            LOCATION '{escape_sql_string_literal(table_location)}'
            """
        )
        spark.sql(f"INSERT INTO {table_name} VALUES (1)")  # noqa: S608
        spark.sql(f"INSERT INTO {table_name} VALUES (2)")  # noqa: S608

        joined = spark.sql(
            f"""
            SELECT h.is_current_ancestor, count(*) AS snapshot_count
            FROM {table_name}.snapshots AS s
            JOIN {table_name}.history AS h ON s.snapshot_id = h.snapshot_id
            GROUP BY h.is_current_ancestor
            """  # noqa: S608
        ).collect()
        assert [(row.is_current_ancestor, row.snapshot_count) for row in joined] == [(True, 2)]

        current_ref = spark.sql(
            f"""
            SELECT r.name, s.operation
            FROM {table_name}.refs AS r
            JOIN {table_name}.snapshots AS s ON r.snapshot_id = s.snapshot_id
            """  # noqa: S608
        ).collect()
        assert [(row.name, row.operation) for row in current_ref] == [("main", "append")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
