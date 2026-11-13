# ruff: noqa: S608

from datetime import datetime, timezone

import pytest
from pyiceberg.manifest import ManifestContent
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType, TimestampType
from pyspark.errors import PySparkException
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing._dev.spark.steps.iceberg import _current_snapshot, _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.iceberg.utils import _current_manifest_entries, _local_file_path

APIS = ["sql", "save_table", "replace", "createOrReplace"]
SOURCE = "iceberg_replace_source"


def test_iceberg_named_overwrite_schema_drops_partition_column(spark, sql_catalog):
    identifier = "default.overwrite_schema_partition_replacement"
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="event_ts", field_type=TimestampType(), required=False),
        NestedField(field_id=3, name="value", field_type=StringType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="event_ts_identity")
    )
    table = sql_catalog.create_table(identifier=identifier, schema=schema, partition_spec=spec)
    spark_table = "tmp_overwrite_schema_partition_replacement"
    spark.sql(f"DROP TABLE IF EXISTS {spark_table}")
    spark.sql(
        f"""
        CREATE TABLE {spark_table}
        USING iceberg
        LOCATION '{escape_sql_string_literal(table.location())}'
        """
    )
    try:
        seed_df = spark.createDataFrame(
            [(1, datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc), "seed")],
            schema="id LONG, event_ts TIMESTAMP, value STRING",
        )
        seed_df.write.format("iceberg").mode("overwrite").save(table.location())

        replacement_df = spark.createDataFrame(
            [(10, "overwrite")],
            schema="id LONG, value STRING",
        )
        path = _local_file_path(table.location())
        before = _find_latest_metadata(path)
        seed_rows = spark.read.format("iceberg").load(table.location()).collect()
        seed_schema = spark.table(spark_table).schema
        replacement_df.write.format("iceberg").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            spark_table
        )
        after = _find_latest_metadata(path)
        _assert_replacement_history(before, after)
        assert _current_spec(before)["fields"][0]["source-id"] == 2  # noqa: PLR2004
        assert _current_spec(after)["fields"] == []
        assert after["default-spec-id"] != before["default-spec-id"]
        assert after["current-schema-id"] != before["current-schema-id"]
        assert [field["name"] for field in _current_schema(after)["fields"]] == ["id", "value"]
        assert spark.table(spark_table).schema == replacement_df.schema
        assert spark.table(spark_table).collect() == replacement_df.collect()
        assert spark.read.format("iceberg").load(table.location()).collect() == replacement_df.collect()
        historical = spark.read.format("iceberg").option("snapshot-id", str(before["current-snapshot-id"]))
        assert historical.load(table.location()).select(seed_schema.fieldNames()).collect() == seed_rows
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {spark_table}")
        sql_catalog.drop_table(identifier)


@pytest.fixture
def replacement_target(spark, tmp_path):
    name = "iceberg_replace_api"
    path = tmp_path / "table"
    try:
        yield name, path
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {SOURCE}")
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def _create(spark, target, *, seed=True):
    name, path = target
    spark.sql(
        f"CREATE TABLE {name} (a INT, b INT) USING iceberg PARTITIONED BY (a) "
        f"LOCATION '{escape_sql_string_literal(str(path))}' "
        "TBLPROPERTIES ('format-version'='2', 'keep'='old', 'change'='old')"
    )
    if seed:
        spark.sql(f"INSERT INTO {name} VALUES (1, 2), (3, 4)")


def _replace(spark, target, source, api, *, overwrite_schema=None, partition=False, properties=None, create=False):
    name, path = target
    if properties is True:
        properties = {"change": "new", "added": "new"}
    if api == "sql":
        source.createOrReplaceTempView(SOURCE)
        command = "CREATE OR REPLACE" if create else "REPLACE"
        options = " OPTIONS ('overwriteSchema'='false')" if overwrite_schema is False else ""
        spec = " PARTITIONED BY (bucket(4, c))" if partition else ""
        props = ""
        if properties:
            values = ", ".join(
                f"'{escape_sql_string_literal(key)}'='{escape_sql_string_literal(value)}'"
                for key, value in properties.items()
            )
            props = f" TBLPROPERTIES ({values})"
        spark.sql(
            f"{command} TABLE {name} USING iceberg{spec} "
            f"LOCATION '{escape_sql_string_literal(str(path))}'{options}{props} AS SELECT * FROM {SOURCE}"
        ).collect()
    elif api == "save_table":
        writer = source.write.format("iceberg").mode("overwrite").option("path", str(path))
        if overwrite_schema is False:
            writer = writer.option("overwriteSchema", "false")
        writer.saveAsTable(name)
    else:
        writer = source.writeTo(name).using("iceberg").option("path", str(path))
        if overwrite_schema is False:
            writer = writer.option("overwriteSchema", "false")
        if partition:
            writer = writer.partitionedBy(F.bucket(4, "c"))
        for key, value in (properties or {}).items():
            writer = writer.tableProperty(key, value)
        getattr(writer, api)()


def _current_schema(metadata):
    return next(schema for schema in metadata["schemas"] if schema["schema-id"] == metadata["current-schema-id"])


def _current_spec(metadata):
    return next(spec for spec in metadata["partition-specs"] if spec["spec-id"] == metadata["default-spec-id"])


def _metadata_files(path):
    return {file.name: file.read_bytes() for file in (path / "metadata").glob("*.metadata.json")}


def _create_unpartitioned(spark, target, version):
    name, path = target
    spark.sql(
        f"CREATE TABLE {name} (a INT, b INT) USING iceberg "
        f"LOCATION '{escape_sql_string_literal(str(path))}' "
        f"TBLPROPERTIES ('format-version'='{version}', 'keep'='old', 'change'='old')"
    )
    spark.sql(f"INSERT INTO {name} VALUES (1, 2), (3, 4)")


def _visible_state(spark, target):
    name, path = target
    return (
        spark.table(name).schema,
        spark.catalog.listColumns(name),
        sorted(tuple(row) for row in spark.table(name).select("a", "b").collect()),
        {file.relative_to(path): file.read_bytes() for file in path.rglob("*") if file.is_file()},
    )


def _assert_rows(frame, rows, names):
    assert frame.columns == names
    assert sorted(tuple(row) for row in frame.collect()) == sorted(rows)


def _assert_replacement_history(before, after):
    assert after["table-uuid"] == before["table-uuid"]
    historical = before.get("snapshots", [])
    if before["format-version"] == 1 and after["format-version"] > 1:
        # Iceberg spec, v2: reading v1 snapshot sequence-number must default to 0.
        historical = [{"sequence-number": 0, **snapshot} for snapshot in historical]
    assert after["snapshots"][:-1] == historical
    assert after["snapshot-log"][:-1] == before.get("snapshot-log", [])
    assert len(after["metadata-log"]) == len(before.get("metadata-log", [])) + 1
    for key in ("schemas", "partition-specs", "sort-orders"):
        assert all(item in after[key] for item in before.get(key, []))
    snapshot = _current_snapshot(after)
    assert snapshot["summary"]["operation"] == "overwrite"
    assert "parent-snapshot-id" not in snapshot
    assert after["refs"]["main"]["snapshot-id"] == snapshot["snapshot-id"]
    for ref, value in before.get("refs", {}).items():
        if ref != "main":
            assert after["refs"][ref] == value


@pytest.mark.parametrize("api", APIS)
@pytest.mark.parametrize("overwrite_schema", [None, False], ids=["default", "explicit-false"])
@pytest.mark.parametrize("shape", ["reordered", "incompatible"])
def test_iceberg_table_replacement_lifecycle(spark, replacement_target, api, overwrite_schema, shape):
    _create(spark, replacement_target)
    name, path = replacement_target
    before = _find_latest_metadata(path)
    old_snapshot = before["current-snapshot-id"]
    before_metadata = _metadata_files(path)
    old_files = {
        _local_file_path(entry.data_file.file_path) for entry in _current_manifest_entries(path, ManifestContent.DATA)
    }
    if shape == "reordered":
        rows, schema, names = [(20, 10)], "b INT, a INT", ["b", "a"]
    else:
        # The old partition source disappears and an existing column changes incompatibly.
        rows, schema, names = [("twenty", 10)], "b STRING, c BIGINT", ["b", "c"]
    source = spark.createDataFrame(rows, schema)
    _replace(spark, replacement_target, source, api, overwrite_schema=overwrite_schema)

    _assert_rows(spark.table(name), rows, names)
    assert spark.table(name).schema == source.schema
    _assert_rows(spark.read.format("iceberg").load(str(path)), rows, names)
    after = _find_latest_metadata(path)
    _assert_replacement_history(before, after)
    assert [field["name"] for field in _current_schema(after)["fields"]] == names
    assert _current_spec(after)["fields"] == []
    assert after["default-sort-order-id"] == 0
    assert after["properties"]["keep"] == "old"
    assert after["properties"]["change"] == "old"
    assert len(_metadata_files(path)) == len(before_metadata) + 1
    assert all(_metadata_files(path)[key] == value for key, value in before_metadata.items())
    assert all(file.exists() for file in old_files)
    current_files = {
        _local_file_path(entry.data_file.file_path) for entry in _current_manifest_entries(path, ManifestContent.DATA)
    }
    assert current_files.isdisjoint(old_files)
    assert _current_manifest_entries(path, ManifestContent.DELETES) == []
    historical = spark.read.format("iceberg").option("snapshot-id", str(old_snapshot)).load(str(path))
    _assert_rows(historical, [(1, 2), (3, 4)], ["a", "b"])

    # Drop/re-register from storage: no cached catalog schema may be necessary for correctness.
    spark.sql(f"DROP TABLE {name}")
    spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
    _assert_rows(spark.table(name), rows, names)
    source.writeTo(name).append()
    _assert_rows(spark.table(name), rows + rows, names)
    appended = _find_latest_metadata(path)
    assert _current_snapshot(appended)["parent-snapshot-id"] == after["current-snapshot-id"]
    assert appended["schemas"] == after["schemas"]
    assert appended["partition-specs"] == after["partition-specs"]


@pytest.mark.parametrize("api", ["sql", "replace", "createOrReplace"])
def test_iceberg_replacement_partition_and_property_definition(spark, replacement_target, api):
    _create(spark, replacement_target)
    name, path = replacement_target
    before = _find_latest_metadata(path)
    source = spark.createDataFrame([("x", 10), ("y", 20)], "b STRING, c BIGINT")
    _replace(spark, replacement_target, source, api, overwrite_schema=False, partition=True, properties=True)
    after = _find_latest_metadata(path)
    _assert_replacement_history(before, after)
    fields = _current_schema(after)["fields"]
    partition = _current_spec(after)["fields"]
    assert len(partition) == 1
    assert partition[0]["source-id"] == next(field["id"] for field in fields if field["name"] == "c")
    assert partition[0]["transform"] == "bucket[4]"
    assert after["properties"]["keep"] == "old"
    assert after["properties"]["change"] == "new"
    assert after["properties"]["added"] == "new"
    entries = _current_manifest_entries(path, ManifestContent.DATA)
    assert sum(entry.data_file.record_count for entry in entries) == 2  # noqa: PLR2004
    assert all(len(entry.data_file.partition) == 1 for entry in entries)
    source.writeTo(name).append()
    _assert_rows(spark.table(name), [("x", 10), ("y", 20)] * 2, ["b", "c"])


@pytest.mark.parametrize("api", APIS)
@pytest.mark.parametrize("seed", [False, True], ids=["empty-table", "populated-table"])
def test_iceberg_empty_replacement_still_commits(spark, replacement_target, api, seed):
    _create(spark, replacement_target, seed=seed)
    name, path = replacement_target
    before = _find_latest_metadata(path)
    source = spark.createDataFrame([], "b STRING, c BIGINT")
    _replace(spark, replacement_target, source, api, overwrite_schema=False)
    _assert_rows(spark.table(name), [], ["b", "c"])
    after = _find_latest_metadata(path)
    _assert_replacement_history(before, after)
    assert _current_spec(after)["fields"] == []
    assert _current_manifest_entries(path, ManifestContent.DATA) == []
    spark.createDataFrame([("later", 1)], source.schema).writeTo(name).append()
    _assert_rows(spark.table(name), [("later", 1)], ["b", "c"])


@pytest.mark.parametrize("api", APIS)
@pytest.mark.parametrize("empty", [False, True])
def test_iceberg_replacement_absent_target(spark, replacement_target, api, empty):
    name, path = replacement_target
    rows = [] if empty else [("created", 1)]
    source = spark.createDataFrame(rows, "b STRING, c BIGINT")
    if api == "replace":
        with pytest.raises(PySparkException, match=r"(?i)(not found|cannot be found)"):
            _replace(spark, replacement_target, source, api)
        assert not spark.catalog.tableExists(name)
        assert _metadata_files(path) == {}
    else:
        _replace(spark, replacement_target, source, api, create=True)
        _assert_rows(spark.table(name), rows, ["b", "c"])
        metadata = _find_latest_metadata(path)
        assert len(_metadata_files(path)) == 1
        assert len(metadata["snapshots"]) == 1
        assert "parent-snapshot-id" not in _current_snapshot(metadata)
        source.writeTo(name).append()
        _assert_rows(spark.table(name), rows + rows, ["b", "c"])


@pytest.mark.parametrize(
    ("api", "exists"), [(api, exists) for api in APIS for exists in (False, True) if exists or api != "replace"]
)
def test_iceberg_failed_replacement_preserves_visible_definition(spark, replacement_target, api, exists):
    name, path = replacement_target
    if exists:
        _create(spark, replacement_target)
        before_schema = spark.table(name).schema
        before_columns = spark.catalog.listColumns(name)
        _assert_rows(spark.table(name).select("a", "b"), [(1, 2), (3, 4)], ["a", "b"])
    before = _metadata_files(path)
    source = spark.range(8).selectExpr(
        "CAST(CASE WHEN id % 2 = 0 THEN raise_error('replacement source failed') ELSE id END AS STRING) AS c"
    )
    with pytest.raises(PySparkException, match="replacement source failed"):
        _replace(spark, replacement_target, source, api, create=not exists)
    assert _metadata_files(path) == before
    if exists:
        assert spark.table(name).schema == before_schema
        assert spark.catalog.listColumns(name) == before_columns
        _assert_rows(spark.table(name).select("a", "b"), [(1, 2), (3, 4)], ["a", "b"])
        spark.createDataFrame([(5, 6)], "a INT, b INT").writeTo(name).append()
        _assert_rows(spark.table(name).select("a", "b"), [(1, 2), (3, 4), (5, 6)], ["a", "b"])
    else:
        assert not spark.catalog.tableExists(name)


@pytest.mark.parametrize("key", ["write.format.default", "write.delete.format.default"])
@pytest.mark.parametrize("file_format", ["orc", "avro"])
def test_iceberg_replacement_rejects_unsupported_format_before_writing(spark, replacement_target, key, file_format):
    _create(spark, replacement_target)
    name, _ = replacement_target
    before = _visible_state(spark, replacement_target)
    source = spark.createDataFrame([("new", 10)], "b STRING, c BIGINT")
    with pytest.raises(PySparkException, match="unsupported Iceberg replacement file format"):
        _replace(spark, replacement_target, source, "replace", properties={key: file_format})
    assert _visible_state(spark, replacement_target) == before
    spark.createDataFrame([(5, 6)], "a INT, b INT").writeTo(name).append()
    _assert_rows(spark.table(name).select("a", "b"), [(1, 2), (3, 4), (5, 6)], ["a", "b"])


@pytest.mark.parametrize("api", APIS)
def test_iceberg_replacement_registered_v1_keeps_authoritative_version(spark, replacement_target, api):
    _create_unpartitioned(spark, replacement_target, 1)
    name, path = replacement_target
    spark.sql(f"DROP TABLE {name}")
    spark.sql(f"CREATE TABLE {name} USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
    before = _metadata_files(path)
    source = spark.createDataFrame([("new", 10)], "b STRING, c BIGINT")
    _replace(spark, replacement_target, source, api)
    metadata = _find_latest_metadata(path)
    assert metadata["format-version"] == 1
    assert dict(spark.sql(f"SHOW TBLPROPERTIES {name}").collect())["format-version"] == "1"
    assert all(_metadata_files(path)[key] == value for key, value in before.items())
    _assert_rows(spark.table(name), [("new", 10)], ["b", "c"])
    source.writeTo(name).append()
    assert _find_latest_metadata(path)["format-version"] == 1
    _assert_rows(spark.table(name), [("new", 10)] * 2, ["b", "c"])


@pytest.mark.parametrize("api", ["sql", "replace", "createOrReplace"])
@pytest.mark.parametrize(("version", "requested"), [(2, 1), (3, 2)])
def test_iceberg_replacement_rejects_format_downgrade(spark, replacement_target, api, version, requested):
    _create_unpartitioned(spark, replacement_target, version)
    before = _visible_state(spark, replacement_target)
    source = spark.createDataFrame([("new", 10)], "b STRING, c BIGINT")
    with pytest.raises(PySparkException, match="cannot downgrade Iceberg table format version"):
        _replace(spark, replacement_target, source, api, properties={"format-version": str(requested)})
    assert _visible_state(spark, replacement_target) == before
    # A rejected version request must not reserve or freeze the catalog name.
    _replace(spark, replacement_target, source, api)
    assert _find_latest_metadata(replacement_target[1])["format-version"] == version


@pytest.mark.parametrize("api", ["sql", "replace", "createOrReplace"])
@pytest.mark.parametrize(("version", "requested"), [(1, 2), (2, 3)])
def test_iceberg_replacement_upgrades_explicit_format_version(spark, replacement_target, api, version, requested):
    _create_unpartitioned(spark, replacement_target, version)
    name, path = replacement_target
    before = _find_latest_metadata(path)
    source = spark.createDataFrame([("new", 10)], "b STRING, c BIGINT")
    _replace(spark, replacement_target, source, api, properties={"format-version": str(requested)})
    after = _find_latest_metadata(path)
    assert after["format-version"] == requested
    assert "format-version" not in after["properties"]
    assert dict(spark.sql(f"SHOW TBLPROPERTIES {name}").collect())["format-version"] == str(requested)
    _assert_replacement_history(before, after)
    source.writeTo(name).append()
    _assert_rows(spark.table(name), [("new", 10)] * 2, ["b", "c"])


@pytest.mark.parametrize("api", ["sql", "replace"])
@pytest.mark.parametrize("override", [False, True], ids=["empty-delta", "explicit-override"])
def test_iceberg_replacement_preserves_authoritative_property_changes(spark, replacement_target, api, override):
    _create(spark, replacement_target)
    name, path = replacement_target
    original = _find_latest_metadata(path)
    visible_before = dict(spark.sql(f"SHOW TBLPROPERTIES {name}").collect())
    assert original["properties"]["keep"] == visible_before["keep"] == "old"
    assert original["properties"]["change"] == visible_before["change"] == "old"
    # An independent registration leaves the original name's bookkeeping stale.
    reference = f"{name}_external"
    try:
        spark.sql(f"CREATE TABLE {reference} USING iceberg LOCATION '{escape_sql_string_literal(str(path))}'")
        spark.sql(f"ALTER TABLE {reference} SET TBLPROPERTIES ('keep'='external')")
        spark.sql(f"ALTER TABLE {reference} UNSET TBLPROPERTIES ('change')")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {reference}")
    # SHOW reads authoritative metadata even though registration properties are stale.
    visible_mutated = dict(spark.sql(f"SHOW TBLPROPERTIES {name}").collect())
    assert visible_mutated == {key: value for key, value in visible_before.items() if key != "change"} | {
        "keep": "external"
    }
    before = _find_latest_metadata(path)
    assert before["properties"]["keep"] == "external"
    assert "change" not in before["properties"]
    assert before["table-uuid"] == original["table-uuid"]
    assert before["snapshots"] == original["snapshots"]
    source = spark.createDataFrame([("new", 10)], "b STRING, c BIGINT")
    properties = {"keep": "requested", "change": "requested"} if override else None
    _replace(spark, replacement_target, source, api, properties=properties)
    after = _find_latest_metadata(path)
    _assert_replacement_history(before, after)
    assert after["properties"]["keep"] == ("requested" if override else "external")
    assert after["properties"].get("change") == ("requested" if override else None)
    visible = dict(spark.sql(f"SHOW TBLPROPERTIES {name}").collect())
    assert visible["keep"] == after["properties"]["keep"]
    assert visible.get("change") == after["properties"].get("change")
    _assert_rows(spark.table(name), [("new", 10)], ["b", "c"])


@pytest.mark.parametrize("api", APIS)
@pytest.mark.parametrize("version", [2, 3])
@pytest.mark.parametrize("invalid", ["missing-source", "invalid-transform"])
def test_iceberg_replacement_rejects_unbindable_historical_sort_order(spark, replacement_target, api, version, invalid):
    # Historical sort orders must remain bindable even when the new default is unsorted.
    _create_unpartitioned(spark, replacement_target, version)
    name, _ = replacement_target
    spark.sql(f"ALTER TABLE {name} WRITE ORDERED BY bucket(4, a)")
    spark.sql(f"ALTER TABLE {name} WRITE UNORDERED")
    before = _visible_state(spark, replacement_target)
    source = (
        spark.createDataFrame([("new", 10)], "b STRING, c BIGINT")
        if invalid == "missing-source"
        else spark.createDataFrame([(True, "new")], "a BOOLEAN, b STRING")
    )
    with pytest.raises(PySparkException, match="unsupported Iceberg replacement: historical sort order"):
        _replace(spark, replacement_target, source, api)
    assert _visible_state(spark, replacement_target) == before
    spark.createDataFrame([(5, 6)], "a INT, b INT").writeTo(name).append()
    _assert_rows(spark.table(name), [(1, 2), (3, 4), (5, 6)], ["a", "b"])


@pytest.mark.parametrize("api", APIS)
@pytest.mark.parametrize("version", [2, 3])
def test_iceberg_replacement_retains_bindable_historical_sort_order(spark, replacement_target, api, version):
    name, path = replacement_target
    spark.sql(
        f"CREATE TABLE {name} (a INT, b INT, c BIGINT) USING iceberg PARTITIONED BY (a) "
        f"LOCATION '{escape_sql_string_literal(str(path))}' TBLPROPERTIES ('format-version'='{version}')"
    )
    spark.sql(f"ALTER TABLE {name} WRITE ORDERED BY c")
    spark.sql(f"INSERT INTO {name} VALUES (1, 2, 3)")
    before = _find_latest_metadata(path)
    source = spark.createDataFrame([("new", 10)], "b STRING, c BIGINT")
    _replace(spark, replacement_target, source, api)
    after = _find_latest_metadata(path)
    _assert_replacement_history(before, after)
    assert after["default-sort-order-id"] == 0
    assert _current_spec(after)["fields"] == []
    source.writeTo(name).append()
    _assert_rows(spark.table(name), [("new", 10)] * 2, ["b", "c"])


@pytest.mark.parametrize("api", APIS)
def test_iceberg_replacement_resets_main_retention(spark, replacement_target, api):
    # Spark 4.1 / Iceberg 1.12 replacement creates a new main reference, not a retained branch.
    _create(spark, replacement_target)
    name, path = replacement_target
    spark.sql(f"ALTER TABLE {name} REPLACE BRANCH main WITH SNAPSHOT RETENTION 3 SNAPSHOTS 2 DAYS")
    before = _find_latest_metadata(path)
    min_snapshots = 3
    max_snapshot_age_ms = 2 * 24 * 60 * 60 * 1000
    assert before["refs"]["main"]["min-snapshots-to-keep"] == min_snapshots
    assert before["refs"]["main"]["max-snapshot-age-ms"] == max_snapshot_age_ms
    source = spark.createDataFrame([(10,)], "c BIGINT")
    _replace(spark, replacement_target, source, api)
    after = _find_latest_metadata(path)
    assert after["refs"]["main"] == {"snapshot-id": after["current-snapshot-id"], "type": "branch"}
    _assert_replacement_history(before, after)
