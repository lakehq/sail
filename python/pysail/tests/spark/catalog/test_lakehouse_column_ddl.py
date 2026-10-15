# ruff: noqa: S608
import json

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Requires lakehouse extensions")


@pytest.fixture(params=["delta", "iceberg"])
def lake_table(spark, tmp_path, request):
    fmt = request.param
    name = f"ddl_columns_{fmt}"
    location = tmp_path / fmt
    properties = "TBLPROPERTIES ('delta.columnMapping.mode'='name')" if fmt == "delta" else ""
    spark.sql(
        f"CREATE TABLE {name} (id INT NOT NULL, value STRING, nested STRUCT<a: INT, b: STRING>) "
        f"USING {fmt} LOCATION '{location.as_uri()}' {properties}"
    )
    spark.sql(f"INSERT INTO {name} VALUES (1, 'old', named_struct('a', 10, 'b', 'nested'))")
    try:
        yield name, location, fmt
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def metadata_schema(location, fmt):
    if fmt == "delta":
        metadata = None
        for log in sorted((location / "_delta_log").glob("*.json")):
            for line in log.read_text().splitlines():
                metadata = json.loads(line).get("metaData", metadata)
        return json.loads(metadata["schemaString"])
    from pysail.testing.spark.steps.iceberg import _find_latest_metadata

    metadata = _find_latest_metadata(location)
    return next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])


def identity(field, fmt):
    if fmt == "delta":
        return field["metadata"]["delta.columnMapping.id"], field["metadata"]["delta.columnMapping.physicalName"]
    return field["id"]


def test_column_evolution_preserves_existing_data_and_ids(spark, lake_table):
    table, location, fmt = lake_table
    before = metadata_schema(location, fmt)
    old_value_id = identity(before["fields"][1], fmt)
    spark.sql(f"ALTER TABLE {table} RENAME COLUMN value TO renamed")
    spark.sql(f"ALTER TABLE {table} ADD COLUMNS (extra BIGINT COMMENT 'new column' AFTER id)")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN renamed FIRST")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN renamed COMMENT 'updated comment'")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN id DROP NOT NULL")
    assert spark.table(table).columns == ["renamed", "id", "extra", "nested"]
    assert spark.table(table).schema["id"].nullable
    assert spark.table(table).first().asDict(recursive=True) == {
        "renamed": "old",
        "id": 1,
        "extra": None,
        "nested": {"a": 10, "b": "nested"},
    }
    spark.sql(f"INSERT INTO {table} (id, renamed, extra) VALUES (2, 'new', 20)")
    spark.sql(f"UPDATE {table} SET extra = 11 WHERE id = 1")
    after = metadata_schema(location, fmt)
    assert identity(after["fields"][0], fmt) == old_value_id
    field = after["fields"][0]
    assert (field["metadata"]["comment"] if fmt == "delta" else field["doc"]) == "updated comment"
    assert [tuple(r) for r in spark.sql(f"SELECT id, renamed, extra FROM {table} ORDER BY id").collect()] == [
        (1, "old", 11),
        (2, "new", 20),
    ]
    spark.sql(f"ALTER TABLE {table} DROP COLUMNS (extra, nested)")
    assert [tuple(r) for r in spark.table(table).orderBy("id").collect()] == [("old", 1), ("new", 2)]
    spark.sql(f"ALTER TABLE {table} DROP COLUMN IF EXISTS absent")
    # A dropped name can be reused, but never its old field identity or values.
    spark.sql(f"ALTER TABLE {table} ADD COLUMN extra STRING")
    assert [r.extra for r in spark.table(table).collect()] == [None, None]


def test_nested_column_evolution(spark, lake_table):
    table, location, fmt = lake_table
    before = metadata_schema(location, fmt)
    old = before["fields"][2]["type"]["fields"][0]
    spark.sql(f"ALTER TABLE {table} RENAME COLUMN nested.a TO renamed")
    spark.sql(f"ALTER TABLE {table} ADD COLUMN nested.c BIGINT FIRST")
    spark.sql(f"ALTER TABLE {table} DROP COLUMN nested.b")
    assert spark.sql(f"SELECT nested FROM {table}").first().asDict(recursive=True) == {
        "nested": {"c": None, "renamed": 10}
    }
    after = metadata_schema(location, fmt)
    renamed = after["fields"][2]["type"]["fields"][1]
    assert identity(old, fmt) == identity(renamed, fmt)


@pytest.mark.parametrize(
    ("statement", "error"),
    [
        ("ADD COLUMNS (fresh INT, id INT)", "already exists"),
        ("RENAME COLUMN value TO id", "already exists"),
        ("DROP COLUMNS (value, absent)", "does not exist"),
        ("ALTER COLUMN value SET NOT NULL", "nullability"),
        ("ALTER COLUMN value AFTER value", "itself"),
        ("ADD COLUMN required INT NOT NULL", "required column"),
    ],
)
def test_invalid_column_change_is_atomic(spark, lake_table, statement, error):
    table, location, fmt = lake_table
    before = metadata_schema(location, fmt)
    with pytest.raises(Exception, match=error):
        spark.sql(f"ALTER TABLE {table} {statement}")
    assert metadata_schema(location, fmt) == before
    assert spark.table(table).count() == 1


def test_column_comments_remain_strings(spark, lake_table):
    table, location, fmt = lake_table
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN value COMMENT '123'")
    spark.sql(f"ALTER TABLE {table} ADD COLUMN extra INT COMMENT 'true'")
    spark.sql(f"ALTER TABLE {table} RENAME COLUMN value TO renamed")
    fields = {field["name"]: field for field in metadata_schema(location, fmt)["fields"]}
    assert (fields["renamed"]["metadata"]["comment"] if fmt == "delta" else fields["renamed"]["doc"]) == "123"
    assert (fields["extra"]["metadata"]["comment"] if fmt == "delta" else fields["extra"]["doc"]) == "true"


def test_partition_column_rename_preserves_reads_and_writes(spark, tmp_path):
    table = "partition_rename"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, part STRING) USING delta PARTITIONED BY (part) LOCATION '{tmp_path.as_uri()}' TBLPROPERTIES ('delta.columnMapping.mode'='name')"
        )
        spark.sql(f"INSERT INTO {table} VALUES (1, 'a')")
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN part TO renamed")
        spark.sql(f"INSERT INTO {table} VALUES (2, 'b')")
        assert [tuple(r) for r in spark.table(table).orderBy("id").collect()] == [(1, "a"), (2, "b")]
        assert next(c for c in spark.catalog.listColumns(table) if c.name == "renamed").isPartition
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_iceberg_nested_column_type_promotion(spark, tmp_path):
    table = "nested_type_promotion"
    try:
        spark.sql(
            f"CREATE TABLE {table} (payload STRUCT<value:INT>, items ARRAY<STRUCT<value:INT>>) USING iceberg LOCATION '{tmp_path.as_uri()}'"
        )
        spark.sql(f"INSERT INTO {table} SELECT named_struct('value', 1), array(named_struct('value', 2))")
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN payload.value TYPE BIGINT")
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN items.element.value TYPE BIGINT")
        assert (
            spark.table(table).schema.simpleString()
            == "struct<payload:struct<value:bigint>,items:array<struct<value:bigint>>>"
        )
        assert spark.table(table).first().asDict(recursive=True) == {"payload": {"value": 1}, "items": [{"value": 2}]}
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_iceberg_name_mapping_tracks_renames_and_reused_names(spark, tmp_path):
    table = "mapping_evolution"
    mapping = json.dumps([{"field-id": 1, "names": ["id", "old_id"]}])
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT) USING iceberg LOCATION '{tmp_path.as_uri()}' "
            f"TBLPROPERTIES ('schema.name-mapping.default'='{mapping}')"
        )
        spark.sql(f"INSERT INTO {table} VALUES (1)")
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN id TO renamed")
        spark.sql(f"ALTER TABLE {table} ADD COLUMN id STRING")
        from pysail.testing.spark.steps.iceberg import _find_latest_metadata

        metadata = _find_latest_metadata(tmp_path)
        mapping = json.loads(metadata["properties"]["schema.name-mapping.default"])
        assert mapping == [
            {"field-id": 1, "names": ["old_id", "renamed"]},
            {"field-id": 2, "names": ["id"]},
        ]
        assert tuple(spark.table(table).first()) == (1, None)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_table_rename_preserves_storage_and_schema(spark, lake_table):
    table, location, fmt = lake_table
    renamed = table + "_renamed"
    before = metadata_schema(location, fmt)
    try:
        spark.sql(f"ALTER TABLE {table} RENAME TO {renamed}")
        assert not spark.catalog.tableExists(table)
        assert spark.table(renamed).first().value == "old"
        assert metadata_schema(location, fmt) == before
        spark.sql(f"CREATE TABLE {table} (id INT) USING {fmt} LOCATION '{(location.parent / 'collision').as_uri()}'")
        with pytest.raises(Exception, match="already exists"):
            spark.sql(f"ALTER TABLE {renamed} RENAME TO {table}")
        assert spark.table(renamed).count() == 1
        spark.sql(f"DROP TABLE {table}")
        spark.sql(f"ALTER TABLE {renamed} RENAME TO {table}")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {renamed}")


def test_delta_statistics_columns_follow_renames_and_drops(spark, tmp_path):
    table = "delta_stats_evolution"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, payload STRUCT<value:INT>, other STRING) USING delta "
            f"LOCATION '{tmp_path.as_uri()}' TBLPROPERTIES ('delta.columnMapping.mode'='name', "
            "'delta.dataSkippingStatsColumns'='id,payload.value,other')"
        )
        spark.sql(f"INSERT INTO {table} VALUES (1, named_struct('value', 10), 'old')")
        spark.sql(f"ALTER TABLE {table} RENAME COLUMN payload TO `new.payload`")
        spark.sql(f"ALTER TABLE {table} DROP COLUMN other")
        description = {r.col_name: r.data_type for r in spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()}
        properties = dict(item.split("=", 1) for item in description["Table Properties"].strip("[]").split(", "))
        assert properties["delta.dataSkippingStatsColumns"] == "`id`,`new.payload`.`value`"
        spark.sql(f"INSERT INTO {table} VALUES (2, named_struct('value', 20))")
        assert [tuple(r) for r in spark.sql(f"SELECT id, `new.payload`.value FROM {table} ORDER BY id").collect()] == [
            (1, 10),
            (2, 20),
        ]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("fmt", ["delta", "iceberg"])
@pytest.mark.parametrize(
    ("definition", "value", "column"),
    [
        ("Value INT", "2", "value"),
        ("Value STRUCT<Leaf: INT, other: INT>", "named_struct('Leaf', 2, 'other', 3)", "Value.leaf"),
    ],
)
def test_drop_column_respects_case_sensitive_resolution(spark, tmp_path, fmt, definition, value, column):
    table = "case_sensitive_drop"
    key = "spark.sql.caseSensitive"
    previous = spark.conf.get(key)
    try:
        properties = "TBLPROPERTIES ('delta.columnMapping.mode'='name')" if fmt == "delta" else ""
        spark.sql(
            f"CREATE TABLE {table} (id INT, {definition}) USING {fmt} LOCATION '{tmp_path.as_uri()}' {properties}"
        )
        spark.sql(f"INSERT INTO {table} VALUES (1, {value})")
        before = metadata_schema(tmp_path, fmt)
        rows = spark.table(table).collect()
        spark.conf.set(key, "true")

        with pytest.raises(Exception, match=r"(?i)not exist|not found|cannot be resolved"):
            spark.sql(f"ALTER TABLE {table} DROP COLUMN {column}")

        assert metadata_schema(tmp_path, fmt) == before
        assert spark.table(table).collect() == rows
    finally:
        spark.conf.set(key, previous)
        spark.sql(f"DROP TABLE IF EXISTS {table}")
