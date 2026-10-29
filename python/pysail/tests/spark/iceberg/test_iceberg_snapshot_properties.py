"""Per-write snapshot annotations."""

import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.steps.iceberg import _current_snapshot, _find_latest_metadata
from pysail.testing.spark.utils.sql import escape_sql_string_literal

_RESERVED_KEYS = (
    "operation",
    "replace-partitions",
    "added-data-files",
    "deleted-data-files",
    "total-data-files",
    "added-records",
    "deleted-records",
    "total-records",
    "added-delete-files",
    "removed-delete-files",
    "total-delete-files",
    "added-position-delete-files",
    "removed-position-delete-files",
    "added-position-deletes",
    "removed-position-deletes",
    "total-position-deletes",
    "added-equality-delete-files",
    "removed-equality-delete-files",
    "added-equality-deletes",
    "removed-equality-deletes",
    "total-equality-deletes",
    "added-dvs",
    "removed-dvs",
    "total-files-size",
)

_OPTIONS = {
    "snapshot-property.run-id": "FirstRun",
    "snapshot-property.Run.ID": "CaseValue",
    "SNAPSHOT-PROPERTY.Prefix": "UpperPrefix",
    "snapshot-property.empty": "",
    "snapshot-property.snapshot-property.inner": " OnePrefix ",
    "snapshot-property.added-custom": "allowed",
    "snapshot-property.total-custom": "allowed",
}
_ANNOTATIONS = {
    "run-id": "FirstRun",
    "run.id": "CaseValue",
    "prefix": "UpperPrefix",
    "empty": "",
    "snapshot-property.inner": " OnePrefix ",
    "added-custom": "allowed",
    "total-custom": "allowed",
}


def _assert_annotations(spark, location, table, expected, previous=(), *, added_rows, total_rows, operation):
    metadata = _find_latest_metadata(location)
    snapshot = _current_snapshot(metadata)
    summary = snapshot["summary"]
    assert {key: summary[key] for key in expected} == expected
    assert not (set(_ANNOTATIONS) - set(expected)) & summary.keys()
    assert "Run.ID" not in summary
    assert "snapshot-property.run-id" not in summary
    assert summary["operation"] == operation
    assert summary["total-records"] == str(total_rows)
    assert summary["total-delete-files"] == "0"
    assert summary["total-position-deletes"] == "0"
    assert summary["total-equality-deletes"] == "0"
    if added_rows:
        assert summary["added-records"] == str(added_rows)
        assert int(summary["added-data-files"]) > 0
        assert int(summary["total-data-files"]) > 0
    else:
        assert summary["total-data-files"] == "0"
    # Existing summary objects, not just their custom keys, must remain unchanged.
    snapshots = {item["snapshot-id"]: item for item in metadata["snapshots"]}
    for old in previous:
        assert snapshots[old["snapshot-id"]] == old
    property_maps = [metadata.get("properties", {})]
    if table is not None:
        property_maps.append(dict(spark.sql(f"SHOW TBLPROPERTIES {table}").collect()))
    for properties in property_maps:
        assert not any(
            key.lower().startswith(("snapshot-property.", "option.snapshot-property.")) for key in properties
        )
    return snapshot


def _v1_write(df, entry, table, location, mode, options):
    writer = df.write.format("iceberg").mode(mode).options(**options)
    if entry == "path":
        writer.save(str(location))
    else:
        writer.option("path", str(location)).saveAsTable(table)


@pytest.mark.parametrize("entry", ["path", "table"])
@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_v1_snapshot_properties_are_scoped_to_each_write(spark, tmp_path, entry, mode):
    table = "snapshot_properties_v1"
    location = tmp_path / table
    catalog_table = table if entry == "table" else None
    try:
        first = spark.createDataFrame([(1,), (2,)], "id BIGINT").repartition(4)
        _v1_write(first, entry, table, location, "append", _OPTIONS)
        history = [
            _assert_annotations(
                spark, location, catalog_table, _ANNOTATIONS, added_rows=2, total_rows=2, operation="append"
            )
        ]
        for value, annotations in [(3, {}), (4, {"run-id": "ThirdRun"})]:
            df = spark.createDataFrame([(value,)], "id BIGINT").repartition(4)
            options = {f"snapshot-property.{key}": item for key, item in annotations.items()}
            _v1_write(df, entry, table, location, mode, options)
            history.append(
                _assert_annotations(
                    spark,
                    location,
                    catalog_table,
                    annotations,
                    history,
                    added_rows=1,
                    total_rows=value if mode == "append" else 1,
                    operation=mode,
                )
            )
        rows = spark.read.format("iceberg").load(str(location)).orderBy("id").collect()
        assert [row.id for row in rows] == ([1, 2, 3, 4] if mode == "append" else [4])
    finally:
        if catalog_table is not None:
            spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("operation", ["append", "replace", "predicate", "dynamic"])
def test_v2_snapshot_properties_cover_create_and_existing_writes(spark, tmp_path, operation):
    table = "snapshot_properties_v2"
    location = tmp_path / table
    schema = "id BIGINT, category STRING"
    try:
        df = spark.createDataFrame([(1, "A"), (2, "B")], schema).repartition(4)
        (
            df.writeTo(table)
            .using("iceberg")
            .option("path", str(location))
            .partitionedBy("category")
            .options(**_OPTIONS)
            .create()
        )
        history = [
            _assert_annotations(spark, location, table, _ANNOTATIONS, added_rows=2, total_rows=2, operation="append")
        ]
        for value, annotations in [(3, {}), (4, {"run-id": "ThirdRun"})]:
            df = spark.createDataFrame([(value, "A")], schema).repartition(4)
            writer = df.writeTo(table).options(
                **{f"snapshot-property.{key}": item for key, item in annotations.items()}
            )
            if operation == "replace":
                writer.using("iceberg").option("path", str(location)).partitionedBy("category").replace()
            elif operation == "predicate":
                writer.overwrite(F.col("category") == "A")
            elif operation == "dynamic":
                writer.overwritePartitions()
            else:
                writer.append()
            history.append(
                _assert_annotations(
                    spark,
                    location,
                    table,
                    annotations,
                    history,
                    added_rows=1,
                    total_rows=value if operation == "append" else (1 if operation == "replace" else 2),
                    operation="append" if operation == "append" else "overwrite",
                )
            )
        rows = [tuple(row) for row in spark.table(table).orderBy("id").collect()]
        expected = [(1, "A"), (2, "B"), (3, "A"), (4, "A")]
        if operation != "append":
            expected = [(4, "A")] if operation == "replace" else [(2, "B"), (4, "A")]
        assert rows == expected
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("key", [*_RESERVED_KEYS, pytest.param("", id="empty-suffix")])
def test_snapshot_properties_reject_invalid_keys_before_creating_table(spark, tmp_path, key):
    # Sail rejects all engine-owned keys and empty suffixes, deliberately stricter than Spark.
    table = "snapshot_properties_invalid"
    location = tmp_path / table
    try:
        with pytest.raises(Exception, match=r"snapshot property .*?(reserved|empty)"):
            (
                spark.range(1)
                .writeTo(table)
                .using("iceberg")
                .option("path", str(location))
                .option(f"snapshot-property.{key}", "999")
                .create()
            )
        assert not spark.catalog.tableExists(table)
        assert not [path for path in location.rglob("*") if path.is_file()]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("entry", ["path", "table", "v2-append", "v2-replace"])
def test_invalid_snapshot_properties_do_not_mutate_existing_tables(spark, tmp_path, entry):
    table = "snapshot_properties_protected"
    location = tmp_path / table
    try:
        spark.range(2).writeTo(table).using("iceberg").option("path", str(location)).create()
        before = {path.relative_to(location): path.read_bytes() for path in location.rglob("*") if path.is_file()}
        replacement = spark.range(5, 6)
        options = {"snapshot-property.OPERATION": "override"}

        def write_invalid_properties():
            if entry.startswith("v2"):
                writer = replacement.writeTo(table).options(**options)
                if entry == "v2-replace":
                    writer.using("iceberg").option("path", str(location)).replace()
                else:
                    writer.append()
            else:
                _v1_write(replacement, entry, table, location, "overwrite", options)

        with pytest.raises(Exception, match="snapshot property 'operation' is reserved"):
            write_invalid_properties()
        after = {path.relative_to(location): path.read_bytes() for path in location.rglob("*") if path.is_file()}
        assert after == before
        assert [row.id for row in spark.table(table).orderBy("id").collect()] == [0, 1]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("existing_empty_table", [False, True], ids=["new-path", "existing-empty-table"])
def test_snapshot_properties_reach_empty_bootstrap_writes(spark, tmp_path, existing_empty_table):
    table = "snapshot_properties_empty"
    location = tmp_path / table
    try:
        if existing_empty_table:
            escaped = escape_sql_string_literal(str(location))
            spark.sql(f"CREATE TABLE {table} (id BIGINT) USING iceberg LOCATION '{escaped}'")
        df = spark.createDataFrame([], "id BIGINT").repartition(4)
        if existing_empty_table:
            df.writeTo(table).options(**_OPTIONS).append()
        else:
            df.write.format("iceberg").options(**_OPTIONS).save(str(location))
        _assert_annotations(
            spark,
            location,
            table if existing_empty_table else None,
            _ANNOTATIONS,
            added_rows=0,
            total_rows=0,
            operation="append",
        )
        assert spark.read.format("iceberg").load(str(location)).collect() == []
    finally:
        if existing_empty_table:
            spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("operation", ["dynamic", "ignore"])
def test_snapshot_properties_do_not_force_noop_snapshots(spark, tmp_path, operation):
    table = "snapshot_properties_noop"
    location = tmp_path / table
    try:
        spark.range(2).writeTo(table).using("iceberg").option("path", str(location)).create()
        before = _find_latest_metadata(location)
        files_before = set(location.rglob("*"))
        if operation == "dynamic":
            spark.range(0).repartition(4).writeTo(table).options(**_OPTIONS).overwritePartitions()
        else:
            spark.range(1).write.format("iceberg").mode("ignore").options(**_OPTIONS).save(str(location))
        assert _find_latest_metadata(location) == before
        assert set(location.rglob("*")) == files_before
        assert [row.id for row in spark.table(table).orderBy("id").collect()] == [0, 1]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
