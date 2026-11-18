# SPDX-FileComment: Credit: https://github.com/apache/iceberg/blob/212aa894e919ff9fdf89160da4aadcbe6858ac01/core/src/test/java/org/apache/iceberg/TestRemoveSnapshots.java#L342-L594

# ruff: noqa: S608

from datetime import datetime, timezone

import pytest
from pyspark.errors import PySparkException

from pysail.testing._dev.spark.steps.iceberg import _find_latest_metadata
from pysail.testing.spark.session import spark_connect_server, spark_session_factory
from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.iceberg.test_iceberg_expire_snapshots import FUTURE, ZERO, expire
from pysail.tests._dev.spark.iceberg.test_iceberg_procedures import _edit_latest_metadata


@pytest.fixture(params=[1, 2, 3])
def history(spark, tmp_path, request):
    name = "expiration_retention"
    path = tmp_path / name
    spark.sql(
        f"CREATE TABLE {name} (id BIGINT) USING iceberg LOCATION '{escape_sql_string_literal(str(path))}' "
        f"TBLPROPERTIES ('format-version'='{request.param}')"
    )
    try:
        yield name, path
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def append_history(spark, history, count=4):
    name, path = history
    for value in range(count):
        spark.sql(f"INSERT INTO {name} VALUES ({value})")

    def age(metadata):
        for index, snapshot in enumerate(metadata["snapshots"]):
            snapshot["timestamp-ms"] = 1_700_000_000_000 + index * 1000
        for index, entry in enumerate(metadata["snapshot-log"]):
            entry["timestamp-ms"] = 1_700_000_000_000 + index * 1000

    _edit_latest_metadata(path, age)
    return _find_latest_metadata(path)


def snapshot_ids(path):
    return [snapshot["snapshot-id"] for snapshot in _find_latest_metadata(path)["snapshots"]]


@pytest.mark.parametrize("retain", [1, 2, 4, 5])
def test_retention_count_and_strict_timestamp_boundary(spark, history, retain):
    name, path = history
    before = append_history(spark, history)
    ids = [snapshot["snapshot-id"] for snapshot in before["snapshots"]]
    cutoff = datetime.fromtimestamp(before["snapshots"][2]["timestamp-ms"] / 1000, tz=timezone.utc)
    counts = expire(spark, name, f", older_than => TIMESTAMP '{cutoff.isoformat()}', retain_last => {retain}")
    removed = min(2, max(0, len(ids) - retain))
    assert snapshot_ids(path) == ids[removed:]
    assert counts == (0, 0, 0, 0, removed, 0)
    assert [row.id for row in spark.table(name).orderBy("id").collect()] == list(range(4))


@pytest.mark.parametrize(
    ("property_name", "value", "remaining"),
    [
        ("history.expire.min-snapshots-to-keep", "4", 4),
        ("history.expire.max-snapshot-age-ms", "1", 1),
    ],
)
def test_default_retention_properties(spark, history, property_name, value, remaining):
    name, path = history
    append_history(spark, history)
    ids = snapshot_ids(path)
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('{property_name}'='{value}')")
    counts = expire(spark, name)
    assert snapshot_ids(path) == ids[-remaining:]
    assert counts == (0, 0, 0, 0, len(ids) - remaining, 0)


@pytest.mark.parametrize("style", ["positional", "named", "comments", "stream", "concurrent"])
def test_procedure_argument_forms(spark, history, style):
    name, path = history
    append_history(spark, history)
    queries = {
        "positional": f"CALL system.expire_snapshots('{name}', {FUTURE}, 1)",
        "named": f"CALL system.expire_snapshots(older_than => {FUTURE}, table => '{name}')",
        "comments": f"/* before */ -- line\n CALL /* routine */ system.expire_snapshots('{name}', {FUTURE})",
        "stream": f"CALL system.expire_snapshots('{name}', {FUTURE}, stream_results => true)",
        "concurrent": f"CALL system.expire_snapshots('{name}', {FUTURE}, max_concurrent_deletes => 4)",
    }
    assert tuple(spark.sql(queries[style]).first()) == (0, 0, 0, 0, 3, 0)
    assert len(snapshot_ids(path)) == 1
    assert [row.id for row in spark.table(name).orderBy("id").collect()] == list(range(4))


@pytest.mark.parametrize("kind", ["tag", "branch"])
@pytest.mark.parametrize("aged", [False, True])
def test_reference_head_expiration_and_file_protection(spark, history, kind, aged):
    name, path = history
    spark.sql(f"INSERT INTO {name} VALUES (1)")
    old_id = _find_latest_metadata(path)["current-snapshot-id"]
    old_data = {file: file.read_bytes() for file in path.rglob("*.parquet")}
    spark.sql(f"ALTER TABLE {name} CREATE {kind.upper()} audit AS OF VERSION {old_id}")
    spark.sql(f"INSERT OVERWRITE {name} VALUES (2)")

    def configure(metadata):
        metadata["snapshots"][0]["timestamp-ms"] = 1_700_000_000_000
        if aged:
            metadata["refs"]["audit"]["max-ref-age-ms"] = 1

    _edit_latest_metadata(path, configure)
    counts = expire(spark, name, f", older_than => {FUTURE}")
    after = _find_latest_metadata(path)
    assert ("audit" in after["refs"]) is not aged
    assert (old_id in snapshot_ids(path)) is not aged
    assert counts[0] == (len(old_data) if aged else 0)
    if aged:
        assert all(not file.exists() for file in old_data)
    else:
        assert all(file.read_bytes() == data for file, data in old_data.items())
        with pytest.raises(Exception, match="Still referenced"):
            expire(spark, name, f", snapshot_ids => array({old_id}L)")
        assert _find_latest_metadata(path) == after
    assert [row.id for row in spark.table(name).collect()] == [2]


@pytest.mark.parametrize("kind", ["tag", "branch"])
def test_tag_and_branch_have_different_ancestry_retention(spark, history, kind):
    name, path = history
    append_history(spark, history, count=3)
    ids = snapshot_ids(path)
    spark.sql(f"ALTER TABLE {name} CREATE {kind.upper()} audit AS OF VERSION {ids[-1]}")
    spark.sql(f"CALL system.set_current_snapshot('{name}', {ids[0]})").collect()

    def configure(metadata):
        if kind == "branch":
            metadata["refs"]["audit"]["max-snapshot-age-ms"] = 9_223_372_036_854_775_807

    _edit_latest_metadata(path, configure)
    expire(spark, name, f", older_than => {FUTURE}")
    assert snapshot_ids(path) == (ids if kind == "branch" else [ids[0], ids[-1]])
    assert [row.id for row in spark.table(name).collect()] == [0]
    rows = spark.read.format("iceberg").option("snapshot-id", ids[-1]).load(str(path)).orderBy("id").collect()
    assert [row.id for row in rows] == [0, 1, 2]


def test_rollback_preserves_current_files_and_removes_abandoned_data(spark, history):
    name, path = history
    spark.sql(f"INSERT INTO {name} VALUES (1)")
    first = _find_latest_metadata(path)["current-snapshot-id"]
    retained = {file: file.read_bytes() for file in path.rglob("*.parquet")}
    spark.sql(f"INSERT OVERWRITE {name} VALUES (2)")
    abandoned = set(path.rglob("*.parquet")) - retained.keys()
    spark.sql(f"CALL system.rollback_to_snapshot('{name}', {first})").collect()
    counts = expire(spark, name, f", older_than => {FUTURE}")
    assert snapshot_ids(path) == [first]
    assert counts[0] == len(abandoned)
    assert all(not file.exists() for file in abandoned)
    assert all(file.read_bytes() == data for file, data in retained.items())
    assert [row.id for row in spark.table(name).collect()] == [1]
    assert expire(spark, name, f", older_than => {FUTURE}") == ZERO


@pytest.mark.parametrize("key", ["statistics", "partition-statistics"])
@pytest.mark.parametrize("reuse", [False, True])
def test_statistics_reuse_preserves_shared_files(spark, history, key, reuse):
    name, path = history
    append_history(spark, history, count=2)
    ids = snapshot_ids(path)
    old = path / "metadata" / "old.stats"
    current = old if reuse else path / "metadata" / "current.stats"
    old.write_bytes(b"old-statistics")
    current.write_bytes(b"current-statistics")

    def configure(metadata):
        entries = []
        for snapshot_id, file in zip(ids, [old, current], strict=True):
            entry = {
                "snapshot-id": snapshot_id,
                "statistics-path": str(file),
                "file-size-in-bytes": file.stat().st_size,
            }
            if key == "statistics":
                entry.update({"file-footer-size-in-bytes": 0, "blob-metadata": []})
            entries.append(entry)
        metadata[key] = entries

    _edit_latest_metadata(path, configure)
    counts = expire(spark, name, f", older_than => {FUTURE}")
    assert counts[5] == (0 if reuse else 1)
    assert old.exists() is reuse
    assert current.read_bytes() == b"current-statistics"
    assert [entry["snapshot-id"] for entry in _find_latest_metadata(path)[key]] == ids[-1:]


@pytest.mark.parametrize("populated", [False, True])
@pytest.mark.parametrize("clean", [False, True])
def test_metadata_cleanup_keeps_current_schema_and_default_spec(spark, history, populated, clean):
    name, path = history
    if populated:
        spark.sql(f"INSERT INTO {name} VALUES (1)")
    spark.sql(f"ALTER TABLE {name} ADD COLUMN extra STRING")
    spark.sql(f"ALTER TABLE {name} ADD PARTITION FIELD bucket(4, id) AS id_bucket")
    if populated:
        spark.sql(f"INSERT OVERWRITE {name} VALUES (2, 'current')")
    before = _find_latest_metadata(path)
    expire(spark, name, f", older_than => {FUTURE}, clean_expired_metadata => {str(clean).lower()}")
    after = _find_latest_metadata(path)
    expected_schemas = [before["current-schema-id"]] if clean else [s["schema-id"] for s in before["schemas"]]
    assert [s["schema-id"] for s in after["schemas"]] == expected_schemas
    # Some retained manifests may still use the old spec; those specs must survive.
    assert before["default-spec-id"] in [s["spec-id"] for s in after["partition-specs"]]
    if not populated:
        expected_specs = [before["default-spec-id"]] if clean else [s["spec-id"] for s in before["partition-specs"]]
        assert [s["spec-id"] for s in after["partition-specs"]] == expected_specs
    assert [tuple(row) for row in spark.table(name).collect()] == ([(2, "current")] if populated else [])
    assert expire(spark, name, f", older_than => {FUTURE}, clean_expired_metadata => {str(clean).lower()}") == ZERO
    assert _find_latest_metadata(path) == after


@pytest.mark.parametrize("override", ["count", "age"])
def test_multiple_branch_retention_overrides(spark, history, override):
    name, path = history
    append_history(spark, history, count=3)
    ids = snapshot_ids(path)
    spark.sql(f"ALTER TABLE {name} CREATE BRANCH audit AS OF VERSION {ids[-1]}")
    spark.sql(f"CALL system.set_current_snapshot('{name}', {ids[1]})").collect()

    def configure(metadata):
        metadata["properties"]["history.expire.max-snapshot-age-ms"] = "1"
        branch = metadata["refs"]["audit"]
        branch["min-snapshots-to-keep"] = 3 if override == "count" else 1
        branch["max-snapshot-age-ms"] = 1 if override == "count" else 9_223_372_036_854_775_807

    _edit_latest_metadata(path, configure)
    assert expire(spark, name) == ZERO
    assert snapshot_ids(path) == ids

    def relax(metadata):
        metadata["refs"]["audit"].update({"min-snapshots-to-keep": 1, "max-snapshot-age-ms": 1})

    _edit_latest_metadata(path, relax)
    assert expire(spark, name) == (0, 0, 0, 0, 1, 0)
    assert snapshot_ids(path) == ids[1:]
    assert [row.id for row in spark.table(name).orderBy("id").collect()] == [0, 1]


def test_explicit_ids_remove_multiple_snapshots_despite_retention_count(spark, history):
    name, path = history
    append_history(spark, history)
    ids = snapshot_ids(path)
    # A past cutoff isolates explicit IDs from the default snapshot-age policy.
    counts = expire(
        spark,
        name,
        f", older_than => TIMESTAMP '2000-01-01', retain_last => 100, snapshot_ids => array({ids[0]}L, {ids[2]}L)",
    )
    assert counts == (0, 0, 0, 0, 2, 0)
    assert snapshot_ids(path) == [ids[1], ids[-1]]
    assert [row.id for row in spark.table(name).orderBy("id").collect()] == list(range(4))


@pytest.mark.parametrize(
    "arguments",
    [
        "'{name}', table => '{name}'",
        "",
        "'{name}', 2.2",
        "''",
        "'{name}', max_concurrent_deletes => -1",
    ],
)
def test_invalid_procedure_arguments_do_not_mutate_table(spark, history, arguments):
    name, path = history
    before = {file: file.read_bytes() for file in path.rglob("*") if file.is_file()}
    with pytest.raises(PySparkException):
        spark.sql(f"CALL system.expire_snapshots({arguments.format(name=name)})").collect()
    assert {file: file.read_bytes() for file in path.rglob("*") if file.is_file()} == before


# SPDX-SnippetBegin
# SPDX-SnippetComment: Credit: https://github.com/apache/iceberg/blob/212aa894e919ff9fdf89160da4aadcbe6858ac01/spark/v4.1/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestExpireSnapshotsProcedure.java#L196-L217
def test_cross_catalog_expiration_rejection_preserves_both_tables(tmp_path):
    catalogs = '[{name="left", type="memory", initial_database=["default"]}, {name="right", type="memory", initial_database=["default"]}]'
    with (
        spark_connect_server(envs={"SAIL_CATALOG__LIST": catalogs, "SAIL_CATALOG__DEFAULT_CATALOG": "left"}) as server,
        spark_session_factory(server.remote) as sessions,
    ):
        client = sessions.create()
        for catalog in ["left", "right"]:
            location = escape_sql_string_literal(str(tmp_path / catalog))
            client.sql(f"CREATE TABLE {catalog}.default.target (id INT) USING iceberg LOCATION '{location}'")
            client.sql(f"INSERT INTO {catalog}.default.target VALUES (1)")
            client.sql(f"INSERT OVERWRITE {catalog}.default.target VALUES (2)")
        before = {file: file.read_bytes() for file in tmp_path.rglob("*") if file.is_file()}
        for procedure_catalog, table_catalog in [("left", "right"), ("right", "left")]:
            with pytest.raises(PySparkException, match="Cannot run procedure from catalog"):
                client.sql(
                    f"CALL {procedure_catalog}.system.expire_snapshots(table => '{table_catalog}.default.target', "
                    f"older_than => {FUTURE})"
                ).collect()
            assert {file: file.read_bytes() for file in tmp_path.rglob("*") if file.is_file()} == before
        for catalog in ["left", "right"]:
            assert [row.id for row in client.table(f"{catalog}.default.target").collect()] == [2]
            result = client.sql(
                f"CALL {catalog}.system.expire_snapshots(table => '{catalog}.default.target', older_than => {FUTURE})"
            ).first()
            assert result.deleted_data_files_count == 1
            assert len(_find_latest_metadata(tmp_path / catalog)["snapshots"]) == 1


# SPDX-SnippetEnd
