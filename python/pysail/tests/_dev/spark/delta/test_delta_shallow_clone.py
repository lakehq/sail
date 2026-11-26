# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: S608
from __future__ import annotations

import json
import uuid
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.errors import AnalysisException, PySparkException

from pysail.testing.spark.utils.sql import escape_sql_identifier, escape_sql_string_literal

# [CREDIT]: https://github.com/delta-io/delta/blob/bb1057f4367f555b01e299fd1690d85aafc0ac4c/spark/src/test/scala/org/apache/spark/sql/delta/CloneTableSuiteBase.scala#L100-L255


def _identifier(path):
    return f"delta.`{escape_sql_identifier(str(path))}`"


def _create_delta_table(spark, path, columns, options=""):
    name = f"clone_setup_{uuid.uuid4().hex}"
    location = escape_sql_string_literal(str(path))
    try:
        spark.sql(f"CREATE TABLE {name} ({columns}) USING DELTA LOCATION '{location}' {options}")
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def _append(spark, path, rows):
    spark.createDataFrame(rows, "id BIGINT, p STRING").write.format("delta").mode("append").save(str(path))


def _actions(path):
    return [
        json.loads(line)
        for log in sorted((path / "_delta_log").glob("*.json"))
        for line in log.read_text().splitlines()
    ]


def _latest(path, key):
    return next(action[key] for action in reversed(_actions(path)) if key in action)


def _rows(spark, table):
    return [tuple(row) for row in spark.sql(f"SELECT id, p FROM {table} ORDER BY id").collect()]


def _logs(path):
    return {log.name: log.read_bytes() for log in (path / "_delta_log").glob("*.json")}


@pytest.mark.parametrize("named", [False, True], ids=["path", "catalog"])
@pytest.mark.parametrize("empty", [False, True], ids=["partitioned", "empty"])
def test_shallow_clone_snapshot_lifecycle(spark, tmp_path, named, empty):
    source_path = tmp_path / "source space % hash#"
    destination_path = tmp_path / "destination"
    source = _identifier(source_path)
    name = f"clone_{uuid.uuid4().hex}"
    destination = name if named else _identifier(destination_path)
    location = f" LOCATION '{escape_sql_string_literal(str(destination_path))}'" if named else ""
    _create_delta_table(
        spark, source_path, "id BIGINT, p STRING", "PARTITIONED BY (p) TBLPROPERTIES ('community.owner'='clone-test')"
    )
    initial = [] if empty else [(1, "a/b"), (2, "x% y#")]
    if not empty:
        _append(spark, source_path, initial)
    try:
        spark.sql(f"CREATE OR REPLACE TABLE {destination} SHALLOW CLONE {source}{location}")
        assert _rows(spark, destination) == initial
        assert spark.table(destination).schema == spark.table(source).schema
        assert list(destination_path.iterdir()) == [destination_path / "_delta_log"]
        original_metadata = _latest(source_path, "metaData")
        cloned_metadata = _latest(destination_path, "metaData")
        assert cloned_metadata["id"] != original_metadata["id"]
        for key in ("schemaString", "partitionColumns", "configuration", "format"):
            assert cloned_metadata[key] == original_metadata[key]
        assert _latest(destination_path, "protocol") == _latest(source_path, "protocol")
        adds = [action["add"] for action in _actions(destination_path) if "add" in action]
        source_files = set(source_path.rglob("*.parquet"))
        source_files = {path for path in source_files if "_delta_log" not in path.parts}
        assert {Path(unquote(urlparse(add["path"]).path)) for add in adds} == source_files
        assert all(urlparse(add["path"]).scheme == "file" for add in adds)
        assert _latest(destination_path, "commitInfo")["operation"] == "CLONE"

        _append(spark, source_path, [(3, "source/new")])
        before = _logs(destination_path)
        assert spark.sql(f"CREATE TABLE IF NOT EXISTS {destination} SHALLOW CLONE {source}{location}").collect() == []
        assert _logs(destination_path) == before
        assert _rows(spark, destination) == initial

        # Replacing a nonempty clone is an intentional extension beyond Delta Spark 3.3.2.
        spark.sql(f"REPLACE TABLE {destination} SHALLOW CLONE {source}{location}")
        assert _rows(spark, destination) == [*initial, (3, "source/new")]
        assert _latest(destination_path, "metaData")["id"] == cloned_metadata["id"]
        _append(spark, destination_path, [(4, "destination/new")])
        assert _rows(spark, source) == [*initial, (3, "source/new")]
        assert _rows(spark, destination) == [*initial, (3, "source/new"), (4, "destination/new")]

        second = _identifier(tmp_path / "second-clone")
        spark.sql(f"CREATE TABLE {second} SHALLOW CLONE {destination}")
        assert _rows(spark, second) == _rows(spark, destination)
        assert list((tmp_path / "second-clone").iterdir()) == [tmp_path / "second-clone" / "_delta_log"]
    finally:
        if named:
            spark.sql(f"DROP TABLE IF EXISTS {destination}")
    assert _rows(spark, source) == [*initial, (3, "source/new")]


def test_shallow_clone_managed_catalog_and_replacement(spark, tmp_path):
    source, destination = [f"clone_{uuid.uuid4().hex}" for _ in range(2)]
    source_path = tmp_path / "source"
    try:
        spark.sql(
            f"CREATE TABLE {source} (id BIGINT, p STRING) USING DELTA "
            f"LOCATION '{escape_sql_string_literal(str(source_path))}' TBLPROPERTIES ('community.owner'='clone-test')"
        )
        spark.sql(f"INSERT INTO {source} VALUES (1, 'original')")
        spark.sql(f"CREATE OR REPLACE TABLE {destination} SHALLOW CLONE {source}")
        spark.sql(f"INSERT INTO {destination} VALUES (2, 'discarded')")
        spark.sql(f"CREATE OR REPLACE TABLE {destination} SHALLOW CLONE {source}")
        assert _rows(spark, destination) == [(1, "original")]
        properties = {row.key: row.value for row in spark.sql(f"SHOW TBLPROPERTIES {destination}").collect()}
        assert properties["community.owner"] == "clone-test"
        assert _rows(spark, source) == [(1, "original")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {destination}")
        spark.sql(f"DROP TABLE IF EXISTS {source}")
    assert _rows(spark, _identifier(source_path)) == [(1, "original")]


def test_shallow_clone_rejections_preserve_destination(spark, tmp_path):
    source_path, destination_path = tmp_path / "source", tmp_path / "destination"
    source, destination = _identifier(source_path), _identifier(destination_path)
    _create_delta_table(spark, source_path, "id BIGINT, p STRING")
    _append(spark, source_path, [(1, "source")])
    _create_delta_table(spark, destination_path, "id BIGINT, p STRING")
    _append(spark, destination_path, [(2, "destination")])
    advanced = _identifier(tmp_path / "mapped")
    _create_delta_table(spark, tmp_path / "mapped", "id BIGINT", "TBLPROPERTIES ('delta.columnMapping.mode'='name')")
    parquet_path = tmp_path / "parquet"
    spark.range(1).write.parquet(str(parquet_path))
    before = _logs(destination_path)
    source_before = _logs(source_path)
    for sql in [
        f"CREATE TABLE {destination} SHALLOW CLONE {source}",
        f"CREATE OR REPLACE TABLE {destination} SHALLOW CLONE {_identifier(tmp_path / 'missing')}",
        f"CREATE OR REPLACE TABLE {destination} SHALLOW CLONE {advanced}",
        f"CREATE OR REPLACE TABLE {destination} SHALLOW CLONE parquet.`{escape_sql_identifier(str(parquet_path))}`",
        f"CREATE OR REPLACE TABLE {destination} SHALLOW CLONE {source} TBLPROPERTIES ('delta.enableDeletionVectors'='true')",
        f"CREATE OR REPLACE TABLE {source} SHALLOW CLONE {source}",
        f"CREATE OR REPLACE TABLE {source} SHALLOW CLONE {_identifier(source_path / '..' / 'source')}",
        f"CREATE TABLE {_identifier(parquet_path)} SHALLOW CLONE {source}",
        f"REPLACE TABLE {_identifier(tmp_path / 'absent')} SHALLOW CLONE {source}",
        f"CREATE TABLE {_identifier(tmp_path / 'deep')} DEEP CLONE {source}",
        f"CREATE TABLE {_identifier(tmp_path / 'historical')} SHALLOW CLONE {source} VERSION AS OF 0",
    ]:
        with pytest.raises(PySparkException):
            spark.sql(sql).collect()
        assert _logs(destination_path) == before
        assert _logs(source_path) == source_before
        assert _rows(spark, destination) == [(2, "destination")]
        assert _rows(spark, source) == [(1, "source")]
    assert not (parquet_path / "_delta_log").exists()
    assert not (tmp_path / "absent" / "_delta_log").exists()


@pytest.mark.parametrize("link_to_directory", [True, False], ids=["directory-link", "file-link"])
def test_clone_refresh_rejects_symlink_to_destination_file(spark, tmp_path, link_to_directory):
    source, destination = tmp_path / "source", tmp_path / "destination"
    for path in (source, destination):
        _create_delta_table(spark, path, "id BIGINT, p STRING")
    _append(spark, destination, [(2, "destination")])
    original = _latest(destination, "add")
    alias = tmp_path / "alias"
    data_file = unquote(original["path"])
    if link_to_directory:
        alias.symlink_to(destination, target_is_directory=True)
        reference = (alias / data_file).as_uri()
    else:
        alias.symlink_to(destination / data_file)
        reference = alias.as_uri()
    source_log = source / "_delta_log" / "00000000000000000001.json"
    source_log.write_text(json.dumps({"add": {**original, "path": reference}}) + "\n")
    before = _logs(destination)
    assert _rows(spark, _identifier(source)) == [(2, "destination")]
    with pytest.raises(PySparkException, match="cannot reference files inside its destination"):
        spark.sql(f"REPLACE TABLE {_identifier(destination)} SHALLOW CLONE {_identifier(source)}").collect()
    assert _logs(destination) == before
    assert _rows(spark, _identifier(destination)) == [(2, "destination")]

    # A different target can safely reference this file through its physical location.
    other = _identifier(tmp_path / "other")
    spark.sql(f"CREATE TABLE {other} SHALLOW CLONE {_identifier(source)}").collect()
    alias.unlink()
    assert _rows(spark, other) == [(2, "destination")]


@pytest.mark.parametrize(
    ("directory", "log_directory"),
    [
        ("p=x:", "p=x%3A"),
        ("p=x:", "p=x:"),
        ("archive:", "archive%3A"),
        ("file:", "file%3A"),
        ("archive%3A", "archive%253A"),
    ],
)
def test_shallow_clone_relative_colon_paths(spark, tmp_path, directory, log_directory):
    source_path = tmp_path / "source"
    source, destination = _identifier(source_path), _identifier(tmp_path / "clone")
    _create_delta_table(spark, source_path, "id BIGINT, p STRING", "PARTITIONED BY (p)")
    partition = source_path / directory
    partition.mkdir()
    actions = []
    for value in (7, 9):
        file = partition / f"part-{value}.parquet"
        pq.write_table(pa.table({"id": [value]}), file)
        actions.append(
            {
                "add": {
                    "path": f"{log_directory}/{file.name}",
                    "partitionValues": {"p": "x:"},
                    "size": file.stat().st_size,
                    "modificationTime": 0,
                    "dataChange": True,
                    "stats": json.dumps({"numRecords": 1}),
                }
            }
        )
    (source_path / "_delta_log" / "00000000000000000001.json").write_text(
        "\n".join(json.dumps(action) for action in actions) + "\n"
    )
    expected = [(7, "x:"), (9, "x:")]
    assert _rows(spark, source) == expected
    spark.sql(f"CREATE TABLE {destination} SHALLOW CLONE {source}")
    assert _rows(spark, destination) == expected
    for table, path in ((source, source_path), (destination, tmp_path / "clone")):
        spark.sql(f"OPTIMIZE {table}").collect()
        assert _rows(spark, table) == expected
        assert _latest(path, "commitInfo")["operation"] == "OPTIMIZE"
        assert sum("remove" in action for action in _actions(path)) == 2
    assert _rows(spark, source) == expected
    assert {a["remove"]["path"] for a in _actions(source_path) if "remove" in a} == {a["add"]["path"] for a in actions}


@pytest.mark.integration
def test_shallow_clone_delta_spark_interoperability(spark, delta_jvm_spark, tmp_path):
    source = _identifier(tmp_path / "source space % hash#")
    destination = _identifier(tmp_path / "sail-clone")
    reference_clone = _identifier(tmp_path / "spark-clone")
    delta_jvm_spark.sql(f"CREATE TABLE {source} (id BIGINT, p STRING) USING DELTA PARTITIONED BY (p)")
    delta_jvm_spark.sql(f"INSERT INTO {source} VALUES (1, 'a/b'), (2, 'x% y#')")
    spark.sql(f"CREATE TABLE {destination} SHALLOW CLONE {source}")
    assert _rows(delta_jvm_spark, destination) == [(1, "a/b"), (2, "x% y#")]
    delta_jvm_spark.sql(f"INSERT INTO {destination} VALUES (3, 'JVM')")
    _append(spark, tmp_path / "sail-clone", [(4, "Sail")])
    expected = [(1, "a/b"), (2, "x% y#"), (3, "JVM"), (4, "Sail")]
    assert _rows(delta_jvm_spark, destination) == expected
    assert _rows(spark, destination) == expected
    delta_jvm_spark.sql(f"CREATE TABLE {reference_clone} SHALLOW CLONE {destination}")
    assert _rows(spark, reference_clone) == expected
    assert _rows(spark, source) == [(1, "a/b"), (2, "x% y#")]


def test_clone_refresh_keeps_file_actions_disjoint(spark, tmp_path):
    source, destination = tmp_path / "source", tmp_path / "destination"
    spark.range(1).write.format("delta").save(str(source))
    for _ in range(2):
        spark.sql(f"CREATE OR REPLACE TABLE {_identifier(destination)} SHALLOW CLONE {_identifier(source)}").collect()

    def refresh_actions():
        latest = sorted((destination / "_delta_log").glob("*.json"))[-1]
        return [json.loads(line) for line in latest.read_text().splitlines()]

    assert all("add" not in action and "remove" not in action for action in refresh_actions())

    # A metadata-only update for a retained file is a single Add with the new metadata.
    add = _latest(source, "add")
    add["tags"] = {"owner": "finance"}
    add["dataChange"] = False
    (source / "_delta_log" / "00000000000000000001.json").write_text(json.dumps({"add": add}) + "\n")
    spark.sql(f"REPLACE TABLE {_identifier(destination)} SHALLOW CLONE {_identifier(source)}").collect()
    actions = refresh_actions()
    assert all("remove" not in action for action in actions)
    updates = [action["add"] for action in actions if "add" in action]
    assert [(update["tags"], update["dataChange"]) for update in updates] == [(add["tags"], False)]


def test_clone_reusing_renamed_table_name_preserves_archived_rows(spark, tmp_path):
    database = f"clone_{uuid.uuid4().hex}"
    warehouse = escape_sql_string_literal(str(tmp_path / "warehouse"))
    spark.sql(f"CREATE DATABASE {database} LOCATION '{warehouse}'")
    original, archived = f"{database}.original", f"{database}.archived"
    try:
        for name, value in (("first", 1), ("second", 2)):
            spark.range(value, value + 1).write.format("delta").save(str(tmp_path / name))
        spark.sql(f"CREATE TABLE {original} SHALLOW CLONE {_identifier(tmp_path / 'first')}")
        spark.sql(f"ALTER TABLE {original} RENAME TO {archived}")
        with pytest.raises(AnalysisException, match="destination already exists"):
            spark.sql(f"CREATE OR REPLACE TABLE {original} SHALLOW CLONE {_identifier(tmp_path / 'second')}").collect()
        assert [row.id for row in spark.table(archived).collect()] == [1]
    finally:
        spark.sql(f"DROP DATABASE {database} CASCADE")


def test_named_clone_with_metadata_prefixed_property(spark, tmp_path):
    source = tmp_path / "source"
    _create_delta_table(spark, source, "id BIGINT", "TBLPROPERTIES ('metadata.owner'='finance')")
    target = f"clone_{uuid.uuid4().hex}"
    try:
        spark.sql(f"CREATE TABLE {target} SHALLOW CLONE {_identifier(source)}")
        properties = {row.key: row.value for row in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()}
        assert properties["metadata.owner"] == "finance"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {target}")


def test_clone_returns_file_metrics(spark, tmp_path):
    source = tmp_path / "source"
    spark.range(1).write.format("delta").save(str(source))
    adds = [action["add"] for action in _actions(source) if "add" in action]
    result = spark.sql(f"CREATE TABLE {_identifier(tmp_path / 'destination')} SHALLOW CLONE {_identifier(source)}")
    assert all(field.nullable and field.dataType.simpleString() == "bigint" for field in result.schema)
    assert [row.asDict() for row in result.collect()] == [
        {
            "source_table_size": sum(add["size"] for add in adds),
            "source_num_of_files": len(adds),
            "num_removed_files": 0,
            "num_copied_files": 0,
            "removed_files_size": 0,
            "copied_files_size": 0,
        }
    ]
    destination = tmp_path / "destination"
    spark.range(3).write.format("delta").mode("append").save(str(destination))
    local_files = [a["add"] for a in _actions(destination) if "add" in a and not urlparse(a["add"]["path"]).scheme]
    refresh = spark.sql(f"REPLACE TABLE {_identifier(destination)} SHALLOW CLONE {_identifier(source)}").first()
    assert refresh.num_removed_files == len(local_files)
    assert refresh.removed_files_size == sum(add["size"] for add in local_files)
    history = _latest(destination, "commitInfo")["operationMetrics"]
    assert int(history["sourceTableSize"]) == refresh.source_table_size
    assert int(history["numRemovedFiles"]) == refresh.num_removed_files
    assert int(history["removedFilesSize"]) == refresh.removed_files_size


def test_clone_preserves_source_format_options(spark, tmp_path):
    source, destination = tmp_path / "source", tmp_path / "destination"
    _create_delta_table(spark, source, "id BIGINT")
    metadata = _latest(source, "metaData")
    metadata["format"]["options"]["compression"] = "snappy"
    (source / "_delta_log" / "00000000000000000001.json").write_text(json.dumps({"metaData": metadata}) + "\n")
    spark.sql(f"CREATE TABLE {_identifier(destination)} SHALLOW CLONE {_identifier(source)}")
    assert _latest(destination, "metaData")["format"] == metadata["format"]


def test_clone_catalog_target_in_delta_namespace(spark, tmp_path):
    source = tmp_path / "source"
    spark.range(1).write.format("delta").save(str(source))
    spark.sql("CREATE DATABASE delta")
    target = f"delta.clone_{uuid.uuid4().hex}"
    location = escape_sql_string_literal(str(tmp_path / "target"))
    try:
        qualified = f"{spark.catalog.currentCatalog()}.{target}"
        spark.sql(f"CREATE TABLE {target} SHALLOW CLONE {_identifier(source)} LOCATION '{location}'")
        assert [row.id for row in spark.table(qualified).collect()] == [0]
        managed = f"{target}_managed"
        spark.sql(f"CREATE TABLE {managed} SHALLOW CLONE {_identifier(source)}")
        assert [row.id for row in spark.table(f"{qualified}_managed").collect()] == [0]
        spark.sql(f"REPLACE TABLE {target} SHALLOW CLONE {_identifier(source)} LOCATION '{location}'")
        assert [row.id for row in spark.table(qualified).collect()] == [0]
        second = _identifier(tmp_path / "second")
        spark.sql(f"CREATE TABLE {second} SHALLOW CLONE {target}")
        assert [row.id for row in spark.table(second).collect()] == [0]
    finally:
        spark.sql("DROP DATABASE delta CASCADE")


@pytest.mark.parametrize("source_kind", ["missing_path", "missing_table", "unsupported_protocol"])
def test_existing_path_clone_skips_source(spark, tmp_path, source_kind):
    destination = tmp_path / "existing"
    spark.range(1).write.format("delta").save(str(destination))
    source_path = tmp_path / "source"
    source = f"missing_{uuid.uuid4().hex}" if source_kind == "missing_table" else _identifier(source_path)
    if source_kind == "unsupported_protocol":
        _create_delta_table(spark, source_path, "id BIGINT", "TBLPROPERTIES ('delta.columnMapping.mode'='name')")
    before = _logs(destination)
    result = spark.sql(f"CREATE TABLE IF NOT EXISTS {_identifier(destination)} SHALLOW CLONE {source}")
    assert result.collect() == []
    assert _logs(destination) == before
