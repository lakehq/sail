# ruff: noqa: S608
from __future__ import annotations

import json
import uuid
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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
        spark.sql(f"CREATE TABLE IF NOT EXISTS {destination} SHALLOW CLONE {source}{location}")
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
        with pytest.raises(Exception):  # noqa: B017
            spark.sql(sql).collect()
        assert _logs(destination_path) == before
        assert _logs(source_path) == source_before
        assert _rows(spark, destination) == [(2, "destination")]
        assert _rows(spark, source) == [(1, "source")]
    assert not (parquet_path / "_delta_log").exists()
    assert not (tmp_path / "absent" / "_delta_log").exists()


def test_shallow_clone_relative_colon_paths(spark, tmp_path):
    source_path = tmp_path / "source"
    source, destination = _identifier(source_path), _identifier(tmp_path / "clone")
    _create_delta_table(spark, source_path, "id BIGINT, p STRING", "PARTITIONED BY (p)")
    partition = source_path / "p=x:"
    partition.mkdir()
    actions = []
    for value in (7, 9):
        file = partition / f"part-{value}.parquet"
        pq.write_table(pa.table({"id": [value]}), file)
        actions.append(
            {
                "add": {
                    "path": f"p=x%3A/{file.name}",
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
