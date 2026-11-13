from __future__ import annotations

import os
import time
from urllib.parse import unquote

import pytest
from pyspark.sql import Row
from pyspark.sql.types import LongType, StringType, StructField, StructType

from pysail.testing.spark.utils.sql import escape_sql_string_literal
from pysail.tests._dev.spark.delta.utils import _active_files, _commits, _paths, _settings, _tree


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DeltaVacuumSuite.scala#L591-L627
@pytest.mark.parametrize("partition", ["part", "_underscore_col_"])
@pytest.mark.parametrize("value", ["a b", "#hash", "a%b", "a/b", "a=b", None])
def test_vacuum_hidden_and_escaped_partitions(spark, tmp_path, partition, value):
    path = tmp_path / "vacuum"
    data = spark.createDataFrame([(1, value), (2, value)], f"id LONG, {partition} STRING")
    data.coalesce(1).write.format("delta").partitionBy(partition).save(str(path))
    active = path / unquote(_active_files(path)[0].path)
    orphan = active.parent / "orphan % #.txt"
    orphan.write_bytes(b"expired")
    hidden = [path / ".hidden.txt", path / "_hidden_dir" / "keep", path / ".hoodie" / "00001.commit"]
    for file in hidden:
        file.parent.mkdir(exist_ok=True)
        file.write_bytes(b"protected")
    old = time.time() - 8 * 24 * 3600
    for file in [active, orphan, *hidden]:
        os.utime(file, (old, old))
    before = _tree(path)
    assert _paths(spark.sql(f"VACUUM delta.`{path}` FULL DRY RUN").collect()) == {str(orphan)}
    assert _tree(path) == before
    assert _paths(spark.sql(f"VACUUM delta.`{path}` FULL").collect()) == {str(path)}
    assert not orphan.exists()
    assert active.read_bytes() == before[str(active.relative_to(path))]
    assert all(file.read_bytes() == b"protected" for file in hidden)
    assert spark.read.format("delta").load(str(path)).orderBy("id").collect() == data.orderBy("id").collect()


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DeltaVacuumSuite.scala#L816-L846
@pytest.mark.skipif(os.name != "posix", reason="Safe local empty-directory cleanup requires Unix descriptor APIs")
@pytest.mark.parametrize("populated", [False, True], ids=["empty", "active"])
def test_vacuum_directories_and_base_protection(spark, semantic_table, populated):
    name, path = semantic_table
    if populated:
        spark.sql(f"INSERT INTO {name} VALUES (1, 'kept')").collect()  # noqa: S608
    nested = path / "abc" / "def" / "orphan.txt"
    nested.parent.mkdir(parents=True)
    nested.write_bytes(b"expired")
    empty = path / "empty"
    empty.mkdir()
    old = time.time() - 8 * 24 * 3600
    for entry in [nested, nested.parent, nested.parent.parent, empty]:
        os.utime(entry, (old, old))
    assert _paths(spark.sql(f"VACUUM {name} DRY RUN").collect()) == {str(nested), str(empty)}
    with _settings(spark, **{"spark.databricks.delta.retentionDurationCheck.enabled": "false"}):
        for _ in range(3):
            assert _paths(spark.sql(f"VACUUM {name} RETAIN 0 HOURS").collect()) == {str(path)}
    assert not (path / "abc").exists()
    assert not empty.exists()
    assert path.is_dir()
    assert (path / "_delta_log").is_dir()
    assert spark.table(name).collect() == ([Row(id=1, part="kept")] if populated else [])


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DeltaVacuumSuite.scala#L296-L317
@pytest.mark.parametrize("target_kind", ["name", "path", "delta-path"])
@pytest.mark.parametrize("clause", ["", "FULL", "RETAIN 168 HOURS", "FULL RETAIN 168 HOURS", "RETAIN 168.0 HOURS"])
def test_vacuum_retention_forms(spark, semantic_table, target_kind, clause):
    name, path = semantic_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a')").collect()  # noqa: S608
    orphan = path / "orphan.txt"
    orphan.write_bytes(b"expired")
    recent = path / "recent.txt"
    recent.write_bytes(b"recent")
    old = time.time() - 8 * 24 * 3600
    os.utime(orphan, (old, old))
    target = {
        "name": name,
        "path": f"'{escape_sql_string_literal(str(path))}'",
        "delta-path": f"delta.`{path}`",
    }[target_kind]
    before = _tree(path)
    assert _paths(spark.sql(f"VACUUM {target} {clause} DRY RUN").collect()) == {str(orphan)}
    assert _tree(path) == before
    assert _paths(spark.sql(f"VACUUM {target} {clause}").collect()) == {str(path)}
    assert not orphan.exists()
    assert recent.read_bytes() == b"recent"
    assert spark.table(name).collect() == [Row(id=1, part="a")]


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DeltaVacuumSuite.scala#L1359-L1459
@pytest.mark.parametrize(
    ("dry_run", "logging", "expired"),
    [(False, True, True), (True, True, True), (False, False, False), (False, True, False)],
    ids=["delete-audit", "dry-no-audit", "logging-disabled", "retain-audit"],
)
def test_vacuum_audit_metrics(spark, semantic_table, dry_run, logging, expired):
    name, path = semantic_table
    spark.sql(f"INSERT INTO {name} VALUES (1, 'a')").collect()  # noqa: S608
    orphan = path / "orphan.txt"
    orphan.write_bytes(b"123456789")
    if expired:
        old = time.time() - 8 * 24 * 3600
        os.utime(orphan, (old, old))
    before = _commits(path)
    with _settings(
        spark,
        **{
            "spark.databricks.delta.vacuum.logging.enabled": str(logging).lower(),
            "spark.databricks.delta.vacuum.retentionWindowIgnore.enabled": "false",
        },
    ):
        result = spark.sql(f"VACUUM {name} RETAIN 168 HOURS {'DRY RUN' if dry_run else ''}").collect()
    expected_paths = ({str(orphan)} if expired else set()) if dry_run else {str(path)}
    assert _paths(result) == expected_paths
    assert orphan.exists() == (dry_run or not expired)
    if dry_run or not logging:
        assert _commits(path) == before
    else:
        end, start = spark.sql(f"DESCRIBE HISTORY {name} LIMIT 2").collect()
        assert [end.operation, start.operation] == ["VACUUM END", "VACUUM START"]
        assert start.operationParameters == {
            "retentionCheckEnabled": "true",
            "specifiedRetentionMillis": str(168 * 3600 * 1000),
            "defaultRetentionMillis": str(168 * 3600 * 1000),
        }
        assert start.operationMetrics == {
            "numFilesToDelete": str(int(expired)),
            "sizeOfDataToDelete": str(9 * int(expired)),
        }
        assert end.operationParameters == {"status": "COMPLETED"}
        assert (path / "part=a").is_dir()
        assert end.operationMetrics == {"numDeletedFiles": str(int(expired)), "numVacuumedDirectories": "2"}
        assert max(_commits(path)) == max(before) + 2
    assert spark.table(name).collect() == [Row(id=1, part="a")]


_SCHEMA = StructType([StructField("id", LongType(), True), StructField("value", StringType(), True)])
_SETTINGS = {
    "spark.databricks.delta.vacuum.logging.enabled": "false",
    "spark.databricks.delta.vacuum.retentionWindowIgnore.enabled": "false",
    "spark.databricks.delta.retentionDurationCheck.enabled": "true",
}


@pytest.fixture
def vacuum_safety_table(spark, tmp_path):
    path = tmp_path / "vacuum_safety"
    spark.createDataFrame([(1, "kept"), (2, None)], _SCHEMA).coalesce(1).write.format("delta").save(str(path))
    # Old active files must remain protected independently of their modification time.
    old = time.time() - 8 * 24 * 3600
    for action in _active_files(path):
        os.utime(path / unquote(action.path), (old, old))
    return path


def _assert_table(spark, path):
    reopened = spark.read.format("delta").load(str(path))
    assert reopened.schema == _SCHEMA
    assert reopened.orderBy("id").collect() == [Row(id=1, value="kept"), Row(id=2, value=None)]


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/test/scala/org/apache/spark/sql/delta/DeltaVacuumSuite.scala#L1359-L1459
def test_vacuum_logging_disabled_deletes_expired_orphan(spark, vacuum_safety_table):
    path = vacuum_safety_table
    orphan = path / "expired-orphan.txt"
    orphan.write_bytes(b"expired and deletable")
    old = time.time() - 8 * 24 * 3600
    os.utime(orphan, (old, old))
    before = _vacuum_tree(path)
    commits = _commits(path)
    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").collect()
    active = _active_files(path)
    _assert_table(spark, path)

    with _settings(spark, **_SETTINGS):
        command = f"VACUUM delta.`{path}` FULL RETAIN 168 HOURS"
        assert _paths(spark.sql(f"{command} DRY RUN").collect()) == {str(orphan)}
        assert _vacuum_tree(path) == before
        assert _paths(spark.sql(command).collect()) == {str(path)}
        assert not orphan.exists()
        assert _vacuum_tree(path) == {key: value for key, value in before.items() if key != orphan.name}
        assert _commits(path) == commits
        assert spark.sql(f"DESCRIBE HISTORY delta.`{path}`").collect() == history
        assert _active_files(path) == active
        _assert_table(spark, path)

        assert _paths(spark.sql(f"{command} DRY RUN").collect()) == set()
        assert _paths(spark.sql(command).collect()) == {str(path)}
        assert _vacuum_tree(path) == {key: value for key, value in before.items() if key != orphan.name}
        assert _commits(path) == commits
        assert spark.sql(f"DESCRIBE HISTORY delta.`{path}`").collect() == history
        _assert_table(spark, path)


# [CREDIT]: https://github.com/delta-io/delta/blob/v4.4.1/spark/src/main/scala/org/apache/spark/sql/delta/commands/VacuumCommand.scala#L1021-L1065
@pytest.mark.skipif(os.name != "posix", reason="Local directory candidate discovery requires Unix descriptor APIs")
@pytest.mark.parametrize("child_name", ["recent.txt", ".hidden", "_hidden", "_hidden_dir/keep.txt"])
def test_vacuum_visible_directory_preserves_protected_child(spark, vacuum_safety_table, child_name):
    path = vacuum_safety_table
    visible = path / "visible"
    child = visible / child_name
    child.parent.mkdir(parents=True)
    child.write_bytes(b"protected child")
    orphan = visible / "expired-orphan.txt"
    orphan.write_bytes(b"expired sibling")
    old = time.time() - 8 * 24 * 3600
    for entry in [orphan, visible]:
        os.utime(entry, (old, old))
    if child_name != "recent.txt":
        os.utime(child, (old, old))
    before = _vacuum_tree(path)
    commits = _commits(path)
    active = _active_files(path)
    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").collect()
    expected = {key: value for key, value in before.items() if key != "visible/expired-orphan.txt"}

    with _settings(spark, **_SETTINGS):
        command = f"VACUUM delta.`{path}` FULL RETAIN 168 HOURS"
        assert _paths(spark.sql(f"{command} DRY RUN").collect()) == {str(orphan)}
        assert _vacuum_tree(path) == before
        assert _paths(spark.sql(command).collect()) == {str(path)}
        assert not orphan.exists()
        assert _vacuum_tree(path) == expected
        # Recent/hidden children do not count as expired entries in directory discovery.
        # Non-recursive removal must still refuse their nonempty parent on every attempt.
        for _ in range(2):
            assert _paths(spark.sql(f"{command} DRY RUN").collect()) == {str(visible)}
            assert _vacuum_tree(path) == expected
            assert _paths(spark.sql(command).collect()) == {str(path)}
            assert visible.is_dir()
            assert child.parent.is_dir()
            assert child.read_bytes() == b"protected child"
            assert path.is_dir()
            assert (path / "_delta_log").is_dir()
            assert _vacuum_tree(path) == expected
            assert _commits(path) == commits
            assert _active_files(path) == active
            assert spark.sql(f"DESCRIBE HISTORY delta.`{path}`").collect() == history
            _assert_table(spark, path)


def _vacuum_tree(path):
    # Spark records VACUUM LITE bookkeeping even when transaction-log auditing is off.
    auxiliary = {"_delta_log/_last_vacuum_info", "_delta_log/._last_vacuum_info.crc"}
    return {key: value for key, value in _tree(path).items() if key not in auxiliary}
