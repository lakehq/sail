from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import Row

from pysail.testing.spark.session import spark_session_factory
from pysail.testing.spark.utils.common import pyspark_version

if TYPE_CHECKING:
    from pathlib import Path


USER_METADATA_CONFIG = "spark.databricks.delta.commitInfo.userMetadata"


@pytest.fixture(autouse=True)
def restore_metadata_config(spark):
    previous = spark.conf.get(USER_METADATA_CONFIG, None)
    yield
    if previous is None:
        spark.conf.unset(USER_METADATA_CONFIG)
    else:
        spark.conf.set(USER_METADATA_CONFIG, previous)


def _read_latest_commit_info(table_path: Path) -> dict:
    log_dir = table_path / "_delta_log"
    logs = sorted(log_dir.glob("*.json"))
    assert logs, f"no delta logs found in {log_dir}"
    latest = logs[-1]
    with latest.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            if "commitInfo" in obj:
                return obj["commitInfo"]
    msg = f"commitInfo action not found in {latest}"
    raise AssertionError(msg)


def test_append_with_user_metadata_records_value(spark, tmp_path):
    delta_path = tmp_path / "delta_user_metadata_append"
    df = spark.createDataFrame([Row(id=1), Row(id=2)])

    df.write.format("delta").mode("append").option("userMetadata", "job=daily-load run=42").save(str(delta_path))

    commit_info = _read_latest_commit_info(delta_path)
    assert commit_info.get("userMetadata") == "job=daily-load run=42"
    # First write to a non-existing path is a CREATE TABLE commit.
    assert commit_info.get("operation") == "CREATE TABLE"


def test_overwrite_with_user_metadata_records_value(spark, tmp_path):
    delta_path = tmp_path / "delta_user_metadata_overwrite"
    spark.createDataFrame([Row(id=1)]).write.format("delta").mode("append").save(str(delta_path))

    spark.createDataFrame([Row(id=2), Row(id=3)]).write.format("delta").mode("overwrite").option(
        "userMetadata", "audit=overwrite-v2"
    ).save(str(delta_path))

    commit_info = _read_latest_commit_info(delta_path)
    assert commit_info.get("userMetadata") == "audit=overwrite-v2"
    assert commit_info.get("operation") == "WRITE"


def test_user_metadata_is_per_commit(spark, tmp_path):
    """A subsequent write without `userMetadata` must not inherit the previous value."""
    delta_path = tmp_path / "delta_user_metadata_per_commit"

    spark.createDataFrame([Row(id=1)]).write.format("delta").mode("append").option("userMetadata", "first-commit").save(
        str(delta_path)
    )
    first_commit = _read_latest_commit_info(delta_path)
    assert first_commit.get("userMetadata") == "first-commit"

    spark.createDataFrame([Row(id=2)]).write.format("delta").mode("append").save(str(delta_path))
    second_commit = _read_latest_commit_info(delta_path)
    assert "userMetadata" not in second_commit


def test_empty_user_metadata_is_preserved(spark, tmp_path):
    delta_path = tmp_path / "delta_user_metadata_empty"

    spark.createDataFrame([Row(id=1)]).write.format("delta").mode("append").option("userMetadata", "").save(
        str(delta_path)
    )

    commit_info = _read_latest_commit_info(delta_path)
    assert commit_info["userMetadata"] == ""


@pytest.mark.parametrize(
    "alias_key",
    ["userMetadata", "user_metadata"],
)
def test_user_metadata_alias_keys(spark, tmp_path, alias_key):
    """Both the camelCase and snake_case option keys are accepted."""
    delta_path = tmp_path / f"delta_user_metadata_alias_{alias_key}"

    spark.createDataFrame([Row(id=1)]).write.format("delta").mode("append").option(
        alias_key, f"label-via-{alias_key}"
    ).save(str(delta_path))

    commit_info = _read_latest_commit_info(delta_path)
    assert commit_info.get("userMetadata") == f"label-via-{alias_key}"


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_session_user_metadata_write_lifecycle(spark, tmp_path, mode):
    path = tmp_path / "lifecycle"
    df = spark.createDataFrame([Row(id=1)])
    for metadata in ["run=first", "run=second", "", None]:
        if metadata is None:
            spark.conf.unset(USER_METADATA_CONFIG)
        else:
            spark.conf.set(USER_METADATA_CONFIG, metadata)
        df.write.format("delta").mode(mode).save(str(path))
        commit = _read_latest_commit_info(path)
        if metadata is None:
            assert "userMetadata" not in commit
        else:
            assert commit["userMetadata"] == metadata
    assert spark.read.format("delta").load(str(path)).count() == (4 if mode == "append" else 1)


@pytest.mark.parametrize("alias", ["userMetadata", "user_metadata"])
@pytest.mark.parametrize("metadata", ["per-write", "", '  {"任务": "load", "run": 42}\n'])
def test_writer_user_metadata_overrides_session(spark, tmp_path, alias, metadata):
    path = tmp_path / "override"
    spark.conf.set(USER_METADATA_CONFIG, "session-default")
    df = spark.createDataFrame([Row(id=1)])
    df.write.format("delta").option(alias, metadata).save(str(path))
    assert _read_latest_commit_info(path)["userMetadata"] == metadata

    df.write.format("delta").mode("append").save(str(path))
    assert _read_latest_commit_info(path)["userMetadata"] == "session-default"
    assert spark.conf.get(USER_METADATA_CONFIG) == "session-default"


@pytest.mark.parametrize("deletion_vectors", [False, True])
def test_session_user_metadata_sql_operations(spark, tmp_path, deletion_vectors):
    path = tmp_path / "sql_operations"
    table = "delta_metadata_sql"
    spark.conf.set(USER_METADATA_CONFIG, "sql-commits")
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value STRING) USING DELTA LOCATION '{path}' "
            f"TBLPROPERTIES ('delta.enableDeletionVectors' = '{str(deletion_vectors).lower()}')"
        )
        assert _read_latest_commit_info(path)["userMetadata"] == "sql-commits"
        for query, operation in [
            ("INSERT INTO delta_metadata_sql VALUES (1, 'a'), (2, 'b')", "WRITE"),
            ("UPDATE delta_metadata_sql SET value = 'updated' WHERE id = 1", "UPDATE"),
            (
                f"MERGE INTO {table} t USING (SELECT 2 id, 'merged' value UNION ALL SELECT 3, 'c') s "
                "ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
                "MERGE",
            ),
            ("DELETE FROM delta_metadata_sql WHERE id = 2", "DELETE"),
        ]:
            spark.sql(query)
            commit = _read_latest_commit_info(path)
            assert commit["operation"] == operation
            assert commit["userMetadata"] == "sql-commits"
        assert spark.table(table).orderBy("id").collect() == [(1, "updated"), (3, "c")]

        spark.conf.set(USER_METADATA_CONFIG, "delete-all")
        spark.sql("DELETE FROM delta_metadata_sql")
        assert _read_latest_commit_info(path)["userMetadata"] == "delete-all"
        assert spark.table(table).count() == 0
        spark.conf.unset(USER_METADATA_CONFIG)
        spark.sql("INSERT INTO delta_metadata_sql VALUES (4, 'unlabeled')")
        assert "userMetadata" not in _read_latest_commit_info(path)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("as_select", [False, True])
def test_session_user_metadata_create_and_replace(spark, tmp_path, as_select):
    path = tmp_path / "create_replace"
    table = "delta_metadata_create_replace"
    columns = "" if as_select else "(id BIGINT)"
    query = "AS SELECT 1L AS id" if as_select else ""
    try:
        for command, metadata in [("CREATE TABLE", "create"), ("CREATE OR REPLACE TABLE", "replace")]:
            spark.conf.set(USER_METADATA_CONFIG, metadata)
            spark.sql(f"{command} {table} {columns} USING DELTA LOCATION '{path}' {query}")
            assert _read_latest_commit_info(path)["userMetadata"] == metadata
            assert spark.table(table).collect() == ([(1,)] if as_select else [])
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(
    "alteration",
    [
        "SET TBLPROPERTIES ('label' = 'updated')",
        "UNSET TBLPROPERTIES ('label')",
        "ALTER COLUMN id TYPE BIGINT",
        "ALTER COLUMN value SET DEFAULT 'fallback'",
        "ALTER COLUMN value DROP DEFAULT",
        "ADD CONSTRAINT positive_id CHECK (id > 0)",
    ],
)
def test_session_user_metadata_alter_table(spark, tmp_path, alteration):
    path = tmp_path / "alter"
    table = "delta_metadata_alter"
    try:
        spark.sql(
            f"CREATE TABLE {table} (id INT, value STRING DEFAULT 'initial') USING DELTA LOCATION '{path}' "
            "TBLPROPERTIES ('label' = 'initial', 'delta.feature.allowColumnDefaults' = 'supported', "
            "'delta.enableTypeWidening' = 'true')"
        )
        spark.conf.set(USER_METADATA_CONFIG, "ddl-commit")
        spark.sql(f"ALTER TABLE {table} {alteration}")
        commit = _read_latest_commit_info(path)
        assert commit["operation"] != "CREATE TABLE"
        assert commit["userMetadata"] == "ddl-commit"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.mergeInto requires Spark 4+")
def test_session_user_metadata_dataframe_merge_into(spark, tmp_path):
    path = tmp_path / "dataframe_merge"
    table = "delta_metadata_dataframe_merge"
    try:
        spark.createDataFrame([(1, "aaa"), (2, "bbb")], "id INT, val STRING").write.format("delta").option(
            "path", str(path)
        ).saveAsTable(table)
        spark.conf.set(USER_METADATA_CONFIG, "my_id=123")
        source = spark.createDataFrame([(2, "bbb_updated"), (3, "ccc")], "id INT, val STRING")
        (
            source.alias("source")
            .mergeInto(table, F.expr(f"{table}.id = source.id"))
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .merge()
        )
        commit = _read_latest_commit_info(path)
        assert commit["operation"] == "MERGE"
        assert commit["userMetadata"] == "my_id=123"
        assert spark.table(table).orderBy("id").collect() == [(1, "aaa"), (2, "bbb_updated"), (3, "ccc")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_session_user_metadata_isolation(remote, tmp_path):
    path = tmp_path / "isolation"
    with spark_session_factory(remote) as sessions:
        first, second = sessions.create(), sessions.create()
        first.conf.set(USER_METADATA_CONFIG, "first-session")
        for session, expected in [(first, "first-session"), (second, None), (first, "first-session")]:
            session.range(1).write.format("delta").mode("append").save(str(path))
            assert _read_latest_commit_info(path).get("userMetadata") == expected


def test_save_as_table_user_metadata_is_per_commit(spark, tmp_path):
    path = tmp_path / "save_as_table"
    table = "delta_metadata_save_as_table"
    spark.conf.set(USER_METADATA_CONFIG, "session-default")
    try:
        spark.range(1).write.format("delta").option("path", str(path)).option("userMetadata", "explicit").saveAsTable(
            table
        )
        assert _read_latest_commit_info(path)["userMetadata"] == "explicit"
        spark.sql("INSERT INTO delta_metadata_save_as_table VALUES (2)")
        assert _read_latest_commit_info(path)["userMetadata"] == "session-default"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
