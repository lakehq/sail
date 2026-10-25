# ruff: noqa: S608
"""TRUNCATE contract from Delta 4.4 / Spark 4.2 TruncateTableSuite."""

import json

import pytest
from pyspark.sql.types import StructType


def _commits(path):
    return sorted((path / "_delta_log").glob("[0-9]" * 20 + ".json"))


def _actions(commit):
    return [json.loads(line) for line in commit.read_text().splitlines()]


@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.parametrize("by_path", [False, True])
def test_truncate_metadata_only_preserves_history_and_cdf(spark, tmp_path, partitioned, by_path):
    name = "delta_truncate"
    path = tmp_path / "table"
    target = f"delta.`{path.as_uri()}`" if by_path else name
    partition = "PARTITIONED BY (p)" if partitioned else ""
    moved = []
    try:
        spark.sql(f"""CREATE TABLE {name} (id INT, p STRING) USING delta {partition}
            LOCATION '{path.as_uri()}' TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'note'='keep')""")
        empty_commits = _commits(path)
        result = spark.sql(f"TRUNCATE TABLE {target}")
        assert result.schema == StructType([])
        assert result.collect() == []
        assert _commits(path) == empty_commits
        spark.sql(f"INSERT INTO {name} VALUES (1, 'a'), (2, 'b'), (3, NULL)")
        before = _commits(path)
        version = int(before[-1].stem)
        schema = spark.table(name).schema
        files = set(path.rglob("*.parquet"))
        assert files
        # Missing data files make accidental scans (including CDF scans) fail.
        for index, file in enumerate(sorted(files)):
            backup = tmp_path / f"saved-{index}.parquet"
            file.rename(backup)
            moved.append((file, backup))
        result = spark.sql(f"TRUNCATE TABLE {target}")
        assert result.schema == StructType([])
        assert result.collect() == []
        for file, backup in moved:
            backup.rename(file)
        moved.clear()
        after = _commits(path)
        assert after[:-1] == before
        actions = _actions(after[-1])
        info = next(action["commitInfo"] for action in actions if "commitInfo" in action)
        assert info["operation"] == "TRUNCATE"
        assert info["operationParameters"] == {}
        assert int(info["operationMetrics"]["numDeletedRows"]) == 3  # noqa: PLR2004
        assert {next(iter(action)) for action in actions} == {"commitInfo", "remove"}
        assert all(action["remove"]["dataChange"] for action in actions if "remove" in action)
        assert spark.table(name).schema == schema
        assert spark.table(name).collect() == []
        assert set(path.rglob("*.parquet")) == files
        historical = spark.read.format("delta").option("versionAsOf", version).load(path.as_uri())
        assert [row.id for row in historical.orderBy("id").collect()] == [1, 2, 3]
        changes = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", version + 1)
            .option("endingVersion", version + 1)
            .load(path.as_uri())
        )
        assert [(row.id, row["_change_type"]) for row in changes.orderBy("id").collect()] == [
            (1, "delete"),
            (2, "delete"),
            (3, "delete"),
        ]
        spark.sql(f"TRUNCATE TABLE {target}").collect()
        assert _commits(path) == after
        assert dict(spark.sql(f"SHOW TBLPROPERTIES {name}").collect())["note"] == "keep"
        spark.sql(f"INSERT INTO {name} VALUES (4, 'c')")
        assert [row.id for row in spark.table(name).collect()] == [4]
    finally:
        for file, backup in moved:
            backup.rename(file)
        spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("populated", [False, True])
def test_truncate_rejects_append_only_even_when_empty(spark, tmp_path, populated):
    name = "delta_truncate_append_only"
    path = tmp_path / name
    try:
        spark.sql(f"""CREATE TABLE {name} (id INT) USING delta LOCATION '{path.as_uri()}'
            TBLPROPERTIES ('delta.appendOnly'='true')""")
        if populated:
            spark.sql(f"INSERT INTO {name} VALUES (1)")
        before = _commits(path)
        with pytest.raises(Exception, match=r"(?i)append.only"):
            spark.sql(f"TRUNCATE TABLE {name}").collect()
        assert _commits(path) == before
        assert spark.table(name).count() == int(populated)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def test_truncate_resolves_delta_namespace_as_named_table(spark, tmp_path):
    catalog = spark.sql("SELECT current_catalog()").first()[0]
    name = f"`{catalog}`.delta.truncate_namespace"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{catalog}`.delta")
    try:
        spark.sql(f"CREATE TABLE {name} (id INT) USING delta LOCATION '{(tmp_path / 'named').as_uri()}'")
        spark.sql(f"INSERT INTO {name} VALUES (1)")
        spark.sql("TRUNCATE TABLE delta.truncate_namespace").collect()
        assert spark.table(name).collect() == []
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        spark.sql(f"DROP DATABASE `{catalog}`.delta")


def test_truncate_rejects_views_missing_tables_and_partition_clause(spark, tmp_path):
    name = "delta_truncate_guard"
    path = tmp_path / name
    try:
        spark.sql(f"CREATE TABLE {name} (id INT, p INT) USING delta PARTITIONED BY (p) LOCATION '{path.as_uri()}'")
        spark.sql(f"INSERT INTO {name} VALUES (1, 1), (2, 2)")
        before = _commits(path)
        with pytest.raises(Exception, match=r"(?i)syntax|parse|partition"):
            spark.sql(f"TRUNCATE TABLE {name} PARTITION (p=1)").collect()
        spark.sql(f"CREATE TEMP VIEW {name} AS SELECT 9 AS id")
        try:
            with pytest.raises(Exception, match=r"expects a table.*view"):
                spark.sql(f"TRUNCATE TABLE {name}").collect()
        finally:
            spark.sql(f"DROP VIEW {name}")
        with pytest.raises(Exception, match=r"(?i)not found|does not exist"):
            spark.sql("TRUNCATE TABLE delta_truncate_missing").collect()
        assert _commits(path) == before
        assert [row.id for row in spark.table(name).orderBy("id").collect()] == [1, 2]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")
