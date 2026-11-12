# ruff: noqa: S608
from collections import Counter
from contextlib import contextmanager
from urllib.parse import unquote
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pysail.testing.spark.utils.sql import escape_sql_string_literal

OVERWRITE_MODE = "spark.sql.sources.partitionOverwriteMode"


@pytest.fixture(autouse=True)
def restore_partition_overwrite_mode(spark):
    previous = spark.conf.get(OVERWRITE_MODE, None)
    try:
        yield
    finally:
        if previous is None:
            spark.conf.unset(OVERWRITE_MODE)
        else:
            spark.conf.set(OVERWRITE_MODE, previous)


@contextmanager
def partitioned_table(spark, path, *, schema="msg STRING, id INT", partitions="id"):
    name = f"partition_overwrite_{uuid4().hex}"
    try:
        spark.sql(
            f"CREATE TABLE {name} ({schema}) USING parquet PARTITIONED BY ({partitions}) "
            f"LOCATION '{escape_sql_string_literal(str(path))}'"
        )
        yield name
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {name}")


def parquet_files(path):
    return {file.relative_to(path): file.read_bytes() for file in path.rglob("*.parquet")}


def partition_rows(path, columns):
    """Decode files and Hive partition paths independently of Sail's reader."""
    rows = []
    for file in path.rglob("*.parquet"):
        partitions = {}
        for segment in file.relative_to(path).parts[:-1]:
            key, value = segment.split("=", 1)
            partitions[unquote(key)] = None if value == "__HIVE_DEFAULT_PARTITION__" else unquote(value)
        for row in pq.ParquetFile(file).read().to_pylist():
            row.update(partitions)
            rows.append(tuple(row[column] for column in columns))
    return Counter(rows)


# [CREDIT]: https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/core/src/test/scala/org/apache/spark/sql/sources/InsertSuite.scala#L575-L618
@pytest.mark.parametrize("clause", ["", "PARTITION (id)"], ids=["implicit", "all-dynamic"])
@pytest.mark.parametrize("mode", ["DYNAMIC", "STATIC"])
def test_sql_partition_overwrite(spark, tmp_path, clause, mode):
    path = tmp_path / "table"
    with partitioned_table(spark, path) as table:
        spark.sql(f"INSERT INTO {table} VALUES ('hello', 1), ('goodbye', 2)")
        before = parquet_files(path / "id=1")
        source = f"overwrite_source_{uuid4().hex}"
        spark.createDataFrame([(2, "yo"), (3, "anyway")], "id INT, msg STRING").createOrReplaceTempView(source)
        try:
            spark.sql(f"SET {OVERWRITE_MODE}={mode}")
            # Positional inserts use data columns followed by dynamic partition columns.
            spark.sql(f"INSERT OVERWRITE TABLE {table} {clause} SELECT msg, id FROM {source}")
        finally:
            spark.catalog.dropTempView(source)

        expected = [(2, "yo"), (3, "anyway")]
        if mode == "DYNAMIC":
            expected.insert(0, (1, "hello"))
            assert before
            assert parquet_files(path / "id=1") == before
        else:
            assert not parquet_files(path / "id=1")
        assert spark.table(table).select("id", "msg").orderBy("id").collect() == expected
        assert partition_rows(path, ["id", "msg"]) == Counter((str(key), value) for key, value in expected)


# [CREDIT]: https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/core/src/test/scala/org/apache/spark/sql/sources/InsertSuite.scala#L913-L936
@pytest.mark.parametrize(
    ("session_mode", "write_mode"),
    [("STATIC", "dynamic"), ("DYNAMIC", "static")],
)
def test_dataframe_partition_overwrite_option_precedes_session(spark, tmp_path, session_mode, write_mode):
    path = tmp_path / "data"
    spark.createDataFrame([(1, "hello"), (2, "goodbye")], "id INT, msg STRING").write.partitionBy("id").parquet(
        str(path)
    )
    spark.conf.set(OVERWRITE_MODE, session_mode)
    (
        spark.createDataFrame([(2, "yo"), (3, "anyway")], "id INT, msg STRING")
        .write.partitionBy("id")
        .mode("overwrite")
        .option("PARTITIONOVERWRITEMODE", write_mode)
        .parquet(str(path))
    )
    expected = [("2", "yo"), ("3", "anyway")]
    if write_mode == "dynamic":
        expected.append(("1", "hello"))
    assert partition_rows(path, ["id", "msg"]) == Counter(expected)
    assert spark.conf.get(OVERWRITE_MODE) == session_mode


@pytest.mark.parametrize(("compression", "suffix"), [("gzip", "gz.parquet"), ("snappy", "snappy.parquet")])
def test_dynamic_overwrite_preserves_compression_suffix(spark, tmp_path, compression, suffix):
    path = tmp_path / "data"
    schema = "msg STRING, id INT"
    spark.createDataFrame([("hello", 1), ("old", 2)], schema).write.partitionBy("id").option(
        "compression", compression
    ).parquet(str(path))
    untouched = parquet_files(path / "id=1")
    spark.conf.set(OVERWRITE_MODE, "DYNAMIC")
    spark.createDataFrame([("new", 2), ("added", 3)], schema).write.partitionBy("id").option(
        "compression", compression
    ).mode("overwrite").parquet(str(path))
    rows = (
        spark.read.schema(schema)
        .option("pathGlobFilter", f"*.{suffix}")
        .parquet(str(path))
        .select("id", "msg")
        .orderBy("id")
        .collect()
    )
    assert rows == [(1, "hello"), (2, "new"), (3, "added")]
    assert parquet_files(path / "id=1") == untouched
    for file in path.rglob("*.parquet"):
        assert file.name.endswith(f".{suffix}")
        assert pq.ParquetFile(file).metadata.row_group(0).column(0).compression == compression.upper()


def test_invalid_partition_overwrite_mode_preserves_data(spark, tmp_path):
    path = tmp_path / "data"
    spark.createDataFrame([(1, "hello")], "id INT, msg STRING").write.partitionBy("id").parquet(str(path))
    before = parquet_files(path)
    with pytest.raises(Exception, match=r"(?i)invalid|mode|partition|enumeration"):
        (
            spark.createDataFrame([(1, "new")], "id INT, msg STRING")
            .write.partitionBy("id")
            .mode("overwrite")
            .option("partitionOverwriteMode", "invalid")
            .parquet(str(path))
        )
    assert parquet_files(path) == before


@pytest.mark.parametrize("entry", ["sql", "dataframe"])
def test_empty_dynamic_partition_overwrite_is_noop(spark, tmp_path, entry):
    path = tmp_path / "table"
    with partitioned_table(spark, path) as table:
        spark.sql(f"INSERT INTO {table} VALUES ('hello', 1), ('goodbye', 2)")
        before = parquet_files(path)
        assert before
        spark.conf.set(OVERWRITE_MODE, "DYNAMIC")
        if entry == "sql":
            spark.sql(
                f"INSERT OVERWRITE TABLE {table} PARTITION (id) "
                "SELECT CAST(NULL AS STRING), CAST(NULL AS INT) WHERE FALSE"
            )
        else:
            spark.createDataFrame([], "msg STRING, id INT").write.partitionBy("id").mode("overwrite").parquet(str(path))
        assert parquet_files(path) == before
        assert partition_rows(path, ["id", "msg"]) == Counter([("1", "hello"), ("2", "goodbye")])


def test_dynamic_mode_unpartitioned_overwrite_still_replaces_all(spark, tmp_path):
    path = tmp_path / "data"
    spark.createDataFrame([(1,), (2,)], "id INT").write.parquet(str(path))
    spark.conf.set(OVERWRITE_MODE, "DYNAMIC")
    spark.createDataFrame([(3,)], "id INT").write.mode("overwrite").parquet(str(path))
    assert partition_rows(path, ["id"]) == Counter([(3,)])
    spark.createDataFrame([], "id INT").write.mode("overwrite").parquet(str(path))
    assert not partition_rows(path, ["id"])


@pytest.mark.parametrize(
    "value", [None, "", "a/b=c%?#", "%2F", "é'\""], ids=["null", "empty", "escaped", "literal-escape", "unicode"]
)
def test_dynamic_overwrite_matches_complete_partition_key(spark, tmp_path, value):
    path = tmp_path / "data"
    schema = "msg STRING, part STRING, bucket STRING"
    initial = [("old", value, "one"), ("sibling", value, "two"), ("untouched", "other", "one")]
    spark.createDataFrame(initial, schema).write.partitionBy("part", "bucket").parquet(str(path))
    before = parquet_files(path)
    spark.conf.set(OVERWRITE_MODE, "DYNAMIC")
    replacement = [("new", value, "one"), ("another", value, "one"), ("added", value, "three")]
    (
        spark.createDataFrame(replacement, schema)
        .repartition(2)
        .write.partitionBy("part", "bucket")
        .mode("overwrite")
        .parquet(str(path))
    )
    expected = [(msg, None if part == "" else part, bucket) for msg, part, bucket in [*initial[1:], *replacement]]
    assert partition_rows(path, ["msg", "part", "bucket"]) == Counter(expected)
    data = spark.read.parquet(str(path))
    assert data.select("msg", "part", "bucket").orderBy("msg").collect() == sorted(expected)
    assert data.filter("part IS NULL").count() == sum(part is None for _, part, _ in expected)
    after = parquet_files(path)
    untouched = {
        name: data for name, data in before.items() if "bucket=two" in name.parts or "part=other" in name.parts
    }
    assert untouched
    assert {name: after[name] for name in untouched} == untouched
    spark.createDataFrame([("again", value, "one")], schema).write.partitionBy("part", "bucket").mode(
        "overwrite"
    ).parquet(str(path))
    expected = [
        (msg, None if part == "" else part, bucket)
        for msg, part, bucket in [*initial[1:], ("added", value, "three"), ("again", value, "one")]
    ]
    assert partition_rows(path, ["msg", "part", "bucket"]) == Counter(expected)


@pytest.mark.spark(skip_if_jvm=True, reason="Compatibility with Sail's legacy numeric partition paths")
@pytest.mark.parametrize("partition_type", ["FLOAT", "DOUBLE"])
@pytest.mark.parametrize("existing_key", ["1", "1.0"], ids=["sail-layout", "spark-layout"])
def test_dynamic_overwrite_matches_existing_numeric_partition(spark, tmp_path, partition_type, existing_key):
    path = tmp_path / "table"
    for key, msg in [(existing_key, "old"), ("2", "keep")]:
        partition = path / f"p={key}"
        partition.mkdir(parents=True)
        pq.write_table(pa.table({"msg": [msg]}), partition / "existing.parquet")
    untouched = parquet_files(path / "p=2")
    with partitioned_table(spark, path, schema=f"msg STRING, p {partition_type}", partitions="p") as table:
        spark.sql(f"INSERT INTO {table} SELECT 'appended', CAST(1 AS {partition_type})")
        assert parquet_files(path / "p=1")
        expected_initial_count = 3
        assert spark.table(table).count() == expected_initial_count
        spark.conf.set(OVERWRITE_MODE, "DYNAMIC")
        spark.sql(f"INSERT OVERWRITE {table} PARTITION (p) SELECT 'new', CAST(1 AS {partition_type})")
        expected = [("new", 1.0), ("keep", 2.0)]
        assert spark.table(table).select("msg", "p").orderBy("p").collect() == expected
        assert parquet_files(path / "p=2") == untouched
        assert sum(pq.ParquetFile(file).metadata.num_rows for file in path.rglob("*.parquet")) == len(expected)


@pytest.mark.parametrize("partition_type", ["TIMESTAMP", "TIMESTAMP_NTZ"])
def test_timestamp_partition_equality_before_and_after_overwrite(spark, tmp_path, partition_type):
    path = tmp_path / "table"
    value = f"{partition_type} '2026-01-02 03:04:05'"
    with partitioned_table(spark, path, schema=f"msg STRING, p {partition_type}", partitions="p") as table:
        spark.sql(f"INSERT INTO {table} SELECT 'old', {value}")
        assert spark.sql(f"SELECT msg FROM {table} WHERE p = {value}").collect() == [("old",)]
        spark.conf.set(OVERWRITE_MODE, "DYNAMIC")
        spark.sql(f"INSERT OVERWRITE {table} PARTITION (p) SELECT 'new', {value}")
        assert spark.sql(f"SELECT msg FROM {table} WHERE p = {value}").collect() == [("new",)]
        assert spark.table(table).count() == 1


def test_failed_dynamic_overwrite_preserves_data(spark, tmp_path):
    path = tmp_path / "data"
    spark.createDataFrame([(10, 1), (20, 2)], "value INT, id INT").write.partitionBy("id").parquet(str(path))
    before = parquet_files(path)
    spark.conf.set(OVERWRITE_MODE, "DYNAMIC")
    source = spark.createDataFrame([("30", 1), ("invalid", 2)], "value STRING, id INT").repartition(2)
    with pytest.raises(Exception, match=r"(?i)cast|invalid|parse"):
        source.selectExpr("CAST(value AS INT) AS value", "id").write.partitionBy("id").mode("overwrite").parquet(
            str(path)
        )
    assert parquet_files(path) == before
    assert partition_rows(path, ["id", "value"]) == Counter([("1", 10), ("2", 20)])


@pytest.mark.parametrize("entry", ["sql", "dataframe"])
def test_dynamic_mode_does_not_change_append(spark, tmp_path, entry):
    path = tmp_path / "table"
    with partitioned_table(spark, path) as table:
        spark.sql(f"INSERT INTO {table} VALUES ('hello', 1), ('goodbye', 2)")
        before = parquet_files(path)
        spark.conf.set(OVERWRITE_MODE, "DYNAMIC")
        if entry == "sql":
            spark.sql(f"INSERT INTO {table} PARTITION (id) VALUES ('yo', 2), ('anyway', 3)")
        else:
            spark.createDataFrame([("yo", 2), ("anyway", 3)], "msg STRING, id INT").write.partitionBy("id").mode(
                "append"
            ).option("partitionOverwriteMode", "dynamic").parquet(str(path))
        assert partition_rows(path, ["id", "msg"]) == Counter(
            [("1", "hello"), ("2", "goodbye"), ("2", "yo"), ("3", "anyway")]
        )
        after = parquet_files(path)
        assert {name: after[name] for name in before} == before


@pytest.mark.parametrize(
    "clause",
    ["PARTITION (missing)", "PARTITION (msg)", "PARTITION (id, id)"],
    ids=["unknown", "not-partition-column", "duplicate"],
)
def test_invalid_partition_clause_preserves_data(spark, tmp_path, clause):
    path = tmp_path / "table"
    with partitioned_table(spark, path) as table:
        spark.sql(f"INSERT INTO {table} VALUES ('hello', 1), ('goodbye', 2)")
        before = parquet_files(path)
        spark.conf.set(OVERWRITE_MODE, "DYNAMIC")
        with pytest.raises(Exception, match=r"(?i)partition|duplicate"):
            spark.sql(f"INSERT OVERWRITE TABLE {table} {clause} SELECT 'yo', 2").collect()
        assert parquet_files(path) == before
        assert partition_rows(path, ["id", "msg"]) == Counter([("1", "hello"), ("2", "goodbye")])


def test_incomplete_partition_clause_preserves_data(spark, tmp_path):
    path = tmp_path / "table"
    with partitioned_table(spark, path, schema="msg STRING, id INT, bucket STRING", partitions="id, bucket") as table:
        spark.sql(f"INSERT INTO {table} VALUES ('hello', 1, 'one'), ('goodbye', 2, 'two')")
        before = parquet_files(path)
        spark.conf.set(OVERWRITE_MODE, "DYNAMIC")
        with pytest.raises(Exception, match=r"(?i)partition"):
            spark.sql(f"INSERT OVERWRITE TABLE {table} PARTITION (id) SELECT 'yo', 2, 'two'").collect()
        assert parquet_files(path) == before


@pytest.mark.spark(skip_if_jvm=True, reason="Static and mixed partition clauses remain unsupported in Sail")
@pytest.mark.parametrize("clause", ["PARTITION (id=1, bucket='one')", "PARTITION (id=1, bucket)"])
def test_unsupported_partition_clause_preserves_data(spark, tmp_path, clause):
    path = tmp_path / "table"
    with partitioned_table(spark, path, schema="msg STRING, id INT, bucket STRING", partitions="id, bucket") as table:
        spark.sql(f"INSERT INTO {table} VALUES ('hello', 1, 'one'), ('goodbye', 2, 'two')")
        before = parquet_files(path)
        spark.conf.set(OVERWRITE_MODE, "DYNAMIC")
        select = "SELECT 'yo', 'one'" if clause.endswith("bucket)") else "SELECT 'yo'"
        with pytest.raises(Exception, match=r"(?i)partition|unsupported|not supported"):
            spark.sql(f"INSERT OVERWRITE TABLE {table} {clause} {select}").collect()
        assert parquet_files(path) == before
        assert partition_rows(path, ["id", "bucket", "msg"]) == Counter(
            [("1", "one", "hello"), ("2", "two", "goodbye")]
        )
