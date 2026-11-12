# ruff: noqa: S608
import os
import stat
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


def data_files(path):
    return (file for file in path.rglob("*.parquet") if file.relative_to(path).parts[0] != ".sail-staging")


def parquet_files(path):
    return {file.relative_to(path): file.read_bytes() for file in data_files(path)}


def partition_rows(path, columns):
    """Decode files and Hive partition paths independently of Sail's reader."""
    rows = []
    for file in data_files(path):
        partitions = {}
        for segment in file.relative_to(path).parts[:-1]:
            key, value = segment.split("=", 1)
            partitions[unquote(key)] = None if value == "__HIVE_DEFAULT_PARTITION__" else unquote(value)
        for row in pq.ParquetFile(file).read().to_pylist():
            row.update(partitions)
            rows.append(tuple(row[column] for column in columns))
    return Counter(rows)


@pytest.mark.parametrize("empty", [False, True], ids=["nonempty-input", "empty-input"])
@pytest.mark.parametrize("entry", ["sql", "dataframe"])
def test_dynamic_overwrite_needs_only_table_write_permission(spark, tmp_path, empty, entry):
    if os.name != "posix" or os.geteuid() == 0:
        pytest.skip("Requires a non-root POSIX writer to enforce directory permissions")
    parent = tmp_path / "warehouse"
    parent.mkdir()
    path = parent / "table"
    with partitioned_table(spark, path) as table:
        spark.sql(f"INSERT INTO {table} VALUES ('keep', 1), ('old', 2)")
        untouched = parquet_files(path / "id=1")
        original = stat.S_IMODE(parent.stat().st_mode)
        spark.conf.set(OVERWRITE_MODE, "DYNAMIC")
        try:
            parent.chmod(0o555)
            with pytest.raises(PermissionError):
                (parent / "permission-probe").mkdir()
            if entry == "sql":
                condition = " WHERE FALSE" if empty else ""
                spark.sql(f"INSERT OVERWRITE {table} SELECT 'new', 2{condition}")
            else:
                rows = [] if empty else [("new", 2)]
                spark.createDataFrame(rows, "msg STRING, id INT").write.partitionBy("id").mode("overwrite").parquet(
                    str(path)
                )
            expected = [("keep", 1), ("old" if empty else "new", 2)]
            assert spark.table(table).select("msg", "id").orderBy("id").collect() == expected
            assert parquet_files(path / "id=1") == untouched
            assert partition_rows(path, ["msg", "id"]) == Counter((msg, str(key)) for msg, key in expected)
        finally:
            parent.chmod(original)
        assert sorted(child.name for child in parent.iterdir()) == ["table"]


def test_reserved_attempts_are_excluded_from_table_reads(spark, tmp_path):
    path = tmp_path / "table"
    spark.createDataFrame([("keep", "one"), ("old", "two")], "msg STRING, id STRING").write.partitionBy("id").parquet(
        str(path)
    )
    stage = path / ".sail-staging" / str(uuid4())
    attempt = stage / "attempts" / str(uuid4()) / "id=999"
    attempt.mkdir(parents=True)
    (stage / "commit.json").write_text("incomplete commit")
    (attempt / "corrupt.parquet").write_bytes(b"not parquet")
    valid = attempt / "valid.parquet"
    pq.write_table(pa.table({"msg": ["uncommitted"]}), valid)
    expected = [("keep", "one"), ("old", "two")]
    for reader, target in [
        (spark.read, str(path)),
        (spark.read.schema("msg STRING, id STRING"), str(path)),
        (spark.read.option("pathGlobFilter", "*.parquet"), str(path)),
        (spark.read.option("basePath", str(path)), str(path / "**" / "*.parquet")),
    ]:
        data = reader.parquet(target)
        assert data.select("msg", "id").orderBy("id").collect() == expected
        files = data.inputFiles()
        assert files
        assert all(".sail-staging" not in file for file in files)
        assert len(files) == len(parquet_files(path))
    # Hidden names are not an ACL: explicitly granted attempt files remain readable.
    assert spark.read.parquet(str(valid)).select("msg").collect() == [("uncommitted",)]


@pytest.mark.spark(skip_if_jvm=True, reason="Sail retains operation fences across static overwrite")
def test_static_overwrite_preserves_dynamic_operation_records_and_failed_sources(spark, tmp_path):
    path = tmp_path / "table"
    schema = "msg STRING, id INT"
    spark.createDataFrame([("keep", 1), ("old", 2)], schema).write.partitionBy("id").parquet(str(path))
    untouched = parquet_files(path / "id=1")
    spark.conf.set(OVERWRITE_MODE, "DYNAMIC")
    spark.createDataFrame([("dynamic", 2)], schema).write.partitionBy("id").mode("overwrite").parquet(str(path))
    assert parquet_files(path / "id=1") == untouched
    assert partition_rows(path, ["msg", "id"]) == Counter([("keep", "1"), ("dynamic", "2")])
    reserved = path / ".sail-staging"
    assert list(reserved.glob("*/complete"))
    unknown = reserved / str(uuid4())
    attempt = unknown / "attempts" / str(uuid4()) / "id=2"
    attempt.mkdir(parents=True)
    (unknown / "commit.json").write_text("unknown publication")
    (attempt / "failed.parquet").write_bytes(b"retained source")
    before = {file.relative_to(reserved): file.read_bytes() for file in reserved.rglob("*") if file.is_file()}
    spark.createDataFrame([("static", 3)], schema).write.partitionBy("id").mode("overwrite").option(
        "partitionOverwriteMode", "static"
    ).parquet(str(path))
    assert {file.relative_to(reserved): file.read_bytes() for file in reserved.rglob("*") if file.is_file()} == before
    assert partition_rows(path, ["msg", "id"]) == Counter([("static", "3")])
    assert spark.read.schema(schema).parquet(str(path)).select("msg", "id").collect() == [("static", 3)]
    assert not parquet_files(path / "id=1")
    assert not parquet_files(path / "id=2")
    spark.createDataFrame([("again", 3)], schema).write.partitionBy("id").mode("overwrite").parquet(str(path))
    assert partition_rows(path, ["msg", "id"]) == Counter([("again", "3")])
    assert {name: (reserved / name).read_bytes() for name in before} == before


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
    for file in data_files(path):
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


@pytest.mark.spark(skip_if_jvm=True, reason="Compatibility with Sail's legacy empty partition paths")
@pytest.mark.parametrize("value", [None, ""], ids=["null", "empty"])
def test_dynamic_overwrite_replaces_legacy_empty_partition(spark, tmp_path, value):
    path = tmp_path / "data"
    for key, msg in [("", "old"), ("keep", "untouched")]:
        partition = path / f"p={key}"
        partition.mkdir(parents=True)
        pq.write_table(pa.table({"msg": [msg]}), partition / "existing.parquet")
    schema = "msg STRING, p STRING"
    before = spark.read.schema(schema).parquet(str(path)).filter("p IS NULL").select("msg").collect()
    untouched = parquet_files(path / "p=keep")
    spark.conf.set(OVERWRITE_MODE, "DYNAMIC")
    spark.createDataFrame([("new", value)], schema).write.partitionBy("p").mode("overwrite").parquet(str(path))
    assert partition_rows(path, ["msg", "p"]) == Counter([("new", None), ("untouched", "keep")])
    assert before == [("old",)]
    assert not parquet_files(path / "p=")
    assert parquet_files(path / "p=keep") == untouched
    assert spark.read.schema(schema).parquet(str(path)).filter("p IS NULL").select("msg").collect() == [("new",)]


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
        assert sum(pq.ParquetFile(file).metadata.num_rows for file in data_files(path)) == len(expected)


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
