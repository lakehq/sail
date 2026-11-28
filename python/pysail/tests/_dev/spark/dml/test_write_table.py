# SPDX-License-Identifier: Apache-2.0

import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import StructType

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.testing.spark.utils.sql import escape_sql_string_literal


@pytest.fixture(autouse=True)
def tables(spark, tmp_path):
    location = str(tmp_path / "t1")
    (tmp_path / "t1").mkdir(parents=True, exist_ok=True)
    spark.sql(f"CREATE TABLE t1 (id LONG, name STRING, age LONG) LOCATION '{escape_sql_string_literal(location)}'")
    yield
    spark.sql("DROP TABLE t1")
    spark.sql("DROP TABLE IF EXISTS t2")
    spark.sql("DROP TABLE IF EXISTS t3")
    spark.sql("DROP TABLE IF EXISTS t4")
    spark.sql("DROP TABLE IF EXISTS t5")
    spark.sql("DROP TABLE IF EXISTS t6")


def test_insert_into_with_values(spark):
    spark.sql("INSERT INTO t1 VALUES (101, 'Alice', 20)")
    df = spark.createDataFrame([(102, "Bob", 30)])
    df.write.insertInto("t1")

    actual = spark.sql("SELECT * FROM t1 WHERE id IN (101, 102) ORDER BY id").toPandas()
    expected = pd.DataFrame(
        {"id": [101, 102], "name": ["Alice", "Bob"], "age": [20, 30]},
    )
    assert_frame_equal(actual, expected)


def test_insert_into_by_position(spark):
    # The field names are ignored.
    spark.sql("INSERT INTO t1 SELECT 201 AS age, 'Alice' AS name, 21 AS id")
    df = spark.createDataFrame([(202, "Bob", 31)], schema=["age", "name", "id"])
    df.write.insertInto("t1")

    actual = spark.sql("SELECT * FROM t1 WHERE id IN (201, 202) ORDER BY id").toPandas()
    expected = pd.DataFrame(
        {"id": [201, 202], "name": ["Alice", "Bob"], "age": [21, 31]},
    )
    assert_frame_equal(actual, expected)


def test_insert_into_by_name(spark):
    spark.sql("INSERT INTO t1 (age, name, id) VALUES (21, 'Alice', 301)")
    spark.sql("INSERT INTO t1 BY NAME SELECT 31 AS age, 'Bob' AS name, 302 AS id")

    actual = spark.sql("SELECT * FROM t1 WHERE id IN (301, 302) ORDER BY id").toPandas()
    expected = pd.DataFrame(
        {"id": [301, 302], "name": ["Alice", "Bob"], "age": [21, 31]},
    )
    assert_frame_equal(actual, expected)


def test_insert_into_with_invalid_options(spark):
    df = spark.createDataFrame([(101, "Alice", 20)])

    with pytest.raises(Exception, match="partition"):
        df.write.partitionBy("id").insertInto("t1")


def test_insert_overwrite(spark):
    spark.sql("INSERT INTO t1 VALUES (401, 'Alice', 22)")
    spark.sql("INSERT OVERWRITE t1 VALUES (402, 'Bob', 32)")
    actual = spark.sql("SELECT * FROM t1").toPandas()
    expected = pd.DataFrame(
        {"id": [402], "name": ["Bob"], "age": [32]},
    )
    assert_frame_equal(actual, expected)

    df = spark.createDataFrame([(403, "Alice", 25)])
    df.write.insertInto("t1", overwrite=True)
    actual = spark.sql("SELECT * FROM t1").toPandas()
    expected = pd.DataFrame(
        {"id": [403], "name": ["Alice"], "age": [25]},
    )
    assert_frame_equal(actual, expected)

    df = spark.createDataFrame([(404, "Bob", 35)])
    df.write.mode("overwrite").insertInto("t1")
    actual = spark.sql("SELECT * FROM t1").toPandas()
    expected = pd.DataFrame(
        {"id": [404], "name": ["Bob"], "age": [35]},
    )
    assert_frame_equal(actual, expected)


def test_save_as_table(spark, tmp_path):
    location = str(tmp_path / "t2")
    df = spark.createDataFrame([(1001, "Alice")], schema="id LONG, name STRING")

    def expected(n: int):
        return pd.DataFrame(
            {"id": [1001] * n, "name": ["Alice"] * n},
        )

    df.write.saveAsTable("t2", path=location)
    actual = spark.sql("SELECT * FROM t2").toPandas()
    assert_frame_equal(actual, expected(1))
    if not is_jvm_spark():
        # The "ignore" mode seems broken in Spark Connect.
        df.write.saveAsTable("t2", mode="ignore", path=location)
        actual = spark.sql("SELECT * FROM t2").toPandas()
        assert_frame_equal(actual, expected(1))

    with pytest.raises(Exception, match=r".*"):
        df.write.saveAsTable("t2", mode="error", location=location)

    with pytest.raises(Exception, match=r".*"):
        df.write.saveAsTable("t2", location=location)

    df.write.saveAsTable("t2", mode="append")
    actual = spark.sql("SELECT * FROM t2").toPandas()
    assert_frame_equal(actual, expected(2))

    # `saveAsTable` matches columns by name.
    df.select("name", "id").write.saveAsTable("t2", mode="append")
    actual = spark.sql("SELECT * FROM t2").toPandas()
    assert_frame_equal(actual, expected(3))

    with pytest.raises(Exception, match=r".*"):
        df.select(F.col("name").alias("n"), "id").write.saveAsTable("t2", mode="append")

    with pytest.raises(Exception, match=r".*"):
        df.select("id").write.saveAsTable("t2", mode="append")

    df.write.saveAsTable("t2", mode="overwrite", path=location)
    actual = spark.sql("SELECT * FROM t2").toPandas()
    assert_frame_equal(actual, expected(1))


@pytest.mark.parametrize("table_format", ["parquet", "csv", "json"])
def test_sql_writes_return_no_columns_and_no_rows(spark, tmp_path, table_format):
    location = {name: escape_sql_string_literal(str(tmp_path / name)) for name in ["t2", "t3", "t4", "out"]}
    spark.sql(f"CREATE TABLE t2 (id LONG) USING {table_format} LOCATION '{location['t2']}'")
    spark.sql(f"CREATE TABLE t4 (id LONG, p INT) USING {table_format} PARTITIONED BY (p) LOCATION '{location['t4']}'")
    for query in [
        "INSERT INTO t2 VALUES (1)",
        "INSERT INTO t2 SELECT 2 WHERE false",
        "INSERT OVERWRITE t2 VALUES (3)",
        f"CREATE TABLE t3 USING {table_format} LOCATION '{location['t3']}' AS SELECT CAST(4 AS LONG) AS id",
        f"INSERT OVERWRITE DIRECTORY '{location['out']}' USING {table_format} SELECT 5 AS id",
        "INSERT INTO t4 SELECT 6 AS id, 1 AS p",
    ]:
        result = spark.sql(query)
        assert result.schema == StructType([])
        assert result.collect() == []
    assert spark.table("t2").collect() == [(3,)]
    assert spark.table("t3").collect() == [(4,)]
    assert spark.table("t4").collect() == [(6, 1)]


def test_insert_into_parquet_table_with_schema_inferred_from_files(spark, tmp_path):
    location = str(tmp_path / "parquet_schema_inference")
    source = spark.createDataFrame([(1, "Alice")], schema="id LONG, name STRING")
    source.write.parquet(location)

    spark.sql(f"""
        CREATE TABLE parquet_schema_inference
        USING PARQUET
        LOCATION '{escape_sql_string_literal(location)}'
    """)
    spark.sql("INSERT INTO parquet_schema_inference VALUES (2, 'Bob')")

    assert spark.sql("SELECT id, name FROM parquet_schema_inference ORDER BY id").collect() == [
        (1, "Alice"),
        (2, "Bob"),
    ]


def test_insert_into_parquet_table_with_binary_schema_inferred_from_files(spark, tmp_path):
    # Binary mirrors the string case above: the raw write records the column as BinaryView while
    # the INSERT records plain Binary, so the directory only merges if inference coerces the view
    # type. Guards against dropping the BinaryView rewrite in normalize_unsupported_fields.
    location = str(tmp_path / "parquet_binary_schema_inference")
    source = spark.createDataFrame([(1, b"Alice")], schema="id LONG, data BINARY")
    source.write.parquet(location)

    spark.sql(f"""
        CREATE TABLE parquet_binary_schema_inference
        USING PARQUET
        LOCATION '{escape_sql_string_literal(location)}'
    """)
    spark.sql("INSERT INTO parquet_binary_schema_inference VALUES (2, X'426f62')")

    assert spark.sql("SELECT id, data FROM parquet_binary_schema_inference ORDER BY id").collect() == [
        (1, bytearray(b"Alice")),
        (2, bytearray(b"Bob")),
    ]


@pytest.mark.integration
def test_save_as_table_without_path_surfaces_managed(spark):
    table_name = "t_managed_default"
    df = spark.createDataFrame([(1, "Alice")], schema="id LONG, name STRING")
    try:
        df.write.saveAsTable(table_name)
        table = spark.catalog.getTable(table_name)
        assert table.tableType == "MANAGED"

        show_rows = spark.sql(f"SHOW TABLE EXTENDED LIKE '{table_name}'").collect()
        show_row = next(row for row in show_rows if row.tableName == table_name)
        assert "Type: MANAGED" in show_row.information
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.integration
def test_save_as_table_with_path_surfaces_external(spark, tmp_path):
    table_name = "t_external_path"
    location = str(tmp_path / table_name)
    (tmp_path / table_name).mkdir(parents=True, exist_ok=True)
    df = spark.createDataFrame([(1, "Alice")], schema="id LONG, name STRING")
    try:
        df.write.saveAsTable(table_name, path=location)
        table = spark.catalog.getTable(table_name)
        assert table.tableType == "EXTERNAL"

        show_rows = spark.sql(f"SHOW TABLE EXTENDED LIKE '{table_name}'").collect()
        show_row = next(row for row in show_rows if row.tableName == table_name)
        assert "Type: EXTERNAL" in show_row.information
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def test_create_or_replace_table_as_select(spark, tmp_path):
    location = str(tmp_path / "t7")
    table_name = "t7"

    try:
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {table_name}
                USING DELTA
            LOCATION '{escape_sql_string_literal(location)}'
            AS SELECT 1 AS id, 'Alice' AS name
            """
        )
        actual = spark.sql(f"SELECT * FROM {table_name}").toPandas()  # noqa: S608
        expected = pd.DataFrame({"id": [1], "name": ["Alice"]})
        assert_frame_equal(actual, expected, check_dtype=False)

        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {table_name}
                USING DELTA
            LOCATION '{escape_sql_string_literal(location)}'
            AS SELECT 2 AS id, 'Bob' AS name
            """
        )
        actual = spark.sql(f"SELECT * FROM {table_name}").toPandas()  # noqa: S608
        expected = pd.DataFrame({"id": [2], "name": ["Bob"]})
        assert_frame_equal(actual, expected, check_dtype=False)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.spark(skip_if_jvm=True, reason="JVM Spark does not handle v1 and v2 tables properly")
def test_save_as_table_append_creates_table(spark):
    # `saveAsTable` with `mode="append"` should create the table if it does not exist.
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], schema="id LONG, name STRING")
    df.write.saveAsTable("t6", mode="append")

    actual = spark.sql("SELECT * FROM t6 ORDER BY id").toPandas()
    expected = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    assert_frame_equal(actual, expected)

    # Appending again should add more rows.
    df2 = spark.createDataFrame([(3, "Charlie")], schema="id LONG, name STRING")
    df2.write.saveAsTable("t6", mode="append")

    actual = spark.sql("SELECT * FROM t6 ORDER BY id").toPandas()
    expected = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    assert_frame_equal(actual, expected)


@pytest.mark.spark(skip_if_jvm=True, reason="JVM Spark does not handle v1 and v2 tables properly")
def test_write_to(spark, tmp_path):
    location = str(tmp_path / "t3")
    df = spark.createDataFrame([(2002, "Bob")], schema="id LONG, name STRING")

    def expected(n: int):
        return pd.DataFrame(
            {"id": [2002] * n, "name": ["Bob"] * n},
        )

    with pytest.raises(Exception, match=r".*"):
        # The table does not exist yet.
        df.writeTo("t3").option("location", location).replace()

    df.writeTo("t3").option("path", location).create()
    actual = spark.sql("SELECT * FROM t3").toPandas()
    assert_frame_equal(actual, expected(1))

    df.writeTo("t3").append()
    actual = spark.sql("SELECT * FROM t3").toPandas()
    assert_frame_equal(actual, expected(2))

    df.writeTo("t3").option("location", location).replace()
    actual = spark.sql("SELECT * FROM t3").toPandas()
    assert_frame_equal(actual, expected(1))

    df.writeTo("t3").option("path", location).createOrReplace()
    actual = spark.sql("SELECT * FROM t3").toPandas()
    assert_frame_equal(actual, expected(1))

    pytest.skip("conditional overwrite is not supported for listing tables")

    df.writeTo("t3").option("location", location).overwrite(F.lit(True))
    actual = spark.sql("SELECT * FROM t3").toPandas()
    assert_frame_equal(actual, expected(1))


@pytest.mark.skipif(not is_jvm_spark(), reason="the options overwrite logic is not fully compatible yet")
def test_write_options(spark, tmp_path):
    location = str(tmp_path / "t4")
    spark.sql(f"""
        CREATE TABLE t4 (a STRING, b STRING) USING CSV
        LOCATION '{escape_sql_string_literal(location)}'
        OPTIONS (header 'false', delimiter '|')
    """)

    df = spark.createDataFrame([("1-2", "3")], schema="a STRING, b STRING")
    # Spark requires `.format()` to match the table definition.
    # If the format is not specified, the default format is used, and an exception is raised
    # if it does not match the table definition.
    df.write.format("csv").saveAsTable("t4", mode="append")

    df = spark.createDataFrame([("1|2", "3")], schema="a STRING, b STRING")
    df.write.format("csv").option("delimiter", "-").saveAsTable("t4", mode="append")
    df.write.format("csv").option("SEP", "-").saveAsTable("t4", mode="append")

    # Spark rewrites the entire table with the new options.
    assert sorted(spark.read.format("csv").table("t4").collect()) == [
        ("1-2", "3"),
        ("1|2", "3"),
        ("1|2", "3"),
    ]
    # Spark ignores the options if it conflicts with the table definition.
    assert sorted(spark.read.format("csv").option("delimiter", "|").table("t4").collect()) == [
        ("1-2", "3"),
        ("1|2", "3"),
        ("1|2", "3"),
    ]


@pytest.mark.spark(skip_if_jvm=True, reason="JVM Spark cannot infer partition columns when reading paths")
def test_write_with_partition_columns(spark, tmp_path):
    location = str(tmp_path / "t5")
    spark.sql(f"""
        CREATE TABLE t5 (a INT, b STRING)
        USING PARQUET
        LOCATION '{escape_sql_string_literal(location)}'
        PARTITIONED BY (a)
    """)
    df = spark.createDataFrame([(42, "foo")], schema="a INT, b STRING")
    df.write.partitionBy("a").format("parquet").saveAsTable("t5", mode="append")

    with pytest.raises(Exception, match=r".*"):
        df.write.partitionBy("b").format("parquet").saveAsTable("t5", mode="append")

    assert spark.read.format("parquet").table("t5").select("a", "b").collect() == [
        (42, "foo"),
    ]

    assert spark.read.parquet(location).select("a", "b").collect() == [
        ("42", "foo"),
    ]


@pytest.mark.parametrize(
    ("table_format", "method"),
    [
        ("parquet", "save_as_table"),
        pytest.param(
            "parquet",
            "write_to",
            marks=pytest.mark.spark(skip_if_jvm=True, reason="JVM Spark can't write into v1 tables with writeTo"),
        ),
        pytest.param(
            "delta",
            "write_to",
            marks=pytest.mark.spark(skip_if_jvm=True, reason="Delta Lake isn't configured for JVM Spark tests"),
        ),
        pytest.param(
            "delta",
            "save_as_table",
            marks=pytest.mark.spark(skip_if_jvm=True, reason="Delta Lake isn't configured for JVM Spark tests"),
        ),
    ],
)
def test_append_matches_mixed_case_columns_by_name(spark, tmp_path, table_format, method):
    # Every column is nullable so the write should take the plain coercion path.
    location = str(tmp_path / "t2")
    spark.sql(f"""
        CREATE TABLE t2 (ColumnOne STRING, ColumnTwo STRING)
        USING {table_format}
        LOCATION '{escape_sql_string_literal(location)}'
    """)

    # Column order reversed intentionally to guarantee columns are matched
    # by name, and not by position.
    # Should ensure that falling back to position in the future fails here.
    df = spark.createDataFrame([("col_two", "col_one")], schema="ColumnTwo STRING, ColumnOne STRING")
    if method == "write_to":
        df.writeTo("t2").append()
    else:
        df.write.format(table_format).saveAsTable("t2", mode="append")

    actual = spark.sql("SELECT * FROM t2").toPandas()
    expected = pd.DataFrame({"ColumnOne": ["col_one"], "ColumnTwo": ["col_two"]})
    assert_frame_equal(actual, expected)


def _create_self_overwrite_tables(spark, tmp_path, table_format="parquet"):
    for name, values in [("t2", "(1, 'a'), (2, 'b'), (3, 'c')"), ("t3", "(4, 'd'), (5, 'e')")]:
        location = escape_sql_string_literal(str(tmp_path / name))
        spark.sql(f"CREATE TABLE {name} (a INT, b STRING) USING {table_format} LOCATION '{location}'")
        spark.sql(f"INSERT INTO {name} VALUES {values}")  # noqa: S608


def _assert_self_overwrite_rejected(spark, write, table="t2"):
    expected = sorted(spark.table(table).collect())
    with pytest.raises(
        Exception,
        match=rf"\[UNSUPPORTED_OVERWRITE\.TABLE\] Can't overwrite the target that is also being read from\. "
        rf"The target table is `\w+`\.`default`\.`{table}`",
    ):
        write()
    assert sorted(spark.table(table).collect()) == expected


@pytest.mark.parametrize("table_format", ["parquet", "json", "csv"])
def test_insert_overwrite_table_reading_itself_is_rejected(spark, tmp_path, table_format):
    _create_self_overwrite_tables(spark, tmp_path, table_format)
    _assert_self_overwrite_rejected(spark, lambda: spark.sql("INSERT OVERWRITE t2 SELECT * FROM t2"))


@pytest.mark.parametrize(
    "query",
    [
        "INSERT OVERWRITE TABLE t2 SELECT * FROM t2 UNION ALL SELECT * FROM t3",
        "INSERT OVERWRITE t2 SELECT * FROM v",
        "WITH c AS (SELECT * FROM t2) INSERT OVERWRITE t2 SELECT * FROM c",
        "INSERT OVERWRITE t2 WITH c AS (SELECT * FROM t2) SELECT * FROM c",
        "INSERT OVERWRITE t2 SELECT a, b FROM (SELECT a, b FROM t2 WHERE a > 1) s",
        "INSERT OVERWRITE t2 SELECT x.a, y.b FROM t2 x JOIN t2 y ON x.a = y.a",
        "INSERT OVERWRITE t2 SELECT * FROM t3 WHERE EXISTS (SELECT 1 FROM t2)",
        "INSERT OVERWRITE t2 SELECT * FROM t3 WHERE a IN (SELECT a + 3 FROM t2)",
        "INSERT OVERWRITE t2 SELECT * FROM t4",
        "INSERT OVERWRITE t2 SELECT * FROM parquet.`{path}`",
    ],
)
def test_insert_overwrite_table_reading_itself_indirectly_is_rejected(spark, tmp_path, query):
    _create_self_overwrite_tables(spark, tmp_path)
    path = str(tmp_path / "t2")
    spark.sql(f"CREATE TABLE t4 (a INT, b STRING) USING parquet LOCATION '{escape_sql_string_literal(path)}'")
    spark.sql("CREATE TEMP VIEW v AS SELECT * FROM t2")
    try:
        _assert_self_overwrite_rejected(spark, lambda: spark.sql(query.format(path=path)))
    finally:
        spark.catalog.dropTempView("v")


def test_dataframe_overwrite_of_table_reading_itself_is_rejected(spark, tmp_path):
    _create_self_overwrite_tables(spark, tmp_path)
    spark.table("t2").createOrReplaceTempView("v")
    try:
        _assert_self_overwrite_rejected(spark, lambda: spark.sql("INSERT OVERWRITE t2 SELECT * FROM v"))
    finally:
        spark.catalog.dropTempView("v")
    _assert_self_overwrite_rejected(spark, lambda: spark.table("t2").write.mode("overwrite").saveAsTable("t2"))
    _assert_self_overwrite_rejected(spark, lambda: spark.table("t2").write.insertInto("t2", overwrite=True))


_SKIP_DYNAMIC_OVERWRITE = pytest.mark.spark(
    skip_if_jvm=True, reason="JVM Spark overwrites the partitions dynamically, which Sail does not do yet"
)


@pytest.mark.parametrize(
    ("conf_mode", "table_mode", "method"),
    [
        ("static", None, "insert_overwrite"),
        ("static", None, "insert_into"),
        ("static", None, "save_as_table"),
        ("dynamic", None, "save_as_table"),
        ("dynamic", "static", "insert_overwrite"),
        pytest.param("dynamic", None, "insert_overwrite", marks=_SKIP_DYNAMIC_OVERWRITE),
        pytest.param("DYNAMIC", None, "insert_overwrite", marks=_SKIP_DYNAMIC_OVERWRITE),
        pytest.param("dynamic", None, "insert_into", marks=_SKIP_DYNAMIC_OVERWRITE),
        pytest.param("static", "dynamic", "insert_overwrite", marks=_SKIP_DYNAMIC_OVERWRITE),
    ],
)
def test_overwrite_partitioned_table_reading_itself_is_rejected(spark, tmp_path, conf_mode, table_mode, method):
    location = escape_sql_string_literal(str(tmp_path / "t5"))
    options = f"OPTIONS (partitionOverwriteMode '{table_mode}')" if table_mode else ""
    spark.sql(f"CREATE TABLE t5 (a INT, p INT) USING parquet PARTITIONED BY (p) {options} LOCATION '{location}'")
    spark.sql("INSERT INTO t5 VALUES (1, 1), (2, 1), (3, 2), (4, 3)")
    writes = {
        "insert_overwrite": lambda: spark.sql("INSERT OVERWRITE t5 SELECT a + 10, p FROM t5 WHERE p = 1"),
        "insert_into": lambda: spark.sql("SELECT a + 10 AS a, p FROM t5 WHERE p = 1").write.insertInto(
            "t5", overwrite=True
        ),
        "save_as_table": lambda: spark.sql("SELECT a + 10 AS a, p FROM t5 WHERE p = 1")
        .write.mode("overwrite")
        .partitionBy("p")
        .saveAsTable("t5"),
    }
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", conf_mode)
    try:
        _assert_self_overwrite_rejected(spark, writes[method], table="t5")
    finally:
        spark.conf.unset("spark.sql.sources.partitionOverwriteMode")


def test_overwrite_unpartitioned_table_reading_itself_in_dynamic_mode_is_rejected(spark, tmp_path):
    _create_self_overwrite_tables(spark, tmp_path)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try:
        _assert_self_overwrite_rejected(spark, lambda: spark.sql("INSERT OVERWRITE t2 SELECT * FROM t2"))
    finally:
        spark.conf.unset("spark.sql.sources.partitionOverwriteMode")


@pytest.mark.spark(skip_if_jvm=True, reason="JVM Spark does not check these reads and loses the data")
def test_insert_overwrite_table_reading_itself_through_subquery_join_or_child_is_rejected(spark, tmp_path):
    _create_self_overwrite_tables(spark, tmp_path)
    # The subquery becomes an anti join that reads the table after the overwrite deletes it.
    _assert_self_overwrite_rejected(
        spark, lambda: spark.sql("INSERT OVERWRITE t2 SELECT * FROM t3 WHERE a NOT IN (SELECT a FROM t2)")
    )
    location = escape_sql_string_literal(str(tmp_path / "t2" / "sub"))
    spark.sql(f"CREATE TABLE t6 (a INT, b STRING) USING parquet LOCATION '{location}'")
    spark.sql("INSERT INTO t6 VALUES (6, 'f')")
    _assert_self_overwrite_rejected(spark, lambda: spark.sql("INSERT OVERWRITE t2 SELECT * FROM t6"))


def test_writes_reading_the_target_table_without_overwriting_it(spark, tmp_path):
    _create_self_overwrite_tables(spark, tmp_path)
    spark.sql("INSERT INTO t2 SELECT * FROM t2")
    assert sorted(spark.table("t2").collect()) == sorted([(1, "a"), (2, "b"), (3, "c")] * 2)
    spark.sql("INSERT OVERWRITE t2 SELECT * FROM t3")
    assert sorted(spark.table("t2").collect()) == [(4, "d"), (5, "e")]
    location = escape_sql_string_literal(str(tmp_path / "t2" / "sub"))
    spark.sql(f"CREATE TABLE t6 (a INT, b STRING) USING parquet LOCATION '{location}'")
    spark.sql("INSERT OVERWRITE t6 SELECT * FROM t2")
    assert sorted(spark.table("t6").collect()) == [(4, "d"), (5, "e")]


@pytest.mark.spark(skip_if_jvm=True, reason="JVM Spark reads the subquery after the overwrite deletes the table")
def test_insert_overwrite_table_from_scalar_subquery_of_itself(spark, tmp_path):
    _create_self_overwrite_tables(spark, tmp_path)
    spark.sql("INSERT OVERWRITE t2 SELECT (SELECT max(a) FROM t2), 'm'")
    assert spark.table("t2").collect() == [(3, "m")]


@pytest.mark.spark(skip_if_jvm=True, reason="JVM Spark reads the subquery after the overwrite deletes the table")
def test_insert_overwrite_table_from_scalar_subquery_reading_itself(spark, tmp_path):
    _create_self_overwrite_tables(spark, tmp_path)
    # The scalar subquery runs before the overwrite deletes the table.
    spark.sql("INSERT OVERWRITE t2 SELECT (SELECT max(a) FROM t2 WHERE b <> 'zz'), 'm'")
    assert spark.table("t2").collect() == [(3, "m")]
