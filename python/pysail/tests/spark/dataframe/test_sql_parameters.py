"""Tests for parameter markers in `spark.sql()`, following Spark's `ParametersSuite`."""

import datetime
from decimal import Decimal

import pyspark.sql.functions as F  # noqa: N812
import pytest

from pysail.testing.spark.utils.common import pyspark_version
from pysail.testing.spark.utils.sql import escape_sql_string_literal

pytestmark = pytest.mark.skipif(
    pyspark_version() < (3, 5),
    reason="spark.sql() named and positional parameters require Spark 3.5+",
)

requires_column_arguments = pytest.mark.skipif(
    pyspark_version() < (4,),
    reason="PySpark accepts a Column as a spark.sql() argument from 4.0",
)


@pytest.fixture
def table(spark, tmp_path):
    location = escape_sql_string_literal(str(tmp_path / "t"))
    spark.sql(f"CREATE TABLE t (c INT) USING parquet LOCATION '{location}'")
    yield "t"
    spark.sql("DROP TABLE IF EXISTS t")
    spark.sql("DROP TABLE IF EXISTS t2")


@pytest.mark.parametrize(
    "value",
    [
        1,
        -(2**40),
        -(2**63),
        -(2**31),
        1.5,
        -0.0,
        1e-7,
        1e300,
        float("inf"),
        True,
        "it's a \\ test",
        "line\nbreak, é, 😀",
        "",
        b"\x00\xab",
        datetime.date(2024, 5, 1),
        datetime.datetime(2024, 5, 1, 12, 30, 45, 123456),  # noqa: DTZ001
        datetime.timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=5),
        pytest.param(
            datetime.time(12, 30, 45, 123456),
            marks=pytest.mark.skipif(pyspark_version() < (4, 1), reason="PySpark has the TIME type from 4.1"),
        ),
        [1, 2, None],
        [[1], [2, 3]],
        ["a", None],
        None,
    ],
    ids=lambda v: type(v).__name__,
)
@pytest.mark.parametrize("session_timezone", ["America/Los_Angeles"], indirect=True)
def test_value_round_trip(spark, value, session_timezone):  # noqa: ARG001
    expected = spark.range(1).select(F.lit(value).alias("v"))
    named = spark.sql("SELECT :v AS v", args={"v": value})
    positional = spark.sql("SELECT ? AS v", args=[value])
    for actual in (named, positional):
        assert actual.dtypes == expected.dtypes
        assert actual.collect() == expected.collect()


def test_decimal_has_the_type_of_its_digits(spark):
    # As in Spark, a decimal argument is written as a `BD` literal, so its type follows its digits
    # rather than the precision of the argument.
    actual = spark.sql("SELECT :v AS v", args={"v": Decimal("-123.450")})
    assert actual.dtypes == [("v", "decimal(6,3)")]
    assert actual.collect() == [(Decimal("-123.450"),)]


@requires_column_arguments
def test_collection_constructor_arguments(spark):
    actual = spark.sql(
        "SELECT :a AS a, :m AS m, :s AS s",
        args={
            "a": F.array(F.lit(1), F.lit(2)),
            "m": F.create_map(F.lit("k"), F.lit(1)),
            "s": F.struct(F.lit(1).alias("x"), F.lit("y")),
        },
    ).collect()
    assert actual[0].a == [1, 2]
    assert actual[0].m == {"k": 1}
    assert actual[0].s.asDict() == {"x": 1, "col2": "y"}


def test_markers_in_strings_and_comments_are_not_substituted(spark):
    actual = spark.sql("SELECT ':p' AS s, '?' AS q, ? AS v /* :x ? */ -- :y ?", args=[1]).collect()
    assert actual == [(":p", "?", 1)]


def test_repeated_named_marker(spark):
    assert spark.sql("SELECT :p + :p AS v", args={"p": 2}).collect() == [(4,)]


def test_unused_arguments_are_ignored(spark):
    assert spark.sql("SELECT :a AS v", args={"a": 1, "unused": 2}).collect() == [(1,)]
    assert spark.sql("SELECT ? AS v", args=[1, 2, 3]).collect() == [(1,)]


def test_names_are_case_sensitive(spark):
    assert spark.sql("SELECT :p AS lower, :P AS upper", args={"p": 1, "P": 2}).collect() == [(1, 2)]


@pytest.mark.usefixtures("table")
def test_insert(spark):
    spark.sql("INSERT INTO t SELECT :p", args={"p": 1})
    spark.sql("INSERT INTO t SELECT ?", args=[2])
    spark.sql("INSERT INTO t VALUES (:p), (:q)", args={"p": 3, "q": 4})
    assert spark.sql("SELECT c FROM t ORDER BY c").collect() == [(1,), (2,), (3,), (4,)]


def test_create_table_as_select(spark, table, tmp_path):
    location = str(tmp_path / "t2")
    spark.sql(
        "CREATE TABLE t2 USING parquet LOCATION :location AS SELECT :p AS c",
        args={"location": location, "p": 5},
    )
    assert spark.sql("SELECT c FROM t2").collect() == [(5,)]
    assert table == "t"


def test_view_body(spark):
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW v_params AS SELECT :p AS c", args={"p": 7})
    assert spark.sql("SELECT c FROM v_params").collect() == [(7,)]


def test_data_type_parameters(spark):
    actual = spark.sql(
        "SELECT CAST(:v AS DECIMAL(:p, :s)) AS d",
        args={"v": "123.456", "p": 10, "s": 2},
    )
    assert actual.dtypes == [("d", "decimal(10,2)")]
    assert actual.collect() == [(Decimal("123.46"),)]


def test_show_tables_like(spark, table):
    actual = spark.sql("SHOW TABLES LIKE :pattern", args={"pattern": table}).collect()
    assert [row.tableName for row in actual] == [table]


def test_string_parameter_in_function(spark):
    # The value is bound before analysis, so the function sees a string literal.
    assert spark.sql("SELECT round(:p, 1) AS r", args={"p": "1.25"}).collect() == [(1.3,)]


def test_unbound_named_parameter(spark):
    with pytest.raises(Exception, match=r"UNBOUND_SQL_PARAMETER.*unbound parameter: b\."):
        spark.sql("SELECT :a, :b", args={"a": 1}).collect()


def test_unbound_positional_parameter(spark):
    # An unbound positional marker is named after its character offset, as in Spark.
    with pytest.raises(Exception, match=r"UNBOUND_SQL_PARAMETER.*unbound parameter: _10\."):
        spark.sql("SELECT ?, ?", args=[1]).collect()


def test_marker_without_arguments(spark):
    with pytest.raises(Exception, match=r"UNBOUND_SQL_PARAMETER.*unbound parameter: p\."):
        spark.sql("SELECT :p", args={}).collect()


def test_mixed_parameters(spark):
    with pytest.raises(Exception, match="INVALID_QUERY_MIXED_QUERY_PARAMETERS"):
        spark.sql("SELECT :a, ?", args={"a": 1}).collect()


@requires_column_arguments
def test_invalid_argument(spark):
    with pytest.raises(Exception, match=r"INVALID_SQL_ARG.*argument a of `sql\(\)` is invalid"):
        spark.sql("SELECT 1", args={"a": F.col("x")}).collect()
