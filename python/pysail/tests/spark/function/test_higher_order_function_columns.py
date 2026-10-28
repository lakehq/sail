import pyspark.sql.functions as F  # noqa: N812
import pytest

from pysail.testing.spark.utils.common import pyspark_version

_DATA = (
    "SELECT * FROM VALUES (1, array(1, 2, 3), 10), (2, array(4, 5), 20), "
    "(3, CAST(NULL AS ARRAY<INT>), 30) AS t(id, arr, k)"
)


@pytest.fixture
def df(spark):
    return spark.sql(_DATA)


def _rows(frame):
    return [tuple(row) for row in frame.collect()]


def test_lambda_over_a_dataframe_column_reference(df):
    out = df.select("id", F.transform(df["arr"], lambda v: v + 1).alias("r")).orderBy("id")
    assert _rows(out) == [(1, [2, 3, 4]), (2, [5, 6]), (3, None)]


def test_predicate_over_a_dataframe_column_reference_in_a_filter(df):
    out = df.filter(F.exists(df["arr"], lambda v: v > 4)).select("id")  # noqa: PLR2004
    assert _rows(out) == [(2,)]


def test_sort_by_a_higher_order_call_over_a_dataframe_column_reference(df):
    out = df.orderBy(F.size(F.filter(df["arr"], lambda v: v > 1)), "id").select("id")
    assert _rows(out) == [(3,), (1,), (2,)]


def test_self_join_with_column_references_to_each_side(df):
    left, right = df.alias("a"), df.alias("b")
    out = (
        left.join(right, F.col("a.id") == F.col("b.id"))
        .select(
            F.col("a.id").alias("i"),
            F.transform(left["arr"], lambda v: v).alias("x"),
            F.exists(right["arr"], lambda v: v > 1).alias("y"),
        )
        .orderBy("i")
    )
    assert _rows(out) == [(1, [1, 2, 3], True), (2, [4, 5], True), (3, None, None)]


def test_plain_expression_through_expr_then_filter_on_a_column_reference(df):
    out = df.select("id", F.expr("transform(arr, k)").alias("r")).filter(df["id"] < 3).orderBy("id")  # noqa: PLR2004
    assert _rows(out) == [(1, [10, 10, 10]), (2, [20, 20])]


_LENS_QUERY = "SELECT id, transform(arr, 9) AS r FROM (" + _DATA + ") ORDER BY id"  # noqa: S608
_EXPECTED = [(1, [9, 9, 9]), (2, [9, 9]), (3, None)]


def test_collect(spark):
    assert _rows(spark.sql(_LENS_QUERY)) == _EXPECTED


def test_to_local_iterator(spark):
    assert [tuple(row) for row in spark.sql(_LENS_QUERY).toLocalIterator()] == _EXPECTED


def test_head_and_tail(spark):
    frame = spark.sql(_LENS_QUERY)
    assert [tuple(row) for row in frame.head(2)] == _EXPECTED[:2]
    assert [tuple(row) for row in frame.tail(1)] == _EXPECTED[2:]


def test_to_pandas(spark):
    records = spark.sql(_LENS_QUERY).toPandas().to_dict("records")
    assert [(r["id"], None if r["r"] is None else list(r["r"])) for r in records] == _EXPECTED


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.toJSON needs a Spark 4+ client")
def test_to_json(spark):
    assert [row.value for row in spark.sql(_LENS_QUERY).toJSON().collect()] == [
        '{"id":1,"r":[9,9,9]}',
        '{"id":2,"r":[9,9]}',
        '{"id":3}',
    ]


def test_to_arrow(spark):
    frame = spark.sql(_LENS_QUERY)
    if not hasattr(frame, "toArrow"):
        pytest.skip("DataFrame.toArrow needs PySpark 4+")
    assert [(r["id"], r["r"]) for r in frame.toArrow().to_pylist()] == _EXPECTED


def test_temp_view_then_sql(spark):
    spark.sql(_LENS_QUERY).createOrReplaceTempView("hof_lens_view")
    out = spark.sql("SELECT id, size(r) AS n FROM hof_lens_view ORDER BY id")
    assert _rows(out) == [(1, 3), (2, 2), (3, None)]


def test_parquet_round_trip(spark, tmp_path):
    path = str(tmp_path / "hof_lens")
    spark.sql(_LENS_QUERY).write.mode("overwrite").parquet(path)
    back = spark.read.parquet(path).orderBy("id")
    assert back.schema.simpleString() == "struct<id:int,r:array<int>>"
    assert _rows(back) == _EXPECTED
