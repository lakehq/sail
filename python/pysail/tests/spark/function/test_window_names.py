import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.errors import AnalysisException
from pyspark.sql.window import Window


@pytest.fixture
def df(spark):
    return spark.sql("SELECT * FROM VALUES (0, 1.0D, 2.0D), (1, 2.0D, 3.0D) AS tab(a, b, c)")


def name(df, column):
    return df.select(column).columns[0]


def test_window_names_with_default_frames(df):
    w = Window.partitionBy("a").orderBy(F.col("b").desc())
    assert name(df, F.row_number().over(w)) == (
        "row_number() OVER (PARTITION BY a ORDER BY b DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    )
    w = Window.orderBy("b")
    assert name(df, F.rank().over(w)) == (
        "RANK() OVER (ORDER BY b ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    )
    assert name(df, F.cume_dist().over(w)) == (
        "cume_dist() OVER (ORDER BY b ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    )
    assert name(df, F.ntile(2).over(w)) == (
        "ntile(2) OVER (ORDER BY b ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    )
    assert name(df, F.sum("c").over(w)) == (
        "sum(c) OVER (ORDER BY b ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    )
    assert name(df, F.max("c").over(Window.partitionBy("a"))) == (
        "max(c) OVER (PARTITION BY a ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
    )


def test_window_names_of_offset_functions(df):
    w = Window.orderBy("b")
    assert name(df, F.lag("c", 1).over(w)) == (
        "lag(c, 1, NULL) OVER (ORDER BY b ASC NULLS FIRST ROWS BETWEEN -1 FOLLOWING AND -1 FOLLOWING)"
    )
    assert name(df, F.lag("c", -1).over(w)) == (
        "lag(c, -1, NULL) OVER (ORDER BY b ASC NULLS FIRST ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)"
    )
    assert name(df, F.lead("c", 2, -1.0).over(w)) == (
        "lead(c, 2, -1.0) OVER (ORDER BY b ASC NULLS FIRST ROWS BETWEEN 2 FOLLOWING AND 2 FOLLOWING)"
    )
    assert name(df, F.nth_value("c", 2, True).over(w)) == (
        "nth_value(c, 2) ignore nulls OVER (ORDER BY b ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    )


def test_window_names_with_frames(df):
    w = Window.partitionBy("a", df.c).orderBy("b", df.c.desc_nulls_last()).rowsBetween(-1, 2)
    assert name(df, F.count("c").over(w)) == (
        "count(c) OVER (PARTITION BY a, c ORDER BY b ASC NULLS FIRST, c DESC NULLS LAST "
        "ROWS BETWEEN -1 FOLLOWING AND 2 FOLLOWING)"
    )
    w = Window.orderBy(F.asc_nulls_last("b")).rowsBetween(Window.currentRow, Window.unboundedFollowing)
    assert name(df, F.sum(df.c + 1).over(w)) == (
        "sum((c + 1)) OVER (ORDER BY b ASC NULLS LAST ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)"
    )
    w = Window.orderBy("b").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    assert name(df, F.avg("c").over(w)) == (
        "avg(c) OVER (ORDER BY b ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
    )


def test_window_names_in_sql(spark, df):
    df.createOrReplaceTempView("window_names")
    queries = {
        "SELECT row_number() OVER (PARTITION BY a ORDER BY b) FROM window_names": (
            "row_number() OVER (PARTITION BY a ORDER BY b ASC NULLS FIRST "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        ),
        "SELECT sum(c) OVER (ORDER BY b DESC ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) FROM window_names": (
            "sum(c) OVER (ORDER BY b DESC NULLS LAST ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)"
        ),
        "SELECT lag(c) OVER w FROM window_names WINDOW w AS (ORDER BY b)": (
            "lag(c, 1, NULL) OVER (ORDER BY b ASC NULLS FIRST ROWS BETWEEN -1 FOLLOWING AND -1 FOLLOWING)"
        ),
        "SELECT count(*) OVER () FROM window_names": (
            "count(1) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
        ),
        "SELECT max(c) OVER (ORDER BY b ROWS UNBOUNDED PRECEDING) FROM window_names": (
            "max(c) OVER (ORDER BY b ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        ),
    }
    for query, expected in queries.items():
        assert spark.sql(query).columns == [expected], query


def test_window_frames_required_by_functions(df):
    rows_to_current = Window.orderBy("b").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    range_to_current = Window.orderBy("b").rangeBetween(Window.unboundedPreceding, Window.currentRow)
    assert df.select(F.row_number().over(rows_to_current)).count() == 2  # noqa: PLR2004
    assert df.select(F.cume_dist().over(range_to_current)).count() == 2  # noqa: PLR2004
    for column in [F.row_number(), F.rank(), F.ntile(2), F.lag("c", 1), F.lead("c", 1)]:
        with pytest.raises(AnalysisException):
            df.select(column.over(range_to_current)).collect()
        with pytest.raises(AnalysisException):
            df.select(column.over(Window.orderBy("b").rowsBetween(Window.currentRow, 5))).collect()
    with pytest.raises(AnalysisException):
        df.select(F.cume_dist().over(rows_to_current)).collect()
    with pytest.raises(AnalysisException):
        df.select(df.b.over(Window.orderBy("b"))).collect()
