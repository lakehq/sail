import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import Row


@pytest.fixture
def events(spark):
    rows = [
        ("a", "2016-03-11 09:00:20", 4),
        ("a", "2016-03-11 09:00:03", 2),
        ("a", "2016-03-11 09:00:00", 1),
        ("b", "2016-03-11 09:00:02", 8),
        ("b", None, 64),
        ("c", "2016-03-11 09:00:00", 16),
        ("c", "2016-03-11 09:00:05", 32),
    ]
    df = spark.createDataFrame(rows, "key string, time string, value int")
    return df.withColumn("time", F.to_timestamp("time"))


def sessions(df):
    return df.select(
        *[c for c in df.columns if c != "session_window"],
        df.session_window.start.cast("string").alias("start"),
        df.session_window.end.cast("string").alias("end"),
    )


def test_session_window_of_a_row(spark):
    df = spark.createDataFrame([("2016-03-11 09:00:07", 1), (None, 2)], "time string, value int")
    df = df.withColumn("time", F.to_timestamp("time"))
    actual = df.select(F.session_window("time", "5 seconds"))
    assert actual.columns == ["session_window"]
    rows = sessions(actual).collect()
    # The row without a time is removed.
    assert rows == [Row(start="2016-03-11 09:00:07", end="2016-03-11 09:00:12")]


def test_sessions_are_merged_for_each_key(events):
    df = events.groupBy("key", F.session_window("time", "5 seconds")).agg(F.sum("value").alias("total"))
    actual = sessions(df).orderBy("key", "start").collect()
    assert actual == [
        # The sessions of the rows at 09:00:00 and 09:00:03 overlap, and the row at 09:00:20 is alone.
        Row(key="a", total=3, start="2016-03-11 09:00:00", end="2016-03-11 09:00:08"),
        Row(key="a", total=4, start="2016-03-11 09:00:20", end="2016-03-11 09:00:25"),
        # The row without a time is ignored.
        Row(key="b", total=8, start="2016-03-11 09:00:02", end="2016-03-11 09:00:07"),
        # The window of the row at 09:00:00 ends when the window of the row at 09:00:05 starts.
        Row(key="c", total=48, start="2016-03-11 09:00:00", end="2016-03-11 09:00:10"),
    ]


def test_sessions_are_merged_without_a_key(events):
    df = events.groupBy(F.session_window("time", "5 seconds")).agg(F.sum("value").alias("total"))
    actual = sessions(df).orderBy("start").collect()
    assert actual == [
        Row(total=59, start="2016-03-11 09:00:00", end="2016-03-11 09:00:10"),
        Row(total=4, start="2016-03-11 09:00:20", end="2016-03-11 09:00:25"),
    ]


def test_session_window_in_a_sql_query(events):
    events.createOrReplaceTempView("session_events")
    df = events.sparkSession.sql(
        "SELECT key, count(*) AS n FROM session_events "
        "GROUP BY key, session_window(time, '5 seconds') ORDER BY key, n DESC"
    )
    assert [(row.key, row.n) for row in df.collect()] == [("a", 2), ("a", 1), ("b", 1), ("c", 2)]


def test_session_window_with_multiple_windows_is_rejected(events):
    with pytest.raises(AnalysisException, match="Multiple time/session window expressions"):
        events.groupBy(F.session_window("time", "5 seconds"), F.session_window("time", "7 seconds")).count().collect()
