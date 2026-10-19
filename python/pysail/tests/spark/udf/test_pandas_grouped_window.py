import pandas as pd
import pytest
from pyspark.sql import functions as F  # noqa: N812


@pytest.mark.parametrize("session_timezone", ["UTC", "Asia/Kathmandu"], indirect=True)
def test_grouped_pandas_sliding_time_window_keys(spark, session_timezone):  # noqa: ARG001
    df = spark.createDataFrame(
        [(0, "1970-01-01 00:00:00"), (1, "1970-01-01 00:00:05"), (2, "1970-01-01 00:00:10"), (3, None)],
        "id long, ts string",
    ).select("id", F.to_timestamp("ts").alias("ts"))
    window = F.window("ts", "10 seconds", "5 seconds")

    def summarize(key, pdf):
        assert list(pdf.columns) == ["id", "ts"]
        start, end = key[0]["start"], key[0]["end"]
        assert all((pdf.ts >= start) & (pdf.ts < end))
        return pd.DataFrame({"start": [start], "end": [end], "ids": [sorted(pdf.id.tolist())]})

    # Windows cannot convert pre-epoch timestamps to local datetimes during collect().
    actual = (
        df.groupBy(window)
        .applyInPandas(summarize, "start timestamp, end timestamp, ids array<long>")
        .selectExpr("CAST(start AS STRING) AS start", "CAST(end AS STRING) AS end", "ids")
        .orderBy("start")
        .collect()
    )
    expected = (
        df.groupBy(window.alias("w"))
        .agg(F.sort_array(F.collect_list("id")).alias("ids"))
        .select("w.start", "w.end", "ids")
        .selectExpr("CAST(start AS STRING) AS start", "CAST(end AS STRING) AS end", "ids")
        .orderBy("start")
        .collect()
    )
    assert actual == expected
    assert [row.ids for row in actual] == [[0], [0, 1], [1, 2], [2]]


def test_grouped_pandas_reuses_window_struct_in_data_and_key(spark):
    df = (
        spark.createDataFrame([(0, "1970-01-01 00:00:00"), (1, "1970-01-01 00:00:05")], "id long, ts string")
        .withColumn("ts", F.to_timestamp("ts"))
        .withColumn("w", F.window("ts", "10 seconds", "5 seconds"))
    )

    def identity(key, pdf):
        assert list(pdf.columns) == ["id", "ts", "w"]
        assert all(value == key[0] for value in pdf.w)
        return pdf

    result = df.groupBy("w").applyInPandas(identity, df.schema)
    assert result.schema == df.schema
    # Keep pre-epoch values in the UDF, but avoid the client local-datetime conversion.
    columns = ["id", "CAST(ts AS STRING) AS ts", "CAST(w.start AS STRING) AS start", "CAST(w.end AS STRING) AS end"]
    actual = result.selectExpr(*columns).orderBy("id", "start").collect()
    expected = df.selectExpr(*columns).orderBy("id", "start").collect()
    assert actual == expected
