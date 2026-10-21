import pandas as pd
import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version


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


@pytest.mark.parametrize("slide", ["5 seconds", "10 seconds"])
def test_grouped_pandas_reuses_window_struct_in_data_and_key(spark, slide):
    df = (
        spark.createDataFrame([(0, "1970-01-01 00:00:00"), (1, "1970-01-01 00:00:05")], "id long, ts string")
        .withColumn("ts", F.to_timestamp("ts"))
        .withColumn("w", F.window("ts", "10 seconds", slide))
    )

    def identity(key, pdf):
        assert list(pdf.columns) == ["id", "ts", "w"]
        assert all(value == key[0] for value in pdf.w)
        return pdf

    result = df.groupBy("w").applyInPandas(identity, df.schema)
    assert result.schema == df.schema
    # Spark 4 caches the supplied schema; a projection also checks the server's schema.
    assert result.select("*").schema == df.schema
    # Keep pre-epoch values in the UDF, but avoid the client local-datetime conversion.
    columns = ["id", "CAST(ts AS STRING) AS ts", "CAST(w.start AS STRING) AS start", "CAST(w.end AS STRING) AS end"]
    actual = result.selectExpr(*columns).orderBy("id", "start").collect()
    expected = df.selectExpr(*columns).orderBy("id", "start").collect()
    assert actual == expected


@pytest.mark.parametrize(
    "names",
    [
        pytest.param(
            ("a", "a"),
            marks=pytest.mark.skipif(is_jvm_spark(), reason="Spark rejects duplicate grouped-Pandas output fields"),
        ),
        ("a.b", "c"),
    ],
)
def test_grouped_pandas_preserves_output_field_names_and_nullability(spark, names):
    from pyspark.sql.types import LongType, StructField, StructType

    schema = StructType([StructField(names[0], LongType(), False), StructField(names[1], LongType(), True)])

    def values(pdf):  # noqa: ARG001
        return pd.DataFrame([[1, 2], [3, None]])

    result = spark.range(1).groupBy().applyInPandas(values, schema)
    assert result.select("*").schema == schema
    assert sorted(tuple(row) for row in result.collect()) == [(1, 2), (3, None)]


def test_grouped_pandas_preserves_empty_output_rows(spark):
    from pyspark.sql import Row
    from pyspark.sql.types import StructType

    result = spark.range(1).groupBy().applyInPandas(lambda _: pd.DataFrame(index=[0, 1]), StructType([]))
    assert result.select("*").schema == StructType([])
    assert result.collect() == [Row(), Row()]


@pytest.mark.parametrize(
    "api",
    [
        "pandas",
        pytest.param(
            "arrow", marks=pytest.mark.skipif(pyspark_version() < (4,), reason="applyInArrow requires PySpark 4+")
        ),
    ],
)
def test_grouped_map_preserves_empty_nested_structs(spark, api):
    import pyarrow as pa
    from pyspark.sql import Row

    grouped = spark.range(1).groupBy()
    schema = "id long, empty struct<>"
    if api == "pandas":
        result = grouped.applyInPandas(lambda _: pd.DataFrame({"id": [1, 2, 3], "empty": [{}, None, {}]}), schema)
    else:
        result = grouped.applyInArrow(
            lambda _: pa.table({"id": [1, 2, 3], "empty": pa.array([{}, None, {}], type=pa.struct([]))}), schema
        )
    assert result.orderBy("id").collect() == [Row(id=1, empty=Row()), Row(id=2, empty=None), Row(id=3, empty=Row())]
