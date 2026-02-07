import pandas as pd
from pandas.testing import assert_frame_equal


def test_vortex_write_and_read_roundtrip(spark, tmpdir):
    """Write a DataFrame as Vortex, read it back, verify data matches."""
    path = str(tmpdir / "vortex_basic")
    df = spark.range(10).selectExpr("id", "CAST(id * 2 AS LONG) AS doubled")
    df.write.format("vortex").save(path)

    result = spark.read.format("vortex").load(path).sort("id")
    expected = pd.DataFrame({"id": range(10), "doubled": [i * 2 for i in range(10)]}).astype("int64")
    assert_frame_equal(result.toPandas(), expected)


def test_vortex_write_and_read_strings(spark, tmpdir):
    """Write string data as Vortex and read it back."""
    path = str(tmpdir / "vortex_strings")
    spark.sql("SELECT 'hello' AS greeting, 42 AS num UNION ALL SELECT 'world', 99").write.format("vortex").save(path)

    result = spark.read.format("vortex").load(path).sort("num")
    rows = result.toPandas()
    assert len(rows) == 2  # noqa: PLR2004
    assert set(rows["greeting"].tolist()) == {"hello", "world"}


def test_vortex_read_with_filter_pushdown(spark, tmpdir):
    """Write data, read with a filter, verify correct rows returned."""
    path = str(tmpdir / "vortex_filter")
    spark.range(100).write.format("vortex").save(path)

    result = spark.read.format("vortex").load(path).filter("id >= 90").sort("id")
    rows = result.toPandas()
    assert len(rows) == 10  # noqa: PLR2004
    assert rows["id"].tolist() == list(range(90, 100))


def test_vortex_read_with_projection(spark, tmpdir):
    """Write multi-column data, read only a subset of columns."""
    path = str(tmpdir / "vortex_projection")
    spark.sql("SELECT 1 AS a, 2 AS b, 3 AS c").write.format("vortex").save(path)

    result = spark.read.format("vortex").load(path).select("a", "c")
    rows = result.toPandas()
    assert list(rows.columns) == ["a", "c"]
    assert rows["a"].iloc[0] == 1
    assert rows["c"].iloc[0] == 3  # noqa: PLR2004
