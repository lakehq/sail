import pyarrow as pa
import pytest


@pytest.mark.parametrize("operation", ["IF(id = 9, {numeric}, v)", "NVL2(NULLIF(id, 9), v, {numeric})", "union"])
@pytest.mark.parametrize("sliced", [False, True])
@pytest.mark.parametrize("container", ["struct", "array", "map", "array_struct", "struct_array"])
def test_conditional_cast_skips_values_hidden_by_null_parents(spark, operation, sliced, container):
    # Arrow permits non-null child values underneath null containers. SQL literals
    # cannot construct these buffers, so exercise the DataFrame Arrow interface.
    values = pa.array(["bad", "7", "bad"])
    mask = pa.array([True, False, True])
    offsets = pa.array([0, 1, 2, 3], type=pa.int32())
    if container == "struct":
        array = pa.StructArray.from_arrays([values], fields=[pa.field("x", pa.string(), nullable=False)], mask=mask)
        numeric, leaf = "named_struct('x', 1)", "v.x"
    elif container == "array":
        array = pa.ListArray.from_arrays(offsets, values, mask=mask)
        numeric, leaf = "array(1)", "v[0]"
    elif container == "map":
        array = pa.MapArray.from_arrays(offsets, pa.array(["x"] * 3), values, mask=mask)
        numeric, leaf = "map('x', 1)", "v['x']"
    elif container == "array_struct":
        child = pa.StructArray.from_arrays([values], names=["x"], mask=mask)
        array = pa.ListArray.from_arrays(offsets, child)
        numeric, leaf = "array(named_struct('x', 1))", "v[0].x"
    else:
        child = pa.ListArray.from_arrays(offsets, values)
        array = pa.StructArray.from_arrays([child], names=["x"], mask=mask)
        numeric, leaf = "named_struct('x', array(1))", "v.x[0]"
    if sliced:
        array = pa.concat_arrays([array, array]).slice(1, 3)
        expected = [(0, 7), (1, None), (2, None)]
    else:
        expected = [(0, None), (1, 7), (2, None)]

    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        frame = spark.createDataFrame(pa.table({"id": pa.array([0, 1, 2]), "v": array}))
        if operation == "union":
            result = frame.union(spark.sql(f"SELECT 3L AS id, {numeric} AS v"))
            expected.append((3, 1))
        else:
            result = frame.selectExpr("id", f"{operation.format(numeric=numeric)} AS v")
        # Reading the complete container forces the strict nested cast, without
        # allowing projection pruning to discard the parent validity bitmap.
        rows = result.orderBy("id").collect()
        assert len(rows) == len(expected)
        assert result.selectExpr("id", f"{leaf} AS value").orderBy("id").collect() == expected
        assert result.selectExpr(f"{leaf} AS value").dtypes == [("value", "bigint")]
        if container != "array_struct":
            assert [row.v is None for row in rows] == [value is None for _, value in expected]
        else:
            assert [row.v[0] is None for row in rows] == [value is None for _, value in expected]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


@pytest.mark.parametrize("container", ["named_struct('x', v)", "array(v)", "map('x', v)"])
def test_conditional_nested_cast_still_rejects_present_invalid_values(spark, container):
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        strings = spark.createDataFrame([("bad",)], "v string").selectExpr(f"{container} AS v")
        numeric = spark.sql("SELECT 1 AS v").selectExpr(f"{container} AS v")
        with pytest.raises(Exception, match=r"(?i)(CAST_INVALID_INPUT|cast error|cannot cast)"):
            strings.union(numeric).collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)


def test_conditional_cast_of_an_all_null_struct(spark):
    array = pa.StructArray.from_arrays(
        [pa.array(["bad", "bad"])],
        fields=[pa.field("x", pa.string(), nullable=False)],
        mask=pa.array([True, True]),
    )
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        frame = spark.createDataFrame(pa.table({"id": [0, 1], "v": array}))
        result = frame.selectExpr("IF(id = 9, named_struct('x', 1), v) AS v")
        assert result.dtypes == [("v", "struct<x:bigint>")]
        assert result.collect() == [(None,), (None,)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
