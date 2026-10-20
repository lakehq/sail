import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pysail.testing.spark.utils.common import pyspark_version

VALID_VALUE = 7

QUERIES = {
    "case": "SELECT CASE WHEN k THEN a ELSE b END AS r FROM left_lists CROSS JOIN right_lists",
    "if": "SELECT if(k, a, b) AS r FROM left_lists CROSS JOIN right_lists",
}


@pytest.mark.parametrize(
    ("list_type", "other_type", "k", "expected", "element_type"),
    [
        # Spark has a single array type, so a `list` branch and a `large_list` branch are the same
        # kind of branch and only their element types are compared.
        pytest.param(pa.list_(pa.int32()), pa.large_list(pa.int32()), True, [1, 2], "int", id="list_then"),
        pytest.param(pa.list_(pa.int32()), pa.large_list(pa.int32()), False, [3, 4], "int", id="large_list_else"),
        pytest.param(pa.large_list(pa.int32()), pa.list_(pa.int32()), True, [1, 2], "int", id="large_list_then"),
        pytest.param(
            pa.list_(pa.int64()), pa.large_list(pa.int32()), False, [3, 4], "bigint", id="wider_list_elements"
        ),
        pytest.param(
            pa.list_(pa.int32()), pa.large_list(pa.int64()), False, [3, 4], "bigint", id="wider_large_elements"
        ),
    ],
)
@pytest.mark.parametrize("builder", ["case", "if"])
def test_list_kinds_are_one_array_type(spark, tmp_path, builder, list_type, other_type, k, expected, element_type):
    """Branches that differ only in the Arrow list flavor merge into one `array`, as in Spark.

    Parquet written by other tools can hold `large_list`; Spark reads it as `array<T>`, and a
    conditional over a mix of both used to type fine on `main`.
    """
    pq.write_table(
        pa.table({"k": [k], "a": pa.array([[1, 2]], type=list_type)}),
        tmp_path / "left.parquet",
    )
    pq.write_table(
        pa.table({"b": pa.array([[3, 4]], type=other_type)}),
        tmp_path / "right.parquet",
    )
    spark.read.parquet(str(tmp_path / "left.parquet")).createOrReplaceTempView("left_lists")
    spark.read.parquet(str(tmp_path / "right.parquet")).createOrReplaceTempView("right_lists")
    df = spark.sql(QUERIES[builder])

    assert df.schema["r"].dataType.simpleString() == f"array<{element_type}>"
    assert df.collect()[0][0] == expected


@pytest.mark.parametrize(
    ("binary_type", "expr", "k", "expected"),
    [
        pytest.param(pa.binary(4), "CASE WHEN k THEN a ELSE X'01' END", True, b"abcd", id="fixed_size_then_case"),
        pytest.param(pa.binary(4), "CASE WHEN k THEN a ELSE X'01' END", False, b"\x01", id="fixed_size_else_case"),
        pytest.param(pa.binary(4), "CASE WHEN k THEN X'01' ELSE a END", False, b"abcd", id="fixed_size_else_taken"),
        pytest.param(pa.binary(4), "if(k, a, X'01')", True, b"abcd", id="fixed_size_if_then"),
        pytest.param(pa.binary(4), "if(k, a, X'01')", False, b"\x01", id="fixed_size_if_else"),
        pytest.param(pa.large_binary(), "CASE WHEN k THEN a ELSE X'01' END", True, b"abcd", id="large_binary_case"),
    ],
)
def test_binary_flavors_are_one_binary_type(spark, tmp_path, binary_type, expr, k, expected):
    """A fixed-size or large binary column merges with a BINARY branch, as Spark has one BINARY type.

    Parquet `FIXED_LEN_BYTE_ARRAY` (UUIDs, hashes) reads as `FixedSizeBinary`; `main` typed a
    conditional over it and a `BINARY` literal without complaint.
    """
    pq.write_table(pa.table({"k": [k], "a": pa.array([b"abcd"], type=binary_type)}), tmp_path / "bin.parquet")
    spark.read.parquet(str(tmp_path / "bin.parquet")).createOrReplaceTempView("binary_flavors")

    df = spark.sql(f"SELECT {expr} AS r FROM binary_flavors")  # noqa: S608

    assert df.schema["r"].dataType.simpleString() == "binary"
    assert df.collect()[0][0] == expected


@pytest.mark.skipif(pyspark_version() < (4,), reason="createDataFrame(pa.Table) requires PySpark 4+")
@pytest.mark.parametrize(
    "query",
    [
        pytest.param(
            "SELECT id, CASE WHEN id < 0 THEN named_struct('a', 1) ELSE s END AS r FROM src ORDER BY id",
            id="struct_else",
        ),
        pytest.param(
            "SELECT id, CASE WHEN id >= 0 THEN s ELSE named_struct('a', 1) END AS r FROM src ORDER BY id",
            id="struct_then",
        ),
        pytest.param("SELECT id, CASE WHEN id < 0 THEN array(1) ELSE l END AS r FROM src ORDER BY id", id="list_else"),
        pytest.param("SELECT id, if(id < 0, array(1), l) AS r FROM src ORDER BY id", id="list_if"),
    ],
)
def test_a_null_parent_hides_the_values_under_it(spark, query):
    """The strings an Arrow array keeps under a NULL struct or list are not converted.

    Arrow only defines the validity of a nested array's slots: the child under a NULL parent can hold
    any value (a compute kernel leaves whatever it had). Spark never reads them, so a value that
    would not parse there must not raise under ANSI.
    """
    garbage = pa.array(["abc", "7"])
    struct_array = pa.StructArray.from_arrays([garbage], names=["a"], mask=pa.array([True, False]))
    list_array = pa.ListArray.from_arrays(pa.array([0, 1, 2], pa.int32()), garbage, mask=pa.array([True, False]))
    spark.createDataFrame(pa.table({"id": [0, 1], "s": struct_array, "l": list_array})).createOrReplaceTempView("src")

    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        rows = spark.sql(query).collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)

    assert rows[0][1] is None
    value = rows[1][1]
    assert value == [VALID_VALUE] if isinstance(value, list) else value.a == VALID_VALUE
