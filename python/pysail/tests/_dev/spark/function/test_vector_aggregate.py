import math
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as sf
from pyspark.sql.types import ArrayType, FloatType, StructField

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

pytestmark = pytest.mark.skipif(
    is_jvm_spark() and pyspark_version() < (4, 2),
    reason="vector aggregates require Spark 4.2+",
)


@pytest.mark.parametrize(("function", "finite_result"), [("vector_avg", 4.0), ("vector_sum", 8.0)])
@pytest.mark.parametrize("aggregation", ["single", "mixed", "filtered", "mixed_distinct", "grouped"])
@pytest.mark.parametrize("large_list", [False, True], ids=["list", "large-list"])
def test_vector_aggregate_distinct_nan_payloads(spark, tmp_path, function, finite_result, aggregation, large_list):
    bits = [0x7FC00001, 0x3F800000, 0x7FC00002, 0x3F800000, 0xFFC12345, 0x3F800000, 0x40000000, 0x40E00000]
    values = pa.Array.from_buffers(pa.float32(), len(bits), [None, pa.py_buffer(struct.pack(f"<{len(bits)}I", *bits))])
    array_class, offset_type = (pa.LargeListArray, pa.int64()) if large_list else (pa.ListArray, pa.int32())
    vectors = array_class.from_arrays(pa.array(range(0, len(bits) + 1, 2), offset_type), values)
    path = tmp_path / "vectors.parquet"
    table = pa.table({"v": pa.concat_arrays([vectors, vectors]), "g": [0] * len(vectors) + [1] * len(vectors)})
    pq.write_table(table, path)

    expression = f"{function}(DISTINCT v)"
    if aggregation == "filtered":
        expression += " FILTER (WHERE v[1] > 0F)"
    columns = [f"{expression} AS result"]
    if aggregation == "mixed":
        columns.append("count(*) AS n")
    if aggregation == "mixed_distinct":
        columns.append("count(DISTINCT v[1]) AS n")
    df = spark.read.parquet(str(path)).repartition(4)
    if aggregation == "grouped":
        result = df.groupBy("g").agg(sf.expr(columns[0])).orderBy("g")
    else:
        result = df.selectExpr(*columns)
    assert result.schema["result"] == StructField("result", ArrayType(FloatType(), containsNull=False), nullable=True)
    rows = result.collect()
    if aggregation == "grouped":
        assert [row.g for row in rows] == [0, 1]
    else:
        assert len(rows) == 1
    for actual in rows:
        assert math.isnan(actual.result[0])
        assert actual.result[1:] == [finite_result]
        if aggregation == "mixed":
            assert actual.n == table.num_rows
        elif aggregation == "mixed_distinct":
            assert actual.n == len({1.0, 7.0})


@pytest.mark.parametrize("function", ["vector_avg", "vector_sum"])
@pytest.mark.parametrize("projection", ["ordinary", "distinct", "mixed"])
@pytest.mark.parametrize("large_list", [False, True], ids=["list", "large-list"])
def test_vector_aggregate_signed_zero_singleton(spark, tmp_path, function, projection, large_list):
    path = tmp_path / "signed_zero.parquet"
    list_type = pa.large_list(pa.float32()) if large_list else pa.list_(pa.float32())
    pq.write_table(pa.table({"v": pa.array([[-0.0]], type=list_type)}), path)
    # Scan directly: Spark 4.2 AQE can elide DISTINCT after repartitioning a single row.
    vectors = spark.read.parquet(str(path))
    assert math.copysign(1.0, vectors.first().v[0]) == -1.0

    # Keep standalone DISTINCT separate: a mixed projection can bypass its GROUP BY rewrite.
    columns = ["ordinary", "distinct"] if projection == "mixed" else [projection]
    result = vectors.selectExpr(
        *(f"{function}({'DISTINCT ' if column == 'distinct' else ''}v) AS {column}" for column in columns)
    )
    (actual,) = result.collect()
    for column in columns:
        assert result.schema[column] == StructField(column, ArrayType(FloatType(), containsNull=False), nullable=True)
        assert actual[column] == [0.0]
        assert math.copysign(1.0, actual[column][0]) == (1.0 if column == "distinct" else -1.0)
