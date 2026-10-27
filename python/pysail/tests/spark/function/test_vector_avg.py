import math
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as sf

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

pytestmark = pytest.mark.skipif(
    is_jvm_spark() and pyspark_version() < (4, 2),
    reason="vector_avg requires Spark 4.2+",
)


def test_vector_avg_with_parquet_large_list(spark, tmp_path):
    # Parquet large lists exercise the `LargeList` input path, which SQL literals cannot produce.
    # The remaining behavior is covered by `features/vector/vector_avg.feature`.
    path = tmp_path / "large_list_vector.parquet"
    pq.write_table(
        pa.table({"v": pa.array([[1.0, 2.0], [3.0, 6.0]], type=pa.large_list(pa.float32()))}),
        path,
    )

    actual = spark.read.parquet(str(path)).selectExpr("vector_avg(v) AS result").first()

    assert actual.result == pytest.approx([2.0, 4.0])


@pytest.mark.parametrize("aggregation", ["single", "mixed", "filtered", "mixed_distinct", "grouped"])
@pytest.mark.parametrize("large_list", [False, True], ids=["list", "large-list"])
def test_vector_avg_distinct_nan_payloads(spark, tmp_path, aggregation, large_list):
    # DISTINCT treats different NaN payloads as the same value in Spark.
    bits = [0x7FC00001, 0x3F800000, 0x7FC00002, 0x3F800000, 0xFFC12345, 0x3F800000, 0x40000000, 0x40E00000]
    values = pa.Array.from_buffers(pa.float32(), len(bits), [None, pa.py_buffer(struct.pack(f"<{len(bits)}I", *bits))])
    array_class, offset_type = (pa.LargeListArray, pa.int64()) if large_list else (pa.ListArray, pa.int32())
    vectors = array_class.from_arrays(pa.array(range(0, len(bits) + 1, 2), offset_type), values)
    path = tmp_path / "vectors.parquet"
    table = pa.table({"v": pa.concat_arrays([vectors, vectors]), "g": [0] * len(vectors) + [1] * len(vectors)})
    pq.write_table(table, path)

    expression = "vector_avg(DISTINCT v)"
    if aggregation == "filtered":
        expression += " FILTER (WHERE v[1] > 0F)"
    columns = [f"{expression} AS result"]
    if aggregation == "mixed":
        columns.append("count(*) AS n")
    if aggregation == "mixed_distinct":
        columns.append("count(DISTINCT v[1]) AS n")
    df = spark.read.parquet(str(path)).repartition(4)
    if aggregation == "grouped":
        rows = df.groupBy("g").agg(sf.expr(columns[0])).orderBy("g").collect()
        assert [row.g for row in rows] == [0, 1]
    else:
        (actual,) = df.selectExpr(*columns).collect()
        rows = [actual]
    for actual in rows:
        assert math.isnan(actual.result[0])
        assert actual.result[1:] == [4.0]
        if aggregation == "mixed":
            assert actual.n == table.num_rows
        elif aggregation == "mixed_distinct":
            assert actual.n == len({1.0, 7.0})
