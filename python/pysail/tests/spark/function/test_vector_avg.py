import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version


@pytest.mark.skipif(
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
