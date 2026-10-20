import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(
    is_jvm_spark(),
    reason="DistributedSequenceID is an expression of the Spark Connect client",
)


def distributed_sequence_id():
    from pyspark.sql.connect.column import Column
    from pyspark.sql.connect.expressions import DistributedSequenceID

    return Column(DistributedSequenceID())


def test_distributed_sequence_id_numbers_rows_in_partition_order(spark):
    df = spark.range(0, 10, 1, 3)
    rows = df.select(distributed_sequence_id().alias("index"), "*").collect()
    assert sorted((row["index"], row["id"]) for row in rows) == [(i, i) for i in range(10)]


def test_distributed_sequence_id_is_consecutive_after_a_filter(spark):
    df = spark.range(0, 20, 1, 4).filter("id % 3 = 0")
    rows = df.select(distributed_sequence_id().alias("index"), "id").collect()
    assert sorted((row["index"], row["id"]) for row in rows) == list(enumerate(range(0, 20, 3)))
