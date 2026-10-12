import pyspark.sql.functions as F  # noqa: N812
import pyspark.sql.types as T  # noqa: N812
import pytest
from pyspark.errors import PySparkException
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.expressions import DistributedSequenceID

from pysail.testing.spark.utils.common import pyspark_version


def sequence_id():
    return Column(DistributedSequenceID())


@pytest.mark.parametrize(("size", "partitions"), [(0, 4), (3, 8), (21, 4), (20000, 3), (13, 1)])
def test_distributed_sequence_id_partition_offsets(spark, size, partitions):
    source = spark.range(0, size, 1, partitions).filter("id % 3 != 1")
    source = source.withColumn("before", F.spark_partition_id())
    source = source.withColumn("ordinal", F.monotonically_increasing_id())
    indexed = source.select("*", sequence_id().alias("index"))
    assert indexed.schema == T.StructType([*source.schema.fields, T.StructField("index", T.LongType(), False)])

    rows = indexed.withColumn("after", F.spark_partition_id()).orderBy("index").collect()
    assert sorted(row.id for row in rows) == [i for i in range(size) if i % 3 != 1]
    assert [row["index"] for row in rows] == list(range(len(rows)))
    # Automatic repartitioning can change the input order; IDs must follow the
    # actual input partition and row order captured before attaching the sequence.
    assert [row.ordinal for row in rows] == sorted(row.ordinal for row in rows)
    assert all(row.before == row.after for row in rows)


def test_distributed_sequence_id_zero_column_input(spark):
    rows = spark.range(0, 17, 1, 5).select().select(sequence_id().alias("index")).collect()
    assert sorted(row["index"] for row in rows) == list(range(17))


def test_distributed_sequence_id_repeated_expressions_and_actions(spark):
    indexed = spark.range(0, 19, 1, 4).select("id", sequence_id().alias("first"), (sequence_id() + 1).alias("second"))
    expected = [(i, i, i + 1) for i in range(19)]
    assert sorted(tuple(row) for row in indexed.collect()) == expected
    assert sorted(tuple(row) for row in indexed.collect()) == expected


def test_distributed_sequence_id_filter_sort_and_limit(spark):
    indexed = spark.range(0, 23, 1, 4).withColumn("index", sequence_id())
    rows = indexed.filter("id % 3 = 0").orderBy(F.desc("id")).limit(4).collect()
    assert [(row.id, row["index"]) for row in rows] == [(21, 21), (18, 18), (15, 15), (12, 12)]
    # Recover a sort key which the projection removed, without moving the sort below the IDs.
    assert [row["index"] for row in indexed.select("index").orderBy(F.desc("id")).limit(4).collect()] == [
        22,
        21,
        20,
        19,
    ]


def test_distributed_sequence_id_respects_input_sort(spark):
    source = spark.range(0, 19, 1, 4).orderBy(F.desc("id"))
    indexed = source.withColumn("index", sequence_id())
    rows = indexed.orderBy("index").collect()
    assert [(row.id, row["index"]) for row in rows] == [(18 - i, i) for i in range(19)]


def test_distributed_sequence_id_repartitioned_input(spark):
    threshold = 0.3
    source = spark.range(0, 1001, 1, 4).filter(F.rand(42) > threshold).repartition(7)
    indexed = source.withColumn("index", sequence_id())
    rows = indexed.collect()
    assert sorted(row["index"] for row in rows) == list(range(len(rows)))
    assert len({row.id for row in rows}) == len(rows)
    assert sorted(row["index"] for row in indexed.orderBy("index").limit(5).collect()) == list(range(5))


def test_distributed_sequence_id_nested_and_joined(spark):
    indexed = spark.range(0, 21, 1, 4).withColumn("first", sequence_id())
    nested = indexed.filter("id % 3 = 0").withColumn("second", sequence_id())
    expected = [(i * 3, i * 3, i) for i in range(7)]
    assert sorted(tuple(row) for row in nested.collect()) == expected
    joined = nested.alias("a").join(nested.alias("b"), F.col("a.second") == F.col("b.second"))
    assert sorted(tuple(row) for row in joined.select("a.id", "a.first", "b.second").collect()) == expected


def test_distributed_sequence_id_in_filter_and_sort(spark):
    source = spark.range(0, 17, 1, 4)
    filtered = source.filter(sequence_id() % 2 == 0)
    assert filtered.schema == source.schema
    assert sorted(row.id for row in filtered.collect()) == list(range(0, 17, 2))
    sorted_frame = source.orderBy(sequence_id().desc())
    assert sorted_frame.schema == source.schema
    assert [row.id for row in sorted_frame.collect()] == list(reversed(range(17)))


def test_distributed_sequence_id_in_aggregate_arguments_and_grouping(spark):
    source = spark.range(0, 17, 1, 4)
    assert source.groupBy().agg(F.sum(sequence_id())).first()[0] == sum(range(17))
    grouped = source.groupBy(sequence_id().alias("key")).agg(F.sum(sequence_id()).alias("total"))
    assert sorted(tuple(row) for row in grouped.collect()) == [(i, i) for i in range(17)]


def test_distributed_sequence_id_ungrouped_aggregate_projection_is_invalid(spark):
    with pytest.raises(PySparkException):
        spark.range(0, 17, 1, 4).groupBy("id").agg(sequence_id().alias("index")).collect()


@pytest.mark.skipif(pyspark_version() < (4, 2), reason="The cache hint requires Spark 4.2+")
def test_distributed_sequence_id_pandas_cache_hint(spark):
    from pyspark.sql.internal import InternalFunction

    source = spark.range(0, 13, 1, 4)
    indexed = source.select(InternalFunction.distributed_sequence_id().alias("index"), "*")
    assert sorted(tuple(row) for row in indexed.collect()) == [(i, i) for i in range(13)]
