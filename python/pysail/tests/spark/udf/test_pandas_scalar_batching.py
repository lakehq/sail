from decimal import Decimal

import pandas as pd
import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.functions import PandasUDFType, pandas_udf


@pytest.fixture
def arrow_batch_size(spark, request):
    key = "spark.sql.execution.arrow.maxRecordsPerBatch"
    previous = spark.conf.get(key)
    spark.conf.set(key, str(request.param))
    yield request.param
    spark.conf.set(key, previous)


@pytest.mark.parametrize("arrow_batch_size", [1, 3, 10000, 0, -1], indirect=True)
@pytest.mark.parametrize("eval_type", [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER])
def test_scalar_pandas_batch_sizes_and_values(spark, arrow_batch_size, eval_type):
    def check(batch):
        if arrow_batch_size > 0:
            assert len(batch) <= arrow_batch_size
        return batch + 1

    def check_iter(batches):
        for batch in batches:
            yield check(batch)

    function = pandas_udf(check if eval_type == PandasUDFType.SCALAR else check_iter, "long", eval_type)
    actual = spark.range(10, numPartitions=1).select(function("id").alias("value")).collect()
    assert [row.value for row in actual] == list(range(1, 11))


@pytest.mark.parametrize("arrow_batch_size", [3], indirect=True)
@pytest.mark.parametrize("count", [10, 20003])
@pytest.mark.parametrize("invocation", ["inline", "registered", "constant"])
def test_scalar_iterator_preserves_state_across_batches(spark, arrow_batch_size, count, invocation):  # noqa: ARG001
    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def running_offset(batches):
        offset = 0
        for batch in batches:
            yield pd.Series(range(offset, offset + len(batch)))
            offset += len(batch)

    if invocation == "registered":
        spark.udf.register("running_offset", running_offset)
        value = F.expr("running_offset(id)")
    elif invocation == "constant":
        value = running_offset(F.lit(1))
    else:
        value = running_offset("id")
    actual = spark.range(count, numPartitions=1).select("id", value.alias("value")).collect()
    assert sorted((row.id, row.value) for row in actual) == [(i, i) for i in range(count)]


@pytest.mark.parametrize("arrow_batch_size", [3], indirect=True)
@pytest.mark.parametrize("eval_type", [PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER])
def test_scalar_pandas_batches_preserve_struct_decimal_and_timestamp(spark, arrow_batch_size, eval_type):  # noqa: ARG001
    def result(ids, timestamps):
        return pd.DataFrame({"id": ids, "amount": [Decimal("1.25")] * len(ids), "ts": timestamps})

    def result_iter(batches):
        for ids, timestamps in batches:
            output = result(ids, timestamps)
            yield output.iloc[:1]
            yield output.iloc[1:]

    function = pandas_udf(
        result if eval_type == PandasUDFType.SCALAR else result_iter,
        "id long, amount decimal(6,2), ts timestamp",
        eval_type,
    )
    df = spark.range(10, numPartitions=1).withColumn("ts", F.to_timestamp(F.lit("2024-01-02 03:04:05")))
    expected = df.select("id", F.lit(Decimal("1.25")).alias("amount"), "ts").collect()
    actual = df.select(function("id", "ts").alias("value")).select("value.*").collect()
    assert actual == expected
    empty = df.limit(0).select(function("id", "ts").alias("value"))
    assert empty.schema["value"].dataType.simpleString() == "struct<id:bigint,amount:decimal(6,2),ts:timestamp>"
    assert empty.collect() == []


@pytest.mark.parametrize("arrow_batch_size", [3], indirect=True)
def test_scalar_pandas_input_spans_engine_batches(spark, arrow_batch_size):
    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def identity(batches):
        for batch in batches:
            assert len(batch) <= arrow_batch_size
            yield batch

    count = 20003
    actual = spark.range(count, numPartitions=1).select(identity("id").alias("id")).orderBy("id").collect()
    assert [row.id for row in actual] == list(range(count))


@pytest.mark.parametrize("arrow_batch_size", [1000], indirect=True)
def test_scalar_iterator_state_is_partition_local(spark, arrow_batch_size):  # noqa: ARG001
    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def running_offset(batches):
        offset = 0
        for batch in batches:
            yield pd.Series(range(offset, offset + len(batch)))
            offset += len(batch)

    count = 20004
    actual = spark.range(count, numPartitions=2).select("id", running_offset("id").alias("value")).collect()
    assert sorted((row.id, row.value) for row in actual) == [(i, i % (count // 2)) for i in range(count)]


@pytest.mark.parametrize("arrow_batch_size", [1000], indirect=True)
def test_scalar_iterator_aligns_prefetched_and_rebatched_output(spark, arrow_batch_size):  # noqa: ARG001
    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def add_prefetched(batches):
        inputs = list(batches)
        ids = pd.concat([batch[0] for batch in inputs], ignore_index=True)
        increments = pd.concat([batch[1] for batch in inputs], ignore_index=True)
        result = ids + increments
        yield result.iloc[:11000]
        yield result.iloc[11000:]

    actual = (
        spark.range(20003, numPartitions=1).select("id", add_prefetched(F.col("id"), F.lit(7)).alias("value")).collect()
    )
    assert sorted((row.id, row.value) for row in actual) == [(i, i + 7) for i in range(20003)]


@pytest.mark.parametrize("arrow_batch_size", [1000], indirect=True)
@pytest.mark.parametrize("operation", ["nested", "filter", "aggregate", "sort", "join", "join_both_sides"])
def test_scalar_iterator_in_relational_expressions(spark, arrow_batch_size, operation):  # noqa: ARG001
    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def add_one(batches):
        for batch in batches:
            yield batch + 1

    df = spark.range(2003, numPartitions=1)
    if operation == "nested":
        actual = df.select("id", (add_one(add_one("id")) + 1).alias("value")).collect()
        assert sorted((row.id, row.value) for row in actual) == [(i, i + 3) for i in range(2003)]
    elif operation == "filter":
        threshold = 2000
        assert sorted(row.id for row in df.filter(add_one("id") > threshold).collect()) == [2000, 2001, 2002]
    elif operation == "aggregate":
        assert df.agg(F.sum(add_one("id"))).first()[0] == sum(range(1, 2004))
    elif operation == "sort":
        assert [row.id for row in df.orderBy(add_one("id").desc()).collect()] == list(reversed(range(2003)))
    else:
        other = spark.range(2000, 2003, numPartitions=1).withColumnRenamed("id", "other")
        condition = (
            add_one(df.id - other.other) == F.lit(0)
            if operation == "join_both_sides"
            else add_one(df.id) == other.other
        )
        actual = df.join(other, condition).collect()
        assert sorted((row.id, row.other) for row in actual) == [(1999, 2000), (2000, 2001), (2001, 2002)]


@pytest.mark.parametrize("arrow_batch_size", [3], indirect=True)
@pytest.mark.parametrize("depth", [1, 2])
@pytest.mark.timeout(30, method="thread")
def test_scalar_iterator_stops_after_limit(spark, arrow_batch_size, depth):  # noqa: ARG001
    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def identity(batches):
        yield from batches

    value = F.col("id")
    for _ in range(depth):
        value = identity(value)
    assert spark.range(20003, numPartitions=1).select(value.alias("id")).limit(1).collect()[0].id == 0


@pytest.mark.parametrize("arrow_batch_size", [1000], indirect=True)
@pytest.mark.parametrize("invalid_output", ["short", "long", "unconsumed"])
def test_scalar_iterator_validates_partition_row_counts(spark, arrow_batch_size, invalid_output):  # noqa: ARG001
    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def invalid(batches):
        if invalid_output == "unconsumed":
            yield next(batches)
        else:
            values = pd.concat(list(batches), ignore_index=True)
            if invalid_output == "short":
                yield values.iloc[:-1]
            else:
                yield pd.concat([values, values.iloc[:1]], ignore_index=True)

    with pytest.raises(
        Exception,
        match=(
            r"RESULT_LENGTH_MISMATCH|RESULT_ROWS_MISMATCH|INPUT_NOT_FULLY_CONSUMED|"
            r"STOP_ITERATION_OCCURRED|OUTPUT_EXCEEDS_INPUT_ROWS|outputted more rows than input rows"
        ),
    ):
        spark.range(2003, numPartitions=1).select(invalid("id")).collect()


@pytest.mark.parametrize("limit", [False, True])
@pytest.mark.timeout(60, method="thread")
def test_scalar_iterator_aligns_large_prefetched_passthrough(spark, limit):
    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def prefetched_identity(batches):
        values = pd.concat(list(batches), ignore_index=True)
        for start in range(0, len(values), 777):
            yield values.iloc[start : start + 777]

    count = 10003
    padding = "x" * 2048
    source = spark.range(count, numPartitions=1).withColumn("payload", F.concat("id", F.lit(padding))).coalesce(1)
    result = source.select("id", "payload", prefetched_identity("id").alias("value"))
    if limit:
        assert result.limit(1).collect() == [(0, "0" + padding, 0)]
    else:
        actual = result.agg(
            F.count("*").alias("count"),
            F.sum(F.when(F.col("payload") != F.concat("id", F.lit(padding)), 1).otherwise(0)).alias("mismatches"),
            F.sum(F.abs(F.col("value") - F.col("id"))).alias("difference"),
        ).first()
        assert actual == (count, 0, 0)
