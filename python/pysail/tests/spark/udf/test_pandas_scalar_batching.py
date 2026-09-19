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
def test_scalar_iterator_preserves_state_across_arrow_slices(spark, arrow_batch_size):  # noqa: ARG001
    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def running_offset(batches):
        offset = 0
        for batch in batches:
            yield pd.Series(range(offset, offset + len(batch)))
            offset += len(batch)

    actual = spark.range(10, numPartitions=1).select(running_offset("id").alias("value")).collect()
    assert [row.value for row in actual] == list(range(10))


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
