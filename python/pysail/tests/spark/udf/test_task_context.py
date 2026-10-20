from collections.abc import Iterator

import pandas as pd
from pyspark import TaskContext
from pyspark.sql.functions import pandas_udf

EXPECTED = [(i, i // 5) for i in range(10)]


def test_task_context_in_map_in_pandas(spark):
    def with_partition(batches):
        partition = TaskContext.get().partitionId()
        for batch in batches:
            yield pd.DataFrame({"id": batch["id"], "partition": partition})

    rows = spark.range(0, 10, 1, 2).mapInPandas(with_partition, "id long, partition int").collect()

    assert sorted((row.id, row.partition) for row in rows) == EXPECTED


def test_task_context_in_scalar_iterator_pandas_udf(spark):
    @pandas_udf("int")
    def partition_id(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
        partition = TaskContext.get().partitionId()
        for batch in batches:
            yield pd.Series([partition] * len(batch))

    rows = spark.range(0, 10, 1, 2).select("id", partition_id("id").alias("partition")).collect()

    assert sorted((row.id, row.partition) for row in rows) == EXPECTED
