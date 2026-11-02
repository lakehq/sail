import pandas as pd
import pytest
from pyspark.sql import functions as F  # noqa: N812


@pytest.mark.parametrize("operand", ["subject", "pattern"])
@pytest.mark.parametrize("explicit_index", [False, True])
def test_regexp_instr_evaluates_nondeterministic_input_once(spark, operand, explicit_index):
    calls = 0

    @F.pandas_udf("string")
    def alternating_batch(ids):
        nonlocal calls
        calls += 1
        return pd.Series(["a" if calls % 2 else None] * len(ids))

    value = alternating_batch.asNondeterministic()("id")
    arguments = [value, F.lit("a")] if operand == "subject" else [F.lit("a"), value]
    if explicit_index:
        arguments.append(F.lit("0"))
    result = spark.range(8, numPartitions=1).select(F.regexp_instr(*arguments).alias("position"))
    assert [row.position for row in result.collect()] == [1] * 8
