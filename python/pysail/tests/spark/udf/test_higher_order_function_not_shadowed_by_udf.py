import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


def test_builtin_higher_order_function_is_not_shadowed_by_a_udf(spark):
    """A user-registered function does NOT shadow a built-in higher-order
    function of the same name.

    Spark 4.2 gives a built-in precedence over a temporary function (a temp
    function cannot shadow a built-in), so `transform(1, 2)` resolves to the
    built-in ``transform`` and fails on the non-array first argument — it does
    NOT invoke the registered UDF. Asserting ``AnalysisException`` (an
    analysis-time type error) rather than any exception rules out the UDF being
    invoked, which would either return a value or raise in the Python worker.
    """
    add = udf(lambda a, b: a + b, IntegerType())
    spark.udf.register("transform", add)
    # Control: the same UDF is reachable under a non-built-in name, so the
    # `raises` below reflects precedence, not a registration that no-oped.
    spark.udf.register("transform_probe", add)
    assert spark.sql("SELECT transform_probe(1, 2) AS r").collect() == [Row(r=3)]

    # Resolves to the built-in HOF, which rejects the non-array argument. The
    # `match` keeps this from passing on an unrelated analysis error (a bare
    # `raises` would); it tolerates both engines' wording (Sail "list", Spark
    # "array").
    with pytest.raises(AnalysisException, match="(?i)list|array"):
        spark.sql("SELECT transform(1, 2) AS r").collect()

    # The built-in higher-order function still works normally.
    assert spark.sql("SELECT transform(array(1, 2, 3), x -> x + 1) AS r").collect() == [Row(r=[2, 3, 4])]


def test_builtin_higher_order_function_wins_even_at_a_non_hof_arity(spark):
    """The built-in wins regardless of argument count, not only when the call
    has a lambda position.

    Spark 4.2 searches the built-in namespace before the session one, so
    `exists(5)` resolves to the built-in ``exists`` and raises (it needs two
    arguments) instead of invoking the 1-argument UDF.
    """
    plus_hundred = udf(lambda x: (x or 0) + 100, IntegerType())
    spark.udf.register("exists", plus_hundred)
    # Control: the same UDF is reachable under a non-built-in name.
    spark.udf.register("exists_probe", plus_hundred)
    assert spark.sql("SELECT exists_probe(5) AS r").collect() == [Row(r=105)]

    # The `match` guards against passing on an unrelated analysis error; it
    # tolerates both engines' wording for the arity mismatch (Sail "two values",
    # Spark "WRONG_NUM_ARGS"/"number of arguments").
    with pytest.raises(AnalysisException, match="(?i)two values|number of arg|WRONG_NUM"):
        spark.sql("SELECT exists(5) AS r").collect()
