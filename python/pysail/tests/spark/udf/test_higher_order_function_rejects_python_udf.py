import re

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, IntegerType

from pysail.testing.spark.utils.common import pyspark_version


@pytest.fixture(scope="module", autouse=True)
def _register_udfs(spark):
    spark.udf.register("plus_one", udf(lambda x: (x or 0) + 1, IntegerType()))
    spark.udf.register("is_even", udf(lambda x: (x or 0) % 2 == 0, BooleanType()))


# Spark rejects Python UDFs inside a higher-order function's lambda during
# analysis (`UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF`), because the
# lambda evaluator cannot drive the Python worker per element. The rejection is
# an analysis-time error, so it does not depend on executing the UDF.
@pytest.mark.parametrize(
    ("query", "udf_name"),
    [
        # Explicit lambda whose body calls a Python UDF.
        ("SELECT transform(array(1, 2, 3), x -> plus_one(x)) AS r", "plus_one"),
        ("SELECT filter(array(1, 2, 3), x -> is_even(x)) AS r", "is_even"),
        ("SELECT exists(array(1, 2, 3), x -> is_even(x)) AS r", "is_even"),
        ("SELECT forall(array(1, 2, 3), x -> is_even(x)) AS r", "is_even"),
        ("SELECT aggregate(array(1, 2, 3), 0, (a, x) -> plus_one(x)) AS r", "plus_one"),
        ("SELECT map_filter(map(1, 2), (k, v) -> is_even(v)) AS r", "is_even"),
        # Bare (non-lambda) Python UDF expression in the lambda position.
        ("SELECT transform(array(1, 2), plus_one(3)) AS r", "plus_one"),
        ("SELECT filter(array(1, 2), is_even(2)) AS r", "is_even"),
        ("SELECT exists(array(1, 2), is_even(2)) AS r", "is_even"),
        ("SELECT forall(array(1, 2), is_even(2)) AS r", "is_even"),
        ("SELECT aggregate(array(1, 2), 0, plus_one(3)) AS r", "plus_one"),
        ("SELECT map_filter(map(1, 2), is_even(2)) AS r", "is_even"),
        ("SELECT transform_keys(map(1, 2), (k, v) -> is_even(k)) AS r", "is_even"),
        ("SELECT transform_values(map(1, 2), (k, v) -> plus_one(v)) AS r", "plus_one"),
        ("SELECT transform_keys(map(1, 2), plus_one(1)) AS r", "plus_one"),
        ("SELECT transform_values(map(1, 2), plus_one(1)) AS r", "plus_one"),
    ],
)
@pytest.mark.skipif(pyspark_version() < (4,), reason="Python UDF rejection in higher-order functions needs Spark 4+")
def test_python_udf_inside_higher_order_function_is_rejected(spark, query, udf_name):
    # The rejection is an analysis-time error; narrowing to AnalysisException (not
    # a bare Exception) ensures a Python-worker crash — the very outcome this
    # feature prevents — cannot masquerade as a pass. The UDF name pins that this
    # guard fired and not another analysis error.
    with pytest.raises(AnalysisException, match=rf'Python UDF "{udf_name}'):
        spark.sql(query).collect()


# The argument count is checked before the lambda is inspected, so a Python UDF in a
# call with the wrong number of arguments reports the count, as Spark does.
@pytest.mark.parametrize(
    ("query", "name", "accepted", "actual"),
    [
        ("SELECT transform(array(1))", "transform", "2", 1),
        ("SELECT transform(array(1), is_even(2), is_even(2))", "transform", "2", 3),
        ("SELECT transform(array(1), is_even(2), is_even(2), is_even(2))", "transform", "2", 4),
        ("SELECT transform(array(1), is_even(2), is_even(2), is_even(2), is_even(2))", "transform", "2", 5),
        ("SELECT filter(array(1))", "filter", "2", 1),
        ("SELECT filter(array(1), is_even(2), is_even(2))", "filter", "2", 3),
        ("SELECT filter(array(1), is_even(2), is_even(2), is_even(2))", "filter", "2", 4),
        ("SELECT filter(array(1), is_even(2), is_even(2), is_even(2), is_even(2))", "filter", "2", 5),
        ("SELECT exists(array(1))", "exists", "2", 1),
        ("SELECT exists(array(1), is_even(2), is_even(2))", "exists", "2", 3),
        ("SELECT exists(array(1), is_even(2), is_even(2), is_even(2))", "exists", "2", 4),
        ("SELECT exists(array(1), is_even(2), is_even(2), is_even(2), is_even(2))", "exists", "2", 5),
        ("SELECT forall(array(1))", "forall", "2", 1),
        ("SELECT forall(array(1), is_even(2), is_even(2))", "forall", "2", 3),
        ("SELECT forall(array(1), is_even(2), is_even(2), is_even(2))", "forall", "2", 4),
        ("SELECT forall(array(1), is_even(2), is_even(2), is_even(2), is_even(2))", "forall", "2", 5),
        ("SELECT map_filter(map(1, 2))", "map_filter", "2", 1),
        ("SELECT map_filter(map(1, 2), is_even(2), is_even(2))", "map_filter", "2", 3),
        ("SELECT map_filter(map(1, 2), is_even(2), is_even(2), is_even(2))", "map_filter", "2", 4),
        ("SELECT map_filter(map(1, 2), is_even(2), is_even(2), is_even(2), is_even(2))", "map_filter", "2", 5),
        ("SELECT array_sort(array(1), is_even(2), is_even(2))", "array_sort", "[1, 2]", 3),
        ("SELECT array_sort(array(1), is_even(2), is_even(2), is_even(2))", "array_sort", "[1, 2]", 4),
        ("SELECT array_sort(array(1), is_even(2), is_even(2), is_even(2), is_even(2))", "array_sort", "[1, 2]", 5),
        ("SELECT aggregate(array(1))", "aggregate", "[3, 4]", 1),
        ("SELECT aggregate(array(1), is_even(2))", "aggregate", "[3, 4]", 2),
        ("SELECT aggregate(array(1), is_even(2), is_even(2), is_even(2), is_even(2))", "aggregate", "[3, 4]", 5),
        ("SELECT reduce(array(1))", "reduce", "[3, 4]", 1),
        ("SELECT reduce(array(1), is_even(2))", "reduce", "[3, 4]", 2),
        ("SELECT reduce(array(1), is_even(2), is_even(2), is_even(2), is_even(2))", "reduce", "[3, 4]", 5),
    ],
)
def test_wrong_argument_count_is_reported_before_the_python_udf(spark, query, name, accepted, actual):
    expected = (
        rf"WRONG_NUM_ARGS.*The `{name}` requires {re.escape(accepted)} parameters but the actual number is {actual}\b"
    )
    with pytest.raises(AnalysisException, match=expected):
        spark.sql(query).collect()


# Spark reports a wrongly typed value argument before it inspects the lambda for a Python UDF.
@pytest.mark.parametrize(
    "query",
    [
        "SELECT map_filter(42, is_even(1)) AS r",
        "SELECT transform_keys(42, plus_one(1)) AS r",
        "SELECT transform_values(42, plus_one(1)) AS r",
    ],
)
def test_wrong_value_argument_type_is_reported_before_the_python_udf(spark, query):
    with pytest.raises(AnalysisException) as error:
        spark.sql(query).collect()
    assert "Python UDF" not in str(error.value)


# Spark validates subqueries over the whole call before it looks for Python UDFs.
@pytest.mark.skipif(pyspark_version() < (4,), reason="Subquery rejection in higher-order functions needs Spark 4+")
def test_subquery_is_reported_before_a_python_udf_in_another_argument(spark):
    query = "SELECT aggregate(array(1), 0, (a, x) -> plus_one(a), a -> (SELECT 1)) AS r"
    with pytest.raises(AnalysisException, match="Subquery expressions are not supported"):
        spark.sql(query).collect()
