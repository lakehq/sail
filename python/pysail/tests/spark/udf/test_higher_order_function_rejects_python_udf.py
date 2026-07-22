import pytest
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, IntegerType

# Runs alongside the higher-order function suite.
pytestmark = [pytest.mark.transform, pytest.mark.filter, pytest.mark.exists]


@pytest.fixture(scope="module", autouse=True)
def _register_udfs(spark):
    spark.udf.register("plus_one", udf(lambda x: (x or 0) + 1, IntegerType()))
    spark.udf.register("is_even", udf(lambda x: (x or 0) % 2 == 0, BooleanType()))


# Spark rejects Python UDFs inside a higher-order function's lambda during
# analysis (`UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF`), because the
# lambda evaluator cannot drive the Python worker per element. The rejection is
# an analysis-time error, so it does not depend on executing the UDF.
@pytest.mark.parametrize(
    "query",
    [
        # Explicit lambda whose body calls a Python UDF.
        "SELECT transform(array(1, 2, 3), x -> plus_one(x)) AS r",
        "SELECT filter(array(1, 2, 3), x -> is_even(x)) AS r",
        "SELECT exists(array(1, 2, 3), x -> is_even(x)) AS r",
        # Bare (non-lambda) Python UDF expression in the lambda position.
        "SELECT transform(array(1, 2), plus_one(3)) AS r",
        "SELECT filter(array(1, 2), is_even(2)) AS r",
    ],
)
def test_python_udf_inside_higher_order_function_is_rejected(spark, query):
    with pytest.raises(Exception, match="Python UDF"):
        spark.sql(query).collect()
