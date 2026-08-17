import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import Row

_CONF = "spark.sql.analyzer.allowSubqueryExpressionsInLambdasOrHigherOrderFunctions"
_QUERY = "SELECT transform((SELECT array(1, 2)), x -> x + 1) AS r"


def test_subquery_in_higher_order_function_is_gated_by_the_config(spark):
    """The subquery guard (SPARK-47509) is controlled by
    ``spark.sql.analyzer.allowSubqueryExpressionsInLambdasOrHigherOrderFunctions``.

    With ``false`` (the default) a subquery in a higher-order function is rejected
    at analysis; setting it to ``true`` restores the legacy behavior and the
    subquery is evaluated. Both halves are asserted so the test proves the config
    actually changes behavior, not just that the enabled path works.
    """
    previous = spark.conf.get(_CONF, "false")
    try:
        spark.conf.set(_CONF, "false")
        with pytest.raises(AnalysisException, match="Subquery expressions are not supported"):
            spark.sql(_QUERY).collect()

        spark.conf.set(_CONF, "true")
        assert spark.sql(_QUERY).collect() == [Row(r=[2, 3])]
    finally:
        spark.conf.set(_CONF, previous)
