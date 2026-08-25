import pytest

from pysail.spark import diagnostics
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail diagnostics require a Sail Spark Connect server")


def test_distributed_explain_rejects_local_execution_mode(spark):
    with pytest.raises(Exception, match="distributed explain is not supported in local execution mode"):
        diagnostics.explain(spark.range(1))

    with pytest.raises(Exception, match="distributed explain is not supported in local execution mode"):
        spark.sql("EXPLAIN (TYPE DISTRIBUTED) SELECT 1").collect()


def test_distributed_explain_validates_public_options(spark):
    dataframe = spark.range(1)

    with pytest.raises(ValueError, match="unsupported explain type"):
        diagnostics.explain(dataframe, type="pipeline")
    with pytest.raises(ValueError, match="unsupported explain format"):
        diagnostics.explain(dataframe, format="yaml")
    with pytest.raises(TypeError, match="analyze and verbose must be bool values"):
        diagnostics.explain(dataframe, analyze=1)
