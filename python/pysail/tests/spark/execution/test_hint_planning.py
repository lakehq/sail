import pytest
from pyspark.sql import Row

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Sail distributed plans")


@pytest.mark.parametrize("hint", ["BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"])
@pytest.mark.yamlsnapshot(group="plan")
def test_join_hint_distributed_plan_and_results(spark, snapshot, hint):
    query = f"""
        SELECT /*+ {hint}(l) */ l.id AS a, r.id AS b
        FROM range(0, 6, 1, 3) l JOIN range(0, 3, 1, 2) r ON l.id % 3 = r.id
    """  # noqa: S608
    assert sorted(spark.sql(query).collect()) == [Row(a=i, b=i % 3) for i in range(6)]
    explain = spark.sql(f"EXPLAIN CODEGEN {query}").first()[0]
    distributed = explain.split("== Distributed Plan ==\n", 1)[1]
    assert normalize_plan_text(distributed) == snapshot


@pytest.mark.parametrize("hint", ["REPARTITION_BY_RANGE(3, id)", "REBALANCE(3, id)"])
@pytest.mark.yamlsnapshot(group="plan")
def test_partition_hint_distributed_plan_and_results(spark, snapshot, hint):
    query = f"SELECT /*+ {hint} */ id FROM range(0, 12, 1, 4)"  # noqa: S608
    assert sorted(spark.sql(query).collect()) == [Row(id=i) for i in range(12)]
    explain = spark.sql(f"EXPLAIN CODEGEN {query}").first()[0]
    distributed = explain.split("== Distributed Plan ==\n", 1)[1]
    assert normalize_plan_text(distributed) == snapshot
