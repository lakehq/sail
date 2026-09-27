import pytest

from pysail.testing.spark.utils.common import is_jvm_spark


@pytest.mark.skipif(is_jvm_spark(), reason="Sail only")
@pytest.mark.parametrize("join", ["LEFT SEMI", "LEFT ANTI"])
def test_conditionless_semi_anti_join_reads_one_filtering_row(spark, join):
    # Spark's LimitPushDown limits the right side of a conditionless semi/anti join
    # to one row, because the join only tests whether that side is empty.
    plan = spark.sql(f"EXPLAIN SELECT * FROM range(10) AS l {join} JOIN range(1000) AS r").collect()[0][0]  # noqa: S608
    assert "fetch=1" in plan
