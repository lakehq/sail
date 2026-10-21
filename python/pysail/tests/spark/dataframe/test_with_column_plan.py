"""The plan `withColumn` builds, for the two shapes whose nodes this rewrite decides.

A plan snapshot moves with anything that changes the plan, DataFusion included, so there are only
the two here that this rewrite is about, each with what it is pinning:

- over a `USING` join, the key the join hid is read by the new expression and dropped in the same
  projection, so no projection of its own is left behind;
- over an aliased input, the qualifier is written on each column the input passes through rather
  than on the whole output, so there is no `SubqueryAlias` above the projection.
"""

import pytest
from pyspark.sql.functions import col, lit

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="The plan is the one Sail builds")


@pytest.mark.yamlsnapshot(group="plan")
def test_the_plan_of_with_column_reading_a_key_the_join_hid(spark, snapshot):
    left = spark.sql("SELECT 1 AS a, 11 AS b").alias("l")
    right = spark.sql("SELECT 1 AS a, 22 AS c").alias("r")

    df = left.join(right, "a").withColumn("z", col("r.a"))

    assert normalize_plan_text(df._explain_string(mode="extended")) == snapshot  # noqa: SLF001
    assert [tuple(row) for row in df.collect()] == [(1, 11, 22, 1)]


@pytest.mark.yamlsnapshot(group="plan")
def test_the_plan_of_with_column_over_an_aliased_input(spark, snapshot):
    df = spark.sql("SELECT 1 AS a, 2 AS b").alias("q").withColumn("c", lit(9))

    assert normalize_plan_text(df._explain_string(mode="extended")) == snapshot  # noqa: SLF001
    assert df.select("q.a").columns == ["a"]
