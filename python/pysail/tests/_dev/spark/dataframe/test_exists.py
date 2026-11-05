import pytest
from pyspark.sql import functions as F  # noqa: N812


@pytest.mark.spark(version=">=4", reason="Uses DataFrame.exists")
@pytest.mark.spark(
    skip_if_jvm=True, reason="Tests Sail rejection of correlated EXISTS with nested aggregation or LIMIT/OFFSET"
)
@pytest.mark.parametrize("negated", [False, True], ids=["exists", "not-exists"])
def test_correlated_exists_rejects_nested_limit_before_grouping(spark, negated):
    candidate = spark.createDataFrame([(1,), (2,), (3,)], "id INT").alias("c")
    lookup = spark.createDataFrame([(1,), (1,), (2,), (2,)], "id INT").alias("l")
    # PySpark 4.0 resolves string grouping keys before the outer query is bound.
    subquery = (
        lookup.where(F.col("l.id") == F.col("c.id").outer())
        .limit(1)
        .groupBy(F.col("id"))
        .count()
        .where(F.col("count") > 1)
    )
    present = subquery.exists()
    if negated:
        present = ~present
    with pytest.raises(Exception, match="projected correlated EXISTS with nested scalar aggregation or LIMIT/OFFSET"):
        candidate.select("id", present.alias("present")).collect()
