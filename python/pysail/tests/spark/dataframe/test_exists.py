import pytest
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.exists requires PySpark 4+")
@pytest.mark.skipif(is_jvm_spark(), reason="Sail planner rejection")
@pytest.mark.parametrize("negated", [False, True], ids=["exists", "not-exists"])
def test_correlated_exists_rejects_nested_limit_before_grouping(spark, negated):
    candidate = spark.createDataFrame([(1,), (2,), (3,)], "id INT").alias("c")
    lookup = spark.createDataFrame([(1,), (1,), (2,), (2,)], "id INT").alias("l")
    subquery = (
        lookup.where(F.col("l.id") == F.col("c.id").outer()).limit(1).groupBy("id").count().where(F.col("count") > 1)
    )
    present = subquery.exists()
    if negated:
        present = ~present
    with pytest.raises(Exception, match="projected correlated EXISTS with nested scalar aggregation or LIMIT/OFFSET"):
        candidate.select("id", present.alias("present")).collect()
