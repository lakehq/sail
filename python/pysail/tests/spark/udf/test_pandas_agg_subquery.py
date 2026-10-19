import pandas as pd
from pyspark.sql.functions import pandas_udf


def test_correlated_pandas_aggregate_does_not_evaluate_an_empty_group(spark):
    @pandas_udf("long")
    def first_nonempty(values: pd.Series) -> int:
        return int(values.iloc[0])

    spark.udf.register("first_nonempty_group", first_nonempty)
    rows = spark.sql(
        """
        SELECT t.id,
               (SELECT first_nonempty_group(u.id) FROM range(2) u WHERE u.id = t.id) AS value
        FROM range(3) t ORDER BY t.id
        """
    ).collect()
    assert [(row.id, row.value) for row in rows] == [(0, 0), (1, 1), (2, None)]
