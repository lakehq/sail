import pytest


@pytest.mark.parametrize("fn", ["nvl", "ifnull"])
def test_nvl_of_a_date_list_and_a_timestamp_list_is_collected(spark, fn):
    # Spark widens the pair to `array<timestamp>` (`TypeCoercionHelper.scala:141`); the query must at
    # least be answered, as it was before `nvl` was routed to `coalesce`.
    query = f"SELECT {fn}(array(DATE'2024-01-01'), array(TIMESTAMP'2024-01-01 00:00:00')) AS v"
    assert len(spark.sql(query).collect()) == 1
    assert len(spark.sql(f"SELECT to_json(named_struct('v', ({query})))").collect()) == 1
