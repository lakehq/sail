import pytest


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_parity_dataframe.py,
# DataFrameParityTests.test_extended_hint_types; checks the API without Catalyst plan text.
@pytest.mark.parametrize(
    "parameters", [(1.2345,), ("what",), (["itworks1", "itworks2"],), (1.2345, "what", ["itworks"])]
)
def test_unknown_hint_accepts_parameters_and_preserves_rows(spark, parameters):
    df = spark.range(12)
    hinted = df.hint("my awesome hint", *parameters)
    assert isinstance(hinted, type(df))
    assert hinted.schema == df.schema
    assert hinted.orderBy("id").collect() == df.orderBy("id").collect()


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_parity_dataframe.py,
# DataFrameParityTests.test_extended_hint_types; checks the API without Catalyst plan text.
@pytest.mark.parametrize("parameters", [[], ["foo", "bar"]])
def test_hint_accepts_list_parameters(spark, parameters):
    df = spark.range(12)
    assert isinstance(df.hint("broadcast", parameters), type(df))
