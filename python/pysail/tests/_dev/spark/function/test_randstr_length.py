import pytest


@pytest.mark.parametrize("query", ["SELECT randstr(-1, 0)", "SELECT randstr(-1)"])
def test_randstr_rejects_negative_length(spark, query):
    with pytest.raises(
        Exception,
        match=r"INVALID_PARAMETER_VALUE\.LENGTH.*Expects `length` greater than or equal to 0, but got -1",
    ):
        spark.sql(query).collect()


def test_randstr_accepts_zero_length(spark):
    assert spark.sql("SELECT randstr(0, 0) AS s").collect()[0].s == ""
