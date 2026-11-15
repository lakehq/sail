import pytest


# Ported from Spark 3 (3.5.9): pyspark/sql/dataframe.py, DataFrame.explain doctest.
# Checks modes and output sections without depending on Spark operator names.
@pytest.mark.parametrize(
    ("args", "kwargs", "sections"),
    [
        ((), {}, ["== Physical Plan =="]),
        ((False,), {}, ["== Physical Plan =="]),
        ((True,), {}, ["== Parsed Logical Plan ==", "== Optimized Logical Plan ==", "== Physical Plan =="]),
        (("simple",), {}, ["== Physical Plan =="]),
        ((), {"mode": "extended"}, ["== Parsed Logical Plan ==", "== Physical Plan =="]),
        (("cost",), {}, ["== Optimized Logical Plan =="]),
        ((), {"mode": "cost"}, ["== Optimized Logical Plan =="]),
        ((), {"mode": "formatted"}, ["Physical Plan"]),
        ((), {"mode": "codegen"}, ["codegen"]),
    ],
)
def test_explain_prints_requested_plan(spark, capsys, args, kwargs, sections):
    df = spark.createDataFrame([(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])
    assert df.explain(*args, **kwargs) is None
    output = capsys.readouterr().out
    for section in sections:
        assert section.lower() in output.lower()


# Regression extending Spark 3 (3.5.9): pyspark/sql/dataframe.py, DataFrame.explain doctest.
# Also verify invalid argument combinations and types.
@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"extended": True, "mode": "simple"}, ValueError),
        ({"extended": 1}, TypeError),
        ({"mode": 1}, TypeError),
        ({"mode": "unknown"}, ValueError),
    ],
)
def test_explain_rejects_invalid_arguments(spark, kwargs, error):
    with pytest.raises(error):
        spark.range(1).explain(**kwargs)
