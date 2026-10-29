import operator

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version


def _optimized_plan(spark, query):
    plan = spark.sql(f"EXPLAIN EXTENDED {query}").first()[0]
    return plan.split("== Optimized Logical Plan ==", 1)[1].split("== Physical Plan ==", 1)[0]


_COMPARISONS = {
    "=": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "IS DISTINCT FROM": operator.ne,
    "IS NOT DISTINCT FROM": operator.eq,
}


@pytest.mark.parametrize("comparison", _COMPARISONS)
@pytest.mark.parametrize("reversed_operands", [False, True])
@pytest.mark.parametrize(("source", "target", "literal"), [("FLOAT", "DOUBLE", 1.25), ("SMALLINT", "FLOAT", 1)])
def test_exact_widening_literal_comparisons_remove_the_cast(
    spark, comparison, reversed_operands, source, target, literal
):
    values = [-2, -0.0, 0, 1, 1.25, 3, None] if source == "FLOAT" else [-32768, -1, 0, 1, 32767, None]
    schema = f"seq int, k {source.lower()}"
    raw = [(i, float(x) if source == "FLOAT" and x is not None else x) for i, x in enumerate(values)]
    spark.createDataFrame(raw, schema).createOrReplaceTempView("optimizer_literal_keys")
    left, right = f"CAST(k AS {target})", f"CAST({literal} AS {target})"
    if reversed_operands:
        left, right = right, left
    query = f"SELECT seq,{left} {comparison} {right} AS result FROM optimizer_literal_keys ORDER BY seq"  # noqa: S608
    expected = []
    for index, value in enumerate(values):
        if value is None:
            result = comparison == "IS DISTINCT FROM" if "DISTINCT" in comparison else None
        else:
            operands = (literal, value) if reversed_operands else (value, literal)
            result = _COMPARISONS[comparison](*operands)
        expected.append((index, result))
    try:
        assert [tuple(row) for row in spark.sql(query).collect()] == expected
        if not is_jvm_spark():
            plan = _optimized_plan(spark, query)
            assert "CAST(" not in plan
            assert "TableScan:" in plan
    finally:
        spark.catalog.dropTempView("optimizer_literal_keys")


@pytest.mark.skipif(is_jvm_spark(), reason="Spark simplifies nonrepresentable comparisons beyond Sail's exact rewrite")
@pytest.mark.parametrize("literal", ["1.0000000000000002", "16777217", "'NaN'"])
def test_float_widening_literal_comparison_keeps_inexact_or_nan_casts(spark, literal):
    spark.createDataFrame([(0, 1.0), (1, 16777216.0), (2, None)], "seq int,k float").createOrReplaceTempView(
        "optimizer_literal_guards"
    )
    query = f"""SELECT seq,CAST(k AS DOUBLE)=CAST({literal} AS DOUBLE) AS result
                FROM optimizer_literal_guards ORDER BY seq"""  # noqa: S608
    try:
        assert [tuple(row) for row in spark.sql(query).collect()] == [(0, False), (1, False), (2, None)]
        assert "AS Float64)" in _optimized_plan(spark, query)
    finally:
        spark.catalog.dropTempView("optimizer_literal_guards")


@pytest.mark.skipif(is_jvm_spark(), reason="Preserve Sail's existing TRY_CAST schema contract")
@pytest.mark.parametrize(
    ("query", "nullable", "values"),
    [
        (
            "SELECT CAST(CAST(id AS FLOAT) AS DOUBLE)=CAST(1.5 AS DOUBLE) AS matched FROM range(0,3)",
            False,
            [False, False, False],
        ),
        (
            "SELECT TRY_CAST(CAST(id AS FLOAT) AS DOUBLE)=CAST(1.5 AS DOUBLE) AS matched FROM range(0,3)",
            True,
            [False, False, False],
        ),
        (
            "SELECT TRY_CAST(k AS DOUBLE)=CAST(1.5 AS DOUBLE) AS matched "
            "FROM VALUES(CAST(0 AS FLOAT)),(CAST(1.5 AS FLOAT)),(CAST(NULL AS FLOAT)) AS t(k)",
            True,
            [False, True, None],
        ),
    ],
)
def test_widening_comparisons_preserve_analyzed_and_execution_nullability(spark, query, nullable, values):
    frame = spark.sql(query)
    assert frame.schema["matched"].nullable is nullable
    if pyspark_version() < (4,):
        client = frame._session.client  # noqa: SLF001
        arrow, execution_schema = client.to_table(frame._plan.to_proto(client))  # noqa: SLF001
    else:
        arrow, execution_schema = frame._to_table()  # noqa: SLF001
    assert (execution_schema or frame.schema)["matched"].nullable is nullable
    assert arrow.schema.field("matched").nullable is nullable
    assert arrow.column("matched").to_pylist() == values
