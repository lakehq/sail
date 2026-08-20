from decimal import Decimal

import pytest

from pysail.testing.spark.utils.common import pyspark_version

pytestmark = pytest.mark.skipif(
    pyspark_version() < (4,),
    reason="DataFrame.toArrow requires PySpark 4+",
)


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        # Each case picks a value the FIRST branch's type cannot hold, so it only passes when the
        # conditional was typed from every branch: `collect()` reads the data and survives a narrow
        # schema, but `toArrow()` builds from the declared schema and does not.
        pytest.param(
            "SELECT CASE WHEN c THEN CAST(1 AS INT) ELSE CAST(3000000000 AS BIGINT) END AS r "
            "FROM VALUES (false) AS t(c)",
            3000000000,
            id="else_bigint_past_int_max",
        ),
        pytest.param(
            "SELECT CASE WHEN c THEN CAST(1 AS INT) ELSE CAST(9223372036854775807 AS BIGINT) END AS r "
            "FROM VALUES (false) AS t(c)",
            9223372036854775807,
            id="else_bigint_at_max",
        ),
        pytest.param(
            "SELECT CASE WHEN c THEN CAST(1 AS SMALLINT) ELSE CAST(70000 AS INT) END AS r "
            "FROM VALUES (false) AS t(c)",
            70000,
            id="else_int_past_smallint",
        ),
        pytest.param(
            "SELECT CASE WHEN c THEN CAST(1 AS TINYINT) ELSE CAST(300 AS BIGINT) END AS r "
            "FROM VALUES (false) AS t(c)",
            300,
            id="else_bigint_past_tinyint",
        ),
        pytest.param(
            "SELECT CASE WHEN c THEN CAST(1 AS INT) ELSE CAST(2.5 AS DOUBLE) END AS r "
            "FROM VALUES (false) AS t(c)",
            2.5,
            id="else_double_against_int",
        ),
        pytest.param(
            "SELECT CASE WHEN c THEN CAST(1 AS INT) ELSE CAST(1.5 AS DECIMAL(12,4)) END AS r "
            "FROM VALUES (false) AS t(c)",
            Decimal("1.5000"),
            id="else_decimal_against_int",
        ),
        pytest.param(
            "SELECT CASE WHEN c THEN CAST(1 AS DECIMAL(5,2)) ELSE CAST(99999 AS INT) END AS r "
            "FROM VALUES (false) AS t(c)",
            Decimal("99999.00"),
            id="else_int_past_decimal_precision",
        ),
        pytest.param(
            "SELECT CASE WHEN c THEN CAST(1 AS DECIMAL(10,2)) ELSE CAST(123456789012 AS BIGINT) END AS r "
            "FROM VALUES (false) AS t(c)",
            Decimal("123456789012.00"),
            id="else_bigint_past_decimal_precision",
        ),
        # The wide branch as a COLUMN, not a foldable literal: a rule that only inspects literals
        # would look correct on every case above and still narrow real data.
        pytest.param(
            "SELECT CASE WHEN c THEN CAST(1 AS INT) ELSE b END AS r "
            "FROM VALUES (false, CAST(3000000000 AS BIGINT)) AS t(c, b)",
            3000000000,
            id="else_bigint_column",
        ),
        # The wide branch in a middle WHEN rather than the ELSE, so neither the first branch nor
        # the else position can be the one that decides the type.
        pytest.param(
            "SELECT CASE WHEN c THEN CAST(1 AS INT) WHEN NOT c THEN CAST(3000000000 AS BIGINT) "
            "ELSE CAST(2 AS INT) END AS r FROM VALUES (false) AS t(c)",
            3000000000,
            id="middle_when_bigint",
        ),
        # `IF` goes through the same builder and must widen the same way.
        pytest.param(
            "SELECT if(c, CAST(1 AS INT), CAST(3000000000 AS BIGINT)) AS r FROM VALUES (false) AS t(c)",
            3000000000,
            id="if_else_bigint",
        ),
        pytest.param(
            "SELECT if(c, CAST(1 AS INT), CAST(2.5 AS DOUBLE)) AS r FROM VALUES (false) AS t(c)",
            2.5,
            id="if_else_double",
        ),
        # Nested: the element type of an array branch has to widen too, or the declared
        # `array<int>` cannot carry the `array<bigint>` the data actually holds.
        pytest.param(
            "SELECT CASE WHEN c THEN array(CAST(1 AS INT)) ELSE array(CAST(3000000000 AS BIGINT)) END AS r "
            "FROM VALUES (false) AS t(c)",
            [3000000000],
            id="array_element_bigint",
        ),
    ],
)
def test_conditional_arrow_schema_holds_the_widened_value(spark, query, expected):
    """A conditional's declared schema must describe the data it carries.

    Spark widens every branch of a `CASE`/`IF` before typing it, so the reported schema is the wider
    type and an Arrow consumer can build from it. Typing the conditional from the first `THEN`
    instead leaves the schema narrow while the data stays wide: `collect()` still returns the right
    value because it reads the data, but `toArrow()` builds from the declared schema and raises
    "Integer value 3000000000 not in range" — and so would a Parquet write or an Arrow IPC stream.
    """
    df = spark.sql(query)

    assert df.toArrow().column("r").to_pylist() == [expected]
    assert df.collect()[0][0] == expected
