from decimal import Decimal

import pytest
from pyspark.sql.types import Row


@pytest.fixture(scope="module")
def df(spark):
    return spark.sql("SELECT * FROM VALUES (1, 1, 0, NULL), (2, NULL, 1, 2.0), (3, 3, 4, 3.5) AS tab(a, b, c, d)")


# Ported from Spark 3 (3.5.9): pyspark/sql/tests/connect/test_connect_column.py,
# SparkConnectColumnTests.test_decimal, which also compares the generated column names.
# Spark rounds a decimal literal compared with an integral column (b < 2.5 becomes b < 3)
# and names the column after the rewritten comparison, so only the values are checked here.
@pytest.mark.parametrize(
    ("column", "op", "literal", "expected"),
    [
        ("a", "<", Decimal(3), [True, True, False]),
        ("a", "!=", Decimal(2), [True, False, True]),
        ("a", "==", Decimal(2), [False, True, False]),
        ("b", "<", Decimal("2.5"), [True, None, False]),
        ("b", ">", Decimal("2.5"), [False, None, True]),
        ("b", "<=", Decimal("1.0"), [True, None, False]),
        ("d", ">=", Decimal("3.0"), [None, False, True]),
    ],
)
def test_compare_with_decimal_literal(df, column, op, literal, expected):
    compare = {
        "<": lambda c: c < literal,
        ">": lambda c: c > literal,
        "<=": lambda c: c <= literal,
        ">=": lambda c: c >= literal,
        "==": lambda c: c == literal,
        "!=": lambda c: c != literal,
    }[op]
    rows = df.select("a", compare(df[column]).alias("v")).orderBy("a").collect()
    assert rows == [Row(a=a, v=v) for a, v in zip([1, 2, 3], expected, strict=True)]
