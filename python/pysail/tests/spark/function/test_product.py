"""DataFrame-API tests for ``pyspark.sql.functions.product``.

This is the DataFrame-API equivalent of ``features/product.feature``. Spark SQL
does NOT currently recognise ``product`` as a routine (``SELECT product(...)``
raises ``UNRESOLVED_ROUTINE`` on JVM Spark); the function is exposed only through
the DataFrame API. The ``.feature`` file therefore exercises the SQL form (which
only Sail understands, hence ``@sail-only``), while this file exercises the
DataFrame form, which runs against both Sail and JVM Spark.

When Spark SQL eventually enables ``product`` as a routine, drop the ``@sail-only``
tag from ``features/product.feature`` AND delete this file.

Expected values validated against Spark JVM 4.1.
"""

import math

import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.window import Window

# Mirror the `@product` tag on `features/product.feature` so `pytest -m product`
# selects both the SQL feature scenarios and these DataFrame-API tests together.
# NOTE: unlike the feature, this file is NOT `@sail-only` — `F.product` works on
# both Sail and JVM Spark, so these tests run against both engines on purpose.
pytestmark = pytest.mark.product


def _product(spark, sql):
    """Aggregate ``F.product`` over column ``v`` of the relation built from ``sql``."""
    return spark.sql(sql).agg(F.product("v").alias("result")).collect()[0]["result"]


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT * FROM VALUES (1), (2), (3), (4) AS t(v)", 24.0),
        ("SELECT * FROM VALUES (5) AS t(v)", 5.0),
        ("SELECT * FROM VALUES (2), (CAST(NULL AS INT)), (3), (4) AS t(v)", 24.0),
        ("SELECT * FROM VALUES (2), (0), (3) AS t(v)", 0.0),
        ("SELECT * FROM VALUES (-2), (3), (-4) AS t(v)", 24.0),
        ("SELECT * FROM VALUES (1.5D), (2.0D), (2.0D) AS t(v)", 6.0),
        ("SELECT * FROM VALUES (1.5), (2.0), (4.0) AS t(v)", 12.0),
        ("SELECT CAST(v AS FLOAT) AS v FROM VALUES (1.5), (2.0) AS t(v)", 3.0),
        ("SELECT CAST(v AS DECIMAL(10, 2)) AS v FROM VALUES (1), (2), (3) AS t(v)", 6.0),
        ("SELECT * FROM VALUES ('2'), ('3') AS t(v)", 6.0),
        # Sign and magnitude.
        ("SELECT * FROM VALUES (1e-200D), (1e200D) AS t(v)", 1.0),
        ("SELECT * FROM VALUES (1.0D), (1.0D), (1.0D) AS t(v)", 1.0),
        ("SELECT * FROM VALUES (0.5D), (0.5D), (0.5D) AS t(v)", 0.125),
        ("SELECT * FROM VALUES (CAST(NULL AS INT)), (7) AS t(v)", 7.0),
        ("SELECT * FROM VALUES (1e-300D), (1e-300D) AS t(v)", 0.0),
    ],
)
def test_product_value(spark, sql, expected):
    assert _product(spark, sql) == expected


def test_product_bigint_value(spark):
    # As a collected double this equals 1e18 exactly on both Sail and Spark; the
    # `1e18` vs `1.0E18` divergence is a show-string formatting gap (see the
    # @sail-bug scenario in product.feature), not a value difference, so the
    # numeric assertion holds on both engines.
    expected = 1000000.0**3  # 1e18
    sql = "SELECT CAST(v AS BIGINT) AS v FROM VALUES (1000000), (1000000), (1000000) AS t(v)"
    assert _product(spark, sql) == expected


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS t(v)",
        "SELECT * FROM VALUES (1) AS t(v) WHERE v > 100",
    ],
)
def test_product_null(spark, sql):
    assert _product(spark, sql) is None


@pytest.mark.parametrize(
    ("sql", "sign"),
    [
        ("SELECT * FROM VALUES (1e200D), (1e200D) AS t(v)", 1.0),
        ("SELECT * FROM VALUES (1.7e308D), (10.0D) AS t(v)", 1.0),
        ("SELECT * FROM VALUES (double('inf')) AS t(v)", 1.0),
        ("SELECT * FROM VALUES (double('-inf')) AS t(v)", -1.0),
        ("SELECT * FROM VALUES (double('inf')), (double('-inf')) AS t(v)", -1.0),
        ("SELECT * FROM VALUES (double('-inf')), (double('-inf')) AS t(v)", 1.0),
        ("SELECT * FROM VALUES (double('inf')), (-2.0D) AS t(v)", -1.0),
        ("SELECT * FROM VALUES (double('inf')), (CAST(NULL AS DOUBLE)) AS t(v)", 1.0),
        ("SELECT * FROM VALUES (double('-inf')), (CAST(NULL AS DOUBLE)) AS t(v)", -1.0),
    ],
)
def test_product_infinity(spark, sql, sign):
    result = _product(spark, sql)
    assert math.isinf(result)
    assert math.copysign(1.0, result) == sign


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM VALUES (double('nan')), (2.0D) AS t(v)",
        "SELECT * FROM VALUES (double('inf')), (0.0D) AS t(v)",
        "SELECT * FROM VALUES (double('-inf')), (0.0D) AS t(v)",
        "SELECT * FROM VALUES (double('nan')) AS t(v)",
        "SELECT * FROM VALUES (double('nan')), (double('inf')) AS t(v)",
        "SELECT * FROM VALUES (double('nan')), (CAST(NULL AS DOUBLE)) AS t(v)",
    ],
)
def test_product_nan(spark, sql):
    assert math.isnan(_product(spark, sql))


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM VALUES (CAST('-0.0' AS DOUBLE)) AS t(v)",
        "SELECT * FROM VALUES (-2.0D), (0.0D) AS t(v)",
    ],
)
def test_product_negative_zero(spark, sql):
    result = _product(spark, sql)
    assert result == 0.0
    assert math.copysign(1.0, result) == -1.0


def test_product_return_type_is_double(spark):
    df = spark.sql("SELECT * FROM VALUES (1), (2), (3) AS t(v)").agg(F.product("v").alias("result"))
    assert df.schema["result"].dataType.simpleString() == "double"


def test_product_grouped(spark):
    rows = (
        spark.sql("SELECT * FROM VALUES ('a', 2), ('a', 3), ('b', 4), ('b', 5) AS t(g, v)")
        .groupBy("g")
        .agg(F.product("v").alias("result"))
        .orderBy("g")
        .collect()
    )
    assert [(r["g"], r["result"]) for r in rows] == [("a", 6.0), ("b", 20.0)]


def test_product_doctest_group_by(spark):
    rows = (
        spark.sql("SELECT id % 3 AS g, id AS v FROM RANGE(10)")
        .groupBy("g")
        .agg(F.product("v").alias("result"))
        .orderBy("g")
        .collect()
    )
    assert [(r["g"], r["result"]) for r in rows] == [(0, 0.0), (1, 28.0), (2, 80.0)]


def test_product_grouped_with_null_only_group(spark):
    rows = (
        spark.sql(
            """
            SELECT * FROM VALUES
              ('a', 2), ('a', 5),
              ('b', CAST(NULL AS INT)), ('b', CAST(NULL AS INT)),
              ('c', 3), ('c', CAST(NULL AS INT)), ('c', 4)
              AS t(g, v)
            """
        )
        .groupBy("g")
        .agg(F.product("v").alias("result"))
        .orderBy("g")
        .collect()
    )
    assert [(r["g"], r["result"]) for r in rows] == [("a", 10.0), ("b", None), ("c", 12.0)]


def test_product_window_cumulative(spark):
    window = Window.partitionBy("g").orderBy("v").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    rows = (
        spark.sql("SELECT * FROM VALUES ('a', 2), ('a', 3), ('a', 4) AS t(g, v)")
        .select("g", "v", F.product("v").over(window).alias("result"))
        .orderBy("v")
        .collect()
    )
    assert [r["result"] for r in rows] == [2.0, 6.0, 24.0]


def test_product_rejects_boolean(spark):
    # Both Spark (DATATYPE_MISMATCH) and Sail (unsupported-type error from
    # `coerce_types`) reject boolean input — `product` requires a DOUBLE.
    with pytest.raises(Exception):  # noqa: B017, PT011
        spark.sql("SELECT * FROM VALUES (true), (false) AS t(v)").agg(F.product("v").alias("result")).collect()
