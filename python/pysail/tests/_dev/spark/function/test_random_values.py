"""Tests that seeded random values match Spark bit for bit.

The expected values were verified on JVM Spark 4.2.0, for a single partition.
"""

import pytest


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        pytest.param(
            "randn(42)",
            [
                2.384479054241165,
                0.1920934041293524,
                0.7337336533286575,
                -0.5224480195716871,
                2.060084179317831,
                0.20963383826633825,
                -0.548526047771831,
                1.907968332171668,
                -0.41705865828987015,
                1.0872260173387922,
                0.952245128862656,
                0.8510993210731433,
                -0.14118066384892058,
                1.7477132048926163,
                0.10149926853578273,
                0.9645306446729486,
                1.0996250111363042,
                -0.878471078118558,
                -0.6581329650674823,
                -0.5268540568138337,
                0.5749488892067608,
                0.8799444982255561,
                0.6553338995859576,
            ],
            id="randn",
        ),
        pytest.param(
            "uniform(CAST(-100 AS TINYINT), CAST(100 AS TINYINT), 42)",
            [23, 1, 66, -47, 34, 3, 99, -86, 93, 59, -10, 35, -25, 66, 49, 44, -96, 26, -84, 67, -3, 46, 8],
            id="tinyint",
        ),
        pytest.param(
            "uniform(CAST(-30000 AS SMALLINT), CAST(30000 AS SMALLINT), 42)",
            [
                7151,
                576,
                19951,
                -14206,
                10217,
                1039,
                29948,
                -25804,
                28180,
                17757,
                -3094,
                10763,
                -7655,
                19956,
                14877,
                13297,
                -29036,
                7842,
                -25477,
                20335,
                -1119,
                13864,
                2481,
            ],
            id="smallint",
        ),
        pytest.param(
            "uniform(-10, -3, 42)",
            [-5, -6, -4, -8, -5, -6, -3, -9, -3, -4, -6, -5, -7, -4, -4, -4, -9, -5, -9, -4, -6, -4, -6],
            id="int",
        ),
        pytest.param(
            "uniform(-5000000000L, 5000000000L, 42)",
            [
                1191893702,
                96018842,
                3325259388,
                -2367719095,
                1702867696,
                173283545,
                4991441647,
                -4300676627,
                4696695610,
                2959575617,
                -515749415,
                1793959570,
                -1275886137,
                3326094725,
                2479557402,
                2216183163,
                -4839487789,
                1307120027,
                -4246291762,
                3389305582,
                -186663888,
                2310762213,
                413617395,
            ],
            id="bigint",
        ),
        # The double value rounds up to 2^63, and the cast saturates as in Spark.
        pytest.param(
            "uniform(9223372036854775797L, 9223372036854775807L, 42)",
            [9223372036854775807] * 23,
            id="bigint-near-max",
        ),
    ],
)
def test_seeded_random_values(spark, expr, expected):
    rows = spark.sql(f"SELECT {expr} AS v FROM range(0, 23, 1, 1)").collect()  # noqa: S608
    assert [row.v for row in rows] == expected


@pytest.mark.parametrize(
    ("expr", "data_type", "expected"),
    [
        pytest.param(
            "uniform(16777217, CAST(16777219 AS FLOAT), 0)",
            "float",
            [
                "16777220.0",
                "16777218.0",
                "16777218.0",
                "16777218.0",
                "16777220.0",
                "16777218.0",
                "16777218.0",
                "16777218.0",
            ],
            id="int-and-float-bounds",
        ),
        pytest.param(
            "uniform(CAST(1.5 AS FLOAT), 100000001, 3)",
            "float",
            [
                "25738144.0",
                "66988860.0",
                "94166512.0",
                "72985032.0",
                "11490073.0",
                "20734098.0",
                "84594136.0",
                "75852120.0",
            ],
            id="float-and-int-bounds",
        ),
        pytest.param(
            "uniform(CAST(1.1 AS DECIMAL(5,4)), CAST(9.9 AS DECIMAL(4,1)), 7)",
            "decimal(4,1)",
            ["3.6", "5.1", "7.9", "6.9", "5.7", "9.0", "3.6", "9.8"],
            id="decimal-type-of-max",
        ),
        pytest.param(
            "uniform(CAST(1.1 AS DECIMAL(4,1)), CAST(9.9 AS DECIMAL(5,4)), 7)",
            "decimal(5,4)",
            ["3.5806", "5.1479", "7.8681", "6.9356", "5.6808", "9.0401", "3.6126", "9.8338"],
            id="decimal-type-of-max-wider",
        ),
        pytest.param(
            "uniform(CAST(1 AS DECIMAL(38,18)), CAST(2 AS DECIMAL(38,18)), 9)",
            "decimal(38,18)",
            [
                "1.714567015824273600",
                "1.466217749507128500",
                "1.029547784789351700",
                "1.214743802011070600",
                "1.947078789224876900",
                "1.739304388587289200",
                "1.659281656815985400",
                "1.778777519799359400",
            ],
            id="decimal-rounding",
        ),
        pytest.param(
            "uniform(5, CAST(9.5 AS DECIMAL(2,1)), 0)",
            "decimal(2,1)",
            ["8.4", "7.4", "5.4", "6.4", "8.2", "6.2", "6.1", "7.5"],
            id="int-and-decimal-bounds",
        ),
        pytest.param(
            "uniform(CAST(61822791393531.8852 AS DECIMAL(18,4)), CAST(61822791393531.8852 AS DOUBLE), 0)",
            "double",
            ["61822791393531.88"] * 8,
            id="decimal-bound-to-double",
        ),
        pytest.param(
            "uniform(CAST(61822791393531.8852 AS DECIMAL(18,4)), CAST(61822791393531.8852 AS DECIMAL(18,4)), 0)",
            "decimal(18,4)",
            ["61822791393531.8800"] * 8,
            id="decimal-bound-to-decimal",
        ),
    ],
)
def test_uniform_values(spark, expr, data_type, expected):
    df = spark.sql(f"SELECT {expr} AS v FROM range(0, 8, 1, 1)")  # noqa: S608
    assert df.schema["v"].dataType.simpleString() == data_type
    assert [str(row.v) for row in df.collect()] == expected


def test_uniform_decimal_overflow(spark):
    # The integer bound is converted to DOUBLE, and only the result overflows DECIMAL(2, 1).
    with pytest.raises(Exception, match=r"cannot be represented as Decimal\(2, 1\)"):
        spark.sql("SELECT uniform(1000, CAST(1.5 AS DECIMAL(2,1)), 0) AS v FROM range(0, 8, 1, 1)").collect()
