import re

import pyarrow as pa
import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F  # noqa: N812

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.scalar requires PySpark 4+")
@pytest.mark.parametrize(
    ("boundary", "counts", "bad"),
    [
        ("alias", (2, 1, 0), None),
        ("double_alias", (2, 1, 0), None),
        ("cross", (6, 3, 0), None),
        ("inner", (2, 0, 0), None),
        ("left", (2, 1, 0), None),
        ("left_semi", (2, 0, 0), None),
        ("left_anti", (0, 1, 0), None),
        ("right_inner", (2, 0, 0), None),
        ("right", (2, 1, 0), None),
        ("right_cross", (6, 3, 0), None),
        # As in Spark, semi and anti joins remove an uncastable row before the cast.
        ("left_semi", (2, 0, 0), 9),
        ("left_anti", (0, 1, 0), 2),
    ],
)
def test_cast_correlation_through_alias_and_join(spark, boundary, counts, bad):
    outer = spark.sql("SELECT * FROM VALUES (0L), (0L), (1L), (2L) AS o(id)").alias("o")
    values = "('0', 2), ('00', 3), ('1', 4)" + ("" if bad is None else f", ('bad', {bad})")
    inner = spark.sql(f"SELECT * FROM VALUES {values} AS i(k, v)").alias("i")  # noqa: S608
    matching = inner.where(F.col("i.k").cast("long") == F.col("o.id").outer())
    if boundary == "alias":
        matching = matching.alias("a")
    elif boundary == "double_alias":
        matching = matching.alias("a").alias("b")
    else:
        other = spark.sql("SELECT * FROM VALUES (2), (3), (7) AS j(w)")
        condition = F.col("v") == F.col("w")
        if boundary == "cross":
            matching = matching.crossJoin(other)
        elif boundary == "right_cross":
            matching = other.crossJoin(matching)
        elif boundary in ("right_inner", "right"):
            matching = other.join(matching, condition, "inner" if boundary == "right_inner" else "right")
        else:
            matching = matching.join(other, condition, boundary)
    result = outer.select("id", matching.select(F.count("*")).scalar().alias("n")).orderBy("id")
    assert [tuple(row) for row in result.collect()] == [(0, counts[0]), (0, counts[0]), (1, counts[1]), (2, counts[2])]


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.scalar requires PySpark 4+")
@pytest.mark.parametrize("cast_below_window", [True, False])
def test_cast_correlation_keeps_aliased_window_partition_keys(spark, cast_below_window):
    outer = spark.sql("SELECT * FROM VALUES (0L, 10), (0L, 20), (1L, 10), (2L, 10) AS o(id, g)").alias("o")
    inner = spark.sql(
        "SELECT * FROM VALUES ('0', 10, 1), ('00', 10, 2), ('0', 20, 3), ('00', 20, 4), ('1', 10, 5) AS i(k, g, v)"
    ).alias("i")
    matching = inner.where(F.col("i.g") == F.col("o.g").outer())
    if cast_below_window:
        matching = matching.where(F.col("i.k").cast("long") == F.col("o.id").outer())
    matching = matching.alias("a").withColumn("w", F.row_number().over(Window.orderBy("v")))
    if not cast_below_window:
        matching = matching.where(F.col("a.k").cast("long") == F.col("o.id").outer())
    result = outer.select("id", "g", matching.select(F.max("w")).scalar().alias("n")).orderBy("id", "g")
    assert [tuple(row) for row in result.collect()] == [
        (0, 10, 2),
        (0, 20, 2),
        (1, 10, 1 if cast_below_window else 3),
        (2, 10, None),
    ]


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.scalar requires PySpark 4+")
def test_cast_correlation_keeps_shared_dataframe_aliases_distinct(spark):
    source = spark.sql("SELECT * FROM VALUES ('0', 1), ('00', 2), ('1', 3) AS t(k, v)")
    outer = source.select(F.col("k").cast("long").alias("id")).alias("o")
    matching = source.alias("i").where(F.col("i.k").cast("long") == F.col("o.id").outer()).alias("a")
    matching = matching.withColumn("w", F.row_number().over(Window.orderBy("v")))
    result = outer.select("id", matching.select(F.max("w")).scalar().alias("n")).orderBy("id")
    assert result.columns == ["id", "n"]
    assert [tuple(row) for row in result.collect()] == [(0, 2), (0, 2), (1, 1)]


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.scalar requires PySpark 4+")
@pytest.mark.parametrize("ansi", [True, False])
@pytest.mark.parametrize("boundary", ["filter", "renamed_semi", "replaced_semi", "filtered_generator"])
def test_cast_correlation_evaluates_keys_after_row_elimination(spark, boundary, ansi):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    try:
        outer = spark.sql("SELECT * FROM VALUES (0L), (0L), (1L), (2L), (CAST(NULL AS BIGINT)) AS o(id)").alias("o")
        inner = spark.sql(
            "SELECT * FROM VALUES ('0', 1), ('00', 2), ('1', 3), ('bad', 9), (CAST(NULL AS STRING), 4) AS i(k, v)"
        ).alias("i")
        matching = inner.where(F.col("i.k").cast("long") == F.col("o.id").outer())
        if boundary == "renamed_semi":
            matching = matching.select(F.col("k").alias("renamed"), "v").alias("a")
        elif boundary == "replaced_semi":
            matching = matching.select(F.lit("unrelated").alias("k"), "v").alias("a")
        if boundary in ("filter", "filtered_generator"):
            matching = matching.alias("a").where(F.col("v") < 9)  # noqa: PLR2004
            if boundary == "filtered_generator":
                matching = matching.select("k", F.explode(F.array("v", F.col("v") + 1)).alias("x"))
        else:
            other = spark.sql("SELECT * FROM VALUES (1), (2), (2), (3) AS j(w)")
            matching = matching.join(other, F.col("v") == F.col("w"), "left_semi")
        result = outer.select("id", matching.select(F.count("*")).scalar().alias("n")).orderBy(
            F.col("id").asc_nulls_first(), "n"
        )
        # The two raw keys cast to the same value; a fallback that groups by the
        # raw strings would duplicate the outer row instead of returning count 2.
        zero, one = (4, 2) if boundary == "filtered_generator" else (2, 1)
        assert [tuple(row) for row in result.collect()] == [(None, 0), (0, zero), (0, zero), (1, one), (2, 0)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.scalar requires PySpark 4+")
def test_cast_correlation_still_validates_rows_kept_by_a_semi_join(spark):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        outer = spark.sql("SELECT * FROM VALUES (0L), (1L) AS o(id)").alias("o")
        inner = spark.sql("SELECT * FROM VALUES ('0', 1), ('00', 2), ('bad', 9) AS i(k, v)").alias("i")
        matching = inner.where(F.col("i.k").cast("long") == F.col("o.id").outer())
        other = spark.sql("SELECT * FROM VALUES (1), (2), (9) AS j(w)")
        matching = matching.join(other, F.col("v") == F.col("w"), "left_semi")
        result = outer.select("id", matching.select(F.count("*")).scalar().alias("n")).orderBy("id")
        with pytest.raises(Exception, match=r"(?i)cast"):
            result.collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.skipif(
    is_jvm_spark(),
    reason="Spark rejects this correlated grouping-set shape; per-outer references validate the expected flags",
)
@pytest.mark.parametrize(
    ("width", "kind", "key_count", "floating"),
    [
        (3, "duplicate", 1, False),
        (4, "duplicate", 1, False),
        (7, "duplicate", 1, False),
        (8, "duplicate", 1, False),
        (15, "duplicate", 1, False),
        (16, "duplicate", 1, False),
        (31, "duplicate", 1, False),
        (32, "duplicate", 1, False),
        (7, "permuted", 1, False),
        (15, "permuted", 1, False),
        (31, "permuted", 1, False),
        (62, "duplicate", 1, False),
        (62, "duplicate", 2, False),
        (63, "duplicate", 1, False),
        (63, "permuted", 1, False),
        # A floating-point key compared directly still needs a separate cast key,
        # since -0.0 and 0.0 are equal but cast to different strings.
        (3, "duplicate", 1, True),
        (7, "duplicate", 1, True),
        (8, "duplicate", 1, True),
        (15, "duplicate", 1, True),
        (16, "duplicate", 1, True),
        (31, "duplicate", 1, True),
        (32, "duplicate", 1, True),
        (7, "permuted", 1, True),
        (61, "duplicate", 2, True),
        (62, "duplicate", 2, True),
        (62, "single", 2, True),
        (63, "single", 1, True),
        (63, "single", 2, True),
    ],
)
def test_cast_correlation_preserves_grouping_mask_width_and_capacity(spark, width, kind, key_count, floating):
    columns = ["k", *[f"g{i}" for i in range(1, width)]]
    group = ", ".join(columns)
    second = ", ".join([columns[1], columns[0], *columns[2:]]) if kind == "permuted" else group
    sets = f"(({group}))" if kind == "single" else f"(({group}), ({second}))"
    if floating:
        keys = [("1D", 1, 1), ("1D", 2, 1), ("2D", 3, 1), ("9D", 9, 2)]
        outers = [("'1.0'", "1D"), ("'2.0'", "2D"), ("'3.0'", "3D"), ("CAST(NULL AS STRING)", "1D")]
        predicates = ["CAST(i.k AS STRING) = o.id", "i.k = o.s"]
        second_key = "CAST(i.k AS FLOAT) = CAST(o.s AS FLOAT)"
        expected = [(None, 1.0, None), ("1.0", 1.0, 0), ("2.0", 2.0, 0), ("3.0", 3.0, None)]
    else:
        keys = [("'0'", 1, 1), ("'0'", 2, 1), ("'00'", 3, 1), ("'1'", 4, 1), ("'9'", 9, 2)]
        outers = [("0L", "'0'"), ("0L", "'00'"), ("1L", "'1'"), ("2L", "'2'"), ("CAST(NULL AS BIGINT)", "'0'")]
        predicates = ["CAST(i.k AS BIGINT) = o.id", "i.k = o.s"]
        second_key = "CAST(i.k AS SMALLINT) = CAST(o.id AS SMALLINT)"
        expected = [(None, "0", None), (0, "0", 0), (0, "00", 0), (1, "1", 0), (2, "2", None)]
    inner_rows = ", ".join("(" + ", ".join([k, *([str(d)] * (width - 1)), str(v)]) + ")" for k, v, d in keys)
    outer_rows = ", ".join("(" + ", ".join([identifier, k, *(["1"] * (width - 1))]) + ")" for identifier, k in outers)
    predicates.extend(f"i.g{i} = o.a{i}" for i in range(1, width))
    if key_count == 2:  # noqa: PLR2004
        predicates.append(second_key)
    # Every visible column belongs to every grouping set, so GROUPING is zero.
    # Duplicate sets and private keys widen the backend's internal grouping ID,
    # and keys that would not fit in its 64 bits fall back to native grouping.
    query = f"""
        SELECT o.id, o.s,
          (SELECT sum(z) FROM (
            SELECT CAST(grouping(k) AS BIGINT) * 10L
                 + CAST(grouping({columns[-1]}) AS BIGINT) AS z
            FROM VALUES {inner_rows} AS i({", ".join([*columns, "v"])})
            WHERE {" AND ".join(predicates)}
            GROUP BY GROUPING SETS {sets}
          )) AS result
        FROM VALUES {outer_rows} AS o(id, s, {", ".join(f"a{i}" for i in range(1, width))})
        ORDER BY o.id NULLS FIRST, o.s, result
    """  # noqa: S608
    assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.mark.skipif(
    is_jvm_spark(),
    reason="Spark rejects this correlated grouping-set shape; per-outer references validate the expected value",
)
def test_cast_correlation_keeps_grouping_id_within_bigint(spark):
    columns = ["k", *[f"g{i}" for i in range(1, 62)]]
    group = ", ".join(columns)
    ones = ", ".join(["1"] * 61)
    predicates = ["CAST(i.k AS STRING) = o.id", "i.k = o.s", *(f"i.g{i} = o.a{i}" for i in range(1, 62))]
    # A key would move the duplicate set's ordinal into the sign bit of the BIGINT
    # that GROUPING_ID returns.
    query = f"""
        SELECT o.id,
          (SELECT min(z) FROM (
            SELECT grouping_id({group}) AS z
            FROM VALUES (1D, {ones}), (2D, {ones}) AS i({group})
            WHERE {" AND ".join(predicates)}
            GROUP BY GROUPING SETS (({group}), ({group}))
          )) AS n
        FROM VALUES ('1.0', 1D, {ones}), ('3.0', 3D, {ones}) AS o(id, s, {", ".join(f"a{i}" for i in range(1, 62))})
        ORDER BY o.id
    """  # noqa: S608
    assert [tuple(row) for row in spark.sql(query).collect()] == [("1.0", 0), ("3.0", None)]


@pytest.mark.skipif(
    is_jvm_spark(),
    reason="Spark's correlated cast grouping can duplicate outer rows; per-outer references validate scalar results",
)
@pytest.mark.parametrize("kind", ["single", "duplicate", "grouping_id", "permuted"])
def test_cast_correlation_combines_lossy_keys_with_grouping_functions(spark, kind):
    sets = {
        "single": "((g))",
        "duplicate": "((g), (g))",
        "grouping_id": "((g))",
        "permuted": "((g, h), (h, g))",
    }[kind]
    flag = "grouping_id(g)" if kind.startswith("grouping_id") else "grouping(g)"
    predicate = "AND i.h = o.h" if kind == "permuted" else ""
    if kind == "permuted":
        flag += " + grouping(h)"
    # Both integer inputs have the same FLOAT value. Preserve one scalar result
    # per outer row while retaining duplicate grouping-set rows inside it.
    query = f"""
        SELECT o.id, o.g,
          (SELECT sum(c + flag) FROM (
            SELECT count(*) AS c, CAST({flag} AS BIGINT) AS flag
            FROM VALUES (16777216, 1, 1), (16777217, 1, 1), (33554432, 1, 1) AS i(k, g, h)
            WHERE CAST(i.k AS FLOAT) = o.id AND i.g = o.g {predicate}
            GROUP BY GROUPING SETS {sets}
          )) AS n
        FROM VALUES (CAST(0 AS FLOAT), 1, 1),
                    (CAST(16777216 AS FLOAT), 1, 1),
                    (CAST(16777216 AS FLOAT), 1, 1),
                    (CAST(16777216 AS FLOAT), 2, 1),
                    (CAST(33554432 AS FLOAT), 1, 1),
                    (CAST(NULL AS FLOAT), 1, 1) AS o(id, g, h)
        ORDER BY id NULLS FIRST, g, n
    """  # noqa: S608
    copies = 2 if kind in ("duplicate", "permuted") else 1
    assert [tuple(row) for row in spark.sql(query).collect()] == [
        (None, 1, None),
        (0.0, 1, None),
        (16777216.0, 1, 2 * copies),
        (16777216.0, 1, 2 * copies),
        (16777216.0, 2, None),
        (33554432.0, 1, copies),
    ]


@pytest.mark.skipif(
    is_jvm_spark(),
    reason="Spark's correlated cast grouping can duplicate outer rows; per-outer references validate scalar results",
)
@pytest.mark.parametrize(("width", "duplicate"), [(7, True), (8, False)])
def test_cast_correlation_combines_lossy_keys_at_grouping_id_width_boundaries(spark, width, duplicate):
    columns = ", ".join(f"g{i}" for i in range(1, width + 1))
    ones = ", ".join(["1"] * width)
    sets = f"(({columns}), ({columns}))" if duplicate else f"(({columns}))"
    # The key widens the backend's internal grouping ID, which no GROUPING reads here.
    query = f"""
        SELECT o.id,
          (SELECT sum(c) FROM (
            SELECT count(*) AS c
            FROM VALUES (16777216, {ones}), (16777217, {ones}), (33554432, {ones}) AS i(k, {columns})
            WHERE CAST(i.k AS FLOAT) = o.id
            GROUP BY GROUPING SETS {sets}
          )) AS n
        FROM VALUES (CAST(16777216 AS FLOAT)), (CAST(33554432 AS FLOAT)), (CAST(1 AS FLOAT)) AS o(id)
        ORDER BY o.id
    """  # noqa: S608
    copies = 2 if duplicate else 1
    assert [tuple(row) for row in spark.sql(query).collect()] == [
        (1.0, None),
        (16777216.0, 2 * copies),
        (33554432.0, copies),
    ]


@pytest.mark.skipif(
    is_jvm_spark(),
    reason="Spark rejects this correlated grouping-set shape; per-outer references validate the expected flags",
)
def test_injective_cast_correlation_keeps_grouping_flags(spark):
    # The raw input of an injective cast is prefixed to every grouping set like any
    # other correlation key, so GROUPING still reads the bit of its own column.
    query = """
        SELECT o.id,
          (SELECT sum(c * 10 + flag) FROM (
            SELECT count(*) AS c, CAST(grouping(g) AS BIGINT) AS flag
            FROM VALUES (1, 1), (1, 2), (2, 1) AS i(k, g)
            WHERE CAST(i.k AS STRING) = o.id
            GROUP BY GROUPING SETS ((g), ())
          )) AS n
        FROM VALUES ('1'), ('2'), ('3'), (CAST(NULL AS STRING)) AS o(id)
        ORDER BY o.id NULLS FIRST
    """
    assert [tuple(row) for row in spark.sql(query).collect()] == [(None, None), ("1", 41), ("2", 21), ("3", None)]


@pytest.mark.skipif(is_jvm_spark(), reason="Sail-specific plan shape")
def test_injective_cast_correlation_groups_by_raw_key(spark):
    def cast_key(query):
        plan = spark.sql(f"EXPLAIN {query}").first()[0]
        return re.search(r"CAST\([^)]*\) as __sail_correlation_key", plan) is not None

    # Grouping by an injective cast's input finds the same groups without casting
    # every row; the cast is compared once per group instead.
    assert not cast_key(
        "SELECT o.id, (SELECT count(*) FROM VALUES (1), (2) AS i(k) WHERE CAST(i.k AS STRING) = o.id) AS n"
        " FROM VALUES ('1') AS o(id)"
    )
    assert cast_key(
        "SELECT o.id, (SELECT count(*) FROM VALUES ('1'), ('01') AS i(k) WHERE CAST(i.k AS BIGINT) = o.id) AS n"
        " FROM VALUES (1L) AS o(id)"
    )


@pytest.mark.parametrize(
    ("source", "target", "low", "high", "ansi"),
    [
        pytest.param(
            *case,
            ansi,
            # TODO: Remove these marks when correlated TRY_CAST keys are supported.
            marks=pytest.mark.xfail(
                not is_jvm_spark() and not ansi and case[0] == "BIGINT",
                reason="Existing DataFusion alias error for ANSI-off BIGINT-to-decimal TRY_CAST correlation",
                strict=True,
            ),
        )
        for case in [
            ("BOOLEAN", "TINYINT", "false", "true"),
            ("BOOLEAN", "BIGINT", "false", "true"),
            ("BOOLEAN", "STRING", "false", "true"),
            ("TINYINT", "FLOAT", "-128", "127"),
            ("SMALLINT", "FLOAT", "-32768", "32767"),
            ("INT", "DOUBLE", "-2147483648", "2147483647"),
            ("BIGINT", "DECIMAL(19,0)", "-9223372036854775808", "9223372036854775807"),
            ("BIGINT", "DECIMAL(21,2)", "-9223372036854775808", "9223372036854775807"),
            ("DECIMAL(9,2)", "DECIMAL(12,4)", "-9999999.99", "9999999.99"),
            ("DECIMAL(9,2)", "STRING", "-9999999.99", "9999999.99"),
            ("DECIMAL(2,0)", "TINYINT", "-99", "99"),
            ("DECIMAL(4,0)", "SMALLINT", "-9999", "9999"),
            ("DECIMAL(9,0)", "INT", "-999999999", "999999999"),
            ("DECIMAL(18,0)", "BIGINT", "-999999999999999999", "999999999999999999"),
        ]
        for ansi in [True, False]
    ],
)
def test_exact_cast_keys_keep_duplicates_nulls_and_boundary_values(spark, ansi, source, target, low, high):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    try:
        # These casts preserve grouping across the entire source type, including
        # its limits. The repeated outer row must survive and a NULL key cannot match.
        source_rows = f"""VALUES
            (CAST('{low}' AS {source}), 10),
            (CAST('{low}' AS {source}), 100),
            (CAST('{high}' AS {source}), 7),
            (CAST(NULL AS {source}), 999)
        """
        query = f"""
            SELECT o.ix,
              (SELECT count(*) FROM {source_rows} AS i(k, v) WHERE CAST(i.k AS {target}) = o.id) AS n,
              (SELECT sum(v) FROM {source_rows} AS i(k, v) WHERE CAST(i.k AS {target}) = o.id) AS total
            FROM VALUES
              (0, CAST(CAST('{low}' AS {source}) AS {target})),
              (1, CAST(CAST('{high}' AS {source}) AS {target})),
              (2, CAST(NULL AS {target})),
              (3, CAST(CAST('{low}' AS {source}) AS {target})) AS o(ix, id)
            ORDER BY o.ix
        """  # noqa: S608
        assert [tuple(row) for row in spark.sql(query).collect()] == [(0, 2, 110), (1, 1, 7), (2, 0, None), (3, 2, 110)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.parametrize("data_type", ["FLOAT", "DOUBLE"])
@pytest.mark.parametrize("key", ["k", "array(k)", "array(array(k))", "named_struct('x', k)"])
@pytest.mark.parametrize("role", ["partition", "order"])
@pytest.mark.parametrize("correlated", [False, True])
def test_signed_zero_window_keys_share_partitions_and_ordering_peers(spark, data_type, key, role, correlated):
    source_rows = f"""VALUES
        (CAST('-0.0' AS {data_type}), 1, 10),
        (CAST('0.0' AS {data_type}), 1, 20),
        (CAST('0.0' AS {data_type}), 2, 30)
    """
    if role == "partition":
        window = f"row_number() OVER (PARTITION BY {key} ORDER BY v)"
        aggregate = "max"
    else:
        window = f"dense_rank() OVER (ORDER BY {key} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        aggregate = "sum"
    if correlated:
        query = f"""
            SELECT o.id,
              (SELECT {aggregate}(w) FROM (
                SELECT {window} AS w FROM {source_rows} AS i(k, g, v)
                WHERE CAST(i.g AS STRING) = o.id
              )) AS n
            FROM VALUES ('1'), ('2'), ('3') AS o(id)
            ORDER BY o.id
        """  # noqa: S608
        expected = [("1", 2), ("2", 1), ("3", None)]
    else:
        query = f"""
            SELECT {aggregate}(w) AS n FROM (
              SELECT {window} AS w FROM {source_rows} AS i(k, g, v) WHERE g = 1
            )
        """  # noqa: S608
        expected = [(2,)]
    assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.mark.parametrize("data_type", ["FLOAT", "DOUBLE"])
def test_signed_zero_window_normalization_preserves_selected_values(spark, data_type):
    query = f"""
        SELECT v, CAST(k AS STRING) AS raw,
          row_number() OVER (PARTITION BY array(k) ORDER BY v) AS n
        FROM VALUES (CAST('-0.0' AS {data_type}), 1),
                    (CAST('0.0' AS {data_type}), 2) AS i(k, v)
        ORDER BY v
    """  # noqa: S608
    assert [tuple(row) for row in spark.sql(query).collect()] == [(1, "-0.0", 1), (2, "0.0", 2)]


@pytest.mark.parametrize("data_type", ["FLOAT", "DOUBLE"])
@pytest.mark.parametrize("correlation_type", ["INT", "FLOAT", "DOUBLE"])
def test_signed_zero_user_partition_preserves_uncast_correlation_domain(spark, data_type, correlation_type):
    query = f"""
        SELECT o.x,
          (SELECT max(c) FROM (
            SELECT count(*) OVER (PARTITION BY k) AS c
            FROM VALUES (CAST('0.0' AS {data_type}), CAST(1 AS {correlation_type}), 10),
                        (CAST('-0.0' AS {data_type}), CAST(1 AS {correlation_type}), 20),
                        (CAST('-0.0' AS {data_type}), CAST(2 AS {correlation_type}), 30),
                        (CAST('1.5' AS {data_type}), CAST(2 AS {correlation_type}), 40),
                        (CAST('1.5' AS {data_type}), CAST(2 AS {correlation_type}), 50),
                        (CAST(NULL AS {data_type}), CAST(1 AS {correlation_type}), 60) AS i(k, g, v)
            WHERE i.g = o.x
          )) AS n
        FROM VALUES (CAST(1 AS {correlation_type})), (CAST(2 AS {correlation_type})) AS o(x)
        ORDER BY o.x
    """  # noqa: S608
    assert [tuple(row) for row in spark.sql(query).collect()] == [(1, 2), (2, 2)]


@pytest.mark.parametrize("data_type", ["FLOAT", "DOUBLE"])
@pytest.mark.parametrize("shape", ["flat", "array", "struct"])
def test_mixed_nan_and_cast_correlation_uses_one_window_domain(spark, data_type, shape):
    def wrap(value):
        if shape == "array":
            return f"array({value})"
        if shape == "struct":
            return f"named_struct('x', {value})"
        return value

    positive = wrap(f"CAST('NaN' AS {data_type})")
    negative = wrap(f"-CAST('NaN' AS {data_type})")
    rows = spark.sql(f"""
        SELECT o.ix,
          (SELECT min(w) FROM (
             SELECT row_number() OVER (PARTITION BY g ORDER BY v) AS w
             FROM VALUES ({positive}, '1', 1, 10), ({negative}, '01', 1, 20) i(k,t,g,v)
             WHERE i.k=o.k AND CAST(i.t AS BIGINT)=o.t
           )) AS smallest,
          (SELECT max(w) FROM (
             SELECT row_number() OVER (PARTITION BY g ORDER BY v) AS w
             FROM VALUES ({positive}, '1', 1, 10), ({negative}, '01', 1, 20) i(k,t,g,v)
             WHERE i.k=o.k AND CAST(i.t AS BIGINT)=o.t
           )) AS largest
        FROM VALUES ({positive}, 1L, 0), ({negative}, 1L, 1) o(k,t,ix)
        ORDER BY o.ix
    """)
    assert [tuple(row) for row in rows.collect()] == [(0, 1, 2), (1, 1, 2)]


@pytest.mark.parametrize("data_type", ["FLOAT", "DOUBLE"])
def test_nan_correlation_keeps_window_partitions_and_order_peers_consistent(spark, data_type):
    rows = spark.sql(f"""
        SELECT o.ix,
          (SELECT sum(r) FROM (
             SELECT dense_rank() OVER (ORDER BY d) AS r
             FROM VALUES (CAST('NaN' AS {data_type})), (-CAST('NaN' AS {data_type})) i(d)
             WHERE d=o.id
           )) AS ranks,
          (SELECT count(*) FROM (
             SELECT row_number() OVER (PARTITION BY d ORDER BY v) AS rn
             FROM VALUES (CAST('NaN' AS {data_type}), 10), (-CAST('NaN' AS {data_type}), 20) i(d,v)
             WHERE d=o.id
           ) WHERE rn=1) AS partitions
        FROM VALUES (CAST('NaN' AS {data_type}), 0), (-CAST('NaN' AS {data_type}), 1) o(id,ix)
        ORDER BY o.ix
    """)
    assert [tuple(row) for row in rows.collect()] == [(0, 2, 1), (1, 2, 1)]


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow table ingestion requires PySpark 4+")
@pytest.mark.parametrize(
    ("data_type", "bits"),
    [
        (pa.float32(), [0x7FC00001, 0x7F800001]),
        (pa.float64(), [0x7FF8000000000001, 0x7FF0000000000001]),
    ],
)
@pytest.mark.parametrize("computed_partition", [False, True])
def test_correlated_window_normalizes_keys_without_changing_nan_values(spark, data_type, bits, computed_partition):
    # Spark puts every NaN in one window partition. Only the private key may be
    # normalized: the selected user column must retain its original payload bits.
    width = data_type.bit_width // 8
    raw = b"".join(value.to_bytes(width, "little") for value in bits)
    values = pa.Array.from_buffers(data_type, len(bits), [None, pa.py_buffer(raw)])
    table = pa.Table.from_arrays(
        [pa.array(["1", "1"]), values, pa.array([0, 1], type=pa.int32())],
        names=["tag", "k", "seq"],
    )
    source = spark.createDataFrame(table)
    source.createOrReplaceTempView("scalar_window_payload_values")
    partition = "CASE WHEN seq < 0 THEN NULL ELSE k END" if computed_partition else "k"
    try:
        result = spark.sql(
            "SELECT (SELECT array_sort(collect_list(named_struct('seq', seq, 'k', k, 'w', w))) "
            "FROM (SELECT seq, k, row_number() OVER (PARTITION BY "
            f"{partition} ORDER BY seq) AS w FROM scalar_window_payload_values "
            "WHERE CAST(tag AS BIGINT) = o.id)) AS events FROM VALUES (1L) AS o(id)"
        )
        arrow = result._to_table()[0] if hasattr(result, "_to_table") else result.toArrow()
        events = arrow.column("events").combine_chunks()
        assert events.to_pylist()[0] is not None
        assert events.values.field("w").to_pylist() == [1, 2]
        actual = events.values.field("k")
        assert actual.type == data_type
        payload = actual.buffers()[1].to_pybytes()
        payload = payload[actual.offset * width : (actual.offset + len(actual)) * width]
        assert payload == raw
    finally:
        spark.catalog.dropTempView("scalar_window_payload_values")


@pytest.mark.parametrize("correlated", [False, True])
@pytest.mark.parametrize("sorted_input", [False, True])
def test_floating_window_keys_share_completed_volatile_values(spark, correlated, sorted_input):
    import math

    source = spark.range(64, numPartitions=3).select(
        F.lit("1").alias("tag"),
        F.col("id").alias("seq"),
        F.when((F.col("id") % 7) == 0, F.lit(None).cast("double"))
        .when(F.expr("pmod(xxhash64(uuid()), 1000)") < 500, F.lit(float("nan")))
        .otherwise(F.lit(-0.0))
        .alias("p"),
        F.expr("CAST(pmod(xxhash64(uuid()), 1000) AS DOUBLE) / 1000.0").alias("r"),
    )
    if sorted_input:
        source = source.repartition(3, F.col("p")).sortWithinPartitions(F.col("p"), F.col("r").desc(), F.col("seq"))
    source.createOrReplaceTempView("scalar_window_volatile_keys")
    partition = "CASE WHEN seq < 0 THEN NULL ELSE p END"
    ordering = "CASE WHEN seq < 0 THEN NULL ELSE r END"
    window = f"PARTITION BY {partition} ORDER BY {ordering} ASC NULLS FIRST, seq"
    try:
        if correlated:
            inner = (
                f"SELECT row_number() OVER ({window}) AS w, "
                f"count(*) OVER (PARTITION BY {partition}) AS n, "
                f"lag(r) OVER ({window}) AS previous, r "
                "FROM scalar_window_volatile_keys WHERE CAST(tag AS BIGINT) = o.id"
            )
            result = spark.sql(
                f"SELECT o.id, (SELECT sum(2 * w - n - 1) FROM ({inner})) AS rank_check, "
                f"(SELECT sum(CASE WHEN previous > r THEN 1 ELSE 0 END) FROM ({inner})) AS order_check "
                "FROM VALUES (1L) AS o(id)"
            )
            assert [tuple(row) for row in result.collect()] == [(1, 0, 0)]
        else:
            rows = spark.sql(
                f"SELECT seq, p, r, row_number() OVER ({window}) AS w FROM scalar_window_volatile_keys"
            ).collect()
            assert len(rows) == 64
            assert len({row.seq for row in rows}) == 64
            groups = {}
            for row in rows:
                key = None if row.p is None else ("NaN" if math.isnan(row.p) else row.p)
                groups.setdefault(key, []).append(row)
            for group in groups.values():
                ordered = sorted(group, key=lambda row: (row.r, row.seq))
                assert [row.w for row in ordered] == list(range(1, len(ordered) + 1))
    finally:
        spark.catalog.dropTempView("scalar_window_volatile_keys")


@pytest.mark.parametrize("shape", ["volatile-order", "volatile-filter", "scalar-order"])
def test_window_correlation_domains_survive_unrelated_expressions(spark, shape):
    order = {"volatile-order": "uuid()", "scalar-order": "(SELECT max(id) FROM range(2))"}.get(shape)
    window = "PARTITION BY k"
    if order:
        window += f" ORDER BY {order} ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
    predicate = "i.g = o.x" + (" AND rand() < 2.0" if shape == "volatile-filter" else "")
    query = f"""
        SELECT o.x, (SELECT max(c) FROM (
          SELECT count(*) OVER ({window}) AS c
          FROM VALUES (CAST('0.0' AS DOUBLE), 1, 10),
                      (CAST('-0.0' AS DOUBLE), 1, 20),
                      (CAST('-0.0' AS DOUBLE), 2, 30),
                      (CAST('1.5' AS DOUBLE), 2, 40),
                      (CAST('1.5' AS DOUBLE), 2, 50),
                      (CAST(NULL AS DOUBLE), 1, 60) AS i(k, g, v)
          WHERE {predicate}
        )) AS n
        FROM VALUES (1), (2) AS o(x)
        ORDER BY o.x
    """  # noqa: S608
    assert [tuple(row) for row in spark.sql(query).collect()] == [(1, 2), (2, 2)]


@pytest.mark.parametrize(
    "outer_key",
    [
        "o.x + CAST(rand() * 0 AS INT)",
        "o.x + CAST(rand() < 2.0 AS INT) - 1",
        "o.x + (SELECT max(id) FROM range(1))",
        "CASE WHEN (SELECT max(id) FROM range(1)) = 0 THEN o.x ELSE 99 END",
    ],
)
def test_window_domains_preserve_outer_only_key_expressions(spark, outer_key):
    query = f"""
        SELECT o.x, (SELECT max(c) FROM (
          SELECT count(*) OVER (PARTITION BY k) AS c
          FROM VALUES (CAST('0.0' AS DOUBLE), 1, 10),
                      (CAST('-0.0' AS DOUBLE), 1, 20),
                      (CAST('-0.0' AS DOUBLE), 2, 30),
                      (CAST('1.5' AS DOUBLE), 2, 40),
                      (CAST('1.5' AS DOUBLE), 2, 50),
                      (CAST(NULL AS DOUBLE), 1, 60) AS i(k, g, v)
          WHERE i.g = {outer_key}
        )) AS n
        FROM VALUES (1), (2) AS o(x)
        ORDER BY o.x
    """  # noqa: S608
    assert [tuple(row) for row in spark.sql(query).collect()] == [(1, 2), (2, 2)]


@pytest.mark.parametrize(
    "outer_key",
    [
        "o.x + CAST(rand() < 2.0 AS INT) - 1",
        pytest.param(
            "o.x + (SELECT max(id) FROM range(1))",
            marks=pytest.mark.skipif(
                is_jvm_spark(),
                reason="Spark fails an uncorrelated scalar in this correlated join condition; per-outer evaluation returns 2",
            ),
        ),
    ],
)
@pytest.mark.parametrize("ansi", [True, False])
def test_window_cast_domains_keep_collisions_with_outer_expressions(spark, outer_key, ansi):
    # The outer MAX also detects regressions in a previously correct consumer of
    # native decorrelation, which could expose duplicate scalar rows.
    query = f"""
        SELECT x, max(n) AS n FROM (
          SELECT o.x, (SELECT max(c) FROM (
            SELECT count(*) OVER (PARTITION BY k) AS c
            FROM VALUES (CAST('0.0' AS DOUBLE), '1'),
                        (CAST('-0.0' AS DOUBLE), '01'),
                        (CAST('0.0' AS DOUBLE), '2'),
                        (CAST('-0.0' AS DOUBLE), '02') AS i(k, g)
            WHERE CAST(g AS BIGINT) = {outer_key}
          )) AS n
          FROM VALUES (1L), (2L) AS o(x)
        ) q GROUP BY x ORDER BY x
    """  # noqa: S608
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    try:
        assert [tuple(row) for row in spark.sql(query).collect()] == [(1, 2), (2, 2)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)
