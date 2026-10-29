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
        # Raw numeric and string comparisons still need separate private keys:
        # Sail keeps raw NaN payloads apart, while their string spellings coincide.
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


@pytest.mark.parametrize("ansi", [True, False])
@pytest.mark.parametrize("comparison", ["CAST(i.k AS DOUBLE) = o.id", "i.k = o.id"])
def test_float_to_double_cast_keys_keep_duplicates_nulls_and_special_values(spark, ansi, comparison):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    try:
        # FLOAT to DOUBLE is exact, so grouping by the FLOAT input finds the same groups.
        # A DOUBLE that no FLOAT widens to, such as 1.1, must not match.
        source_rows = """VALUES
            (CAST('-3.4028235E38' AS FLOAT), 10),
            (CAST('-3.4028235E38' AS FLOAT), 100),
            (CAST('3.4028235E38' AS FLOAT), 7),
            (CAST('1.1' AS FLOAT), 5),
            (CAST('NaN' AS FLOAT), 3),
            (CAST('Infinity' AS FLOAT), 2),
            (CAST(NULL AS FLOAT), 999)
        """
        query = f"""
            SELECT o.ix,
              (SELECT count(*) FROM {source_rows} AS i(k, v) WHERE {comparison}) AS n,
              (SELECT sum(v) FROM {source_rows} AS i(k, v) WHERE {comparison}) AS total
            FROM VALUES
              (0, CAST(CAST('-3.4028235E38' AS FLOAT) AS DOUBLE)),
              (1, CAST(CAST('3.4028235E38' AS FLOAT) AS DOUBLE)),
              (2, CAST(CAST('1.1' AS FLOAT) AS DOUBLE)),
              (3, CAST('1.1' AS DOUBLE)),
              (4, CAST('NaN' AS DOUBLE)),
              (5, CAST('Infinity' AS DOUBLE)),
              (6, CAST(NULL AS DOUBLE)),
              (7, CAST(CAST('-3.4028235E38' AS FLOAT) AS DOUBLE)) AS o(ix, id)
            ORDER BY o.ix
        """  # noqa: S608
        assert [tuple(row) for row in spark.sql(query).collect()] == [
            (0, 2, 110),
            (1, 1, 7),
            (2, 1, 5),
            (3, 0, None),
            (4, 1, 3),
            (5, 1, 2),
            (6, 0, None),
            (7, 2, 110),
        ]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.parametrize("data_type", ["FLOAT", "DOUBLE"])
@pytest.mark.parametrize("key", ["k", "array(k)", "array(array(k))", "named_struct('x', k)"])
@pytest.mark.parametrize("role", ["partition", "order"])
# TODO: Sail normalizes signed zeros only in the windows that correlated scalar subquery
#  keys rewrite. Spark's ordinary windows also merge -0.0 and 0.0; match them separately.
@pytest.mark.parametrize(
    "correlated",
    [pytest.param(False, marks=pytest.mark.skip(reason="Ordinary windows keep signed zeros apart")), True],
)
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


# TODO: Sail normalizes signed zeros only in the windows that correlated scalar subquery
#  keys rewrite. Spark's ordinary windows also merge -0.0 and 0.0; match them separately.
@pytest.mark.skip(reason="Ordinary windows keep signed zeros apart")
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
def test_correlated_window_normalization_preserves_selected_values(spark, data_type):
    # A rewritten window merges -0.0 and 0.0 in its partitions, while the selected
    # values keep their sign.
    query = f"""
        SELECT (SELECT array_sort(collect_list(named_struct('v', v, 'raw', CAST(k AS STRING), 'n', n))) FROM (
            SELECT v, k, row_number() OVER (PARTITION BY array(k) ORDER BY v) AS n
            FROM VALUES (CAST('-0.0' AS {data_type}), '1', 1), (CAST('0.0' AS {data_type}), '01', 2) AS i(k, g, v)
            WHERE CAST(i.g AS BIGINT) = o.id
          )) AS events
        FROM VALUES (1L) AS o(id)
    """  # noqa: S608
    events = spark.sql(query).collect()[0].events
    assert [tuple(event) for event in events] == [(1, "-0.0", 1), (2, "0.0", 2)]


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


# TODO: Sail keeps NaN payloads apart in GROUP BY, joins and equality, so windows keep them
#  apart too. Spark merges every NaN; that needs one shared fix for all of them.
@pytest.mark.skip(reason="Windows keep NaN payloads apart, like GROUP BY")
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
    """)  # noqa: S608
    assert [tuple(row) for row in rows.collect()] == [(0, 1, 2), (1, 1, 2)]


# TODO: Sail keeps NaN payloads apart in GROUP BY, joins and equality, so windows keep them
#  apart too. Spark merges every NaN; that needs one shared fix for all of them.
@pytest.mark.skip(reason="Windows keep NaN payloads apart, like GROUP BY")
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
    """)  # noqa: S608
    assert [tuple(row) for row in rows.collect()] == [(0, 2, 1), (1, 2, 1)]


# TODO: Sail keeps NaN payloads apart in GROUP BY, joins and equality, so windows keep them
#  apart too. Spark merges every NaN; that needs one shared fix for all of them.
@pytest.mark.skip(reason="Windows keep NaN payloads apart, like GROUP BY")
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
            f"""SELECT (SELECT array_sort(collect_list(named_struct('seq', seq, 'k', k, 'w', w)))
            FROM (SELECT seq, k, row_number() OVER (PARTITION BY
            {partition} ORDER BY seq) AS w FROM scalar_window_payload_values
            WHERE CAST(tag AS BIGINT) = o.id)) AS events FROM VALUES (1L) AS o(id)"""  # noqa: S608
        )
        arrow = result._to_table()[0] if hasattr(result, "_to_table") else result.toArrow()  # noqa: SLF001
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


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow table ingestion requires PySpark 4+")
@pytest.mark.skipif(is_jvm_spark(), reason="Spark merges NaN payloads in window partitions")
@pytest.mark.parametrize(
    ("data_type", "bits"),
    [
        (pa.float32(), [0x7FC00001, 0x7F800001]),
        (pa.float64(), [0x7FF8000000000001, 0x7FF0000000000001]),
    ],
)
def test_correlated_window_keeps_nan_payloads_like_grouping(spark, data_type, bits):
    # Like GROUP BY, a rewritten window keeps distinct NaN payloads apart, and the
    # selected user column retains its original payload bits.
    # TODO: Expect Spark's single NaN partition (w == [1, 2]) once grouping, joins and
    #  windows all normalize NaN keys.
    width = data_type.bit_width // 8
    raw = b"".join(value.to_bytes(width, "little") for value in bits)
    values = pa.Array.from_buffers(data_type, len(bits), [None, pa.py_buffer(raw)])
    table = pa.Table.from_arrays(
        [pa.array(["1", "1"]), values, pa.array([0, 1], type=pa.int32())],
        names=["tag", "k", "seq"],
    )
    spark.createDataFrame(table).createOrReplaceTempView("scalar_window_nan_payloads")
    try:
        result = spark.sql(
            "SELECT (SELECT array_sort(collect_list(named_struct('seq', seq, 'k', k, 'w', w))) "
            "FROM (SELECT seq, k, row_number() OVER (PARTITION BY k ORDER BY seq) AS w "
            "FROM scalar_window_nan_payloads WHERE CAST(tag AS BIGINT) = o.id)) AS events "
            "FROM VALUES (1L) AS o(id)"
        )
        arrow = result._to_table()[0] if hasattr(result, "_to_table") else result.toArrow()  # noqa: SLF001
        events = arrow.column("events").combine_chunks()
        assert events.values.field("w").to_pylist() == [1, 1]
        actual = events.values.field("k")
        payload = actual.buffers()[1].to_pybytes()
        assert payload[actual.offset * width : (actual.offset + len(actual)) * width] == raw
    finally:
        spark.catalog.dropTempView("scalar_window_nan_payloads")


@pytest.mark.parametrize("data_type", ["FLOAT", "DOUBLE"])
@pytest.mark.parametrize("function", ["count(*) OVER (PARTITION BY d)", "count(*) OVER (ORDER BY d)"])
def test_correlated_window_over_grouped_nan_values_counts_groups(spark, data_type, function):
    # The inner GROUP BY yields one row per NaN payload in Sail and one NaN row in Spark;
    # either way, no window partition or ordering peer spans two groups.
    query = f"""
        SELECT o.x,
          (SELECT min(c) FROM (
            SELECT {function} AS c FROM (
              SELECT d, g FROM VALUES (CAST('NaN' AS {data_type}), '1'), (-CAST('NaN' AS {data_type}), '1') AS i(d, g)
              GROUP BY d, g
            ) t WHERE CAST(t.g AS BIGINT) = o.x
          )) AS n
        FROM VALUES (1L), (2L) AS o(x)
        ORDER BY o.x
    """  # noqa: S608
    assert [tuple(row) for row in spark.sql(query).collect()] == [(1, 1), (2, None)]


def test_uncast_correlation_partitions_windows_by_the_correlated_column(spark):
    # Without a cast, each outer row's window must still see only its own correlation group.
    rows = spark.sql("""
        SELECT o.x,
          (SELECT max(c) FROM (
            SELECT count(*) OVER (PARTITION BY d) AS c
            FROM VALUES (7, 1L), (7, 2L), (8, 2L) AS i(d, t)
            WHERE i.t = o.x
          )) AS n
        FROM VALUES (1L), (2L) AS o(x)
        ORDER BY o.x
    """)
    assert [tuple(row) for row in rows.collect()] == [(1, 1), (2, 1)]


def test_uncast_floating_correlation_keeps_native_window_partitions(spark):
    # TODO: Sail's equality keeps NaN payloads apart, so a floating correlation without a
    #  cast keeps native decorrelation, whose window sees every group. Spark agrees here.
    rows = spark.sql("""
        SELECT o.ix,
          (SELECT max(c) FROM (
            SELECT count(*) OVER (PARTITION BY g) AS c
            FROM VALUES (CAST('NaN' AS DOUBLE), 1), (-CAST('NaN' AS DOUBLE), 1),
                        (CAST('NaN' AS DOUBLE), 2) AS i(k, g)
            WHERE i.k = o.x
          )) AS n
        FROM VALUES (0, CAST('NaN' AS DOUBLE)), (1, -CAST('NaN' AS DOUBLE)) AS o(ix, x)
        ORDER BY o.ix
    """)
    assert [tuple(row) for row in rows.collect()] == [(0, 2), (1, 2)]


@pytest.mark.parametrize("correlated", [False, True])
@pytest.mark.parametrize("sorted_input", [False, True])
def test_floating_window_keys_share_completed_volatile_values(spark, correlated, sorted_input):
    import math

    source = spark.range(64, numPartitions=3).select(
        F.lit("1").alias("tag"),
        F.col("id").alias("seq"),
        F.when((F.col("id") % 7) == 0, F.lit(None).cast("double"))
        .when(F.expr("pmod(xxhash64(uuid()), 1000)") < 500, F.lit(float("nan")))  # noqa: PLR2004
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
            inner = f"""SELECT row_number() OVER ({window}) AS w,
                count(*) OVER (PARTITION BY {partition}) AS n,
                lag(r) OVER ({window}) AS previous, r
                FROM scalar_window_volatile_keys WHERE CAST(tag AS BIGINT) = o.id"""  # noqa: S608
            result = spark.sql(
                f"""SELECT o.id, (SELECT sum(2 * w - n - 1) FROM ({inner})) AS rank_check,
                (SELECT sum(CASE WHEN previous > r THEN 1 ELSE 0 END) FROM ({inner})) AS order_check
                FROM VALUES (1L) AS o(id)"""  # noqa: S608
            )
            assert [tuple(row) for row in result.collect()] == [(1, 0, 0)]
        else:
            rows = spark.sql(
                f"SELECT seq, p, r, row_number() OVER ({window}) AS w FROM scalar_window_volatile_keys"  # noqa: S608
            ).collect()
            assert len(rows) == 64  # noqa: PLR2004
            assert len({row.seq for row in rows}) == 64  # noqa: PLR2004
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


@pytest.mark.parametrize(
    "ansi",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                is_jvm_spark(),
                reason="Spark's lossy correlated INT/FLOAT grouping duplicates rows; per-outer queries confirm count 2",
            ),
        ),
    ],
)
def test_implicit_integral_float_correlation_preserves_ansi_precision(spark, ansi):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    try:
        # ANSI mode compares these values as DOUBLE, while an explicit FLOAT cast
        # intentionally combines the two integer values. Preserve both contracts.
        implicit = spark.sql("""
            SELECT o.id,
              (SELECT count(*) FROM VALUES (16777216), (16777217) AS i(k)
               WHERE i.k = o.id) AS n
            FROM VALUES (CAST(NULL AS FLOAT)), (CAST(0 AS FLOAT)),
                        (CAST(16777216 AS FLOAT)) AS o(id)
            ORDER BY id NULLS FIRST
        """)
        assert [tuple(row) for row in implicit.collect()] == [(None, 0), (0.0, 0), (16777216.0, 1 if ansi else 2)]
        # Keep the comparison's precision in a filter that consumes the scalar count.
        filtered = spark.sql("""
            SELECT o.id FROM VALUES (CAST(16777216 AS FLOAT)) AS o(id)
            WHERE 1.5 <= (SELECT count(*) FROM VALUES (16777216), (16777217) AS i(k)
                          WHERE o.id = i.k)
        """)
        assert [tuple(row) for row in filtered.collect()] == ([] if ansi else [(16777216.0,)])
        comparisons = spark.sql("""
            SELECT i.k = o.id AS equal, o.id = i.k AS reversed,
                   i.k <=> o.id AS null_safe, i.k > o.id AS greater,
                   i.k BETWEEN o.id AND o.id AS between_bounds,
                   CAST(i.k AS FLOAT) = o.id AS explicit_cast
            FROM VALUES (16777217), (CAST(NULL AS INT)) AS i(k)
            CROSS JOIN VALUES (CAST(16777216 AS FLOAT)) AS o(id)
            ORDER BY i.k NULLS FIRST
        """)
        assert [tuple(row) for row in comparisons.collect()] == [
            (None, None, False, None, None, None),
            (not ansi, not ansi, not ansi, ansi, not ansi, True),
        ]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.parametrize("ansi", [True, False])
@pytest.mark.parametrize(
    "wrapper", ["{}", "array({})", "named_struct('v', {}, 'other', 7)", "array(named_struct('v', {}))"]
)
def test_integral_float_comparison_consumers_agree(spark, ansi, wrapper):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    try:
        left, right = wrapper.format("i"), wrapper.format("f")
        rows = spark.sql(f"""
            SELECT {left} = {right}, {left} > {right}, {left} <=> {right},
                   {left} IN ({right}), {left} IN ({right}, NULL),
                   {left} NOT IN ({right}),
                   (i = f) = ({left} = {right}),
                   ({left} = {right}) = ({left} IN ({right}))
            FROM VALUES (16777217, CAST(16777216 AS FLOAT)) t(i, f)
        """)  # noqa: S608
        assert [tuple(row) for row in rows.collect()] == [
            (not ansi, ansi, not ansi, not ansi, None if ansi else True, ansi, True, True)
        ]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.parametrize("ansi", [True, False])
@pytest.mark.parametrize("tuple_key", [False, True])
def test_integral_float_in_subquery_uses_the_comparison_type(spark, ansi, tuple_key):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    try:
        key, values = ("(k, 'x')", "f, 'x'") if tuple_key else ("k", "f")
        rows = spark.sql(f"""
            SELECT k FROM VALUES (16777216), (16777217), (CAST(NULL AS INT)) i(k)
            WHERE {key} IN (SELECT {values} FROM VALUES (CAST(16777216 AS FLOAT)) o(f))
            ORDER BY k
        """)  # noqa: S608
        assert [tuple(row) for row in rows.collect()] == ([(16777216,)] if ansi else [(16777216,), (16777217,)])
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.parametrize("wrapper", ["{}", "array({})"])
def test_numeric_struct_comparison_preserves_one_volatile_value(spark, wrapper):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        value = wrapper.format(
            "CASE WHEN rand(1) < 0.5 THEN named_struct('v', 1, 'n', 10) ELSE named_struct('v', 2, 'n', 20) END"
        )
        first = wrapper.format("named_struct('v', CAST(1 AS FLOAT), 'n', 10)")
        second = wrapper.format("named_struct('v', CAST(2 AS FLOAT), 'n', 20)")
        rows = spark.sql(f"""
            SELECT count(*) AS wrong FROM range(10000)
            WHERE NOT ({value} IN ({first}, {second}))
        """)  # noqa: S608
        assert rows.collect()[0].wrong == 0
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.parametrize("companion", ["'0'", "0BD", "CAST(NULL AS STRING)"])
def test_ansi_integral_float_in_list_preserves_precision_with_other_numeric_inputs(spark, companion):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        rows = spark.sql(f"""
            SELECT i IN (f, {companion}), array(i) IN (array(f), array({companion}))
            FROM VALUES (16777217, CAST(16777216 AS FLOAT)) t(i, f)
        """)  # noqa: S608
        expected = None if companion == "CAST(NULL AS STRING)" else False
        assert [tuple(row) for row in rows.collect()] == [(expected, False if expected is None else expected)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.parametrize("ansi", [True, False])
@pytest.mark.parametrize("natural", [False, True])
def test_integral_float_using_join_matches_on_and_preserves_full_join_keys(spark, ansi, natural):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    try:
        left = "VALUES (16777216, 1), (16777217, 2) AS l(k, a)"
        right = "VALUES (CAST(16777216 AS FLOAT), 3) AS r(k, b)"
        using = f"{left} NATURAL JOIN {right}" if natural else f"{left} JOIN {right} USING (k)"
        actual = spark.sql(f"SELECT count(*) FROM {using}").collect()[0][0]  # noqa: S608
        on = spark.sql(f"SELECT count(*) FROM {left} JOIN {right} ON l.k = r.k").collect()[0][0]  # noqa: S608
        assert actual == on == (1 if ansi else 2)
        full = f"{left} NATURAL FULL JOIN {right}" if natural else f"{left} FULL JOIN {right} USING (k)"
        rows = spark.sql(f"SELECT k, a, b, typeof(k) AS t FROM {full} ORDER BY a")  # noqa: S608
        expected = (
            [(16777216.0, 1, 3, "double"), (16777217.0, 2, None, "double")]
            if ansi
            else [(16777216.0, 1, 3, "float"), (16777216.0, 2, 3, "float")]
        )
        assert [tuple(row) for row in rows.collect()] == expected
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


def test_numeric_array_comparison_keeps_nullable_children(spark):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        rows = spark.sql("""
            SELECT id,
              array(i) = array(f) AS equal,
              array(named_struct('v', i)) IN (array(named_struct('v', f))) AS matched
            FROM (SELECT id, CASE WHEN id = 0 THEN CAST(NULL AS INT) ELSE 16777217 END AS i,
                         CASE WHEN id = 0 THEN CAST(NULL AS FLOAT) ELSE CAST(16777216 AS FLOAT) END AS f
                  FROM range(2))
            ORDER BY id
        """)
        assert [tuple(row) for row in rows.collect()] == [(0, True, True), (1, False, False)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.parametrize("ansi", [True, False])
@pytest.mark.parametrize(
    ("predicate", "expected"),
    [("f IN (0, 5)", [1]), ("f NOT IN (0, 5)", [2]), ("f IN (i, l)", [1]), ("f NOT IN (i, l)", [2])],
)
def test_short_float_in_lists_merge_signed_zeros(spark, ansi, predicate, expected):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    try:
        # One predicate per query, so that no shared subexpression hides the IN list's operand.
        rows = spark.sql(f"""
            SELECT id FROM (SELECT * FROM VALUES (1, -CAST('0.0' AS FLOAT), 0, 5L),
                                                 (2, CAST('1.0' AS FLOAT), 0, 5L) AS t(id, f, i, l))
            WHERE {predicate}
            ORDER BY id
        """)  # noqa: S608
        assert [row.id for row in rows.collect()] == expected
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.parametrize("ansi", [True, False])
def test_integral_float_comparison_with_float_arithmetic(spark, ansi):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    try:
        rows = spark.sql("""
            SELECT i = f + 1, i IN (f + 1)
            FROM (SELECT * FROM VALUES (16777217, CAST(16777216 AS FLOAT)), (1, CAST(0 AS FLOAT)) AS t(i, f))
            ORDER BY i
        """)
        assert [tuple(row) for row in rows.collect()] == [(True, True), (True, True)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.parametrize("ansi", [True, False])
def test_integral_float_arithmetic_and_conditionals_widen_like_spark(spark, ansi):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    try:
        # Spark ANSI computes integral and FLOAT values as DOUBLE; otherwise as FLOAT.
        row = spark.sql("""
            SELECT typeof(i + f), i + f, i - f, i * CAST(1 AS FLOAT),
                   coalesce(CAST(NULL AS FLOAT), i), greatest(f, i), array(i, f)[0]
            FROM (SELECT * FROM VALUES (16777217, CAST(16777216 AS FLOAT)) AS t(i, f))
        """).collect()[0]
        if ansi:
            expected = ("double", 33554433.0, 1.0, 16777217.0, 16777217.0, 16777217.0, 16777217.0)
        else:
            expected = ("float", 33554432.0, 0.0, 16777216.0, 16777216.0, 16777216.0, 16777216.0)
        assert tuple(row) == expected
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


def test_ansi_integral_float_case_branches_widen_like_spark(spark):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        row = spark.sql("""
            SELECT CASE WHEN i > 0 THEN i ELSE f END, if(i > 0, i, f), nvl(CAST(NULL AS FLOAT), i)
            FROM (SELECT * FROM VALUES (16777217, CAST(16777216 AS FLOAT)) AS t(i, f))
        """).collect()[0]
        assert tuple(row) == (16777217.0, 16777217.0, 16777217.0)
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


def test_ansi_arithmetic_on_grouping_id_resolves(spark):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        rows = spark.sql("""
            SELECT a, grouping_id() * 10 + count(*) AS n
            FROM VALUES (1), (1), (2) AS t(a)
            GROUP BY ROLLUP(a)
            ORDER BY a NULLS LAST
        """)
        assert [tuple(row) for row in rows.collect()] == [(1, 2), (2, 1), (None, 13)]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow table ingestion requires PySpark 4+")
def test_float_to_double_correlation_keeps_signaling_nan_twins_in_one_row(spark):
    # Widening quiets the signaling NaN into its twin's DOUBLE, so both rows form one group.
    raw = b"".join(value.to_bytes(4, "little") for value in [0x7F800001, 0x7FC00001])
    values = pa.Array.from_buffers(pa.float32(), 2, [None, pa.py_buffer(raw)])
    table = pa.Table.from_arrays([values, pa.array([0, 1], type=pa.int32())], names=["k", "seq"])
    spark.createDataFrame(table).createOrReplaceTempView("scalar_snan_twin_values")
    try:
        query = """
                SELECT o.tag, (SELECT count(*) FROM scalar_snan_twin_values AS i WHERE CAST(i.k AS DOUBLE) = o.id) AS n
                FROM (SELECT 'x' AS tag, CAST(k AS DOUBLE) AS id FROM scalar_snan_twin_values WHERE seq = 1) AS o
                """
        assert [tuple(row) for row in spark.sql(query).collect()] == [("x", 2)]
    finally:
        spark.catalog.dropTempView("scalar_snan_twin_values")


@pytest.mark.skipif(
    is_jvm_spark(),
    reason="Spark rejects this correlated grouping-set shape; per-outer references validate the expected value",
)
def test_cast_correlation_keeps_grouping_id_for_duplicate_grouping_sets(spark):
    # A hidden key would shift the duplicate set's ordinal in the public GROUPING_ID.
    query = """
        SELECT o.id,
          (SELECT sum(z) FROM (
            SELECT grouping_id(k, x2, x1) AS z
            FROM VALUES ('2', CAST(NULL AS INT), 2), ('2', CAST(NULL AS INT), 2),
                        ('2', 1, CAST(NULL AS INT)) AS i(k, x1, x2)
            WHERE CAST(i.k AS BIGINT) = o.id
            GROUP BY GROUPING SETS ((k), (k), (x2, x1))
          )) AS n
        FROM VALUES (2L) AS o(id)
    """
    assert [tuple(row) for row in spark.sql(query).collect()] == [(2, 14)]


def test_duplicate_grouping_sets_keep_public_grouping_id_literal_reference(spark):
    row = spark.sql("""
        SELECT sum(z) AS n FROM (
          SELECT grouping_id(k, x2, x1) AS z
          FROM VALUES ('2', CAST(NULL AS INT), 2), ('2', CAST(NULL AS INT), 2),
                      ('2', 1, CAST(NULL AS INT)) AS i(k, x1, x2)
          WHERE CAST(i.k AS BIGINT) = 2
          GROUP BY GROUPING SETS ((k), (k), (x2, x1))
        )
    """).collect()[0]
    assert row.n == 14  # noqa: PLR2004


@pytest.mark.skipif(is_jvm_spark(), reason="Spark rejects this correlated grouping-set shape")
def test_correlated_duplicate_grouping_flags_equal_ordinary_flags(spark):
    row = spark.sql("""
        SELECT
          (SELECT max(z) > 0 FROM (
             SELECT grouping_id(k) AS z FROM VALUES ('2'), ('02') AS i(k)
             WHERE CAST(i.k AS BIGINT) = o.id GROUP BY GROUPING SETS ((k), (k))
          )) =
          (SELECT max(z) > 0 FROM (
             SELECT grouping_id(k) AS z FROM VALUES ('2'), ('02') AS i(k)
             WHERE CAST(i.k AS BIGINT) = 2 GROUP BY GROUPING SETS ((k), (k))
          )) AS same
        FROM VALUES (2L) AS o(id)
    """).collect()[0]
    assert row.same is True


@pytest.mark.skipif(
    is_jvm_spark(),
    reason="Spark rejects this correlated grouping-set shape; per-outer references validate the expected value",
)
def test_direct_correlation_on_grouping_column_keeps_grouping_id_for_duplicate_grouping_sets(spark):
    # Native pull-up would also group every set by k, including the set that omits it.
    query = """
        SELECT o.id,
          (SELECT sum(z) FROM (
            SELECT grouping_id(k, x2, x1) AS z
            FROM VALUES ('2', CAST(NULL AS INT), 2), ('2', CAST(NULL AS INT), 2),
                        ('2', 1, CAST(NULL AS INT)) AS i(k, x1, x2)
            WHERE CAST(i.k AS BIGINT) = o.id AND i.k = o.k
            GROUP BY GROUPING SETS ((k), (k), (x2, x1))
          )) AS n
        FROM VALUES (2L, '2') AS o(id, k)
    """
    assert [tuple(row) for row in spark.sql(query).collect()] == [(2, 14)]


@pytest.mark.skipif(
    is_jvm_spark(),
    reason="Spark rejects this correlated grouping-set shape; per-outer references validate the expected value",
)
def test_direct_correlation_on_grouping_column_keeps_grouping_of_subtotal(spark):
    query = """
        SELECT o.id,
          (SELECT sum(z) FROM (
            SELECT grouping(k) AS z
            FROM VALUES (2, CAST(NULL AS INT), 2, 10), (2, CAST(NULL AS INT), 2, 20),
                        (2, 1, CAST(NULL AS INT), 30), (3, 5, 5, 40) AS i(k, x1, x2, v)
            WHERE i.k = o.id
            GROUP BY GROUPING SETS ((k), ())
          )) AS n
        FROM VALUES (2), (3), (4) AS o(id)
        ORDER BY 1
    """
    assert [tuple(row) for row in spark.sql(query).collect()] == [(2, 1), (3, 1), (4, None)]


@pytest.mark.parametrize("function", ["grouping(c1)", "grouping_id()"])
def test_grouping_functions_over_permuted_duplicate_sets_of_eight_columns(spark, function):
    # The duplicate-set ordinal widens the grouping ID past eight bits.
    query = f"""
        SELECT {function} AS z, count(*) AS n
        FROM VALUES (1, 2, 3, 4, 5, 6, 7, 8) AS t(c1, c2, c3, c4, c5, c6, c7, c8)
        GROUP BY GROUPING SETS ((c1, c2, c3, c4, c5, c6, c7, c8), (c8, c7, c6, c5, c4, c3, c2, c1))
        ORDER BY z
    """  # noqa: S608
    assert [tuple(row) for row in spark.sql(query).collect()] == [(0, 1), (0, 1)]


@pytest.mark.skipif(is_jvm_spark(), reason="Spark rejects this correlated grouping-set shape")
def test_cast_correlation_with_grouping_id_and_outer_value_in_having_plans(spark):
    # TODO: the values are not Spark's yet; the rule declines rather than mask a grouping
    #  ID that native pull-up compares with the outer query, which it cannot decorrelate.
    query = """
        SELECT o.id, o.x,
          (SELECT count(*) FROM (
            SELECT grouping_id(k, v) AS z
            FROM VALUES ('1', 1), ('1', 2), ('01', CAST(NULL AS INT)), ('2', 3) AS i(k, v)
            GROUP BY GROUPING SETS ((k, v), (k, v), (k))
            HAVING CAST(k AS BIGINT) = o.id AND grouping_id(k, v) = o.x
          )) AS n
        FROM VALUES (1L, 0L), (2L, 1L) AS o(id, x)
    """
    spark.sql(query).collect()


@pytest.mark.skipif(
    is_jvm_spark(),
    reason="Spark rejects this correlated grouping-set shape; per-outer references validate the expected value",
)
def test_cast_correlation_keeps_grouping_id_of_stacked_grouping_sets(spark):
    # The upper grouping sets take the carried key first too, keeping both grouping IDs.
    query = """
        SELECT o.id,
          (SELECT sum(z2 * 100 + z1) FROM (
            SELECT grouping_id(a) AS z2, max(z1) AS z1 FROM (
              SELECT k AS a, grouping_id(k, x2) AS z1
              FROM VALUES ('2', CAST(NULL AS INT), 2, 10), ('02', CAST(NULL AS INT), 2, 20),
                          ('2', 1, CAST(NULL AS INT), 30), ('3', 5, 5, 40) AS i(k, x1, x2, v)
              WHERE CAST(i.k AS BIGINT) = o.id
              GROUP BY GROUPING SETS ((k), (k), (k, x2))
            )
            GROUP BY GROUPING SETS ((a), (a), ())
          )) AS n
        FROM VALUES (2L), (3L), (4L) AS o(id)
        ORDER BY 1
    """
    assert [tuple(row) for row in spark.sql(query).collect()] == [(2, 105), (3, 103), (4, None)]


@pytest.mark.skipif(
    is_jvm_spark(),
    reason="Spark rejects this correlated grouping-set shape; per-outer references validate the expected value",
)
def test_cast_correlation_keeps_stacked_grouping_set_totals(spark):
    query = """
        SELECT o.id,
          (SELECT sum(s) * 100 + count(*) FROM (
            SELECT a, sum(s) AS s FROM (
              SELECT k AS a, x, sum(v) AS s
              FROM VALUES ('2', 1, 10), ('2', 2, 20), ('3', 1, 30),
                          ('3', CAST(NULL AS INT), 40) AS i(k, x, v)
              WHERE CAST(i.k AS BIGINT) = o.id
              GROUP BY GROUPING SETS ((k), (k, x))
            )
            GROUP BY GROUPING SETS ((a), ())
          )) AS n
        FROM VALUES (2L), (3L), (4L) AS o(id)
        ORDER BY o.id
    """
    assert [tuple(row) for row in spark.sql(query).collect()] == [(2, 12002), (3, 28002), (4, None)]


@pytest.mark.skipif(
    is_jvm_spark(),
    reason="Spark rejects this correlated grouping-set shape; per-outer references validate the expected value",
)
def test_cast_correlation_keeps_grouping_id_through_a_grouped_aggregate_between_grouping_sets(spark):
    query = """
        SELECT o.id,
          (SELECT sum(z2 * 100 + z1) FROM (
            SELECT grouping_id(a) AS z2, max(z1) AS z1 FROM (
              SELECT a, max(z1) AS z1 FROM (
                SELECT k AS a, grouping_id(k, x2) AS z1
                FROM VALUES ('2', CAST(NULL AS INT), 2, 10), ('02', CAST(NULL AS INT), 2, 20),
                            ('2', 1, CAST(NULL AS INT), 30), ('3', 5, 5, 40) AS i(k, x1, x2, v)
                WHERE CAST(i.k AS BIGINT) = o.id
                GROUP BY GROUPING SETS ((k), (k), (k, x2))
              )
              GROUP BY a
            )
            GROUP BY GROUPING SETS ((a), (a), ())
          )) AS n
        FROM VALUES (2L), (3L), (4L) AS o(id)
        ORDER BY 1
    """
    assert [tuple(row) for row in spark.sql(query).collect()] == [(2, 105), (3, 103), (4, None)]


@pytest.mark.skipif(
    is_jvm_spark(),
    reason="Spark rejects this correlated grouping-set shape; per-outer references validate the expected value",
)
def test_direct_correlation_on_columns_of_every_set_keeps_grouping_id_for_duplicate_grouping_sets(spark):
    # Native pull-up groups by columns every set already groups, so only the duplicate
    # ordinal needs removing, as when the subquery is not correlated.
    query = """
        SELECT o.c4,
          (SELECT max(gid) FROM (
            SELECT grouping_id(c1, c2, c3, c4) AS gid
            FROM VALUES ('2', '2', '2', '2'), ('2', '2', '2', '3') AS i(c1, c2, c3, c4)
            WHERE i.c1 = o.c1 AND i.c2 = o.c2 AND i.c3 = o.c3 AND i.c4 = o.c4
            GROUP BY GROUPING SETS ((c1, c2, c3, c4), (c1, c2, c3, c4))
          )) AS z
        FROM VALUES ('2', '2', '2', '2'), ('2', '2', '2', '3'), ('2', '2', '2', '9') AS o(c1, c2, c3, c4)
        ORDER BY o.c4
    """
    assert [tuple(row) for row in spark.sql(query).collect()] == [("2", 0), ("3", 0), ("9", None)]


# Match Spark: private numeric grouping merges signed zeros before string matching.
@pytest.mark.parametrize("data_type", ["FLOAT", "DOUBLE"])
@pytest.mark.parametrize("windowed", [False, True])
@pytest.mark.parametrize("ansi", [False, True])
def test_float_string_correlation_uses_spark_numeric_keys(spark, data_type, windowed, ansi):
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    try:
        source = f"""VALUES
            (CAST('-0.0' AS {data_type}), 1),
            (CAST('-0.0' AS {data_type}), 2),
            (CAST('0.0' AS {data_type}), 3),
            (CAST('1.0' AS {data_type}), 4),
            (CAST('NaN' AS {data_type}), 5),
            (CAST('NaN' AS {data_type}), 6),
            (CAST(NULL AS {data_type}), 7)
        """
        matching = f"FROM {source} AS i(k, v) WHERE CAST(i.k AS STRING) = o.k"
        subquery = (
            f"SELECT max(w) FROM (SELECT row_number() OVER (ORDER BY v) AS w {matching})"
            if windowed
            else f"SELECT count(*) {matching}"
        )
        result = spark.sql(f"""
            SELECT o.id, ({subquery}) AS n
            FROM VALUES (0, '-0.0'), (1, '0.0'), (2, '1.0'), (3, 'NaN'),
                        (4, '1'), (5, '+1.0'), (6, '01.0'), (7, 'nan'), (8, ' 1.0'),
                        (9, ''), (10, '1e9999'), (11, CAST(NULL AS STRING)),
                        (12, '-0.0') AS o(id, k)
            ORDER BY o.id
        """)  # noqa: S608
        missing = None if windowed else 0
        assert [tuple(row) for row in result.collect()] == [
            (0, missing),
            (1, 3),
            (2, 1),
            (3, 2),
            *[(i, missing) for i in range(4, 12)],
            (12, missing),
        ]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow table ingestion requires PySpark 4+")
@pytest.mark.parametrize(
    ("data_type", "bits"),
    [
        (
            pa.float32(),
            [
                0x7F800001,
                0x7FC00001,
                0x7FFFFFFF,
                0xFF800001,
                0xFFC00042,
                0xFFFFFFFF,
                0x80000000,
                0x00000000,
                0x3F800000,
            ],
        ),
        (
            pa.float64(),
            [
                0x7FF0000000000001,
                0x7FF8000000000001,
                0x7FFFFFFFFFFFFFFF,
                0xFFF0000000000001,
                0xFFF8000000000042,
                0xFFFFFFFFFFFFFFFF,
                0x8000000000000000,
                0x0000000000000000,
                0x3FF0000000000000,
            ],
        ),
    ],
)
@pytest.mark.parametrize("windowed", [False, True])
@pytest.mark.parametrize("ansi", [False, True])
def test_float_string_correlation_normalizes_private_keys_and_preserves_raw_values(
    spark, data_type, bits, windowed, ansi
):
    # Spark groups floating string correlations by canonical float keys. User values
    # still need their original NaN payloads and signed-zero bits.
    width = data_type.bit_width // 8
    raw = b"".join(value.to_bytes(width, "little") for value in bits)
    values = pa.Array.from_buffers(
        data_type,
        len(bits) + 1,
        [pa.py_buffer(bytes([0xFF, 0x01])), pa.py_buffer(raw + bytes(width))],
    )
    table = pa.Table.from_arrays([values, pa.array(range(len(bits) + 1), type=pa.int32())], names=["k", "seq"])
    spark.createDataFrame(table).createOrReplaceTempView("scalar_float_string_payloads")
    previous_ansi = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    try:
        rank = "row_number() OVER (ORDER BY seq)" if windowed else "1"
        result = spark.sql(f"""
            SELECT o.id,
              (SELECT array_sort(collect_list(named_struct('seq', seq, 'k', k, 'w', w)))
               FROM (SELECT seq, k, {rank} AS w FROM scalar_float_string_payloads AS i
                     WHERE CAST(i.k AS STRING) = o.k)) AS events
            FROM VALUES (0, 'NaN'), (1, '0.0'), (2, '1.0') AS o(id, k)
            ORDER BY o.id
        """)  # noqa: S608
        arrow = result._to_table()[0] if hasattr(result, "_to_table") else result.toArrow()  # noqa: SLF001
        assert arrow.column("id").to_pylist() == [0, 1, 2]
        events = arrow.column("events").combine_chunks()
        assert events.offsets.to_pylist() == [0, 6, 8, 9]
        assert events.values.field("seq").to_pylist() == list(range(9))
        expected_rank = [1, 2, 3, 4, 5, 6, 1, 2, 1] if windowed else [1] * 9
        assert events.values.field("w").to_pylist() == expected_rank
        actual = events.values.field("k")
        assert actual.type == data_type
        assert actual.null_count == 0
        payload = actual.buffers()[1].to_pybytes()
        assert payload[actual.offset * width : (actual.offset + len(actual)) * width] == raw
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous_ansi)
        spark.catalog.dropTempView("scalar_float_string_payloads")
