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
