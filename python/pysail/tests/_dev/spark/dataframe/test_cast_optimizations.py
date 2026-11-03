from contextlib import contextmanager
from datetime import date
from decimal import Decimal

import pytest
from pyspark.errors import SparkRuntimeException
from pyspark.sql.types import FloatType, IntegerType, StructField, StructType

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version


@contextmanager
def _configuration(spark, **settings):
    previous = {name: spark.conf.get(name) for name in settings}
    try:
        for name, value in settings.items():
            spark.conf.set(name, value)
        yield
    finally:
        for name, value in previous.items():
            spark.conf.set(name, value)


@contextmanager
def _view(spark, name, rows, schema):
    spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
    try:
        yield
    finally:
        spark.catalog.dropTempView(name)


def _to_table(frame):
    if pyspark_version() < (4,):
        client = frame._session.client  # noqa: SLF001
        return client.to_table(frame._plan.to_proto(client))  # noqa: SLF001
    return frame._to_table()  # noqa: SLF001


def _physical_plan(spark, query):
    # Aggregate displays cache their original SQL; inspect only visible filters and projections.
    return spark.sql(f"EXPLAIN FORMATTED {query}").first()[0]


@pytest.mark.parametrize("ansi", [False, True])
@pytest.mark.parametrize("explicit_double", [False, True])
def test_mixed_arithmetic_preserves_double_precision_and_nonansi_float(spark, ansi, explicit_double):
    rows = [(0, 16777216.0, 16777217), (1, 3.0, 16777217), (2, None, 1), (3, 1.0, None)]
    use_double = ansi or explicit_double
    k, g = ("CAST(k AS DOUBLE)", "CAST(g AS DOUBLE)") if explicit_double else ("k", "g")
    query = f"""SELECT seq,{k}+{g} AS added,{k}-{g} AS subtracted,
        {g}-{k} AS reversed,{k}*{g} AS multiplied,{k}+1 AS incremented
        FROM followup_mixed ORDER BY seq"""  # noqa: S608
    with (
        _configuration(spark, **{"spark.sql.ansi.enabled": str(ansi).lower()}),
        _view(spark, "followup_mixed", rows, "seq int,k float,g int"),
    ):
        frame = spark.sql(query)
        expected = (
            [
                (0, 33554433.0, -1.0, 1.0, 281474993487872.0, 16777217.0),
                (1, 16777220.0, -16777214.0, 16777214.0, 50331651.0, 4.0),
            ]
            if use_double
            else [
                (0, 33554432.0, 0.0, 0.0, 281474976710656.0, 16777216.0),
                (1, 16777220.0, -16777213.0, 16777213.0, 50331648.0, 4.0),
            ]
        )
        expected += [(2, None, None, None, None, None), (3, None, None, None, None, 2.0)]
        assert [tuple(row) for row in frame.collect()] == expected
        assert all(field.dataType.simpleString() == ("double" if use_double else "float") for field in frame.schema[1:])
        if not is_jvm_spark():
            # Isolate each operation so common-subexpression extraction cannot materialize its casts first.
            for operation, operator in [("add", "+"), ("subtract", "-"), ("multiply", "*")]:
                plan = _physical_plan(spark, f"SELECT {k}{operator}{g} AS value FROM followup_mixed")  # noqa: S608
                assert (f"mixed_numeric_{operation}(" in plan) is use_double


@pytest.mark.parametrize("ansi", [False, True])
def test_mixed_arithmetic_count_filter_and_sum_shapes(spark, ansi):
    rows = [(99.0, 1), (100.0, 2), (101.0, 3), (0.0, 4), (None, 5)]
    count_query = "SELECT COUNT(*) FROM followup_fact_m WHERE kf + 1 > 100"
    sum_query = "SELECT SUM(kf + g) FROM followup_fact_m"
    with (
        _configuration(spark, **{"spark.sql.ansi.enabled": str(ansi).lower()}),
        _view(spark, "followup_fact_m", rows, "kf float,g int"),
    ):
        assert spark.sql(count_query).first()[0] == 2  # noqa: PLR2004
        frame = spark.sql(sum_query)
        assert frame.first()[0] == 310.0  # noqa: PLR2004
        assert frame.schema[0].dataType.simpleString() == "double"
        if not is_jvm_spark():
            assert ("mixed_numeric_add(" in _physical_plan(spark, count_query)) is ansi
            # The Rust aggregate test inspects actual SUM arguments; its display keeps the original name.
            table, execution_schema = _to_table(frame)
            assert execution_schema == frame.schema
            assert table.column_names == frame.columns
            assert table.schema.field(0).nullable == frame.schema[0].nullable
            assert table.column(0).to_pylist() == [310.0]


@pytest.mark.parametrize("ansi", [False, True])
def test_mixed_greatest_least_keep_precision_and_skip_nulls(spark, ansi):
    rows = [(0, 16777216.0, 16777217), (1, None, 7), (2, 2.0, None), (3, None, None)]
    with (
        _configuration(spark, **{"spark.sql.ansi.enabled": str(ansi).lower()}),
        _view(spark, "followup_extrema", rows, "seq int,k float,g int"),
    ):
        frame = spark.sql("SELECT seq,greatest(k,g) AS hi,least(k,g) AS lo FROM followup_extrema ORDER BY seq")
        expected = [(0, 16777217.0 if ansi else 16777216.0, 16777216.0)]
        expected += [(1, 7.0, 7.0), (2, 2.0, 2.0), (3, None, None)]
        assert [tuple(row) for row in frame.collect()] == expected


@pytest.mark.parametrize("ansi", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("null_safe", [False, True])
def test_mixed_join_preserves_rounded_bigint_classes_and_unmatched_rows(spark, ansi, reverse, null_safe):
    floats = [(0, float(2**24)), (1, float(2**53)), (2, float(2**63)), (3, -0.0), (4, None)]
    integers = [(0, 2**24 + 1), (1, 2**53), (2, 2**53 + 1), (3, 2**63 - 1), (4, 0), (5, None)]
    operator = "<=>" if null_safe else "="
    join = f"i RIGHT JOIN f ON i.k {operator} f.k" if reverse else f"f LEFT JOIN i ON f.k {operator} i.k"
    query = f"""WITH f AS (SELECT * FROM followup_join_float),i AS (SELECT * FROM followup_join_int)
        SELECT f.seq,i.seq FROM {join} ORDER BY f.seq,i.seq"""  # noqa: S608
    with (
        _configuration(spark, **{"spark.sql.ansi.enabled": str(ansi).lower()}),
        _view(spark, "followup_join_float", floats, "seq int,k float"),
        _view(spark, "followup_join_int", integers, "seq int,k bigint"),
    ):
        expected = [(0, None if ansi else 0), (1, 1), (1, 2), (2, 3), (3, 4), (4, 5 if null_safe else None)]
        assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.mark.parametrize("ansi", [False, True])
@pytest.mark.parametrize("source", ["float", "double"])
@pytest.mark.parametrize("windowed", [False, True])
def test_float_decimal_correlation_groups_by_rounded_value(spark, ansi, source, windowed, request):
    inner = [(0, 1.0, 10), (1, 1.00390625, 20), (2, 1.0078125, 40), (3, None, 99)]
    outer = [(0, Decimal("1.00")), (1, Decimal("1.01")), (2, None)]
    if windowed:
        aggregate = """SELECT max(rn) FROM (SELECT row_number() OVER(ORDER BY seq) AS rn
            FROM followup_decimal_inner i WHERE CAST(i.k AS DECIMAL(12,2))=o.k) w"""
        outputs = f"({aggregate}) AS result"
        expected = [(0, 2), (1, 1), (2, None)]
    else:
        predicate = "FROM followup_decimal_inner i WHERE CAST(i.k AS DECIMAL(12,2))=o.k"
        outputs = f"(SELECT count(*) {predicate}) AS n,(SELECT sum(v) {predicate}) AS total"
        expected = [(0, 2, 30), (1, 1, 40), (2, 0, None)]
    query = f"SELECT o.seq,{outputs} FROM followup_decimal_outer o ORDER BY o.seq"  # noqa: S608
    with (
        _configuration(spark, **{"spark.sql.ansi.enabled": str(ansi).lower()}),
        _view(spark, "followup_decimal_inner", inner, f"seq int,k {source},v int"),
        _view(spark, "followup_decimal_outer", outer, "seq int,k decimal(12,2)"),
    ):
        if not is_jvm_spark() and windowed and not ansi:
            request.node.add_marker(
                pytest.mark.xfail(
                    strict=True,
                    raises=pytest.RaisesExc(
                        SparkRuntimeException,
                        match=r'^Schema contains qualified field name "\?table\?"\.k and unqualified field name k '
                        r"which would be ambiguous$",
                    ),
                    reason="Non-ANSI decimal windows already failed before #2843; the current qualifier error is unchanged",
                )
            )
        assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.mark.parametrize("ansi", [False, True])
@pytest.mark.parametrize("source", ["float", "double"])
def test_float_decimal_correlation_preserves_native_close_tie_rounding(spark, ansi, source):
    # Spark decimal conversion and Sail's native binary multiply-round can differ.
    inner = [(i, value) for i, value in enumerate([1.005, -1.005, 2.675, -2.675, 0.005, -0.005, None])]
    with (
        _configuration(spark, **{"spark.sql.ansi.enabled": str(ansi).lower()}),
        _view(spark, "followup_decimal_ties", inner, f"seq int,k {source}"),
    ):
        direct = [row[0] for row in spark.sql("SELECT CAST(k AS DECIMAL(12,2)) FROM followup_decimal_ties").collect()]
        keys = list(dict.fromkeys(direct))
        outer = list(enumerate(keys))
        with _view(spark, "followup_decimal_tie_outer", outer, "seq int,k decimal(12,2)"):
            query = """SELECT o.seq,(SELECT count(*) FROM followup_decimal_ties i
                WHERE CAST(i.k AS DECIMAL(12,2))=o.k) AS n
                FROM followup_decimal_tie_outer o ORDER BY o.seq"""
            expected = [(i, direct.count(key) if key is not None else 0) for i, key in enumerate(keys)]
            assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.mark.parametrize("ansi", [False, True])
@pytest.mark.parametrize(
    ("source", "target", "keys", "outer_keys"),
    [
        ("int", "decimal(9,0)", [-999999999, 0, 0, 999999999, None], ["-999999999", "0", "999999999", None]),
        (
            "bigint",
            "decimal(38,20)",
            [1, 2**53, 2**53 + 1, 999999999999999999, None],
            ["1", "1.00000000000000000001", str(2**53), str(2**53 + 1), "999999999999999999", None],
        ),
    ],
)
def test_integer_decimal_correlation_keeps_exact_precision_and_outer_image(
    spark, ansi, source, target, keys, outer_keys
):
    inner = [(i, value, i + 1) for i, value in enumerate(keys)]
    outer = [(i, None if value is None else Decimal(value)) for i, value in enumerate(outer_keys)]
    expected = []
    for i, key in outer:
        weights = [weight for _, value, weight in inner if value is not None and Decimal(value) == key]
        expected.append((i, len(weights), sum(weights) if weights else None))
    query = f"""SELECT o.seq,(SELECT count(*) FROM followup_integer_inner i WHERE CAST(i.k AS {target})=o.k),
        (SELECT sum(v) FROM followup_integer_inner i WHERE CAST(i.k AS {target})=o.k)
        FROM followup_integer_outer o ORDER BY o.seq"""  # noqa: S608
    with (
        _configuration(spark, **{"spark.sql.ansi.enabled": str(ansi).lower()}),
        _view(spark, "followup_integer_inner", inner, f"seq int,k {source},v int"),
        _view(spark, "followup_integer_outer", outer, f"seq int,k {target}"),
    ):
        assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.mark.parametrize("ansi", [False, True])
@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("target", ["TIMESTAMP", "TIMESTAMP_NTZ"])
@pytest.mark.parametrize("timezone", ["UTC", "America/New_York"])
def test_date_timestamp_correlation_preserves_midnight_image(spark, ansi, explicit, target, timezone, request):
    inner = [(date(1970, 1, 1), 10), (date(1970, 1, 1), 20), (None, 99)]
    key = f"CAST(i.k AS {target})" if explicit else "i.k"
    query = f"""WITH o(seq,k) AS (VALUES
        (0,CAST('1970-01-01 00:00:00' AS {target})),
        (1,CAST('1970-01-01 12:00:00' AS {target})),(2,CAST(NULL AS {target})))
        SELECT o.seq,(SELECT count(*) FROM followup_date_inner i WHERE {key}=o.k),
        (SELECT sum(v) FROM followup_date_inner i WHERE {key}=o.k) FROM o ORDER BY o.seq"""  # noqa: S608
    with (
        _configuration(spark, **{"spark.sql.ansi.enabled": str(ansi).lower(), "spark.sql.session.timeZone": timezone}),
        _view(spark, "followup_date_inner", inner, "k date,v int"),
    ):
        historical = not is_jvm_spark() and timezone == "America/New_York" and target == "TIMESTAMP" and not explicit
        if historical:
            request.node.add_marker(
                pytest.mark.xfail(
                    strict=True,
                    raises=pytest.RaisesExc(
                        AssertionError, match="historical implicit DATE/TIMESTAMP midnight mismatch"
                    ),
                    reason="Implicit New York DATE/TIMESTAMP comparison returned no matches before #2843",
                )
            )
        rows = [tuple(row) for row in spark.sql(query).collect()]
        diagnostic = (
            "historical implicit DATE/TIMESTAMP midnight mismatch"
            if historical and rows == [(0, 0, None), (1, 0, None), (2, 0, None)]
            else "unexpected DATE/TIMESTAMP comparison result"
        )
        assert rows == [(0, 2, 30), (1, 0, None), (2, 0, None)], diagnostic


@pytest.mark.parametrize("ansi", [False, True])
@pytest.mark.parametrize("windowed", [False, True])
def test_float_integer_correlation_merges_truncation_classes(spark, ansi, windowed):
    values = [1.25, 1.75, -1.25, -1.75, -0.75, 0.75, -0.0, 0.0, None]
    inner = [(i, value, (i + 1) * 10) for i, value in enumerate(values)]
    outer = [(0, 1), (1, -1), (2, 0), (3, None)]
    if windowed:
        outputs = """(SELECT max(rn) FROM (SELECT row_number() OVER(ORDER BY seq) AS rn
            FROM followup_narrow_inner i WHERE CAST(i.k AS INT)=o.k) w)"""
        expected = [(0, 2), (1, 2), (2, 4), (3, None)]
    else:
        predicate = "FROM followup_narrow_inner i WHERE CAST(i.k AS INT)=o.k"
        outputs = f"(SELECT count(*) {predicate}),(SELECT sum(v) {predicate})"
        expected = [(0, 2, 30), (1, 2, 70), (2, 4, 260), (3, 0, None)]
    query = f"SELECT o.seq,{outputs} FROM followup_narrow_outer o ORDER BY o.seq"  # noqa: S608
    with (
        _configuration(spark, **{"spark.sql.ansi.enabled": str(ansi).lower()}),
        _view(spark, "followup_narrow_inner", inner, "seq int,k float,v int"),
        _view(spark, "followup_narrow_outer", outer, "seq int,k int"),
    ):
        assert [tuple(row) for row in spark.sql(query).collect()] == expected


@pytest.mark.parametrize("ansi", [False, True])
def test_tinyint_try_cast_preserves_overflow_nulls(spark, ansi):
    values = [-129, -128, -1, 0, 127, 128, None]
    with (
        _configuration(spark, **{"spark.sql.ansi.enabled": str(ansi).lower()}),
        _view(spark, "followup_tinyint", list(enumerate(values)), "seq int,k int"),
    ):
        result = spark.sql("SELECT TRY_CAST(k AS TINYINT) FROM followup_tinyint ORDER BY seq").collect()
        assert [row[0] for row in result] == [None, -128, -1, 0, 127, None, None]


@pytest.mark.skipif(is_jvm_spark(), reason="Sail physical expression schema must match its analyzed schema")
@pytest.mark.parametrize("ansi", [False, True])
@pytest.mark.parametrize("nullable", [False, True])
def test_mixed_arithmetic_keeps_analyzed_protocol_arrow_nullability_and_metadata(spark, ansi, nullable):
    schema = StructType(
        [
            StructField("k", FloatType(), nullable, {"source": "raw-float"}),
            StructField("g", IntegerType(), nullable, {"source": "raw-int"}),
        ]
    )
    rows = [(16777216.0, 16777217)] + ([(None, None)] if nullable else [])
    query = "SELECT k,g,CAST(k AS DOUBLE)+CAST(g AS DOUBLE) AS value FROM followup_schema"
    with (
        _configuration(spark, **{"spark.sql.ansi.enabled": str(ansi).lower()}),
        _view(spark, "followup_schema", rows, schema),
    ):
        frame = spark.sql(query)
        assert frame.schema["value"].nullable is nullable
        assert frame.schema["k"].metadata == {"source": "raw-float"}
        assert frame.schema["g"].metadata == {"source": "raw-int"}
        table, execution_schema = _to_table(frame)
        assert (execution_schema or frame.schema).jsonValue() == frame.schema.jsonValue()
        assert table.schema.field("value").nullable is nullable
        assert table.column("value").to_pylist() == [33554433.0] + ([None] if nullable else [])
        assert table.column("k").to_pylist() == [16777216.0] + ([None] if nullable else [])
        assert table.column("g").to_pylist() == [16777217] + ([None] if nullable else [])
