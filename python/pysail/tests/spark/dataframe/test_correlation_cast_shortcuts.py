import re

import pyarrow as pa
import pytest
from pyspark.errors import IllegalArgumentException, SparkRuntimeException

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version


def _optimized_plan(spark, query):
    plan = spark.sql(f"EXPLAIN EXTENDED {query}").first()[0]
    return plan.split("== Optimized Logical Plan ==", 1)[1].split("== Physical Plan ==", 1)[0]


def _computed_correlation_cast(plan):
    casts = (
        match.group(1)
        for line in plan.splitlines()
        if line.lstrip().startswith("Projection:")
        for match in re.finditer(r"CAST\((.*?)\) AS __sail_correlation_key_\d+", line)
    )
    # Restoring a grouped private key is not a cast of each original input row.
    return any("__sail_correlation_key_" not in expression for expression in casts)


def _float_array(bits, width):
    raw = b"".join((value or 0).to_bytes(width, "little") for value in bits)
    validity = bytearray((len(bits) + 7) // 8)
    for index, value in enumerate(bits):
        if value is not None:
            validity[index // 8] |= 1 << (index % 8)
    data_type = pa.float32() if width == 4 else pa.float64()  # noqa: PLR2004
    return pa.Array.from_buffers(data_type, len(bits), [pa.py_buffer(validity), pa.py_buffer(raw)])


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow table ingestion requires PySpark 4+")
@pytest.mark.skipif(is_jvm_spark(), reason="Sail preserves distinct NaN payloads in numeric equality")
@pytest.mark.parametrize("windowed", [False, True])
@pytest.mark.parametrize("ansi", [False, True])
def test_float_to_double_correlation_preserves_target_nan_classes_and_raw_values(spark, windowed, ansi):
    # Widening quiets each signaling NaN, merging its matching quiet payload only.
    # The aggregate argument must retain every original source payload and zero sign.
    bits = [
        0x7F800001,
        0x7FC00001,
        0x7FC00002,
        0xFF800001,
        0xFFC00001,
        0xFFC00002,
        0x00000000,
        0x80000000,
        0x3FA00000,
        None,
        0x7F800000,
        0xFF800000,
    ]
    outer_bits = [
        0x7FF8000020000000,
        0x7FF8000040000000,
        0xFFF8000020000000,
        0xFFF8000040000000,
        0x0000000000000000,
        0x8000000000000000,
        0x3FF4000000000000,
        None,
        0x7FF8000060000000,
        0x7FF8000020000001,  # Nonimage payload would narrow onto an existing NaN key.
        0x7FF0000020000000,  # Widened FLOAT values never retain a signaling NaN.
        0xFFF0000020000000,
        0x3FF4000000000001,  # Neighboring DOUBLE is not exactly representable as FLOAT.
        0x7FF0000000000000,
        0xFFF0000000000000,
        0x7FEFFFFFFFFFFFFF,  # Finite overflow must not match positive infinity.
        0xFFEFFFFFFFFFFFFF,
        0x0000000000000001,  # Underflow must not match the zero group.
        0x8000000000000001,
    ]
    groups = [[0, 1], [2], [3, 4], [5], [6, 7], [6, 7], [8], [], [], [], [], [], [], [10], [11], [], [], [], []]
    inner = pa.Table.from_arrays(
        [pa.array(range(len(bits)), type=pa.int32()), _float_array(bits, 4)], names=["seq", "k"]
    )
    outer = pa.Table.from_arrays(
        [pa.array(range(len(outer_bits)), type=pa.int32()), _float_array(outer_bits, 8)], names=["id", "k"]
    )
    spark.createDataFrame(inner).createOrReplaceTempView("optimizer_float_keys")
    spark.createDataFrame(outer).createOrReplaceTempView("optimizer_double_keys")
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", str(ansi).lower())
    rank = "row_number() OVER (PARTITION BY 1 ORDER BY seq)" if windowed else "CAST(NULL AS INT)"
    query = f"""
        SELECT o.id, (SELECT array_sort(collect_list(named_struct('seq',seq,'k',k,'rn',rn)))
          FROM (SELECT seq,k,{rank} AS rn FROM optimizer_float_keys i WHERE CAST(i.k AS DOUBLE)=o.k) t) AS events
        FROM optimizer_double_keys o ORDER BY o.id
    """  # noqa: S608
    try:
        frame = spark.sql(query)
        arrow = frame._to_table()[0]  # noqa: SLF001
        events = arrow.column("events").combine_chunks()
        expected_offsets = [0]
        for group in groups:
            expected_offsets.append(expected_offsets[-1] + len(group))
        assert events.offsets.to_pylist() == expected_offsets
        values = events.values
        indices = [index for group in groups for index in group]
        assert values.field("seq").to_pylist() == indices
        expected_ranks = [rank for group in groups for rank in range(1, len(group) + 1)]
        assert values.field("rn").to_pylist() == (expected_ranks if windowed else [None] * len(indices))
        raw = values.field("k")
        actual = raw.buffers()[1].to_pybytes()[raw.offset * 4 : (raw.offset + len(raw)) * 4]
        assert actual == b"".join(bits[index].to_bytes(4, "little") for index in indices)
        plan = _optimized_plan(spark, query)
        assert _computed_correlation_cast(plan)
        assert "normalize_float_widening_keys(" not in plan
        assert "float_widening_outer_key(" not in plan
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)
        spark.catalog.dropTempView("optimizer_float_keys")
        spark.catalog.dropTempView("optimizer_double_keys")


@pytest.mark.skipif(is_jvm_spark(), reason="Spark decorrelates these lossy upcasts by distinct raw keys")
@pytest.mark.parametrize(
    ("source", "target", "first"),
    [("INT", "FLOAT", 16777216), ("BIGINT", "FLOAT", 16777216), ("BIGINT", "DOUBLE", 9007199254740992)],
)
@pytest.mark.parametrize("windowed", [False, True])
def test_lossy_integer_float_correlations_keep_computed_keys(spark, source, target, first, windowed):
    if windowed:
        body = f"""SELECT max(rn) FROM (
            SELECT row_number() OVER (PARTITION BY 1 ORDER BY v) AS rn
            FROM i WHERE CAST(i.k AS {target})=o.k) t"""  # noqa: S608
        expected = 2
    else:
        body = f"SELECT named_struct('n',count(*),'s',sum(v),'a',avg(v)) FROM i WHERE CAST(i.k AS {target})=o.k"  # noqa: S608
        expected = (2, 30, 15.0)
    query = f"""
        WITH i(k,v) AS (VALUES (CAST({first} AS {source}),10),(CAST({first + 1} AS {source}),20)),
             o(k) AS (VALUES (CAST({first} AS {target})))
        SELECT ({body}) AS value FROM o
    """  # noqa: S608
    rows = spark.sql(query).collect()
    assert len(rows) == 1
    actual = rows[0].value if windowed else tuple(rows[0].value)
    assert actual == expected
    assert _computed_correlation_cast(_optimized_plan(spark, query))


@pytest.mark.parametrize(
    ("source", "target", "inner", "outer", "computed"),
    [
        ("ARRAY<INT>", "ARRAY<BIGINT>", "array(1,NULL)", "array(1L,NULL)", True),
        (
            "STRUCT<a:INT,b:STRING>",
            "STRUCT<a:BIGINT,b:STRING>",
            "named_struct('a',1,'b',NULL)",
            "named_struct('a',1L,'b',NULL)",
            False,
        ),
    ],
)
def test_exact_nested_cast_correlation_preserves_key_strategy(spark, source, target, inner, outer, computed):
    query = f"""
        WITH i(k,v) AS (VALUES (CAST({inner} AS {source}),10),(CAST({inner} AS {source}),20),
                              (CAST(NULL AS {source}),99)),
             o(id,k) AS (VALUES (0,CAST({outer} AS {target})),(1,CAST(NULL AS {target})))
        SELECT o.id,(SELECT count(*) FROM i WHERE CAST(i.k AS {target})=o.k) AS n FROM o ORDER BY o.id
    """  # noqa: S608
    assert [tuple(row) for row in spark.sql(query).collect()] == [(0, 2), (1, 0)]
    if not is_jvm_spark():
        assert _computed_correlation_cast(_optimized_plan(spark, query)) is computed


@pytest.mark.parametrize(
    ("source", "target", "inner", "outer", "computed"),
    [
        ("BINARY", "STRING", "X'6162'", "'ab'", True),
        ("DATE", "TIMESTAMP_NTZ", "DATE '2000-01-02'", "TIMESTAMP_NTZ '2000-01-02 00:00:00'", True),
        ("DECIMAL(19,0)", "BIGINT", "9223372036854775807", "9223372036854775807L", True),
        ("ARRAY<DATE>", "ARRAY<STRING>", "array(DATE '2000-01-02',NULL)", "array('2000-01-02',NULL)", False),
        pytest.param(
            "MAP<STRING,INT>",
            "MAP<STRING,BIGINT>",
            "map('a',1,'b',NULL)",
            "map('a',1L,'b',NULL)",
            False,
            marks=pytest.mark.skipif(is_jvm_spark(), reason="Spark rejects equality between map values"),
        ),
    ],
)
def test_additional_casts_preserve_values_and_key_strategy(spark, source, target, inner, outer, computed):
    query = f"""
        WITH i(k,v) AS (VALUES (CAST({inner} AS {source}),10),(CAST({inner} AS {source}),20),
                              (CAST(NULL AS {source}),99)),
             o(id,k) AS (VALUES (0,CAST({outer} AS {target})),(1,CAST(NULL AS {target})))
        SELECT o.id,(SELECT count(*) FROM i WHERE CAST(i.k AS {target})=o.k) AS n FROM o ORDER BY o.id
    """  # noqa: S608
    assert [tuple(row) for row in spark.sql(query).collect()] == [(0, 2), (1, 0)]
    if not is_jvm_spark():
        plan = _optimized_plan(spark, query)
        assert _computed_correlation_cast(plan) is computed
        assert "CAST(" in plan  # The final equality still uses the requested target type.


@pytest.mark.parametrize(
    ("source", "target", "good", "bad", "outer", "error"),
    [
        pytest.param(
            "BINARY",
            "STRING",
            "X'6162'",
            "X'ff'",
            "'ab'",
            r"(?i)utf.?8",
            marks=pytest.mark.skipif(
                is_jvm_spark(), reason="Sail rejects invalid UTF-8; Spark uses replacement decoding"
            ),
        ),
        ("DECIMAL(19,0)", "BIGINT", "1", "9223372036854775808", "1L", r"(?i)(range|overflow|cast)"),
    ],
)
def test_partial_casts_preserve_errors_after_local_filters(spark, source, target, good, bad, outer, error):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    prefix = f"""WITH i AS (
        SELECT * FROM VALUES (0,CAST({good} AS {source})),(1,CAST({bad} AS {source})) AS t(seq,k)
        {{local_filter}}), o(k) AS (VALUES (CAST({outer} AS {target})))
        SELECT (SELECT count(*) FROM i WHERE CAST(i.k AS {target})=o.k) AS n FROM o {{outer_filter}}"""  # noqa: S608
    try:
        with pytest.raises(Exception, match=error):
            spark.sql(prefix.format(local_filter="", outer_filter="")).collect()
        # Elimination occurs before the strict cast, preserving the merge-base contract.
        filtered = prefix.format(local_filter="WHERE seq=0", outer_filter="")
        assert [tuple(row) for row in spark.sql(filtered).collect()] == [(1,)]
        assert spark.sql(prefix.format(local_filter="", outer_filter="WHERE false")).collect() == []
        if not is_jvm_spark():
            assert _computed_correlation_cast(_optimized_plan(spark, filtered))
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


@pytest.mark.skipif(is_jvm_spark(), reason="Sail permits raw Time64 values outside the civil-day range")
def test_nested_time_string_correlation_keeps_colliding_computed_keys(spark):
    # Arrow's Time64 formatter wraps seconds through u32. These distinct raw times
    # have the same string key, even though ordinary in-range time formatting is exact.
    query = """
        WITH i(k,v) AS (
          SELECT array(CAST(k AS TIME)),v FROM VALUES (0L,1),(4294967296000000L,2) AS t(k,v)
        ), o(k) AS (VALUES (array('00:00:00')))
        SELECT (SELECT count(*) FROM i WHERE CAST(i.k AS ARRAY<STRING>)=o.k) AS n FROM o
    """
    assert [tuple(row) for row in spark.sql(query).collect()] == [(2,)]
    assert _computed_correlation_cast(_optimized_plan(spark, query))


@pytest.mark.skipif(pyspark_version() < (4,), reason="Arrow table ingestion requires PySpark 4+")
@pytest.mark.skipif(is_jvm_spark(), reason="Sail's strict UTF-8 cast has a distinct non-NULL error contract")
@pytest.mark.parametrize("shape", ["binary", "list", "struct"])
def test_cast_correlation_respects_null_validity(spark, shape, request):
    # Spark skips casting a NULL parent. Keep that desired row oracle even though
    # Sail's computed binary cast retains the baseline hidden-byte validation bug.
    # The key-strategy and required-invalid controls must run before the known error.
    children = pa.array([b"\xff", b"ab"], type=pa.binary())
    mask = pa.array([True, False])
    bad_children = pa.array([b"\xff"], type=pa.binary())
    if shape == "binary":
        values = pa.Array.from_buffers(
            pa.binary(),
            2,
            [pa.py_buffer(b"\x02"), pa.array([0, 1, 3], type=pa.int32()).buffers()[1], pa.py_buffer(b"\xffab")],
        )
        bad = bad_children
        target, key = "STRING", "'ab'"
    elif shape == "list":
        values = pa.ListArray.from_arrays(pa.array([0, 1, 2], type=pa.int32()), children, mask=mask)
        bad = pa.ListArray.from_arrays(pa.array([0, 1], type=pa.int32()), bad_children)
        target, key = "ARRAY<STRING>", "array('ab')"
    else:
        values = pa.StructArray.from_arrays([children], names=["b"], mask=mask)
        bad = pa.StructArray.from_arrays([bad_children], names=["b"])
        target, key = "STRUCT<b:STRING>", "named_struct('b','ab')"
    spark.createDataFrame(pa.Table.from_arrays([values, pa.array([1, 2])], names=["k", "v"])).createOrReplaceTempView(
        "optimizer_hidden_null_keys"
    )
    spark.createDataFrame(pa.Table.from_arrays([bad], names=["k"])).createOrReplaceTempView(
        "optimizer_visible_bad_keys"
    )
    query = f"""
        WITH o(id,k) AS (VALUES (0,{key}),(1,CAST(NULL AS {target})))
        SELECT o.id,(SELECT count(*) FROM optimizer_hidden_null_keys i WHERE CAST(i.k AS {target})=o.k) AS n
        FROM o ORDER BY o.id
    """  # noqa: S608
    try:
        assert spark.sql("SELECT k FROM optimizer_hidden_null_keys ORDER BY v").first().k is None
        assert _computed_correlation_cast(_optimized_plan(spark, query))
        with pytest.raises((IllegalArgumentException, SparkRuntimeException), match=r"(?i)utf.?8"):
            spark.sql(
                f"""SELECT (SELECT count(*) FROM optimizer_visible_bad_keys i WHERE CAST(i.k AS {target})=o.k)
                AS n FROM VALUES ({key}) AS o(k)"""  # noqa: S608
            ).collect()
        request.applymarker(
            pytest.mark.xfail(
                strict=True,
                raises=pytest.RaisesExc((IllegalArgumentException, SparkRuntimeException), match=r"(?i)utf.?8"),
                reason="Baseline Arrow casts validate bytes hidden by NULL; binary cast lifting remains deferred",
            )
        )
        assert [tuple(row) for row in spark.sql(query).collect()] == [(0, 1), (1, 0)]
    finally:
        spark.catalog.dropTempView("optimizer_hidden_null_keys")
        spark.catalog.dropTempView("optimizer_visible_bad_keys")


def test_computed_outer_float_comparison_keeps_the_original_cast_key(spark):
    query = """
        WITH i(k) AS (VALUES (CAST(1.25 AS FLOAT)),(CAST(1.25 AS FLOAT)),(CAST(2.5 AS FLOAT))),
             o(id,k) AS (VALUES (0,CAST(0.625 AS DOUBLE)),(1,CAST(1.25 AS DOUBLE)),
                               (2,CAST(1.2500000000000002 AS DOUBLE)),(3,CAST(NULL AS DOUBLE)))
        SELECT o.id,(SELECT count(*) FROM i WHERE CAST(i.k AS DOUBLE)=o.k*CAST(2 AS DOUBLE)) AS n
        FROM o ORDER BY o.id
    """
    assert [tuple(row) for row in spark.sql(query).collect()] == [(0, 2), (1, 1), (2, 0), (3, 0)]
    if not is_jvm_spark():
        plan = _optimized_plan(spark, query)
        assert _computed_correlation_cast(plan)
        assert "normalize_float_widening_keys(" not in plan
        assert "float_widening_outer_key(" not in plan
        assert "TRY_CAST(" not in plan
