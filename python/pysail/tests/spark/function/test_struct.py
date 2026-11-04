import re

import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.errors import AnalysisException, PySparkException
from pyspark.sql import Row
from pyspark.sql import types as T  # noqa: N812

from pysail.testing.spark.utils.common import pyspark_version


# Spark CreateNamedStruct preserves child nullability/metadata and accepts empty/duplicate names.
@pytest.mark.parametrize(
    ("expression", "fields", "rows"),
    [
        pytest.param("named_struct()", [], [Row(), Row()], id="empty"),
        pytest.param(
            "named_struct('', 1, 'x', 2, 'x', 3)",
            [T.StructField(name, T.IntegerType(), False) for name in ("", "x", "x")],
            [Row(1, 2, 3)] * 2,
            id="empty-and-duplicate-names",
        ),
        pytest.param(
            "named_struct(concat('a', 'b'), id, cast(1 AS string), cast(NULL AS int))",
            [T.StructField("ab", T.LongType(), False), T.StructField("1", T.IntegerType(), True)],
            [Row(i, None) for i in range(2)],
            id="foldable-names-and-null-value",
        ),
        pytest.param(
            "named_struct('s', named_struct('value', CASE WHEN id = 0 THEN id END))",
            [T.StructField("s", T.StructType([T.StructField("value", T.LongType(), True)]), False)],
            [Row(Row(value=0)), Row(Row(value=None))],
            id="nested-dynamic-child",
        ),
    ],
)
def test_named_struct_values_and_schema(spark, expression, fields, rows):
    result = spark.range(2).selectExpr(f"{expression} AS s")
    assert result.schema == T.StructType([T.StructField("s", T.StructType(fields), False)])
    assert [row.s for row in result.collect()] == rows


def test_named_struct_preserves_field_metadata(spark):
    schema = T.StructType(
        [
            T.StructField("a", T.IntegerType(), False, {"tag": "kept"}),
            T.StructField("b", T.IntegerType(), True),
        ]
    )
    source = spark.createDataFrame([(1, None), (2, 3)], schema)
    result = source.selectExpr("named_struct('x', a, 'y', b) AS s")
    expected = T.StructType(
        [
            T.StructField("x", T.IntegerType(), False, {"tag": "kept"}),
            T.StructField("y", T.IntegerType(), True),
        ]
    )
    assert result.schema == T.StructType([T.StructField("s", expected, False)])
    assert result.collect() == [Row(s=Row(x=1, y=None)), Row(s=Row(x=2, y=3))]


@pytest.mark.parametrize(
    "expression",
    [
        "named_struct('a', 1, 'b')",
        "named_struct(1, 2)",
        "named_struct(NULL, 1)",
        "named_struct(cast(NULL AS string), 1)",
        "named_struct(cast(id AS string), 1)",
        "named_struct(cast(rand() AS string), 1)",
    ],
)
def test_named_struct_rejects_invalid_names_and_arity(spark, expression):
    with pytest.raises(AnalysisException):
        spark.range(2).selectExpr(expression).collect()


@pytest.mark.parametrize(
    ("expression", "condition"),
    [
        ("named_struct('a', 1, 'b')", "WRONG_NUM_ARGS.WITHOUT_SUGGESTION"),
        ("named_struct(cast(NULL AS string), 1)", "DATATYPE_MISMATCH.UNEXPECTED_NULL"),
        *[
            (expression, "DATATYPE_MISMATCH.CREATE_NAMED_STRUCT_WITHOUT_FOLDABLE_STRING")
            for expression in [
                "named_struct(1, 2)",
                "named_struct(NULL, 1)",
                "named_struct(cast(id AS string), 1)",
                "named_struct(cast(rand() AS string), 1)",
            ]
        ],
    ],
)
def test_named_struct_invalid_sql_fails_during_analysis(spark, expression, condition):
    with pytest.raises(AnalysisException, match=re.escape(f"[{condition}]")):
        # The expression is a fixed SQL fragment from the parametrization above.
        _ = spark.sql(f"SELECT {expression} FROM range(2)").schema  # noqa: S608


def test_struct_preserves_lambda_field_name(spark):
    observations = spark.createDataFrame(
        [([(-2.5,), (6.0,)],), ([(8.0,), (16.5,)],)],
        "readings array<struct<temperature:double>>",
    )

    result = observations.select(
        F.exists("readings", lambda reading: F.struct(reading.temperature).temperature < 0).alias("freezing")
    )

    assert result.collect() == [Row(freezing=True), Row(freezing=False)]


# Spark TypeCoercionHelper.findTypeForComplex zips fields, using SQLConf.resolver.
@pytest.mark.parametrize("names", [("x", "x"), ("", ""), ("a", "b")])
@pytest.mark.parametrize("wrapper", ["{}", "named_struct('s', {})", "array({})"])
def test_struct_comparison_coerces_each_position(spark, names, wrapper):
    left = wrapper.format(f"named_struct('{names[0]}', cast(id AS int), '{names[1]}', id + 1)")
    equal = wrapper.format(f"named_struct('{names[0]}', id, '{names[1]}', cast(id + 1 AS int))")
    different = wrapper.format(f"named_struct('{names[0]}', id, '{names[1]}', cast(id + 2 AS int))")
    result = spark.range(3).selectExpr(
        f"{left} = {equal} AS eq", f"{left} IN ({different}) AS mismatch", f"{left} IN ({equal}) AS matched"
    )
    assert result.collect() == [Row(eq=True, mismatch=False, matched=True)] * 3


@pytest.mark.parametrize("case_sensitive", [False, True])
@pytest.mark.parametrize("operator", ["=", "IN"])
def test_struct_comparison_uses_configured_resolver(spark, case_sensitive, operator):
    previous = spark.conf.get("spark.sql.caseSensitive")
    try:
        spark.conf.set("spark.sql.caseSensitive", str(case_sensitive).lower())
        left = "named_struct('x', cast(id AS int), 'x', id + 1)"
        # An Arrow name-based cast could incorrectly read the second 'x' twice.
        right = "named_struct('X', id, 'x', cast(id + 1 AS int))"
        expression = f"{left} {operator} ({right}) AS matched"
        if case_sensitive:
            with pytest.raises(PySparkException, match=r"(?i)(struct|data type)"):
                _ = spark.range(3).selectExpr(expression).schema
        else:
            assert spark.range(3).selectExpr(expression).collect() == [Row(matched=True)] * 3
        with pytest.raises(PySparkException, match=r"(?i)(struct|data type)"):
            _ = (
                spark.range(3)
                .selectExpr(
                    f"named_struct('a', cast(id AS int), 'b', id + 1) {operator} (named_struct('b', id, 'a', id + 1))"
                )
                .schema
            )
    finally:
        spark.conf.set("spark.sql.caseSensitive", previous)


# Spark BinaryComparison/In compare structural types, ignoring names and nested nullability.
@pytest.mark.parametrize("case_sensitive", [False, True])
@pytest.mark.parametrize(
    ("left_wrapper", "right_wrapper"),
    [("{}", "{}"), ("named_struct('left', {})", "named_struct('right', {})"), ("array({})", "array({})")],
)
def test_struct_comparison_ignores_names_and_nullability(spark, case_sensitive, left_wrapper, right_wrapper):
    previous = spark.conf.get("spark.sql.caseSensitive")
    try:
        spark.conf.set("spark.sql.caseSensitive", str(case_sensitive).lower())
        left = left_wrapper.format("named_struct('a', id, 'b', id + 1)")
        right = right_wrapper.format("named_struct('b', id, 'a', CASE WHEN id != 1 THEN id + 1 END)")
        reordered = right_wrapper.format("named_struct('b', id + 1, 'a', id)")
        result = spark.range(3).selectExpr(
            f"{left} = {right} AS eq", f"{left} IN ({right}) AS matched", f"{left} = {reordered} AS reordered"
        )
        assert result.collect() == [Row(eq=value, matched=value, reordered=False) for value in (True, False, True)]
    finally:
        spark.conf.set("spark.sql.caseSensitive", previous)


@pytest.mark.parametrize("case_sensitive", [False, True])
@pytest.mark.parametrize("conditional", ["CASE WHEN id = 0 THEN {left} ELSE {right} END", "coalesce({left}, {right})"])
def test_struct_comparison_names_do_not_relax_conditional_types(spark, case_sensitive, conditional):
    previous = spark.conf.get("spark.sql.caseSensitive")
    try:
        spark.conf.set("spark.sql.caseSensitive", str(case_sensitive).lower())
        left = "named_struct('a', cast(id AS int))"
        right = "named_struct('b', CASE WHEN id != 1 THEN cast(id AS int) END)"
        result = spark.range(3).selectExpr(f"{left} = {right} AS eq", f"{left} IN ({right}) AS matched")
        assert result.collect() == [Row(eq=value, matched=value) for value in (True, False, True)]
        with pytest.raises(AnalysisException, match=r"(?i)(struct|data type)"):
            _ = spark.range(3).selectExpr(conditional.format(left=left, right=right)).schema
        # Different primitive types require name-sensitive common-type inference too.
        with pytest.raises(AnalysisException, match=r"(?i)(struct|data type)"):
            _ = spark.range(3).selectExpr(f"{left} = named_struct('b', id)").schema
    finally:
        spark.conf.set("spark.sql.caseSensitive", previous)


def test_struct_in_finishes_numeric_widening_before_comparison(spark):
    source = spark.range(3).selectExpr(
        "named_struct('x', cast(id + 16777216 AS float), 'x', cast(id + 1 AS int)) AS l",
        "named_struct('x', id + 16777216, 'x', id + 1) AS r",
    )
    assert source.selectExpr("l IN (r) AS matched").collect() == [Row(matched=value) for value in (True, False, True)]


@pytest.mark.parametrize("ansi_mode", [False, True])
def test_struct_in_normalizes_after_recursive_timestamp_rewrite(spark, ansi_mode):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    try:
        spark.conf.set("spark.sql.ansi.enabled", str(ansi_mode).lower())
        left = "array(named_struct('x', TIMESTAMP_NTZ'2020-01-01 00:00:00', 'x', cast(id AS int)))"
        right = "array(named_struct('x', '2020-01-01 00:00:00', 'x', id))"
        different = "array(named_struct('x', '2020-01-01 00:00:00', 'x', id + 1))"
        result = spark.range(3).selectExpr(
            f"{left} IN (NULL, {right}, {different}) AS matched", f"{left} IN (NULL, {different}) AS missing"
        )
        assert result.collect() == [Row(matched=True, missing=None)] * 3
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


@pytest.mark.parametrize("conditional", ["case", "nvl", "coalesce"])
@pytest.mark.parametrize("container", ["struct", "nested", "array", "map"])
def test_nested_conditional_common_type_preserves_nulls(spark, conditional, container):
    nullable_struct = T.StructType([T.StructField("a", T.IntegerType(), True)])
    wrappers = {
        "struct": ("{}", "STRUCT<a:INT>", nullable_struct, lambda value: Row(a=value)),
        "nested": (
            "named_struct('s', {})",
            "STRUCT<s:STRUCT<a:INT>>",
            T.StructType([T.StructField("s", nullable_struct, True)]),
            lambda value: Row(s=Row(a=value)),
        ),
        "array": (
            "array({})",
            "ARRAY<STRUCT<a:INT>>",
            T.ArrayType(nullable_struct, True),
            lambda value: [Row(a=value)],
        ),
        "map": (
            "map('k', {})",
            "MAP<STRING,STRUCT<a:INT>>",
            T.MapType(T.StringType(), nullable_struct, True),
            lambda value: {"k": Row(a=value)},
        ),
    }
    wrapper, sql_type, data_type, wrap_value = wrappers[container]
    value = wrapper.format("named_struct('a', cast(id AS int))")
    null_child = wrapper.format("named_struct('a', cast(NULL AS int))")
    branch = f"CASE WHEN id = 0 THEN {value} WHEN id = 1 THEN CAST(NULL AS {sql_type}) ELSE {null_child} END"
    expression = branch if conditional == "case" else f"{conditional}({branch}, {value})"
    result = spark.range(3).selectExpr("id", f"{expression} AS value").orderBy("id")
    assert result.schema["value"] == T.StructField("value", data_type, conditional == "case")
    assert [row.value for row in result.collect()] == [
        wrap_value(0),
        None if conditional == "case" else wrap_value(1),
        wrap_value(None),
    ]


@pytest.mark.parametrize("nullable_first", [False, True])
def test_case_merges_only_nullable_children(spark, nullable_first):
    nonnull = "named_struct('a', id, 'b', id + 1)"
    nullable = "named_struct('a', cast(NULL AS bigint), 'b', id + 1)"
    first, second = (nullable, nonnull) if nullable_first else (nonnull, nullable)
    result = spark.range(2).selectExpr(
        f"CASE WHEN id = 0 THEN {first} ELSE {second} END AS mixed",
        f"CASE WHEN id = 0 THEN {nonnull} ELSE {nonnull} END AS nonnull",
    )
    assert result.schema["mixed"] == T.StructField(
        "mixed", T.StructType([T.StructField("a", T.LongType(), True), T.StructField("b", T.LongType(), False)]), False
    )
    assert result.schema["nonnull"] == T.StructField(
        "nonnull",
        T.StructType([T.StructField("a", T.LongType(), False), T.StructField("b", T.LongType(), False)]),
        False,
    )
    assert result.collect() == [
        Row(mixed=Row(a=None if nullable_first else 0, b=1), nonnull=Row(a=0, b=1)),
        Row(mixed=Row(a=1 if nullable_first else None, b=2), nonnull=Row(a=1, b=2)),
    ]


def test_case_preserves_branch_order_else_and_short_circuit(spark):
    result = spark.range(3).selectExpr(
        "CASE WHEN id = 0 THEN named_struct('a', id) "
        "WHEN id < 2 THEN named_struct('a', cast(12 / id AS bigint)) "
        "ELSE named_struct('a', cast(12 / (id - 1) AS bigint)) END AS guarded",
        "CASE WHEN cast(NULL AS boolean) THEN named_struct('a', id + 10) "
        "WHEN id < 2 THEN named_struct('a', id) "
        "WHEN id = 0 THEN named_struct('a', id + 20) END AS no_else",
    )
    assert result.schema["guarded"] == T.StructField(
        "guarded", T.StructType([T.StructField("a", T.LongType(), True)]), False
    )
    assert result.schema["no_else"] == T.StructField(
        "no_else", T.StructType([T.StructField("a", T.LongType(), False)]), True
    )
    assert result.collect() == [
        Row(guarded=Row(a=0), no_else=Row(a=0)),
        Row(guarded=Row(a=12), no_else=Row(a=1)),
        Row(guarded=Row(a=12), no_else=None),
    ]


def _assert_nested_row_names(actual, expected):
    assert actual == expected
    if isinstance(expected, Row):
        assert list(actual.__fields__) == list(expected.__fields__)
        for value, wanted in zip(actual, expected, strict=True):
            _assert_nested_row_names(value, wanted)
    elif isinstance(expected, list):
        for value, wanted in zip(actual, expected, strict=True):
            _assert_nested_row_names(value, wanted)
    elif isinstance(expected, dict):
        for (key, value), (wanted_key, wanted) in zip(actual.items(), expected.items(), strict=True):
            _assert_nested_row_names(key, wanted_key)
            _assert_nested_row_names(value, wanted)


# Spark 4.2 ArrowUtils and ArrowTableToRowsConversion use separate wire and logical names.
@pytest.mark.parametrize("container", ["struct", "nested", "array", "map-value", "map-key"])
def test_duplicate_struct_result_containers(spark, container):
    value = "named_struct('x', id, 'x', CASE WHEN id != 2 THEN concat('v', id) END)"
    nullable = f"CASE WHEN id != 1 THEN {value} END"
    struct_type = T.StructType([T.StructField("x", T.LongType(), False), T.StructField("x", T.StringType(), True)])
    wrappers = {
        "struct": (nullable, struct_type, lambda v: v),
        "nested": (
            f"named_struct('outer', {nullable})",
            T.StructType([T.StructField("outer", struct_type, True)]),
            lambda v: Row(outer=v),
        ),
        "array": (f"array({nullable})", T.ArrayType(struct_type, True), lambda v: [v]),
        "map-value": (f"map('k', {nullable})", T.MapType(T.StringType(), struct_type, True), lambda v: {"k": v}),
        "map-key": (f"map({value}, id + 10)", T.MapType(struct_type, T.LongType(), False), None),
    }
    expression, data_type, wrap = wrappers[container]
    frame = (
        spark.range(4, numPartitions=2).selectExpr("id", f"CASE WHEN id != 3 THEN {expression} END AS s").orderBy("id")
    )
    null_field_id = 2
    null_container_id = 3
    expected = []
    for i in range(4):
        value = Row("x", "x")(i, f"v{i}" if i != null_field_id else None)
        result = {value: i + 10} if container == "map-key" else wrap(value if i != 1 else None)
        expected.append(Row(id=i, s=result if i != null_container_id else None))
    schema = T.StructType([T.StructField("id", T.LongType(), False), T.StructField("s", data_type, True)])
    assert frame.schema == schema
    _assert_nested_row_names(frame.collect(), expected)
    _assert_nested_row_names(list(frame.toLocalIterator()), expected)
    assert frame.schema == schema


@pytest.mark.parametrize("empty", [False, True])
def test_duplicate_struct_logical_and_wire_schemas(spark, empty):
    source_schema = T.StructType(
        [T.StructField("n", T.IntegerType(), False, {"tag": "kept"}), T.StructField("s", T.StringType(), True)]
    )
    source = spark.createDataFrame([(1, "a"), (2, None)], source_schema)
    if empty:
        source = source.where("n < 0")
    frame = source.selectExpr("named_struct('', n, 'x', n + 10, 'x', s) AS same", "n AS same")
    fields = [
        T.StructField("", T.IntegerType(), False, {"tag": "kept"}),
        T.StructField("x", T.IntegerType(), False),
        T.StructField("x", T.StringType(), True),
    ]
    expected_schema = T.StructType(
        [
            T.StructField("same", T.StructType(fields), False),
            T.StructField("same", T.IntegerType(), False, {"tag": "kept"}),
        ]
    )
    expected = (
        [] if empty else [Row("same", "same")(Row("", "x", "x")(n, n + 10, s), n) for n, s in [(1, "a"), (2, None)]]
    )
    assert frame.schema == expected_schema
    _assert_nested_row_names(frame.collect(), expected)
    # The public toArrow API deliberately rejects this logical schema; inspect raw Connect IPC instead.
    table, logical_schema = frame._to_table()  # noqa: SLF001
    assert logical_schema == expected_schema
    assert table.column_names == ["same", "same"]
    assert [field.name for field in table.schema.field(0).type] == ["", "x_0", "x_1"]
    assert table.num_rows == len(expected)
    for index, row in enumerate(expected):
        assert [table.column(0)[index][i].as_py() for i in range(3)] == list(row[0])
        assert table.column(1)[index].as_py() == row[1]


# Semantic port of Spark 4.2 DataFrameTests.test_duplicate_field_names with dynamic values.
def test_duplicate_struct_mixed_types_and_repeated_groups(spark):
    frame = spark.range(2).selectExpr(
        "named_struct('x', concat('a', id), 'x', id) AS same",
        "named_struct('a', id + 1, 'x', id + 2, 'x', concat('b', id), 'y', id + 3, "
        "'y', concat('c', id), 'x', concat('d', id)) AS same",
    )
    expected = [
        Row("same", "same")(
            Row("x", "x")(f"a{i}", i), Row("a", "x", "x", "y", "y", "x")(i + 1, i + 2, f"b{i}", i + 3, f"c{i}", f"d{i}")
        )
        for i in range(2)
    ]
    _assert_nested_row_names(frame.collect(), expected)
    table, schema = frame._to_table()  # noqa: SLF001
    assert schema == frame.schema
    assert table.column_names == ["same", "same"]
    assert [f.name for f in table.schema.field(1).type] == ["a", "x_0", "x_1", "y_0", "y_1", "x_2"]


@pytest.mark.parametrize("mode", ["row", "dict"])
def test_duplicate_struct_pandas_modes_preserve_values(spark, mode):
    previous = spark.conf.get("spark.sql.execution.pandas.structHandlingMode")
    try:
        spark.conf.set("spark.sql.execution.pandas.structHandlingMode", mode)
        frame = spark.range(3).selectExpr(
            "CASE WHEN id != 2 THEN named_struct('', id, 'x', id + 10, 'x', CASE WHEN id != 1 THEN id + 20 END) END AS s"
        )
        result = frame.toPandas()["s"].tolist()
        if mode == "row":
            _assert_nested_row_names(result, [Row("", "x", "x")(0, 10, 20), Row("", "x", "x")(1, 11, None), None])
        else:
            assert result == [{"": 0, "x_0": 10, "x_1": 20}, {"": 1, "x_0": 11, "x_1": None}, None]
    finally:
        spark.conf.set("spark.sql.execution.pandas.structHandlingMode", previous)


@pytest.mark.skipif(pyspark_version() < (4,), reason="DataFrame.toArrow requires PySpark 4.0+")
def test_duplicate_struct_strict_arrow_and_legacy_pandas_rejection(spark):
    frame = spark.range(2).selectExpr("named_struct('x', id, 'x', id + 1) AS s")
    with pytest.raises(PySparkException, match="DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT"):
        frame.toArrow()
    previous = spark.conf.get("spark.sql.execution.pandas.structHandlingMode")
    try:
        spark.conf.set("spark.sql.execution.pandas.structHandlingMode", "legacy")
        with pytest.raises(PySparkException, match="DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT"):
            frame.toPandas()
    finally:
        spark.conf.set("spark.sql.execution.pandas.structHandlingMode", previous)
    # Top-level duplicates do not need dictionary conversion and must not be renamed.
    top = spark.range(2).selectExpr("id AS x", "id + 1 AS x")
    _assert_nested_row_names(top.collect(), [Row("x", "x")(0, 1), Row("x", "x")(1, 2)])
    table = top.toArrow()
    assert table.column_names == ["x", "x"]
    assert [column.to_pylist() for column in table.columns] == [[0, 1], [1, 2]]


@pytest.mark.parametrize("names", [("x", "x", "x_0"), ("", "", "_0")])
@pytest.mark.parametrize("empty", [False, True])
def test_duplicate_struct_suffix_collision_rejected_safely(spark, names, empty):
    expression = f"named_struct('{names[0]}', id, '{names[1]}', id + 10, '{names[2]}', id + 20)"
    frame = spark.range(0 if empty else 2).selectExpr(f"array({expression}) AS s")
    assert frame.schema["s"].dataType.elementType.names == list(names)
    # Spark Connect's generated names collide too; Sail rejects instead of emitting corrupt IPC.
    with pytest.raises(PySparkException, match="collide after Spark Arrow transport naming"):
        frame.collect()


@pytest.mark.parametrize(
    ("names", "wire_names"),
    [
        (("x", "x_0"), ("x", "x_0")),
        (("x", "X"), ("x", "X")),
        (("", "_0"), ("", "_0")),
        (("x", "", "x", ""), ("x_0", "_0", "x_1", "_1")),
    ],
)
@pytest.mark.parametrize("empty", [False, True])
def test_struct_transport_names_without_suffix_collisions(spark, names, wire_names, empty):
    arguments = ", ".join(f"'{name}', id + {10 * i}" for i, name in enumerate(names))
    frame = spark.range(0 if empty else 2).selectExpr(f"named_struct({arguments}) AS s")
    assert frame.schema["s"].dataType.names == list(names)
    expected = [] if empty else [Row(s=Row(*names)(*(i + 10 * j for j in range(len(names))))) for i in range(2)]
    _assert_nested_row_names(frame.collect(), expected)
    table, schema = frame._to_table()  # noqa: SLF001
    assert schema == frame.schema
    assert [field.name for field in table.schema.field(0).type] == list(wire_names)
    assert table.num_rows == len(expected)
    for i, row in enumerate(expected):
        assert [table.column(0)[i][j].as_py() for j in range(len(names))] == list(row.s)
