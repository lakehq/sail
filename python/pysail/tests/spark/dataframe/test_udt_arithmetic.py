import pytest
from pyspark.errors import AnalysisException
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    MapType,
    StringType,
    StructType,
    UserDefinedType,
)

from pysail.testing.spark.utils.common import is_jvm_spark
from pysail.tests.spark.dataframe.udt import (
    Box,
    BoxPythonUDT,
    DoubleStoragePythonUDT,
    IntegerStoragePythonUDT,
    UnnamedPythonUDT,
)

OPERATORS = ["+", "-", "*", "/", "%"]

# TODO: Sail keeps UDT identity in field metadata, and an expression that returns a UDT builds its
#   result field without it, so a column projected from `coalesce(udt, udt)`, an array function, an
#   aggregate or a generator is its storage type in the schema and in a subquery. Putting the marker on
#   the projected alias breaks execution (DataFusion's physical schema check), so the fix belongs where
#   those result fields are built.
UDT_PROJECTION_XFAIL = pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="a UDT-returning expression projected as a column is its storage type",
)


@pytest.fixture
def udt_view(spark):
    schema = StructType().add("a", UnnamedPythonUDT())
    spark.createDataFrame(data=[], schema=schema).createOrReplaceTempView("udt_arithmetic")
    return "udt_arithmetic"


@pytest.mark.parametrize("op", OPERATORS)
def test_udt_operand_is_rejected(spark, udt_view, op):
    # A UDT is none of the input types the arithmetic operators accept, whatever
    # it is stored as -- this one is stored as STRING, which on its own would let
    # `/` through.
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        spark.sql(f"SELECT a {op} 1 FROM {udt_view}").collect()  # noqa: S608


def test_udt_operand_is_named_udt_not_its_storage_type(spark, udt_view):
    # Both engines spell it exactly this way.
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql(f"SELECT a + 1 FROM {udt_view}").collect()  # noqa: S608
    assert 'UDT("STRING")' in str(excinfo.value)


def test_nested_udt_is_named_by_its_storage_type(spark):
    # Nested, it goes the other way round: Spark names the storage type, so the
    # struct reads STRUCT<u: STRING, ...>. Pinned because the tempting "fix" --
    # naming nested fields the way top-level ones are named -- would diverge.
    schema = StructType().add("s", StructType().add("u", UnnamedPythonUDT()).add("n", "integer"))
    spark.createDataFrame(data=[], schema=schema).createOrReplaceTempView("nested_udt_arithmetic")
    with pytest.raises(AnalysisException) as excinfo:
        spark.sql("SELECT s + 1 FROM nested_udt_arithmetic").collect()
    assert "STRUCT<u: STRING, n: INT>" in str(excinfo.value)


@pytest.mark.parametrize(
    "operand",
    [
        pytest.param("a", id="column"),
        pytest.param("s.u", id="struct-field"),
        pytest.param("coalesce(a, a)", id="coalesce"),
        pytest.param("nvl(a, a)", id="nvl"),
        pytest.param("nvl2(a, a, a)", id="nvl2"),
        pytest.param("if(true, a, a)", id="if"),
        pytest.param("CASE WHEN true THEN a ELSE a END", id="case"),
        pytest.param("nullif(a, a)", id="nullif"),
        pytest.param("arr[0]", id="array-index"),
        pytest.param("element_at(arr, 1)", id="element_at"),
        pytest.param("m['k']", id="map-value"),
        pytest.param("array(a)[0]", id="array-constructor"),
        pytest.param("greatest(a, a)", id="greatest"),
        pytest.param("least(a, a)", id="least"),
    ],
)
def test_udt_reached_through_an_expression_is_rejected(spark, operand):
    # Every one of these is still a UDT in Spark, so `/ 1` fails analysis. Only the column and
    # the struct field carry the UDT metadata on their own field; the rest build their result
    # field without it, so the guard has to look through them. `/` is the operator where a
    # STRING-backed UDT would otherwise resolve, since it casts both sides to DOUBLE.
    udt = UnnamedPythonUDT()
    schema = (
        StructType()
        .add("a", udt)
        .add("s", StructType().add("u", udt))
        .add("arr", ArrayType(udt))
        .add("m", MapType(StringType(), udt))
    )
    spark.createDataFrame(data=[], schema=schema).createOrReplaceTempView("udt_expression_operand")
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        spark.sql(f"SELECT {operand} / 1 FROM udt_expression_operand").collect()  # noqa: S608


@pytest.mark.parametrize(
    "query",
    [
        pytest.param("SELECT first(a) / 1 FROM udt_relation_operand", id="first"),
        pytest.param("SELECT last(a) / 1 FROM udt_relation_operand", id="last"),
        pytest.param("SELECT any_value(a) / 1 FROM udt_relation_operand", id="any_value"),
        pytest.param("SELECT lag(a) OVER (ORDER BY k) / 1 FROM udt_relation_operand", id="lag"),
        pytest.param("SELECT lead(a) OVER (ORDER BY k) / 1 FROM udt_relation_operand", id="lead"),
        pytest.param("SELECT first_value(a) OVER (ORDER BY k) / 1 FROM udt_relation_operand", id="first_value"),
        pytest.param(
            "SELECT e / 1 FROM (SELECT explode(arr) AS e FROM udt_relation_operand)",
            id="explode",
            marks=UDT_PROJECTION_XFAIL,
        ),
        pytest.param("SELECT min(a) / 1 FROM udt_relation_operand", id="min"),
        pytest.param(
            "SELECT u / 1 FROM "
            "(SELECT a AS u FROM udt_relation_operand UNION ALL SELECT a AS u FROM udt_relation_operand)",
            id="union",
        ),
        pytest.param("SELECT max_by(a, k) / 1 FROM udt_relation_operand", id="max_by"),
    ],
)
def test_udt_reached_through_an_aggregate_window_or_relation_is_rejected(spark, query):
    # A UDT that comes out of an aggregate, a window function, a generator or a set operation is
    # still a UDT in Spark, so `/ 1` fails analysis.
    udt = UnnamedPythonUDT()
    schema = StructType().add("k", "integer").add("a", udt).add("arr", ArrayType(udt))
    spark.createDataFrame(data=[], schema=schema).createOrReplaceTempView("udt_relation_operand")
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        spark.sql(query).collect()


@pytest.mark.parametrize("ansi_enabled", ["false", "true"])
@pytest.mark.parametrize(
    "expression",
    [
        pytest.param("CAST(a AS STRING) / 1", id="cast"),
        pytest.param("TRY_CAST(a AS STRING) / 1", id="try_cast"),
        pytest.param("coalesce(CAST(a AS STRING), '0') / 1", id="coalesce-of-cast"),
        pytest.param("CAST(CAST(a AS STRING) AS INT) + 1", id="cast-to-int"),
    ],
)
def test_udt_cast_to_a_plain_type_is_a_plain_operand(spark, udt_view, ansi_enabled, expression):
    # A cast yields its target type, so `CAST(udt AS STRING)` is a plain STRING in Spark and the
    # arithmetic resolves. DataFusion copies the source field's metadata onto the cast, so the
    # guard must not read the UDT marker through it.
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi_enabled)
    try:
        assert spark.sql(f"SELECT {expression} AS r FROM {udt_view}").collect() == []  # noqa: S608
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


@pytest.mark.parametrize("ansi_enabled", ["false", "true"])
@pytest.mark.parametrize(
    "query",
    [
        pytest.param("SELECT x / 1 AS r FROM (SELECT CAST(a AS STRING) AS x FROM {v})", id="cast-divide"),
        pytest.param("SELECT x / 1 AS r FROM (SELECT TRY_CAST(a AS STRING) AS x FROM {v})", id="try_cast-divide"),
        pytest.param(
            "SELECT x + INTERVAL '1' DAY AS r FROM (SELECT CAST(a AS STRING) AS x FROM {v})", id="cast-plus-interval"
        ),
        pytest.param("SELECT -x AS r FROM (SELECT CAST(a AS STRING) AS x FROM {v})", id="cast-unary-minus"),
        # `string(x)` is the same `Cast`, reached through the function registry instead of the parser.
        pytest.param("SELECT x / 1 AS r FROM (SELECT string(a) AS x FROM {v})", id="string-function-divide"),
        pytest.param("SELECT -x AS r FROM (SELECT string(a) AS x FROM {v})", id="string-function-unary-minus"),
    ],
)
def test_udt_cast_projected_by_a_subquery_is_a_plain_operand(spark, udt_view, query, ansi_enabled):
    # A cast yields its target type, so the column a subquery projects from `CAST(udt AS STRING)` is a
    # plain STRING and the arithmetic over it resolves. DataFusion copies the source field's metadata
    # through a cast, so the projected column used to read as a UDT and Sail refused these queries.
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi_enabled)
    try:
        assert spark.sql(query.format(v=udt_view)).collect() == []
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


@pytest.mark.parametrize("cast", ["CAST", "TRY_CAST"])
def test_udt_cast_to_string_is_a_string_in_the_schema(spark, udt_view, cast):
    assert spark.sql(f"SELECT {cast}(a AS STRING) AS x FROM {udt_view}").schema["x"].dataType == StringType()  # noqa: S608


@UDT_PROJECTION_XFAIL
def test_udt_expression_projected_by_a_subquery_is_rejected(spark, udt_view):
    # The column a subquery projects from `coalesce(udt, udt)` is still the UDT in Spark, so `/ 1`
    # fails analysis over it just as it does over the expression in place.
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        spark.sql(f"SELECT x / 1 AS r FROM (SELECT coalesce(a, a) AS x FROM {udt_view})").collect()  # noqa: S608


@pytest.fixture
def udt_wide_view(spark):
    u = UnnamedPythonUDT()
    schema = StructType().add("a", u).add("arr", ArrayType(u)).add("m", MapType(StringType(), u))
    spark.createDataFrame(data=[], schema=schema).createOrReplaceTempView("udt_unary_arithmetic")
    return "udt_unary_arithmetic"


@pytest.mark.parametrize("ansi_enabled", ["false", "true"])
@pytest.mark.parametrize(
    "expression",
    [
        pytest.param("-CAST(a AS STRING)", id="minus"),
        pytest.param("+CAST(a AS STRING)", id="plus"),
        pytest.param("-CAST(a AS STRING) + 1", id="minus-then-add"),
    ],
)
def test_unary_operator_over_a_udt_cast_to_a_plain_type_resolves(spark, udt_wide_view, expression, ansi_enabled):
    # A cast yields its target type, so the unary operator sees a STRING, which string promotion
    # makes a DOUBLE -- the same rule the binary operators already follow for a UDT cast.
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi_enabled)
    try:
        assert spark.sql(f"SELECT {expression} AS r FROM {udt_wide_view}").collect() == []  # noqa: S608
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


@pytest.mark.parametrize("ansi_enabled", ["false", "true"])
@pytest.mark.parametrize(
    "expression",
    [
        pytest.param("-coalesce(a, a)", id="minus-coalesce"),
        pytest.param("+coalesce(a, a)", id="plus-coalesce"),
        pytest.param("-if(true, a, a)", id="minus-if"),
        pytest.param("-arr[0]", id="minus-array-index"),
        pytest.param("-m['k']", id="minus-map-value"),
    ],
)
def test_unary_operator_over_a_udt_reached_through_an_expression_is_rejected(
    spark, udt_wide_view, expression, ansi_enabled
):
    # `UnaryMinus`/`UnaryPositive` take `NumericAndInterval` (`arithmetic.scala:54,124`); an
    # expression that returns its UDT input is still a UDT, as the binary guard already knows.
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi_enabled)
    try:
        with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
            spark.sql(f"SELECT {expression} AS r FROM {udt_wide_view}").collect()  # noqa: S608
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


@pytest.mark.parametrize("ansi_enabled", ["false", "true"])
def test_an_aliased_udt_cast_is_a_plain_operand(spark, udt_wide_view, ansi_enabled):
    # `Alias.dataType = child.dataType` (`namedExpressions.scala:170`): the alias does not bring
    # the UDT back.
    from pyspark.sql import functions as F  # noqa: N812

    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi_enabled)
    try:
        df = spark.table(udt_wide_view).select((F.col("a").cast("string").alias("x") / 1).alias("r"))
        assert df.collect() == []
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


STORAGES = [
    pytest.param((UnnamedPythonUDT, "STRING"), id="string-storage"),
    pytest.param((IntegerStoragePythonUDT, "INT"), id="int-storage"),
    pytest.param((DoubleStoragePythonUDT, "DOUBLE"), id="double-storage"),
]


@pytest.fixture
def storage_view(spark, request):
    udt, storage = request.param
    u = udt()
    schema = (
        StructType()
        .add("k", "integer")
        .add("a", u)
        .add("arr", ArrayType(u))
        .add("m", MapType(StringType(), u))
        .add("s", StructType().add("u", u))
    )
    spark.createDataFrame(data=[], schema=schema).createOrReplaceTempView("udt_storage_operand")
    return "udt_storage_operand", storage


@pytest.fixture
def ansi(spark, request):
    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", request.param)
    yield request.param
    spark.conf.set("spark.sql.ansi.enabled", previous)


ANSI = pytest.mark.parametrize("ansi", ["false", "true"], indirect=True)
STORAGE = pytest.mark.parametrize("storage_view", STORAGES, indirect=True)


@ANSI
@pytest.mark.usefixtures("ansi")
@STORAGE
@pytest.mark.parametrize(
    "template",
    [
        pytest.param("SELECT {x} + 1 AS r FROM {v}", id="plus"),
        pytest.param("SELECT 1 - {x} AS r FROM {v}", id="minus"),
        pytest.param("SELECT {x} * 2 AS r FROM {v}", id="times"),
        pytest.param("SELECT 2 / {x} AS r FROM {v}", id="divide"),
        pytest.param("SELECT {x} % 2 AS r FROM {v}", id="remainder"),
        pytest.param("SELECT -{x} AS r FROM {v}", id="unary-minus"),
        pytest.param("SELECT +{x} AS r FROM {v}", id="unary-plus"),
    ],
)
@pytest.mark.parametrize(
    "operand",
    [
        pytest.param("a", id="column"),
        pytest.param("s.u", id="struct-field"),
        pytest.param("coalesce(a, a)", id="coalesce"),
        pytest.param("if(k > 0, a, a)", id="if"),
        pytest.param("arr[0]", id="array-index"),
        pytest.param("m['k']", id="map-value"),
    ],
)
def test_a_udt_of_any_storage_is_rejected_by_every_operator(spark, storage_view, template, operand):
    # A UDT is none of the arithmetic input types whatever it is stored as, and an INT or DOUBLE
    # storage is the one a storage-type check would let through for every operator.
    view, _ = storage_view
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        spark.sql(template.format(x=operand, v=view)).collect()


@ANSI
@pytest.mark.usefixtures("ansi")
@STORAGE
@pytest.mark.parametrize(
    "template",
    [
        pytest.param("SELECT x + 1 AS r FROM (SELECT {cast}(a AS STRING) AS x FROM {v})", id="plus"),
        pytest.param("SELECT 2 / x AS r FROM (SELECT {cast}(a AS STRING) AS x FROM {v})", id="divide"),
        pytest.param("SELECT -x AS r FROM (SELECT {cast}(a AS STRING) AS x FROM {v})", id="unary-minus"),
        pytest.param(
            "SELECT x - INTERVAL '1' DAY AS r FROM (SELECT {cast}(a AS STRING) AS x FROM {v})", id="minus-interval"
        ),
        pytest.param("SELECT {cast}(a AS STRING) * 2 AS r FROM {v}", id="direct-times"),
    ],
)
@pytest.mark.parametrize("cast", ["CAST", "TRY_CAST"])
def test_a_udt_of_any_storage_cast_to_string_is_a_plain_operand(spark, storage_view, template, cast):
    # A cast yields its target type, so the result is a plain STRING whether it is used in place or
    # projected by a subquery first.
    view, _ = storage_view
    assert spark.sql(template.format(cast=cast, v=view)).collect() == []


@STORAGE
@pytest.mark.parametrize("cast", ["CAST", "TRY_CAST"])
def test_a_udt_of_any_storage_cast_to_string_is_a_string_column(spark, storage_view, cast):
    view, _ = storage_view
    assert spark.sql(f"SELECT {cast}(a AS STRING) AS x FROM {view}").schema["x"].dataType == StringType()  # noqa: S608


@pytest.mark.parametrize(
    "operand",
    [
        pytest.param("coalesce(a, a)", id="coalesce"),
        pytest.param("nvl(a, a)", id="nvl"),
        pytest.param("if(k > 0, a, a)", id="if"),
        pytest.param("CASE WHEN k > 0 THEN a ELSE a END", id="case"),
        pytest.param("nullif(a, a)", id="nullif"),
        pytest.param("arr[0]", id="array-index"),
        pytest.param("m['k']", id="map-value"),
        pytest.param("element_at(arr, 1)", id="element_at"),
        pytest.param("array(a)[0]", id="array-constructor"),
        pytest.param("greatest(a, a)", id="greatest"),
    ],
)
@STORAGE
@UDT_PROJECTION_XFAIL
def test_a_udt_expression_projected_by_a_subquery_is_rejected(spark, storage_view, operand):
    # A UDT keeps its identity through every expression that returns one of its inputs, so the
    # column a subquery projects from one is still the UDT, whatever it is stored as.
    view, _ = storage_view
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        spark.sql(f"SELECT x / 1 AS r FROM (SELECT {operand} AS x FROM {view})").collect()  # noqa: S608


@STORAGE
@pytest.mark.parametrize(
    "query",
    [
        pytest.param("SELECT min(a) AS x FROM {v}", id="min", marks=UDT_PROJECTION_XFAIL),
        pytest.param("SELECT max_by(a, k) AS x FROM {v}", id="max_by", marks=UDT_PROJECTION_XFAIL),
        pytest.param("SELECT explode(arr) AS x FROM {v}", id="explode", marks=UDT_PROJECTION_XFAIL),
        pytest.param("SELECT a AS x FROM {v} UNION ALL SELECT a AS x FROM {v}", id="union"),
    ],
)
def test_a_udt_from_an_aggregate_generator_or_set_operation_projected_by_a_subquery_is_rejected(
    spark, storage_view, query
):
    view, _ = storage_view
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        spark.sql(f"SELECT x / 1 AS r FROM ({query.format(v=view)})").collect()  # noqa: S608


@STORAGE
@pytest.mark.parametrize(
    "expression",
    [
        pytest.param("coalesce(a, a)", id="coalesce"),
        pytest.param("if(k > 0, a, a)", id="if"),
        pytest.param("arr[0]", id="array-index"),
        pytest.param("min(a)", id="min"),
        pytest.param("explode(arr)", id="explode"),
        pytest.param("array_min(arr)", id="array_min"),
        pytest.param("array_max(arr)", id="array_max"),
        pytest.param("collect_list(a)[0]", id="collect_list-item"),
        pytest.param("min(a) OVER (PARTITION BY k)", id="min-over-window"),
        pytest.param("mode(a)", id="mode"),
        pytest.param("named_struct('x', a).x", id="named_struct-field"),
        pytest.param("transform(arr, x -> x)[0]", id="transform-item"),
        pytest.param("filter(arr, x -> true)[0]", id="filter-item"),
    ],
)
@UDT_PROJECTION_XFAIL
def test_a_udt_returning_expression_is_the_udt_in_the_schema(spark, storage_view, expression):
    # Spark types these as the UDT itself, not as its storage type.
    view, _ = storage_view
    field = spark.sql(f"SELECT {expression} AS x FROM {view}").schema["x"]  # noqa: S608
    assert isinstance(field.dataType, UserDefinedType)


@STORAGE
@pytest.mark.parametrize(
    "expression",
    [
        pytest.param("array_min(arr)", id="array_min"),
        pytest.param("array_max(arr)", id="array_max"),
        pytest.param("collect_list(a)[0]", id="collect_list-item"),
        pytest.param("min(a) OVER (PARTITION BY k)", id="min-over-window"),
        pytest.param("mode(a)", id="mode"),
        pytest.param("named_struct('x', a).x", id="named_struct-field"),
        pytest.param("transform(arr, x -> x)[0]", id="transform-item"),
        pytest.param("filter(arr, x -> true)[0]", id="filter-item"),
    ],
)
@pytest.mark.parametrize(
    "template",
    [
        pytest.param("SELECT {e} / 1 AS r FROM {v}", id="in-place"),
        pytest.param(
            "SELECT x / 1 AS r FROM (SELECT {e} AS x FROM {v})", id="through-a-subquery", marks=UDT_PROJECTION_XFAIL
        ),
        pytest.param(
            "SELECT -x AS r FROM (SELECT {e} AS x FROM {v})", id="unary-through-a-subquery", marks=UDT_PROJECTION_XFAIL
        ),
    ],
)
def test_a_udt_from_an_array_aggregate_window_or_struct_function_is_rejected(spark, storage_view, expression, template):
    # Each of these declares its type as the UDT it reads (`ArrayMin`/`ArrayMax` the element type,
    # `Mode` and the window aggregate `child.dataType`, `GetStructField` the field type), so Spark
    # refuses arithmetic over it in place and through a subquery alike.
    view, _ = storage_view
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        spark.sql(template.format(e=expression, v=view)).collect()


@pytest.mark.parametrize(
    "query",
    [
        pytest.param("SELECT coalesce(a, b) AS x FROM udt_rows ORDER BY k", id="coalesce"),
        pytest.param("SELECT if(k = 1, a, b) AS x FROM udt_rows ORDER BY k", id="if"),
        pytest.param("SELECT arr[0] AS x FROM udt_rows ORDER BY k", id="array-index"),
        pytest.param("SELECT x FROM (SELECT k, coalesce(a, b) AS x FROM udt_rows) ORDER BY k", id="subquery"),
        pytest.param("SELECT min(a) OVER (ORDER BY k) AS x FROM udt_rows ORDER BY k", id="window"),
    ],
)
def test_a_udt_returning_expression_over_rows_is_collected(spark, query):
    # Projecting a UDT-returning expression over real rows must execute: this is the query a UDT marker
    # on the projected alias used to break with `Schema field unallowed change`.
    schema = (
        StructType()
        .add("k", "integer")
        .add("a", BoxPythonUDT())
        .add("b", BoxPythonUDT())
        .add("arr", ArrayType(BoxPythonUDT()))
    )
    rows = [(1, Box("x"), Box("y"), [Box("x")]), (2, None, Box("bb"), [Box("bb")])]
    spark.createDataFrame(rows, schema).createOrReplaceTempView("udt_rows")
    values = [getattr(row.x, "value", row.x) for row in spark.sql(query).collect()]
    assert values in (["x", "bb"], ["x", "x"])


@UDT_PROJECTION_XFAIL
def test_a_udt_returning_expression_over_rows_is_collected_as_the_udt_object(spark):
    schema = StructType().add("k", "integer").add("a", BoxPythonUDT()).add("b", BoxPythonUDT())
    spark.createDataFrame([(1, None, Box("bb"))], schema).createOrReplaceTempView("udt_rows_object")
    assert spark.sql("SELECT coalesce(a, b) AS x FROM udt_rows_object").collect()[0].x == Box("bb")


def test_udt_string_function_is_a_string_in_the_schema(spark, udt_view):
    assert spark.sql(f"SELECT string(a) AS x FROM {udt_view}").schema["x"].dataType == StringType()  # noqa: S608


@pytest.mark.parametrize("ansi_enabled", ["false", "true"])
def test_dataframe_negation_of_a_udt_is_rejected(spark, ansi_enabled):
    # PySpark's `-col` is `negative(col)`, which is `UnaryMinus` (`FunctionRegistry.scala:467`).
    from pyspark.sql import functions as F  # noqa: N812

    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", ansi_enabled)
    try:
        df = spark.createDataFrame(data=[], schema=StructType().add("a", IntegerStoragePythonUDT()))
        with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
            df.select(-F.col("a")).collect()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)


@pytest.fixture
def udt_scalar_view(spark):
    # A UDT stored as a DOUBLE is a numeric by its Arrow type, so only its identity keeps it out of
    # arithmetic -- a STRUCT-backed UDT would be refused by its storage alone and prove nothing.
    # `d` is the plain-DOUBLE twin of `a`: every shape refused over `a` must resolve over it.
    schema = StructType().add("a", DoubleStoragePythonUDT()).add("d", DoubleType())
    spark.createDataFrame(data=[], schema=schema).createOrReplaceTempView("udt_scalar")
    return "udt_scalar"


# TODO: the arithmetic guards recognise a UDT operand by a hand-written list of expression shapes
#   that return their input (`operand_udt_field` in `function/scalar/math.rs`). A value that crosses
#   a projection boundary -- a scalar subquery, a generator -- reaches the guard as a column whose
#   field no longer carries `SAIL::spark::udt`, so a DOUBLE storage is a numeric and Sail computes
#   where Spark refuses (`Expression.scala:840`, `DATATYPE_MISMATCH`). Closing this needs the UDT
#   identity to travel in the output field of the subquery and the generator, not another shape.
@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="a UDT a scalar subquery returns is its storage type",
)
@pytest.mark.parametrize(
    "form",
    [
        pytest.param("(SELECT max(a) FROM udt_scalar) + 1", id="scalar_subquery"),
    ],
)
def test_udt_through_an_unrecognised_shape_is_refused(spark, udt_scalar_view, form):
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        _ = spark.sql(f"SELECT {form} FROM {udt_scalar_view}").schema  # noqa: S608


@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="a UDT a generator produces is its storage type",
)
@pytest.mark.parametrize("generator", ["explode", "posexplode"])
def test_udt_from_a_generator_is_refused(spark, udt_scalar_view, generator):
    column = "e" if generator == "explode" else "col"
    alias = " AS e" if generator == "explode" else ""
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        _ = spark.sql(
            f"SELECT {column} + 1 FROM (SELECT {generator}(array(a)){alias} FROM {udt_scalar_view})"  # noqa: S608
        ).schema


# The shapes the guard does recognise: kept green so a change to the list cannot drop one silently.
@pytest.mark.parametrize(
    "form",
    [
        pytest.param("transform(array(a), x -> x)[0] + 1", id="transform"),
        pytest.param("element_at(array(a), 1) + 1", id="element_at"),
        pytest.param("greatest(a, a) + 1", id="greatest"),
        pytest.param("nullif(a, a) + 1", id="nullif"),
        pytest.param("lag(a) OVER (ORDER BY a) + 1", id="lag"),
        pytest.param("collect_list(a)[0] + 1", id="collect_list"),
        pytest.param("abs(a) + 1", id="abs"),
        pytest.param("slice(array(a), 1, 1)[0] + 1", id="slice"),
        pytest.param("-slice(array(a), 1, 1)[0]", id="slice-unary-minus"),
        pytest.param("reverse(array(a))[0] * 2", id="reverse"),
        pytest.param("element_at(reverse(array(a)), 1) + 1", id="reverse-element_at"),
        pytest.param("sort_array(array(a))[0] + 1", id="sort_array"),
        pytest.param("array_sort(array(a))[0] + 1", id="array_sort"),
        pytest.param("flatten(array(array(a)))[0] + 1", id="flatten"),
        pytest.param("concat(array(a), array(a))[0] + 1", id="concat"),
        pytest.param("array_distinct(array(a))[0] + 1", id="array_distinct"),
        pytest.param("filter(array(a), x -> true)[0] + 1", id="filter"),
        pytest.param("map_values(map('k', a))[0] + 1", id="map_values"),
        pytest.param("map('k', a)['k'] + 1", id="map-index"),
        pytest.param("element_at(map('k', a), 'k') + 1", id="map-element_at"),
        pytest.param("map_keys(map(a, 1))[0] + 1", id="map_keys"),
        pytest.param("array_repeat(a, 2)[0] + 1", id="array_repeat"),
        pytest.param("map_concat(map('k', a), map('j', a))['k'] + 1", id="map_concat"),
        pytest.param("map_values(map('k', array(a)))[0][0] + 1", id="element-of-an-element"),
        pytest.param("array(named_struct('f', a))[0].f + 1", id="struct-field-of-an-element"),
    ],
)
def test_udt_through_a_recognised_shape_is_refused(spark, udt_scalar_view, form):
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        _ = spark.sql(f"SELECT {form} FROM {udt_scalar_view}").schema  # noqa: S608


# TODO: Spark has no cast from a UDT to its storage type: `Cast.canCast` only pairs a UDT with a UDT
#   that accepts it (`Cast.scala:304`), so `CAST(udt AS DOUBLE)` is refused at the cast, before any
#   arithmetic. Sail casts to the storage and computes. Closing it is a CAST guard, not an arithmetic
#   one, and a CAST guard is what broke the ClickBench fixture once (`cast("int").cast("date")`), so
#   it belongs to a PR that can measure the datasource suites for it.
@pytest.mark.xfail(
    not is_jvm_spark(),
    strict=True,
    reason="Sail casts a UDT to its storage type, which Spark refuses",
)
@pytest.mark.parametrize("expression", ["CAST(a AS DOUBLE) + 1", "CAST(a AS BIGINT) * 2", "-CAST(a AS DOUBLE)"])
def test_udt_cast_to_its_storage_type_is_refused(spark, udt_scalar_view, expression):
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        _ = spark.sql(f"SELECT {expression} AS r FROM {udt_scalar_view}").schema  # noqa: S608


@pytest.mark.parametrize("expression", ["CAST(d AS DOUBLE) + 1", "CAST(d AS BIGINT) * 2", "-CAST(d AS DOUBLE)"])
def test_a_plain_double_cast_to_its_own_type_resolves(spark, udt_scalar_view, expression):
    assert spark.sql(f"SELECT {expression} AS r FROM {udt_scalar_view}").collect() == []  # noqa: S608


@pytest.mark.parametrize("expression", ["abs(a)", "abs(a) + 1", "-abs(a)"])
def test_abs_of_a_udt_is_refused(spark, udt_scalar_view, expression):
    """`Abs` takes NUMERIC or an ANSI interval (`arithmetic.scala:158`), and no coercion rule casts a
    UDT, so Spark refuses `abs` itself, not only the arithmetic around it."""
    with pytest.raises(AnalysisException, match=r"(?i)cannot resolve"):
        _ = spark.sql(f"SELECT {expression} AS r FROM {udt_scalar_view}").schema  # noqa: S608


@pytest.mark.parametrize("expression", ["abs(-1.0D)", "abs(d)", "abs(d) + 1", "-abs(d)"])
def test_abs_of_a_plain_double_resolves(spark, udt_scalar_view, expression):
    assert spark.sql(f"SELECT {expression} AS r FROM {udt_scalar_view}").collect() == []  # noqa: S608


# The same shapes over a plain DOUBLE stay operands, so recognising them cannot refuse more than Spark.
@pytest.mark.parametrize(
    "form",
    [
        pytest.param("abs(-1.0D) + 1", id="abs"),
        pytest.param("slice(array(1.0D), 1, 1)[0] + 1", id="slice"),
        pytest.param("reverse(array(1.0D))[0] * 2", id="reverse"),
        pytest.param("sort_array(array(1.0D))[0] + 1", id="sort_array"),
        pytest.param("array_sort(array(1.0D))[0] + 1", id="array_sort"),
        pytest.param("flatten(array(array(1.0D)))[0] + 1", id="flatten"),
        pytest.param("concat(array(1.0D), array(2.0D))[0] + 1", id="concat"),
        pytest.param("array_distinct(array(1.0D))[0] + 1", id="array_distinct"),
        pytest.param("filter(array(1.0D), x -> true)[0] + 1", id="filter"),
        pytest.param("map_values(map('k', 1.0D))[0] + 1", id="map_values"),
        pytest.param("map('k', 1.0D)['k'] + 1", id="map-index"),
        pytest.param("element_at(map('k', 1.0D), 'k') + 1", id="map-element_at"),
        pytest.param("map_keys(map(1.0D, 1))[0] + 1", id="map_keys"),
        pytest.param("array_repeat(1.0D, 2)[0] + 1", id="array_repeat"),
        pytest.param("map_concat(map('k', 1.0D), map('j', 2.0D))['k'] + 1", id="map_concat"),
        pytest.param("map_values(map('k', array(1.0D)))[0][0] + 1", id="element-of-an-element"),
        pytest.param("array(named_struct('f', 1.0D))[0].f + 1", id="struct-field-of-an-element"),
    ],
)
def test_plain_double_through_a_recognised_shape_resolves(spark, udt_scalar_view, form):
    assert spark.sql(f"SELECT {form} AS r FROM {udt_scalar_view}").collect() == []  # noqa: S608
