"""Parity of the methods that add, replace or rename a column.

`withColumn`, `withColumns`, `withMetadata`, `withColumnRenamed` and `withColumnsRenamed` all match
the column name through the analyzer resolver, and neither `UnresolvedStarWithColumns.expandStar`
nor `UnresolvedStarWithColumnsRenames.expandStar` reads any configuration of its own, so what they
do is decided by which analyzer runs and how it compares names. Both settings are set explicitly
below rather than relied on, because a default that moves would quietly change what these cases
assert:

- `spark.sql.caseSensitive` picks the resolver, and every case runs under both of its values.
- `spark.sql.analyzer.singlePassResolver.enabled` picks the analyzer. It is internal, defaults to
  false and is still under development, so the cases pin it to false. Measured against Spark, the
  single-pass analyzer returns the same value everywhere and differs only in the condition it
  raises for an unresolvable `withMetadata` name: `UNRESOLVED_COLUMN.WITH_SUGGESTION` instead of
  `CANNOT_RESOLVE_DATAFRAME_COLUMN`.

Every expectation below was measured against Spark before it was written down.

`withColumns({})` is left out on purpose: PySpark rejects the empty map with a bare `AssertionError`
in the client, so it never reaches the engine and says nothing about parity.
"""

# The names under test are deliberately confusable with ASCII ones — that is what makes them
# discriminate between the two case-folding rules — so the ambiguity rules are off for this file.
# ruff: noqa: RUF001

import datetime

import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.functions import col, lit, lower, row_number
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.window import Window

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

_SAIL_BUG = pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)

ANALYZER = {"spark.sql.analyzer.singlePassResolver.enabled": "false"}

# The three rows of the reported repro fold to two distinct products.
_DISTINCT_PRODUCTS = 2

# An offset large enough that the replaced column cannot be mistaken for the original.
_OFFSET = 10

# These conditions were introduced in Spark 4.0, so an older JVM used as the oracle reports a
# plain message instead. Against Sail the message comes from Sail, whatever the client is.
SPARK_4_CONDITIONS = frozenset(
    {
        "INVALID_ATTRIBUTE_NAME_SYNTAX",
        "CANNOT_RESOLVE_DATAFRAME_COLUMN",
        "UNRESOLVED_COLUMN_AMONG_FIELD_NAMES",
    }
)

_SPARK_4 = pytest.mark.skipif(
    is_jvm_spark() and pyspark_version() < (4, 0),
    reason="The error condition was introduced in Spark 4.0",
)


def _error_param(*values, marks=()):
    """Builds an error case, gating it when the condition is one an older JVM oracle lacks."""
    gates = [_SPARK_4] if values[-1] in SPARK_4_CONDITIONS else []
    return pytest.param(*values, marks=[*gates, *marks])


def _normalise(value):
    """Strips the client's own rendering of a value, so the rows read the same anywhere.

    PySpark 3.5 hands a BINARY value back as `bytearray` and 4.x as `bytes`. A TIMESTAMP comes
    back as a naive `datetime` in the time zone of the machine running the test — the session
    time zone decides which instant the engine stores, not how the client prints it — so it has
    to be anchored before it can be compared on another machine.
    """
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, datetime.datetime) and value.tzinfo is None:
        return value.astimezone(datetime.timezone.utc)
    return value


def _row_keys(names):
    """Disambiguates repeated column names so a row keeps every column it has.

    `Row.asDict` keeps only one of a pair of columns that share a name, which is exactly the
    column a case about duplicate names is asserting, so the repeated ones are numbered by
    position instead.
    """
    repeated = {name for name in names if names.count(name) > 1}
    seen = {}
    keys = []
    for name in names:
        if name in repeated:
            seen[name] = seen.get(name, 0) + 1
            keys.append(f"{name}#{seen[name]}")
        else:
            keys.append(name)
    return keys


def _rows(df):
    keys = _row_keys(df.columns)
    return sorted(str(dict(zip(keys, [_normalise(value) for value in row], strict=True))) for row in df.collect())


def _configure(spark, case_sensitive):
    for key, value in {**ANALYZER, "spark.sql.caseSensitive": case_sensitive}.items():
        spark.conf.set(key, value)


def _unconfigure(spark):
    for key in [*ANALYZER, "spark.sql.caseSensitive"]:
        spark.conf.unset(key)


def _cases(spark):
    def base():
        return spark.sql("SELECT 1 AS a, 2 AS b")

    return {
        "with_column_same_case": lambda: base().withColumn("a", lit(9)),
        "with_column_differing_case": lambda: base().withColumn("A", lit(9)),
        "with_column_new_name": lambda: base().withColumn("c", lit(9)),
        "with_column_non_ascii": lambda: spark.sql("SELECT 1 AS `ä`").withColumn("Ä", lit(9)),
        "with_column_dotless_i": lambda: spark.sql("SELECT 1 AS `ıd`").withColumn("Id", lit(9)),
        "with_column_final_sigma": lambda: spark.sql("SELECT 1 AS `ς`").withColumn("Σ", lit(9)),
        "with_column_referring_to_itself": lambda: base().withColumn("A", col("a") + 1),
        "with_column_dotted_name": lambda: spark.sql("SELECT named_struct('x', 1) AS s").withColumn("s.x", lit(9)),
        "with_column_ambiguous_name": lambda: spark.sql("SELECT 1 AS a, 2 AS A").withColumn("a", lit(9)),
        "with_columns_two_entries": lambda: base().withColumns({"A": lit(9), "c": lit(7)}),
        "with_columns_entries_differing_in_case": lambda: base().withColumns({"c": lit(1), "C": lit(2)}),
        "with_column_renamed_same_case": lambda: base().withColumnRenamed("a", "z"),
        "with_column_renamed_differing_case": lambda: base().withColumnRenamed("A", "z"),
        "with_column_renamed_unknown_name": lambda: base().withColumnRenamed("nope", "z"),
        "with_column_renamed_onto_existing": lambda: base().withColumnRenamed("a", "b"),
        "with_column_renamed_onto_existing_case": lambda: base().withColumnRenamed("a", "B"),
        "with_column_renamed_non_ascii": lambda: spark.sql("SELECT 1 AS `ä`").withColumnRenamed("Ä", "z"),
        "with_column_renamed_ambiguous_name": lambda: spark.sql("SELECT 1 AS a, 2 AS A").withColumnRenamed("a", "z"),
        "with_columns_renamed_sequential": lambda: base().withColumnsRenamed({"a": "b", "b": "c"}),
        "with_columns_renamed_swap": lambda: base().withColumnsRenamed({"a": "b", "b": "a"}),
        "with_columns_renamed_targets_differing_in_case": lambda: base().withColumnsRenamed({"a": "z", "b": "Z"}),
        "with_columns_renamed_differing_case": lambda: base().withColumnsRenamed({"A": "z"}),
        "with_columns_renamed_empty": lambda: base().withColumnsRenamed({}),
        "with_columns_renamed_unknown_name": lambda: base().withColumnsRenamed({"nope": "z"}),
        "with_metadata_same_case": lambda: base().withMetadata("a", {"k": "v"}),
        "with_metadata_differing_case": lambda: base().withMetadata("A", {"k": "v"}),
        "with_metadata_unknown_name": lambda: base().withMetadata("nope", {"k": "v"}),
        "with_metadata_non_ascii": lambda: spark.sql("SELECT 1 AS `ä`").withMetadata("Ä", {"k": "v"}),
        "with_metadata_replaces": lambda: base().withMetadata("a", {"k": "v"}).withMetadata("a", {"j": "w"}),
    }


# (case, caseSensitive, columns, rows, metadata of each field)
RESULTS = [
    ("with_column_same_case", "false", ["a", "b"], ["{'a': 9, 'b': 2}"], [{}, {}]),
    ("with_column_same_case", "true", ["a", "b"], ["{'a': 9, 'b': 2}"], [{}, {}]),
    ("with_column_differing_case", "false", ["A", "b"], ["{'A': 9, 'b': 2}"], [{}, {}]),
    ("with_column_differing_case", "true", ["a", "b", "A"], ["{'a': 1, 'b': 2, 'A': 9}"], [{}, {}, {}]),
    ("with_column_new_name", "false", ["a", "b", "c"], ["{'a': 1, 'b': 2, 'c': 9}"], [{}, {}, {}]),
    ("with_column_new_name", "true", ["a", "b", "c"], ["{'a': 1, 'b': 2, 'c': 9}"], [{}, {}, {}]),
    ("with_column_non_ascii", "false", ["Ä"], ["{'Ä': 9}"], [{}]),
    ("with_column_non_ascii", "true", ["ä", "Ä"], ["{'ä': 1, 'Ä': 9}"], [{}, {}]),
    ("with_column_dotless_i", "false", ["Id"], ["{'Id': 9}"], [{}]),
    ("with_column_dotless_i", "true", ["ıd", "Id"], ["{'ıd': 1, 'Id': 9}"], [{}, {}]),
    ("with_column_final_sigma", "false", ["Σ"], ["{'Σ': 9}"], [{}]),
    ("with_column_final_sigma", "true", ["ς", "Σ"], ["{'ς': 1, 'Σ': 9}"], [{}, {}]),
    ("with_column_referring_to_itself", "false", ["A", "b"], ["{'A': 2, 'b': 2}"], [{}, {}]),
    ("with_column_referring_to_itself", "true", ["a", "b", "A"], ["{'a': 1, 'b': 2, 'A': 2}"], [{}, {}, {}]),
    ("with_column_dotted_name", "false", ["s", "s.x"], ["{'s': Row(x=1), 's.x': 9}"], [{}, {}]),
    ("with_column_dotted_name", "true", ["s", "s.x"], ["{'s': Row(x=1), 's.x': 9}"], [{}, {}]),
    ("with_column_ambiguous_name", "false", ["a", "a"], ["{'a#1': 9, 'a#2': 9}"], [{}, {}]),
    ("with_column_ambiguous_name", "true", ["a", "A"], ["{'a': 9, 'A': 2}"], [{}, {}]),
    ("with_columns_two_entries", "false", ["A", "b", "c"], ["{'A': 9, 'b': 2, 'c': 7}"], [{}, {}, {}]),
    ("with_columns_two_entries", "true", ["a", "b", "A", "c"], ["{'a': 1, 'b': 2, 'A': 9, 'c': 7}"], [{}, {}, {}, {}]),
    (
        "with_columns_entries_differing_in_case",
        "true",
        ["a", "b", "c", "C"],
        ["{'a': 1, 'b': 2, 'c': 1, 'C': 2}"],
        [{}, {}, {}, {}],
    ),
    ("with_column_renamed_same_case", "false", ["z", "b"], ["{'z': 1, 'b': 2}"], [{}, {}]),
    ("with_column_renamed_same_case", "true", ["z", "b"], ["{'z': 1, 'b': 2}"], [{}, {}]),
    ("with_column_renamed_differing_case", "false", ["z", "b"], ["{'z': 1, 'b': 2}"], [{}, {}]),
    ("with_column_renamed_differing_case", "true", ["a", "b"], ["{'a': 1, 'b': 2}"], [{}, {}]),
    ("with_column_renamed_unknown_name", "false", ["a", "b"], ["{'a': 1, 'b': 2}"], [{}, {}]),
    ("with_column_renamed_unknown_name", "true", ["a", "b"], ["{'a': 1, 'b': 2}"], [{}, {}]),
    ("with_column_renamed_onto_existing", "false", ["b", "b"], ["{'b#1': 1, 'b#2': 2}"], [{}, {}]),
    ("with_column_renamed_onto_existing", "true", ["b", "b"], ["{'b#1': 1, 'b#2': 2}"], [{}, {}]),
    ("with_column_renamed_onto_existing_case", "false", ["B", "b"], ["{'B': 1, 'b': 2}"], [{}, {}]),
    ("with_column_renamed_onto_existing_case", "true", ["B", "b"], ["{'B': 1, 'b': 2}"], [{}, {}]),
    ("with_column_renamed_non_ascii", "false", ["z"], ["{'z': 1}"], [{}]),
    ("with_column_renamed_non_ascii", "true", ["ä"], ["{'ä': 1}"], [{}]),
    ("with_column_renamed_ambiguous_name", "false", ["z", "z"], ["{'z#1': 1, 'z#2': 2}"], [{}, {}]),
    ("with_column_renamed_ambiguous_name", "true", ["z", "A"], ["{'z': 1, 'A': 2}"], [{}, {}]),
    ("with_columns_renamed_sequential", "false", ["c", "c"], ["{'c#1': 1, 'c#2': 2}"], [{}, {}]),
    ("with_columns_renamed_sequential", "true", ["c", "c"], ["{'c#1': 1, 'c#2': 2}"], [{}, {}]),
    ("with_columns_renamed_swap", "false", ["a", "a"], ["{'a#1': 1, 'a#2': 2}"], [{}, {}]),
    ("with_columns_renamed_swap", "true", ["a", "a"], ["{'a#1': 1, 'a#2': 2}"], [{}, {}]),
    ("with_columns_renamed_targets_differing_in_case", "false", ["z", "Z"], ["{'z': 1, 'Z': 2}"], [{}, {}]),
    ("with_columns_renamed_targets_differing_in_case", "true", ["z", "Z"], ["{'z': 1, 'Z': 2}"], [{}, {}]),
    ("with_columns_renamed_differing_case", "false", ["z", "b"], ["{'z': 1, 'b': 2}"], [{}, {}]),
    ("with_columns_renamed_differing_case", "true", ["a", "b"], ["{'a': 1, 'b': 2}"], [{}, {}]),
    ("with_columns_renamed_empty", "false", ["a", "b"], ["{'a': 1, 'b': 2}"], [{}, {}]),
    ("with_columns_renamed_empty", "true", ["a", "b"], ["{'a': 1, 'b': 2}"], [{}, {}]),
    ("with_columns_renamed_unknown_name", "false", ["a", "b"], ["{'a': 1, 'b': 2}"], [{}, {}]),
    ("with_columns_renamed_unknown_name", "true", ["a", "b"], ["{'a': 1, 'b': 2}"], [{}, {}]),
    ("with_metadata_same_case", "false", ["a", "b"], ["{'a': 1, 'b': 2}"], [{"k": "v"}, {}]),
    ("with_metadata_same_case", "true", ["a", "b"], ["{'a': 1, 'b': 2}"], [{"k": "v"}, {}]),
    ("with_metadata_differing_case", "false", ["A", "b"], ["{'A': 1, 'b': 2}"], [{"k": "v"}, {}]),
    ("with_metadata_non_ascii", "false", ["Ä"], ["{'Ä': 1}"], [{"k": "v"}]),
    ("with_metadata_replaces", "false", ["a", "b"], ["{'a': 1, 'b': 2}"], [{"j": "w"}, {}]),
    ("with_metadata_replaces", "true", ["a", "b"], ["{'a': 1, 'b': 2}"], [{"j": "w"}, {}]),
]

# (case, caseSensitive, error condition)
ERRORS = [
    ("with_columns_entries_differing_in_case", "false", "COLUMN_ALREADY_EXISTS"),
    _error_param("with_metadata_differing_case", "true", "CANNOT_RESOLVE_DATAFRAME_COLUMN"),
    _error_param("with_metadata_unknown_name", "false", "CANNOT_RESOLVE_DATAFRAME_COLUMN"),
    _error_param("with_metadata_unknown_name", "true", "CANNOT_RESOLVE_DATAFRAME_COLUMN"),
    _error_param("with_metadata_non_ascii", "true", "CANNOT_RESOLVE_DATAFRAME_COLUMN"),
]


@pytest.mark.parametrize(("case", "case_sensitive", "columns", "rows", "metadata"), RESULTS)
def test_column_method_result(spark, case, case_sensitive, columns, rows, metadata):
    _configure(spark, case_sensitive)
    try:
        df = _cases(spark)[case]()
        assert df.columns == columns
        assert _rows(df) == rows
        assert [dict(field.metadata) for field in df.schema.fields] == metadata
    finally:
        _unconfigure(spark)


@pytest.mark.parametrize(("case", "case_sensitive", "condition"), ERRORS)
def test_column_method_error(spark, case, case_sensitive, condition):
    _configure(spark, case_sensitive)
    try:
        with pytest.raises(Exception, match=condition):
            _ = _cases(spark)[case]().collect()
    finally:
        _unconfigure(spark)


def test_duplicate_names_are_checked_for_columns_but_not_for_renames(spark):
    # `UnresolvedStarWithColumns.expandStar` calls `SchemaUtils.checkColumnNameDuplication` and
    # `UnresolvedStarWithColumnsRenames.expandStar` does not, so the same pair of names is rejected
    # when it adds columns and accepted when it renames them.
    df = spark.sql("SELECT 1 AS a, 2 AS b")

    with pytest.raises(Exception, match="COLUMN_ALREADY_EXISTS"):
        _ = df.withColumns({"c": lit(1), "C": lit(2)}).collect()

    assert df.withColumnsRenamed({"a": "z", "b": "Z"}).columns == ["z", "Z"]


@pytest.mark.parametrize(("case_sensitive", "columns"), [("false", ["product"]), ("true", ["Product", "product"])])
def test_replacement_survives_a_later_analysis(spark, case_sensitive, columns):
    # The repro reported on https://github.com/lakehq/sail/pull/2343. The duplicate column that the
    # replacement used to append built fine, so the collision only surfaced once a later operation
    # had to resolve the name again. Keeping both columns is the correct result when the names are
    # case sensitive, so only the other setting tells the fix apart from the bug.
    _configure(spark, case_sensitive)
    try:
        df = spark.createDataFrame([("A",), ("a",), ("B",)], ["Product"])
        df = df.withColumn("product", lower("Product"))

        assert df.columns == columns
        assert df.groupBy("product").count().count() == _DISTINCT_PRODUCTS
    finally:
        _unconfigure(spark)


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_an_added_column_carries_no_qualifier(spark):
    # The star expansion returns the input's own attributes for the columns it passes through, so
    # those keep their qualifier, while a column the projection adds is an alias with none.
    df = spark.sql("SELECT 1 AS a, 2 AS b").alias("x")

    assert df.withColumn("c", lit(1)).select("x.a").columns == ["a"]
    with pytest.raises(Exception, match="UNRESOLVED_COLUMN"):
        _ = df.withColumn("c", lit(1)).select("x.c").collect()


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_a_rename_keeps_the_qualifier_of_the_columns_it_did_not_touch(spark):
    # Renaming one column does not take the qualifier away from the others.
    df = spark.sql("SELECT 1 AS a, 2 AS b").alias("x")

    assert df.withColumnRenamed("a", "z").select("x.b").columns == ["b"]


@pytest.mark.parametrize(
    ("expression", "data_type", "nullable"),
    [
        # The two that agree, as the controls: a fix that made every added column nullable, or
        # none of them, would still have to pass these.
        ("1", "int", False),
        ("a", "int", False),
        pytest.param("CAST(1 AS DECIMAL(10,2))", "decimal(10,2)", True, marks=_SAIL_BUG),
        pytest.param("map('k', 1)", "map<string,int>", False, marks=_SAIL_BUG),
        pytest.param("CASE WHEN a > 1 THEN 'big' ELSE 'small' END", "string", False, marks=_SAIL_BUG),
    ],
)
def test_an_added_column_reports_the_nullability_of_its_expression(spark, expression, data_type, nullable):
    # The matrices above compare `schema.simpleString()`, which renders neither `nullable` nor the
    # nullability inside a container, so this is the only place the flag is asserted.
    df = spark.sql(f"SELECT *, {expression} AS c FROM VALUES (1, 'x'), (2, 'y') AS t(a, b)")  # noqa: S608
    field = df.schema["c"]

    assert field.dataType.simpleString() == data_type
    assert field.nullable == nullable


def _annotated(spark):
    """A column that the projection passes through, annotated with metadata."""
    return spark.sql("SELECT * FROM VALUES (1, 'x'), (2, 'y') AS t(a, b)").withMetadata("a", {"k": "v"})


@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_metadata_on_a_passed_through_column_reaches_collect(spark):
    # The metadata rides on an alias over a plain column reference, and it survives into the
    # schema but not into the physical projection, so the plan only fails once it has to produce
    # rows. Every path that returns data to the client fails with it; `count`, `show` and a write
    # do not, because they never build that projection.
    assert [row.asDict() for row in _annotated(spark).collect()] == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]


@pytest.mark.skipif(
    pyspark_version() < (4, 2),
    reason="The client carries the field metadata through `toArrow` from PySpark 4.2 on",
)
@pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True)
def test_metadata_on_a_passed_through_column_reaches_to_arrow(spark):
    table = _annotated(spark).toArrow()

    assert table.to_pydict() == {"a": [1, 2], "b": ["x", "y"]}
    assert table.schema.field("a").metadata == {b"SPARK::metadata::json": b'{"k": "v"}'}


@pytest.mark.skipif(
    pyspark_version() < (4, 2),
    reason="The client carries the field metadata through `toArrow` from PySpark 4.2 on",
)
def test_metadata_on_a_column_the_projection_builds_reaches_the_client(spark):
    # The same metadata on a column produced by the projection itself, rather than passed through
    # from the input, does reach the client. This is what keeps the failure above narrow.
    df = spark.sql("SELECT * FROM VALUES (1, 'x') AS t(a, b)").withColumn("c", lit(1)).withMetadata("c", {"k": "v"})

    assert [row.asDict() for row in df.collect()] == [{"a": 1, "b": "x", "c": 1}]
    assert df.toArrow().schema.field("c").metadata == {b"SPARK::metadata::json": b'{"k": "v"}'}


# A column added or renamed, then consumed by another operation. The reported failure was deferred:
# the replacement built fine and the collision only surfaced when a later operation had to resolve
# the name again, so these cases put an operation after the replacement.


def _composition(spark):
    def base():
        return spark.sql("SELECT * FROM VALUES (1, 'x'), (2, 'y'), (1, 'z') AS t(a, b)")

    def other():
        return spark.sql("SELECT * FROM VALUES (1, 'p'), (3, 'q') AS t(a, c)")

    return {
        # The reported shape: replace, then group by the replaced name.
        "replaced_then_group_by": lambda: base().withColumn("A", col("a")).groupBy("a").count(),
        "replaced_then_group_by_new_case": lambda: base().withColumn("A", col("a")).groupBy("A").count(),
        # Refer to the column by the case it had before the replacement.
        "replaced_then_select_old_case": lambda: base().withColumn("A", col("a") + _OFFSET).select("a"),
        "replaced_then_filter_old_case": lambda: base().withColumn("A", col("a") + _OFFSET).filter(col("a") > _OFFSET),
        "replaced_then_order_by_old_case": lambda: base().withColumn("A", col("a") + _OFFSET).orderBy("a"),
        "replaced_then_drop_old_case": lambda: base().withColumn("A", col("a") + _OFFSET).drop("a"),
        "replaced_then_aggregate_old_case": lambda: base()
        .withColumn("A", col("a") + _OFFSET)
        .agg(spark_sum("a").alias("s")),
        # A join on the replaced name, from either side.
        "replaced_then_join_on_name": lambda: base().withColumn("A", col("a")).join(other(), "a"),
        "replaced_then_join_on_condition": lambda: (
            base().withColumn("A", col("a")).alias("l").join(other().alias("r"), col("l.a") == col("r.a"))
        ),
        "replaced_then_self_join": lambda: (
            base().withColumn("A", col("a")).alias("l").join(base().alias("r"), col("l.a") == col("r.a"))
        ),
        # A window partitioned by the replaced name.
        "replaced_then_window": lambda: (
            base().withColumn("A", col("a")).withColumn("n", row_number().over(Window.partitionBy("a").orderBy("b")))
        ),
        # A set operation after the replacement.
        "replaced_then_union": lambda: base().withColumn("A", col("a")).union(base()),
        "replaced_then_union_by_name": lambda: base().withColumn("A", col("a")).unionByName(base()),
        "replaced_then_distinct": lambda: base().withColumn("A", lit(1)).distinct(),
        "replaced_then_drop_duplicates": lambda: base().withColumn("A", lit(1)).dropDuplicates(["a"]),
        # Chained replacements, and a replacement on top of a rename.
        "replaced_twice": lambda: base().withColumn("A", col("a") + 1).withColumn("a", col("A") + 1),
        "renamed_then_replaced": lambda: base().withColumnRenamed("a", "Z").withColumn("z", lit(9)),
        "renamed_then_group_by": lambda: base().withColumnRenamed("a", "Z").groupBy("z").count(),
        "renamed_then_join_on_name": lambda: base().withColumnRenamed("b", "c").join(other(), "a"),
        "renamed_twice_then_select": lambda: base().withColumnsRenamed({"a": "Z", "b": "Y"}).select("z", "y"),
        # Metadata has to survive the operation that follows it.
        "metadata_then_join": lambda: base().withMetadata("a", {"k": "v"}).join(other(), "a"),
        "metadata_then_select": lambda: base().withMetadata("a", {"k": "v"}).select("A"),
        "metadata_then_group_by": lambda: base().withMetadata("a", {"k": "v"}).groupBy("a").count(),
    }


# (case, caseSensitive, columns, rows)
COMPOSITION_RESULTS = [
    ("replaced_then_group_by", "false", ["a", "count"], ["{'a': 1, 'count': 2}", "{'a': 2, 'count': 1}"]),
    ("replaced_then_group_by", "true", ["a", "count"], ["{'a': 1, 'count': 2}", "{'a': 2, 'count': 1}"]),
    ("replaced_then_group_by_new_case", "false", ["A", "count"], ["{'A': 1, 'count': 2}", "{'A': 2, 'count': 1}"]),
    ("replaced_then_group_by_new_case", "true", ["A", "count"], ["{'A': 1, 'count': 2}", "{'A': 2, 'count': 1}"]),
    ("replaced_then_select_old_case", "false", ["a"], ["{'a': 11}", "{'a': 11}", "{'a': 12}"]),
    ("replaced_then_select_old_case", "true", ["a"], ["{'a': 1}", "{'a': 1}", "{'a': 2}"]),
    (
        "replaced_then_filter_old_case",
        "false",
        ["A", "b"],
        ["{'A': 11, 'b': 'x'}", "{'A': 11, 'b': 'z'}", "{'A': 12, 'b': 'y'}"],
    ),
    ("replaced_then_filter_old_case", "true", ["a", "b", "A"], []),
    (
        "replaced_then_order_by_old_case",
        "false",
        ["A", "b"],
        ["{'A': 11, 'b': 'x'}", "{'A': 11, 'b': 'z'}", "{'A': 12, 'b': 'y'}"],
    ),
    (
        "replaced_then_order_by_old_case",
        "true",
        ["a", "b", "A"],
        ["{'a': 1, 'b': 'x', 'A': 11}", "{'a': 1, 'b': 'z', 'A': 11}", "{'a': 2, 'b': 'y', 'A': 12}"],
    ),
    ("replaced_then_drop_old_case", "false", ["b"], ["{'b': 'x'}", "{'b': 'y'}", "{'b': 'z'}"]),
    (
        "replaced_then_drop_old_case",
        "true",
        ["b", "A"],
        ["{'b': 'x', 'A': 11}", "{'b': 'y', 'A': 12}", "{'b': 'z', 'A': 11}"],
    ),
    ("replaced_then_aggregate_old_case", "false", ["s"], ["{'s': 34}"]),
    ("replaced_then_aggregate_old_case", "true", ["s"], ["{'s': 4}"]),
    (
        "replaced_then_join_on_name",
        "false",
        ["A", "b", "c"],
        ["{'A': 1, 'b': 'x', 'c': 'p'}", "{'A': 1, 'b': 'z', 'c': 'p'}"],
    ),
    (
        "replaced_then_join_on_name",
        "true",
        ["a", "b", "A", "c"],
        ["{'a': 1, 'b': 'x', 'A': 1, 'c': 'p'}", "{'a': 1, 'b': 'z', 'A': 1, 'c': 'p'}"],
    ),
    (
        "replaced_then_join_on_condition",
        "false",
        ["A", "b", "a", "c"],
        ["{'A': 1, 'b': 'x', 'a': 1, 'c': 'p'}", "{'A': 1, 'b': 'z', 'a': 1, 'c': 'p'}"],
    ),
    (
        "replaced_then_join_on_condition",
        "true",
        ["a", "b", "A", "a", "c"],
        ["{'a#1': 1, 'b': 'x', 'A': 1, 'a#2': 1, 'c': 'p'}", "{'a#1': 1, 'b': 'z', 'A': 1, 'a#2': 1, 'c': 'p'}"],
    ),
    (
        "replaced_then_self_join",
        "false",
        ["A", "b", "a", "b"],
        [
            "{'A': 1, 'b#1': 'x', 'a': 1, 'b#2': 'x'}",
            "{'A': 1, 'b#1': 'x', 'a': 1, 'b#2': 'z'}",
            "{'A': 1, 'b#1': 'z', 'a': 1, 'b#2': 'x'}",
            "{'A': 1, 'b#1': 'z', 'a': 1, 'b#2': 'z'}",
            "{'A': 2, 'b#1': 'y', 'a': 2, 'b#2': 'y'}",
        ],
    ),
    (
        "replaced_then_self_join",
        "true",
        ["a", "b", "A", "a", "b"],
        [
            "{'a#1': 1, 'b#1': 'x', 'A': 1, 'a#2': 1, 'b#2': 'x'}",
            "{'a#1': 1, 'b#1': 'x', 'A': 1, 'a#2': 1, 'b#2': 'z'}",
            "{'a#1': 1, 'b#1': 'z', 'A': 1, 'a#2': 1, 'b#2': 'x'}",
            "{'a#1': 1, 'b#1': 'z', 'A': 1, 'a#2': 1, 'b#2': 'z'}",
            "{'a#1': 2, 'b#1': 'y', 'A': 2, 'a#2': 2, 'b#2': 'y'}",
        ],
    ),
    (
        "replaced_then_window",
        "false",
        ["A", "b", "n"],
        ["{'A': 1, 'b': 'x', 'n': 1}", "{'A': 1, 'b': 'z', 'n': 2}", "{'A': 2, 'b': 'y', 'n': 1}"],
    ),
    (
        "replaced_then_window",
        "true",
        ["a", "b", "A", "n"],
        [
            "{'a': 1, 'b': 'x', 'A': 1, 'n': 1}",
            "{'a': 1, 'b': 'z', 'A': 1, 'n': 2}",
            "{'a': 2, 'b': 'y', 'A': 2, 'n': 1}",
        ],
    ),
    (
        "replaced_then_union",
        "false",
        ["A", "b"],
        [
            "{'A': 1, 'b': 'x'}",
            "{'A': 1, 'b': 'x'}",
            "{'A': 1, 'b': 'z'}",
            "{'A': 1, 'b': 'z'}",
            "{'A': 2, 'b': 'y'}",
            "{'A': 2, 'b': 'y'}",
        ],
    ),
    (
        "replaced_then_union_by_name",
        "false",
        ["A", "b"],
        [
            "{'A': 1, 'b': 'x'}",
            "{'A': 1, 'b': 'x'}",
            "{'A': 1, 'b': 'z'}",
            "{'A': 1, 'b': 'z'}",
            "{'A': 2, 'b': 'y'}",
            "{'A': 2, 'b': 'y'}",
        ],
    ),
    ("replaced_then_distinct", "false", ["A", "b"], ["{'A': 1, 'b': 'x'}", "{'A': 1, 'b': 'y'}", "{'A': 1, 'b': 'z'}"]),
    (
        "replaced_then_distinct",
        "true",
        ["a", "b", "A"],
        ["{'a': 1, 'b': 'x', 'A': 1}", "{'a': 1, 'b': 'z', 'A': 1}", "{'a': 2, 'b': 'y', 'A': 1}"],
    ),
    ("replaced_then_drop_duplicates", "false", ["A", "b"], ["{'A': 1, 'b': 'x'}"]),
    (
        "replaced_then_drop_duplicates",
        "true",
        ["a", "b", "A"],
        ["{'a': 1, 'b': 'x', 'A': 1}", "{'a': 2, 'b': 'y', 'A': 1}"],
    ),
    ("replaced_twice", "false", ["a", "b"], ["{'a': 3, 'b': 'x'}", "{'a': 3, 'b': 'z'}", "{'a': 4, 'b': 'y'}"]),
    (
        "replaced_twice",
        "true",
        ["a", "b", "A"],
        ["{'a': 3, 'b': 'x', 'A': 2}", "{'a': 3, 'b': 'z', 'A': 2}", "{'a': 4, 'b': 'y', 'A': 3}"],
    ),
    ("renamed_then_replaced", "false", ["z", "b"], ["{'z': 9, 'b': 'x'}", "{'z': 9, 'b': 'y'}", "{'z': 9, 'b': 'z'}"]),
    (
        "renamed_then_replaced",
        "true",
        ["Z", "b", "z"],
        ["{'Z': 1, 'b': 'x', 'z': 9}", "{'Z': 1, 'b': 'z', 'z': 9}", "{'Z': 2, 'b': 'y', 'z': 9}"],
    ),
    ("renamed_then_group_by", "false", ["z", "count"], ["{'z': 1, 'count': 2}", "{'z': 2, 'count': 1}"]),
    (
        "renamed_then_join_on_name",
        "false",
        ["a", "c", "c"],
        ["{'a': 1, 'c#1': 'x', 'c#2': 'p'}", "{'a': 1, 'c#1': 'z', 'c#2': 'p'}"],
    ),
    (
        "renamed_then_join_on_name",
        "true",
        ["a", "c", "c"],
        ["{'a': 1, 'c#1': 'x', 'c#2': 'p'}", "{'a': 1, 'c#1': 'z', 'c#2': 'p'}"],
    ),
    (
        "renamed_twice_then_select",
        "false",
        ["z", "y"],
        ["{'z': 1, 'y': 'x'}", "{'z': 1, 'y': 'z'}", "{'z': 2, 'y': 'y'}"],
    ),
    ("metadata_then_join", "false", ["a", "b", "c"], ["{'a': 1, 'b': 'x', 'c': 'p'}", "{'a': 1, 'b': 'z', 'c': 'p'}"]),
    ("metadata_then_join", "true", ["a", "b", "c"], ["{'a': 1, 'b': 'x', 'c': 'p'}", "{'a': 1, 'b': 'z', 'c': 'p'}"]),
    pytest.param(*("metadata_then_select", "false", ["A"], ["{'A': 1}", "{'A': 1}", "{'A': 2}"]), marks=[_SAIL_BUG]),
    ("metadata_then_group_by", "false", ["a", "count"], ["{'a': 1, 'count': 2}", "{'a': 2, 'count': 1}"]),
    ("metadata_then_group_by", "true", ["a", "count"], ["{'a': 1, 'count': 2}", "{'a': 2, 'count': 1}"]),
]

# (case, caseSensitive, error condition)
COMPOSITION_ERRORS = [
    pytest.param(*("replaced_then_union", "true", "NUM_COLUMNS_MISMATCH"), marks=[_SAIL_BUG]),
    _error_param("replaced_then_union_by_name", "true", "UNRESOLVED_COLUMN_AMONG_FIELD_NAMES"),
    # The client decides whether the name carries a plan id, and that is what selects between the
    # two conditions Spark raises, so an older client reaches this through
    # `CANNOT_RESOLVE_DATAFRAME_COLUMN` instead.
    pytest.param(
        *("renamed_then_group_by", "true", "UNRESOLVED_COLUMN.WITH_SUGGESTION"),
        marks=pytest.mark.skipif(pyspark_version() < (4, 1), reason="The client sends a plan id from PySpark 4.1 on"),
    ),
    ("renamed_twice_then_select", "true", "UNRESOLVED_COLUMN.WITH_SUGGESTION"),
    ("metadata_then_select", "true", "UNRESOLVED_COLUMN.WITH_SUGGESTION"),
]


@pytest.mark.parametrize(("case", "case_sensitive", "columns", "rows"), COMPOSITION_RESULTS)
def test_composition_result(spark, case, case_sensitive, columns, rows):
    _configure(spark, case_sensitive)
    try:
        df = _composition(spark)[case]()
        assert df.columns == columns
        assert _rows(df) == rows
    finally:
        _unconfigure(spark)


@pytest.mark.parametrize(("case", "case_sensitive", "condition"), COMPOSITION_ERRORS)
def test_composition_error(spark, case, case_sensitive, condition):
    _configure(spark, case_sensitive)
    try:
        with pytest.raises(Exception, match=condition):
            _ = _composition(spark)[case]().collect()
    finally:
        _unconfigure(spark)


# The two axes of the family that the name matrix above does not touch: what the metadata itself
# holds, and what kind of expression the new column is built from. Both are measured against Spark.
def _metadata_and_expression_cases(spark):
    def base():
        return spark.sql("SELECT * FROM VALUES (1,'x'),(2,'y') AS t(a, b)")

    def literal():
        return spark.sql("SELECT 1 AS a, 'x' AS b")

    metadata = {
        "empty": {},
        "one key": {"k": "v"},
        "several keys": {"k": "v", "j": "w", "i": "z"},
        "integer value": {"k": 1},
        "float value": {"k": 1.5},
        "boolean value": {"k": True},
        "null value": {"k": None},
        "list value": {"k": [1, 2]},
        "nested value": {"k": {"n": 1}},
        "empty string value": {"k": ""},
        "unicode key and value": {"ké": "vä"},
        "dotted key": {"a.b": "v"},
        "quoted value": {"k": 'a "quoted" value'},
        "long value": {"k": "z" * 500},
        "reserved looking key": {"comment": "c", "__CHAR_VARCHAR_TYPE_STRING": "x"},
    }

    expressions = {
        "int literal": lambda: F.lit(1),
        "string literal": lambda: F.lit("s"),
        "double literal": lambda: F.lit(1.5),
        "boolean literal": lambda: F.lit(True),
        "null literal": lambda: F.lit(None),
        "decimal cast": lambda: F.lit(1).cast("decimal(10,2)"),
        "date cast": lambda: F.lit("2024-01-15").cast("date"),
        "timestamp cast": lambda: F.lit("2024-01-15 12:00:00").cast("timestamp"),
        "binary cast": lambda: F.lit("s").cast("binary"),
        "array": lambda: F.array(F.lit(1), F.lit(2)),
        "map": lambda: F.create_map(F.lit("k"), F.lit(1)),
        "struct": lambda: F.struct(F.lit(1).alias("n")),
        "column reference": lambda: F.col("a"),
        "arithmetic": lambda: F.col("a") + 1,
        "case when": lambda: F.when(F.col("a") > 1, "big").otherwise("small"),
        "coalesce": lambda: F.coalesce(F.col("a"), F.lit(0)),
        "nested field": lambda: F.struct(F.col("a").alias("n")).getField("n"),
        "window function": lambda: F.row_number().over(Window.orderBy("a")),
        "aggregate function": lambda: F.sum("a"),
        "nondeterministic": lambda: (F.rand(1) * 0).cast("int"),
        "cast of itself": lambda: F.col("a").cast("string"),
    }

    cases = {}
    for name, meta in metadata.items():
        cases[f"meta/{name}"] = lambda m=meta: base().withColumn("c", F.lit(1)).withMetadata("c", m)
        cases[f"meta-literal/{name}"] = lambda m=meta: literal().withMetadata("a", m)
    for name, build in expressions.items():
        cases[f"expr/{name}"] = lambda b=build: base().withColumn("c", b())
        cases[f"expr-replace/{name}"] = lambda b=build: base().withColumn("a", b())
        cases[f"expr-meta/{name}"] = lambda b=build: base().withColumn("c", b()).withMetadata("c", {"k": "v"})
    return cases


# (case, columns, schema, metadata of each field, rows)
METADATA_RESULTS = [
    (
        "meta/empty",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    ("meta-literal/empty", ["a", "b"], "struct<a:int,b:string>", [{}, {}], ["{'a': 1, 'b': 'x'}"]),
    (
        "meta/one key",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    ("meta-literal/one key", ["a", "b"], "struct<a:int,b:string>", [{"k": "v"}, {}], ["{'a': 1, 'b': 'x'}"]),
    (
        "meta/several keys",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"i": "z", "j": "w", "k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    (
        "meta-literal/several keys",
        ["a", "b"],
        "struct<a:int,b:string>",
        [{"i": "z", "j": "w", "k": "v"}, {}],
        ["{'a': 1, 'b': 'x'}"],
    ),
    (
        "meta/integer value",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": 1}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    ("meta-literal/integer value", ["a", "b"], "struct<a:int,b:string>", [{"k": 1}, {}], ["{'a': 1, 'b': 'x'}"]),
    (
        "meta/float value",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": 1.5}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    ("meta-literal/float value", ["a", "b"], "struct<a:int,b:string>", [{"k": 1.5}, {}], ["{'a': 1, 'b': 'x'}"]),
    (
        "meta/boolean value",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": True}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    ("meta-literal/boolean value", ["a", "b"], "struct<a:int,b:string>", [{"k": True}, {}], ["{'a': 1, 'b': 'x'}"]),
    (
        "meta/null value",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": None}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    ("meta-literal/null value", ["a", "b"], "struct<a:int,b:string>", [{"k": None}, {}], ["{'a': 1, 'b': 'x'}"]),
    (
        "meta/list value",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": [1, 2]}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    ("meta-literal/list value", ["a", "b"], "struct<a:int,b:string>", [{"k": [1, 2]}, {}], ["{'a': 1, 'b': 'x'}"]),
    (
        "meta/nested value",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": {"n": 1}}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    ("meta-literal/nested value", ["a", "b"], "struct<a:int,b:string>", [{"k": {"n": 1}}, {}], ["{'a': 1, 'b': 'x'}"]),
    (
        "meta/empty string value",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": ""}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    ("meta-literal/empty string value", ["a", "b"], "struct<a:int,b:string>", [{"k": ""}, {}], ["{'a': 1, 'b': 'x'}"]),
    (
        "meta/unicode key and value",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"ké": "vä"}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    (
        "meta-literal/unicode key and value",
        ["a", "b"],
        "struct<a:int,b:string>",
        [{"ké": "vä"}, {}],
        ["{'a': 1, 'b': 'x'}"],
    ),
    (
        "meta/dotted key",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"a.b": "v"}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    ("meta-literal/dotted key", ["a", "b"], "struct<a:int,b:string>", [{"a.b": "v"}, {}], ["{'a': 1, 'b': 'x'}"]),
    (
        "meta/quoted value",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": 'a "quoted" value'}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    (
        "meta-literal/quoted value",
        ["a", "b"],
        "struct<a:int,b:string>",
        [{"k": 'a "quoted" value'}, {}],
        ["{'a': 1, 'b': 'x'}"],
    ),
    (
        "meta/long value",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [
            {},
            {},
            {
                "k": "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
            },
        ],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    (
        "meta-literal/long value",
        ["a", "b"],
        "struct<a:int,b:string>",
        [
            {
                "k": "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
            },
            {},
        ],
        ["{'a': 1, 'b': 'x'}"],
    ),
    (
        "meta/reserved looking key",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"comment": "c", "__CHAR_VARCHAR_TYPE_STRING": "x"}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    (
        "meta-literal/reserved looking key",
        ["a", "b"],
        "struct<a:int,b:string>",
        [{"comment": "c", "__CHAR_VARCHAR_TYPE_STRING": "x"}, {}],
        ["{'a': 1, 'b': 'x'}"],
    ),
    (
        "expr/int literal",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    (
        "expr-replace/int literal",
        ["a", "b"],
        "struct<a:int,b:string>",
        [{}, {}],
        ["{'a': 1, 'b': 'x'}", "{'a': 1, 'b': 'y'}"],
    ),
    (
        "expr-meta/int literal",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 1}"],
    ),
    (
        "expr/string literal",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:string>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': 's'}", "{'a': 2, 'b': 'y', 'c': 's'}"],
    ),
    (
        "expr-replace/string literal",
        ["a", "b"],
        "struct<a:string,b:string>",
        [{}, {}],
        ["{'a': 's', 'b': 'x'}", "{'a': 's', 'b': 'y'}"],
    ),
    (
        "expr-meta/string literal",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:string>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': 's'}", "{'a': 2, 'b': 'y', 'c': 's'}"],
    ),
    (
        "expr/double literal",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:double>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': 1.5}", "{'a': 2, 'b': 'y', 'c': 1.5}"],
    ),
    (
        "expr-replace/double literal",
        ["a", "b"],
        "struct<a:double,b:string>",
        [{}, {}],
        ["{'a': 1.5, 'b': 'x'}", "{'a': 1.5, 'b': 'y'}"],
    ),
    (
        "expr-meta/double literal",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:double>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': 1.5}", "{'a': 2, 'b': 'y', 'c': 1.5}"],
    ),
    (
        "expr/boolean literal",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:boolean>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': True}", "{'a': 2, 'b': 'y', 'c': True}"],
    ),
    (
        "expr-replace/boolean literal",
        ["a", "b"],
        "struct<a:boolean,b:string>",
        [{}, {}],
        ["{'a': True, 'b': 'x'}", "{'a': True, 'b': 'y'}"],
    ),
    (
        "expr-meta/boolean literal",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:boolean>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': True}", "{'a': 2, 'b': 'y', 'c': True}"],
    ),
    (
        "expr/null literal",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:void>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': None}", "{'a': 2, 'b': 'y', 'c': None}"],
    ),
    (
        "expr-replace/null literal",
        ["a", "b"],
        "struct<a:void,b:string>",
        [{}, {}],
        ["{'a': None, 'b': 'x'}", "{'a': None, 'b': 'y'}"],
    ),
    (
        "expr-meta/null literal",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:void>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': None}", "{'a': 2, 'b': 'y', 'c': None}"],
    ),
    (
        "expr/decimal cast",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:decimal(10,2)>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': Decimal('1.00')}", "{'a': 2, 'b': 'y', 'c': Decimal('1.00')}"],
    ),
    (
        "expr-replace/decimal cast",
        ["a", "b"],
        "struct<a:decimal(10,2),b:string>",
        [{}, {}],
        ["{'a': Decimal('1.00'), 'b': 'x'}", "{'a': Decimal('1.00'), 'b': 'y'}"],
    ),
    (
        "expr-meta/decimal cast",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:decimal(10,2)>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': Decimal('1.00')}", "{'a': 2, 'b': 'y', 'c': Decimal('1.00')}"],
    ),
    (
        "expr/date cast",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:date>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': datetime.date(2024, 1, 15)}", "{'a': 2, 'b': 'y', 'c': datetime.date(2024, 1, 15)}"],
    ),
    (
        "expr-replace/date cast",
        ["a", "b"],
        "struct<a:date,b:string>",
        [{}, {}],
        ["{'a': datetime.date(2024, 1, 15), 'b': 'x'}", "{'a': datetime.date(2024, 1, 15), 'b': 'y'}"],
    ),
    (
        "expr-meta/date cast",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:date>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': datetime.date(2024, 1, 15)}", "{'a': 2, 'b': 'y', 'c': datetime.date(2024, 1, 15)}"],
    ),
    (
        "expr/timestamp cast",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:timestamp>",
        [{}, {}, {}],
        [
            "{'a': 1, 'b': 'x', 'c': datetime.datetime(2024, 1, 15, 12, 0, tzinfo=datetime.timezone.utc)}",
            "{'a': 2, 'b': 'y', 'c': datetime.datetime(2024, 1, 15, 12, 0, tzinfo=datetime.timezone.utc)}",
        ],
    ),
    (
        "expr-replace/timestamp cast",
        ["a", "b"],
        "struct<a:timestamp,b:string>",
        [{}, {}],
        [
            "{'a': datetime.datetime(2024, 1, 15, 12, 0, tzinfo=datetime.timezone.utc), 'b': 'x'}",
            "{'a': datetime.datetime(2024, 1, 15, 12, 0, tzinfo=datetime.timezone.utc), 'b': 'y'}",
        ],
    ),
    (
        "expr-meta/timestamp cast",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:timestamp>",
        [{}, {}, {"k": "v"}],
        [
            "{'a': 1, 'b': 'x', 'c': datetime.datetime(2024, 1, 15, 12, 0, tzinfo=datetime.timezone.utc)}",
            "{'a': 2, 'b': 'y', 'c': datetime.datetime(2024, 1, 15, 12, 0, tzinfo=datetime.timezone.utc)}",
        ],
    ),
    (
        "expr/binary cast",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:binary>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': b's'}", "{'a': 2, 'b': 'y', 'c': b's'}"],
    ),
    (
        "expr-replace/binary cast",
        ["a", "b"],
        "struct<a:binary,b:string>",
        [{}, {}],
        ["{'a': b's', 'b': 'x'}", "{'a': b's', 'b': 'y'}"],
    ),
    (
        "expr-meta/binary cast",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:binary>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': b's'}", "{'a': 2, 'b': 'y', 'c': b's'}"],
    ),
    (
        "expr/array",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:array<int>>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': [1, 2]}", "{'a': 2, 'b': 'y', 'c': [1, 2]}"],
    ),
    (
        "expr-replace/array",
        ["a", "b"],
        "struct<a:array<int>,b:string>",
        [{}, {}],
        ["{'a': [1, 2], 'b': 'x'}", "{'a': [1, 2], 'b': 'y'}"],
    ),
    (
        "expr-meta/array",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:array<int>>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': [1, 2]}", "{'a': 2, 'b': 'y', 'c': [1, 2]}"],
    ),
    (
        "expr/map",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:map<string,int>>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': {'k': 1}}", "{'a': 2, 'b': 'y', 'c': {'k': 1}}"],
    ),
    (
        "expr-replace/map",
        ["a", "b"],
        "struct<a:map<string,int>,b:string>",
        [{}, {}],
        ["{'a': {'k': 1}, 'b': 'x'}", "{'a': {'k': 1}, 'b': 'y'}"],
    ),
    (
        "expr-meta/map",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:map<string,int>>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': {'k': 1}}", "{'a': 2, 'b': 'y', 'c': {'k': 1}}"],
    ),
    (
        "expr/struct",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:struct<n:int>>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': Row(n=1)}", "{'a': 2, 'b': 'y', 'c': Row(n=1)}"],
    ),
    (
        "expr-replace/struct",
        ["a", "b"],
        "struct<a:struct<n:int>,b:string>",
        [{}, {}],
        ["{'a': Row(n=1), 'b': 'x'}", "{'a': Row(n=1), 'b': 'y'}"],
    ),
    (
        "expr-meta/struct",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:struct<n:int>>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': Row(n=1)}", "{'a': 2, 'b': 'y', 'c': Row(n=1)}"],
    ),
    (
        "expr/column reference",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 2}"],
    ),
    (
        "expr-replace/column reference",
        ["a", "b"],
        "struct<a:int,b:string>",
        [{}, {}],
        ["{'a': 1, 'b': 'x'}", "{'a': 2, 'b': 'y'}"],
    ),
    pytest.param(
        *(
            "expr-meta/column reference",
            ["a", "b", "c"],
            "struct<a:int,b:string,c:int>",
            [{}, {}, {"k": "v"}],
            ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 2}"],
        ),
        marks=_SAIL_BUG,
    ),
    (
        "expr/arithmetic",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': 2}", "{'a': 2, 'b': 'y', 'c': 3}"],
    ),
    (
        "expr-replace/arithmetic",
        ["a", "b"],
        "struct<a:int,b:string>",
        [{}, {}],
        ["{'a': 2, 'b': 'x'}", "{'a': 3, 'b': 'y'}"],
    ),
    pytest.param(
        *(
            "expr-meta/arithmetic",
            ["a", "b", "c"],
            "struct<a:int,b:string,c:int>",
            [{}, {}, {"k": "v"}],
            ["{'a': 1, 'b': 'x', 'c': 2}", "{'a': 2, 'b': 'y', 'c': 3}"],
        ),
        marks=_SAIL_BUG,
    ),
    (
        "expr/case when",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:string>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': 'small'}", "{'a': 2, 'b': 'y', 'c': 'big'}"],
    ),
    (
        "expr-replace/case when",
        ["a", "b"],
        "struct<a:string,b:string>",
        [{}, {}],
        ["{'a': 'big', 'b': 'y'}", "{'a': 'small', 'b': 'x'}"],
    ),
    pytest.param(
        *(
            "expr-meta/case when",
            ["a", "b", "c"],
            "struct<a:int,b:string,c:string>",
            [{}, {}, {"k": "v"}],
            ["{'a': 1, 'b': 'x', 'c': 'small'}", "{'a': 2, 'b': 'y', 'c': 'big'}"],
        ),
        marks=_SAIL_BUG,
    ),
    (
        "expr/coalesce",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 2}"],
    ),
    (
        "expr-replace/coalesce",
        ["a", "b"],
        "struct<a:int,b:string>",
        [{}, {}],
        ["{'a': 1, 'b': 'x'}", "{'a': 2, 'b': 'y'}"],
    ),
    pytest.param(
        *(
            "expr-meta/coalesce",
            ["a", "b", "c"],
            "struct<a:int,b:string,c:int>",
            [{}, {}, {"k": "v"}],
            ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 2}"],
        ),
        marks=_SAIL_BUG,
    ),
    (
        "expr/nested field",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 2}"],
    ),
    (
        "expr-replace/nested field",
        ["a", "b"],
        "struct<a:int,b:string>",
        [{}, {}],
        ["{'a': 1, 'b': 'x'}", "{'a': 2, 'b': 'y'}"],
    ),
    pytest.param(
        *(
            "expr-meta/nested field",
            ["a", "b", "c"],
            "struct<a:int,b:string,c:int>",
            [{}, {}, {"k": "v"}],
            ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 2}"],
        ),
        marks=_SAIL_BUG,
    ),
    (
        "expr/window function",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 2}"],
    ),
    (
        "expr-replace/window function",
        ["a", "b"],
        "struct<a:int,b:string>",
        [{}, {}],
        ["{'a': 1, 'b': 'x'}", "{'a': 2, 'b': 'y'}"],
    ),
    (
        "expr-meta/window function",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 2}"],
    ),
    (
        "expr/nondeterministic",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': 0}", "{'a': 2, 'b': 'y', 'c': 0}"],
    ),
    (
        "expr-replace/nondeterministic",
        ["a", "b"],
        "struct<a:int,b:string>",
        [{}, {}],
        ["{'a': 0, 'b': 'x'}", "{'a': 0, 'b': 'y'}"],
    ),
    pytest.param(
        *(
            "expr-meta/nondeterministic",
            ["a", "b", "c"],
            "struct<a:int,b:string,c:int>",
            [{}, {}, {"k": "v"}],
            ["{'a': 1, 'b': 'x', 'c': 0}", "{'a': 2, 'b': 'y', 'c': 0}"],
        ),
        marks=_SAIL_BUG,
    ),
    (
        "expr/cast of itself",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:string>",
        [{}, {}, {}],
        ["{'a': 1, 'b': 'x', 'c': '1'}", "{'a': 2, 'b': 'y', 'c': '2'}"],
    ),
    (
        "expr-replace/cast of itself",
        ["a", "b"],
        "struct<a:string,b:string>",
        [{}, {}],
        ["{'a': '1', 'b': 'x'}", "{'a': '2', 'b': 'y'}"],
    ),
    pytest.param(
        *(
            "expr-meta/cast of itself",
            ["a", "b", "c"],
            "struct<a:int,b:string,c:string>",
            [{}, {}, {"k": "v"}],
            ["{'a': 1, 'b': 'x', 'c': '1'}", "{'a': 2, 'b': 'y', 'c': '2'}"],
        ),
        marks=_SAIL_BUG,
    ),
]

# (case, error condition)
METADATA_ERRORS = [
    pytest.param(*("expr/aggregate function", "MISSING_GROUP_BY"), marks=_SAIL_BUG),
    pytest.param(*("expr-replace/aggregate function", "MISSING_GROUP_BY"), marks=_SAIL_BUG),
    pytest.param(*("expr-meta/aggregate function", "MISSING_GROUP_BY"), marks=_SAIL_BUG),
]


@pytest.mark.parametrize(("case", "columns", "schema", "metadata", "rows"), METADATA_RESULTS)
def test_metadata_or_expression_result(spark, case, columns, schema, metadata, rows):
    # The session time zone decides which instant a cast string maps to, so it is pinned; the
    # rendering of that instant is handled by `_normalise`.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        df = _metadata_and_expression_cases(spark)[case]()

        assert df.columns == columns
        assert df.schema.simpleString() == schema
        assert [dict(field.metadata) for field in df.schema.fields] == metadata
        assert _rows(df) == rows
    finally:
        spark.conf.unset("spark.sql.session.timeZone")


@pytest.mark.parametrize(("case", "condition"), METADATA_ERRORS)
def test_metadata_or_expression_error(spark, case, condition):
    with pytest.raises(Exception, match=condition):
        _ = _metadata_and_expression_cases(spark)[case]().collect()


# The shape of the name itself. A name reaches the analyzer as a string that is split on dots
# outside of backticks, not parsed as SQL, so nothing is folded and no whitespace is skipped: the
# cases below are the ones where a SQL parser and that splitter part ways.
NAMES = {
    "empty": "",
    "single space": " ",
    "leading and trailing space": " a ",
    "inner space": "a b",
    "dot": "a.b",
    "backtick": "a`b",
    "quote": 'a"b',
    "backslash": "a\\b",
    "newline": "a\nb",
    "tab": "a\tb",
    "comma": "a,b",
    "parenthesis": "a(b)",
    "digits only": "1",
    "leading digit": "1a",
    "sql keyword": "select",
    "sql keyword upper": "SELECT",
    "underscore": "_a",
    "very long": "a" * 300,
    "emoji": "😀",
    "cjk": "中文",
    # The same text as one code point and as base plus combining accent. A matcher that folds
    # case does not normalise, so these are two different names.
    "nfc accent": "é",
    "nfd accent": "é",
    "turkish dotted capital": "İ",
    "turkish dotless lower": "ı",
    "german sharp s": "ß",
    "capital sharp s": "ẞ",
    "greek final sigma": "ς",
    "greek sigma": "σ",
    "kelvin sign": "K",
    "long s": "ſ",
}


def _name_cases(spark):
    cases = {}
    for name, value in NAMES.items():
        cases[f"add/{name}"] = lambda v=value: spark.sql("SELECT 1 AS a").withColumn(v, lit(9))
        cases[f"rename/{name}"] = lambda v=value: spark.sql("SELECT 1 AS a").withColumnRenamed("a", v)
        cases[f"replace/{name}"] = lambda v=value: (
            spark.sql("SELECT 1 AS a").withColumnRenamed("a", v).withColumn(v, lit(9))
        )
        cases[f"metadata/{name}"] = lambda v=value: (
            spark.sql("SELECT 1 AS a").withColumnRenamed("a", v).withMetadata(v, {"k": "v"})
        )
    return cases


# (case, columns, rows)
NAME_RESULTS = [
    ("add/empty", ["a", ""], ["{'a': 1, '': 9}"]),
    ("rename/empty", [""], ["{'': 1}"]),
    ("replace/empty", [""], ["{'': 9}"]),
    ("metadata/empty", [""], ["{'': 1}"]),
    ("add/single space", ["a", " "], ["{'a': 1, ' ': 9}"]),
    ("rename/single space", [" "], ["{' ': 1}"]),
    ("replace/single space", [" "], ["{' ': 9}"]),
    ("metadata/single space", [" "], ["{' ': 1}"]),
    ("add/leading and trailing space", ["a", " a "], ["{'a': 1, ' a ': 9}"]),
    ("rename/leading and trailing space", [" a "], ["{' a ': 1}"]),
    ("replace/leading and trailing space", [" a "], ["{' a ': 9}"]),
    ("metadata/leading and trailing space", [" a "], ["{' a ': 1}"]),
    ("add/inner space", ["a", "a b"], ["{'a': 1, 'a b': 9}"]),
    ("rename/inner space", ["a b"], ["{'a b': 1}"]),
    ("replace/inner space", ["a b"], ["{'a b': 9}"]),
    ("metadata/inner space", ["a b"], ["{'a b': 1}"]),
    ("add/dot", ["a", "a.b"], ["{'a': 1, 'a.b': 9}"]),
    ("rename/dot", ["a.b"], ["{'a.b': 1}"]),
    ("replace/dot", ["a.b"], ["{'a.b': 9}"]),
    ("add/backtick", ["a", "a`b"], ["{'a': 1, 'a`b': 9}"]),
    ("rename/backtick", ["a`b"], ["{'a`b': 1}"]),
    ("replace/backtick", ["a`b"], ["{'a`b': 9}"]),
    ("add/quote", ["a", 'a"b'], ["{'a': 1, 'a\"b': 9}"]),
    ("rename/quote", ['a"b'], ["{'a\"b': 1}"]),
    ("replace/quote", ['a"b'], ["{'a\"b': 9}"]),
    ("metadata/quote", ['a"b'], ["{'a\"b': 1}"]),
    ("add/backslash", ["a", "a\\b"], ["{'a': 1, 'a\\\\b': 9}"]),
    ("rename/backslash", ["a\\b"], ["{'a\\\\b': 1}"]),
    ("replace/backslash", ["a\\b"], ["{'a\\\\b': 9}"]),
    ("metadata/backslash", ["a\\b"], ["{'a\\\\b': 1}"]),
    ("add/newline", ["a", "a\nb"], ["{'a': 1, 'a\\nb': 9}"]),
    ("rename/newline", ["a\nb"], ["{'a\\nb': 1}"]),
    ("replace/newline", ["a\nb"], ["{'a\\nb': 9}"]),
    ("metadata/newline", ["a\nb"], ["{'a\\nb': 1}"]),
    ("add/tab", ["a", "a\tb"], ["{'a': 1, 'a\\tb': 9}"]),
    ("rename/tab", ["a\tb"], ["{'a\\tb': 1}"]),
    ("replace/tab", ["a\tb"], ["{'a\\tb': 9}"]),
    ("metadata/tab", ["a\tb"], ["{'a\\tb': 1}"]),
    ("add/comma", ["a", "a,b"], ["{'a': 1, 'a,b': 9}"]),
    ("rename/comma", ["a,b"], ["{'a,b': 1}"]),
    ("replace/comma", ["a,b"], ["{'a,b': 9}"]),
    ("metadata/comma", ["a,b"], ["{'a,b': 1}"]),
    ("add/parenthesis", ["a", "a(b)"], ["{'a': 1, 'a(b)': 9}"]),
    ("rename/parenthesis", ["a(b)"], ["{'a(b)': 1}"]),
    ("replace/parenthesis", ["a(b)"], ["{'a(b)': 9}"]),
    ("metadata/parenthesis", ["a(b)"], ["{'a(b)': 1}"]),
    ("add/digits only", ["a", "1"], ["{'a': 1, '1': 9}"]),
    ("rename/digits only", ["1"], ["{'1': 1}"]),
    ("replace/digits only", ["1"], ["{'1': 9}"]),
    ("metadata/digits only", ["1"], ["{'1': 1}"]),
    ("add/leading digit", ["a", "1a"], ["{'a': 1, '1a': 9}"]),
    ("rename/leading digit", ["1a"], ["{'1a': 1}"]),
    ("replace/leading digit", ["1a"], ["{'1a': 9}"]),
    ("metadata/leading digit", ["1a"], ["{'1a': 1}"]),
    ("add/sql keyword", ["a", "select"], ["{'a': 1, 'select': 9}"]),
    ("rename/sql keyword", ["select"], ["{'select': 1}"]),
    ("replace/sql keyword", ["select"], ["{'select': 9}"]),
    ("metadata/sql keyword", ["select"], ["{'select': 1}"]),
    ("add/sql keyword upper", ["a", "SELECT"], ["{'a': 1, 'SELECT': 9}"]),
    ("rename/sql keyword upper", ["SELECT"], ["{'SELECT': 1}"]),
    ("replace/sql keyword upper", ["SELECT"], ["{'SELECT': 9}"]),
    ("metadata/sql keyword upper", ["SELECT"], ["{'SELECT': 1}"]),
    ("add/underscore", ["a", "_a"], ["{'a': 1, '_a': 9}"]),
    ("rename/underscore", ["_a"], ["{'_a': 1}"]),
    ("replace/underscore", ["_a"], ["{'_a': 9}"]),
    ("metadata/underscore", ["_a"], ["{'_a': 1}"]),
    (
        "add/very long",
        [
            "a",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ],
        [
            "{'a': 1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa': 9}"
        ],
    ),
    (
        "rename/very long",
        [
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ],
        [
            "{'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa': 1}"
        ],
    ),
    (
        "replace/very long",
        [
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ],
        [
            "{'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa': 9}"
        ],
    ),
    (
        "metadata/very long",
        [
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ],
        [
            "{'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa': 1}"
        ],
    ),
    ("add/emoji", ["a", "😀"], ["{'a': 1, '😀': 9}"]),
    ("rename/emoji", ["😀"], ["{'😀': 1}"]),
    ("replace/emoji", ["😀"], ["{'😀': 9}"]),
    ("metadata/emoji", ["😀"], ["{'😀': 1}"]),
    ("add/cjk", ["a", "中文"], ["{'a': 1, '中文': 9}"]),
    ("rename/cjk", ["中文"], ["{'中文': 1}"]),
    ("replace/cjk", ["中文"], ["{'中文': 9}"]),
    ("metadata/cjk", ["中文"], ["{'中文': 1}"]),
    ("add/nfc accent", ["a", "é"], ["{'a': 1, 'é': 9}"]),
    ("rename/nfc accent", ["é"], ["{'é': 1}"]),
    ("replace/nfc accent", ["é"], ["{'é': 9}"]),
    ("metadata/nfc accent", ["é"], ["{'é': 1}"]),
    ("add/nfd accent", ["a", "é"], ["{'a': 1, 'é': 9}"]),
    ("rename/nfd accent", ["é"], ["{'é': 1}"]),
    ("replace/nfd accent", ["é"], ["{'é': 9}"]),
    ("metadata/nfd accent", ["é"], ["{'é': 1}"]),
    ("add/turkish dotted capital", ["a", "İ"], ["{'a': 1, 'İ': 9}"]),
    ("rename/turkish dotted capital", ["İ"], ["{'İ': 1}"]),
    ("replace/turkish dotted capital", ["İ"], ["{'İ': 9}"]),
    ("metadata/turkish dotted capital", ["İ"], ["{'İ': 1}"]),
    ("add/turkish dotless lower", ["a", "ı"], ["{'a': 1, 'ı': 9}"]),
    ("rename/turkish dotless lower", ["ı"], ["{'ı': 1}"]),
    ("replace/turkish dotless lower", ["ı"], ["{'ı': 9}"]),
    ("metadata/turkish dotless lower", ["ı"], ["{'ı': 1}"]),
    ("add/german sharp s", ["a", "ß"], ["{'a': 1, 'ß': 9}"]),
    ("rename/german sharp s", ["ß"], ["{'ß': 1}"]),
    ("replace/german sharp s", ["ß"], ["{'ß': 9}"]),
    ("metadata/german sharp s", ["ß"], ["{'ß': 1}"]),
    ("add/capital sharp s", ["a", "ẞ"], ["{'a': 1, 'ẞ': 9}"]),
    ("rename/capital sharp s", ["ẞ"], ["{'ẞ': 1}"]),
    ("replace/capital sharp s", ["ẞ"], ["{'ẞ': 9}"]),
    ("metadata/capital sharp s", ["ẞ"], ["{'ẞ': 1}"]),
    ("add/greek final sigma", ["a", "ς"], ["{'a': 1, 'ς': 9}"]),
    ("rename/greek final sigma", ["ς"], ["{'ς': 1}"]),
    ("replace/greek final sigma", ["ς"], ["{'ς': 9}"]),
    ("metadata/greek final sigma", ["ς"], ["{'ς': 1}"]),
    ("add/greek sigma", ["a", "σ"], ["{'a': 1, 'σ': 9}"]),
    ("rename/greek sigma", ["σ"], ["{'σ': 1}"]),
    ("replace/greek sigma", ["σ"], ["{'σ': 9}"]),
    ("metadata/greek sigma", ["σ"], ["{'σ': 1}"]),
    ("add/kelvin sign", ["a", "K"], ["{'a': 1, 'K': 9}"]),
    ("rename/kelvin sign", ["K"], ["{'K': 1}"]),
    ("replace/kelvin sign", ["K"], ["{'K': 9}"]),
    ("metadata/kelvin sign", ["K"], ["{'K': 1}"]),
    ("add/long s", ["a", "ſ"], ["{'a': 1, 'ſ': 9}"]),
    ("rename/long s", ["ſ"], ["{'ſ': 1}"]),
    ("replace/long s", ["ſ"], ["{'ſ': 9}"]),
    ("metadata/long s", ["ſ"], ["{'ſ': 1}"]),
]

# (case, error condition)
NAME_ERRORS = [
    _error_param("metadata/dot", "CANNOT_RESOLVE_DATAFRAME_COLUMN"),
    _error_param("metadata/backtick", "INVALID_ATTRIBUTE_NAME_SYNTAX"),
]


@pytest.mark.parametrize(("case", "columns", "rows"), NAME_RESULTS)
def test_column_name_shape_result(spark, case, columns, rows):
    df = _name_cases(spark)[case]()

    assert df.columns == columns
    assert _rows(df) == rows


@pytest.mark.parametrize(("case", "condition"), NAME_ERRORS)
def test_column_name_shape_error(spark, case, condition):
    with pytest.raises(Exception, match=condition):
        _ = _name_cases(spark)[case]().collect()


# The branches of the attribute-name parser, reached through `col()`. A name written by the client
# is split on dots outside of backticks; each case below is one decision of that splitter, and the
# expected value was measured against Spark.
PARSER = {
    # A dot inside backticks is part of the name, not a separator.
    "quoted dot": ("a.b", "`a.b`"),
    # A doubled backtick inside a quoted part is one literal backtick. After the change this is
    # the only spelling that reaches a column named with a backtick.
    "escaped backtick": ("a`b", "`a``b`"),
    # Quoted parts joined by a dot.
    "two quoted parts": ("a b", "`a b`"),
    # Errors.
    "unterminated backtick": ("a", "`a"),
    "backtick after text": ("a", "a`b"),
    "backtick then text": ("a", "`a`b"),
    "leading dot": ("a", ".a"),
    "trailing dot": ("a", "a."),
    "double dot": ("a", "a..b"),
    "only a dot": ("a", "."),
    "only backticks": ("a", "``"),
}


def _parser_cases(spark):
    def named(name):
        # The rename passes the string through untouched, so the column is literally `name`.
        return spark.sql("SELECT 1 AS a").withColumnRenamed("a", name)

    cases = {name: (lambda c=column, p=probe: named(c).select(col(p))) for name, (column, probe) in PARSER.items()}
    cases["three parts"] = lambda: spark.sql("SELECT named_struct('b', named_struct('c', 1)) AS a").select(col("a.b.c"))
    return cases


# The pairs where two spellings may or may not be the same name. Each case creates the column with
# one spelling and adds a column with the other, so the two are put in the same frame and the
# matching rule decides whether the column is replaced or appended. A case that probes a column
# with its own name would pass under any rule.
FOLDING = {
    "umlaut": ("ä", "Ä"),
    "dotless i": ("ıd", "Id"),
    "dotted capital I": ("İ", "i"),
    "final sigma": ("ς", "Σ"),
    "sharp s": ("ß", "ẞ"),
    "kelvin sign": ("K", "k"),
    "long s": ("ſ", "s"),
    # Written with escapes: the same text as one code point and as base plus combining accent.
    "nfc versus nfd": ("\u00e9", "e\u0301"),
    "ascii control": ("a", "A"),
}


def _folding_cases(spark):
    def named(name):
        return spark.sql("SELECT 1 AS a").withColumnRenamed("a", name)

    return {
        name: (lambda left=left, right=right: named(left).withColumn(right, lit(9)))
        for name, (left, right) in FOLDING.items()
    }


# (case, columns, rows)
PARSER_RESULTS = [
    ("quoted dot", ["a.b"], ["{'a.b': 1}"]),
    ("escaped backtick", ["a`b"], ["{'a`b': 1}"]),
    ("two quoted parts", ["a b"], ["{'a b': 1}"]),
    ("three parts", ["c"], ["{'c': 1}"]),
]

# (case, error condition)
PARSER_ERRORS = [
    _error_param("unterminated backtick", "INVALID_ATTRIBUTE_NAME_SYNTAX"),
    _error_param("backtick after text", "INVALID_ATTRIBUTE_NAME_SYNTAX"),
    _error_param("backtick then text", "INVALID_ATTRIBUTE_NAME_SYNTAX"),
    _error_param("leading dot", "INVALID_ATTRIBUTE_NAME_SYNTAX"),
    _error_param("trailing dot", "INVALID_ATTRIBUTE_NAME_SYNTAX"),
    _error_param("double dot", "INVALID_ATTRIBUTE_NAME_SYNTAX"),
    _error_param("only a dot", "INVALID_ATTRIBUTE_NAME_SYNTAX"),
    ("only backticks", "UNRESOLVED_COLUMN.WITH_SUGGESTION"),
]

# (case, caseSensitive, columns, rows)
FOLDING_RESULTS = [
    ("umlaut", "false", ["Ä"], ["{'Ä': 9}"]),
    ("dotless i", "false", ["Id"], ["{'Id': 9}"]),
    ("dotted capital I", "false", ["i"], ["{'i': 9}"]),
    ("final sigma", "false", ["Σ"], ["{'Σ': 9}"]),
    ("sharp s", "false", ["ẞ"], ["{'ẞ': 9}"]),
    ("kelvin sign", "false", ["k"], ["{'k': 9}"]),
    ("long s", "false", ["s"], ["{'s': 9}"]),
    ("nfc versus nfd", "false", ["é", "é"], ["{'é': 1, 'é': 9}"]),
    ("ascii control", "false", ["A"], ["{'A': 9}"]),
    ("umlaut", "true", ["ä", "Ä"], ["{'ä': 1, 'Ä': 9}"]),
    ("dotless i", "true", ["ıd", "Id"], ["{'ıd': 1, 'Id': 9}"]),
    ("dotted capital I", "true", ["İ", "i"], ["{'İ': 1, 'i': 9}"]),
    ("final sigma", "true", ["ς", "Σ"], ["{'ς': 1, 'Σ': 9}"]),
    ("sharp s", "true", ["ß", "ẞ"], ["{'ß': 1, 'ẞ': 9}"]),
    ("kelvin sign", "true", ["K", "k"], ["{'K': 1, 'k': 9}"]),
    ("long s", "true", ["ſ", "s"], ["{'ſ': 1, 's': 9}"]),
    ("nfc versus nfd", "true", ["é", "é"], ["{'é': 1, 'é': 9}"]),
    ("ascii control", "true", ["a", "A"], ["{'a': 1, 'A': 9}"]),
]


@pytest.mark.parametrize(("case", "columns", "rows"), PARSER_RESULTS)
def test_attribute_name_parser_result(spark, case, columns, rows):
    df = _parser_cases(spark)[case]()

    assert df.columns == columns
    assert _rows(df) == rows


@pytest.mark.parametrize(("case", "condition"), PARSER_ERRORS)
def test_attribute_name_parser_error(spark, case, condition):
    with pytest.raises(Exception, match=condition):
        _ = _parser_cases(spark)[case]().collect()


@pytest.mark.parametrize(("case", "case_sensitive", "columns", "rows"), FOLDING_RESULTS)
def test_two_spellings_of_one_name(spark, case, case_sensitive, columns, rows):
    _configure(spark, case_sensitive)
    try:
        df = _folding_cases(spark)[case]()
        assert df.columns == columns
        assert _rows(df) == rows
    finally:
        _unconfigure(spark)
