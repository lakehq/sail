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

import contextlib
import datetime
import re
import uuid

import pytest
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.functions import col, expr, lit, lower, row_number
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window

from pysail.testing.spark.utils.common import is_jvm_spark, pyspark_version

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
        "with_column_generator_one_field": lambda: base().withColumn("z", expr("inline(array(named_struct('x', a)))")),
        "with_column_generator_array": lambda: base().withColumn("z", expr("explode(array(a, b))")),
        "with_column_generator_stack": lambda: base().withColumn("z", expr("stack(2, a, b)")),
        "with_column_generator_two_fields": lambda: base().withColumn(
            "z", expr("inline(array(named_struct('x', a, 'y', b)))")
        ),
        "with_column_generator_posexplode": lambda: base().withColumn("z", expr("posexplode(array(a, b))")),
        "with_column_generator_json_tuple": lambda: base().withColumn(
            "z", expr("""json_tuple('{"k":1,"j":2}', 'k', 'j')""")
        ),
        "with_column_generator_in_expression": lambda: base().withColumn("z", expr("explode(array(a, b)) + 1")),
        "with_column_generator_in_function": lambda: base().withColumn("z", expr("abs(explode(array(a)))")),
        "with_column_generator_nested": lambda: base().withColumn("z", expr("explode(array(explode(array(a))))")),
        "with_column_star": lambda: base().withColumn("z", expr("*")),
        "with_column_qualified_star": lambda: base().alias("q").withColumn("z", expr("q.*")),
        "with_columns_star": lambda: base().withColumns({"z": expr("*")}),
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
        "with_metadata_cleared": lambda: base().withMetadata("a", {"k": "v"}).withMetadata("a", {}),
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
    # A generator that outputs one column takes the name it is given.
    ("with_column_generator_one_field", "false", ["a", "b", "z"], ["{'a': 1, 'b': 2, 'z': 1}"], [{}, {}, {}]),
    (
        "with_column_generator_array",
        "false",
        ["a", "b", "z"],
        ["{'a': 1, 'b': 2, 'z': 1}", "{'a': 1, 'b': 2, 'z': 2}"],
        [{}, {}, {}],
    ),
    (
        "with_column_generator_stack",
        "false",
        ["a", "b", "z"],
        ["{'a': 1, 'b': 2, 'z': 1}", "{'a': 1, 'b': 2, 'z': 2}"],
        [{}, {}, {}],
    ),
    ("with_metadata_replaces", "false", ["a", "b"], ["{'a': 1, 'b': 2}"], [{"j": "w"}, {}]),
    ("with_metadata_replaces", "true", ["a", "b"], ["{'a': 1, 'b': 2}"], [{"j": "w"}, {}]),
    # An empty map is how metadata is removed, since `withMetadata` replaces it (issue #1815).
    ("with_metadata_cleared", "false", ["a", "b"], ["{'a': 1, 'b': 2}"], [{}, {}]),
    ("with_metadata_cleared", "true", ["a", "b"], ["{'a': 1, 'b': 2}"], [{}, {}]),
]

# (case, caseSensitive, error condition)
ERRORS = [
    ("with_columns_entries_differing_in_case", "false", "COLUMN_ALREADY_EXISTS"),
    _error_param("with_metadata_differing_case", "true", "CANNOT_RESOLVE_DATAFRAME_COLUMN"),
    _error_param("with_metadata_unknown_name", "false", "CANNOT_RESOLVE_DATAFRAME_COLUMN"),
    _error_param("with_metadata_unknown_name", "true", "CANNOT_RESOLVE_DATAFRAME_COLUMN"),
    _error_param("with_metadata_non_ascii", "true", "CANNOT_RESOLVE_DATAFRAME_COLUMN"),
    # A generator is only a column of its own, never a part of an expression.
    _error_param("with_column_generator_in_expression", "false", "UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS"),
    _error_param("with_column_generator_in_function", "false", "UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS"),
    _error_param("with_column_generator_nested", "false", "UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS"),
    # One name is not enough for a generator that outputs more than one column.
    _error_param("with_column_generator_two_fields", "false", "UDTF_ALIAS_NUMBER_MISMATCH"),
    _error_param("with_column_generator_posexplode", "false", "UDTF_ALIAS_NUMBER_MISMATCH"),
    _error_param("with_column_generator_json_tuple", "false", "UDTF_ALIAS_NUMBER_MISMATCH"),
    # A star is a whole list of columns, which one name cannot take.
    _error_param("with_column_star", "false", "INVALID_USAGE_OF_STAR_OR_REGEX"),
    _error_param("with_column_qualified_star", "false", "INVALID_USAGE_OF_STAR_OR_REGEX"),
    _error_param("with_columns_star", "false", "INVALID_USAGE_OF_STAR_OR_REGEX"),
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


# A column that comes out of an alias carries no qualifier, and the ones the input passes through
# keep theirs, whatever the operation builds around them. Measured on the Spark JVM.
_QUALIFIER_AFTER_WITH_COLUMN = [
    (lambda df: df.withColumn("c", lit(9)), "q.c", None),
    (lambda df: df.withColumn("c", lit(9)), "q.a", ["a"]),
    (lambda df: df.withColumn("a", lit(9)), "q.a", None),
    (lambda df: df.withMetadata("a", {"k": "v"}), "q.a", None),
    (lambda df: df.withColumns({"c": lit(9), "d": lit(8)}), "q.b", ["b"]),
    (lambda df: df.withColumn("c", lit(9)).withColumn("d", col("q.a")), "q.a", ["a"]),
    # A column copied out of another one is a new name too, however plain its expression reads.
    (lambda df: df.withColumn("c", col("a")), "q.c", None),
    (lambda df: df.withColumn("a", col("a")), "q.a", None),
    (lambda df: df.withColumn("c", col("a")), "q.b", ["b"]),
]

# Every column an aggregate outputs is built by it, so none of them keeps a qualifier either.
_QUALIFIER_AFTER_AGGREGATE = [("q.a", None), ("a", ["a"])]


@pytest.mark.parametrize(("operation", "name", "columns"), _QUALIFIER_AFTER_WITH_COLUMN)
def test_the_qualifier_a_column_keeps(spark, operation, name, columns):
    df = spark.sql("SELECT 1 AS a, 2 AS b").alias("q")

    if columns is None:
        with pytest.raises(Exception, match=r"UNRESOLVED_COLUMN\.WITH_SUGGESTION"):
            _ = operation(df).select(name).collect()
    else:
        assert operation(df).select(name).columns == columns


@pytest.mark.parametrize(("name", "columns"), _QUALIFIER_AFTER_AGGREGATE)
def test_the_qualifier_an_aggregate_does_not_keep(spark, name, columns):
    df = spark.sql("SELECT 1 AS a, 2 AS b").select("a").alias("q").withColumn("a", spark_sum("a"))

    if columns is None:
        with pytest.raises(Exception, match=r"UNRESOLVED_COLUMN\.WITH_SUGGESTION"):
            _ = df.select(name).collect()
    else:
        assert df.select(name).columns == columns


def test_an_added_column_carries_no_qualifier(spark):
    # The star expansion returns the input's own attributes for the columns it passes through, so
    # those keep their qualifier, while a column the projection adds is an alias with none.
    df = spark.sql("SELECT 1 AS a, 2 AS b").alias("x")

    assert df.withColumn("c", lit(1)).select("x.a").columns == ["a"]
    with pytest.raises(Exception, match=r"UNRESOLVED_COLUMN\.WITH_SUGGESTION"):
        _ = df.withColumn("c", lit(1)).select("x.c").collect()


@pytest.mark.parametrize(
    ("expression", "data_type", "nullable", "inner"),
    [
        # The two that agree, as the controls: a fix that made every added column nullable, or
        # none of them, would still have to pass these.
        ("1", "int", False, None),
        ("a", "int", False, None),
        pytest.param(
            "CAST(1 AS DECIMAL(10,2))",
            "decimal(10,2)",
            True,
            None,
            marks=pytest.mark.xfail(not is_jvm_spark(), reason="Known Sail bug", strict=True),
        ),
        ("map('k', 1)", "map<string,int>", False, None),
        # A case is NULL where it falls through to no branch, so the `ELSE` is what makes it not
        # nullable, and the condition never counts (`CaseWhen.nullable`).
        ("CASE WHEN a > 1 THEN 'big' ELSE 'small' END", "string", False, None),
        ("CASE WHEN a > 1 THEN 'big' END", "string", True, None),
        ("CASE WHEN a > 1 THEN 'big' ELSE NULL END", "string", True, None),
        ("CASE WHEN b = 'x' THEN 1 ELSE 2 END", "int", False, None),
        ("IF(a > 1, 'big', 'small')", "string", False, None),
        ("COALESCE(a, 0)", "int", False, None),
        # The containers, whose inner flag `simpleString()` hides as well.
        ("array(1, 2)", "array<int>", False, False),
        ("named_struct('n', 1)", "struct<n:int>", False, False),
    ],
)
def test_an_added_column_reports_the_nullability_of_its_expression(spark, expression, data_type, nullable, inner):
    # The matrices above compare `schema.simpleString()`, which renders neither `nullable` nor the
    # nullability inside a container, so this is the only place the flag is asserted. The column is
    # added with `withColumn`, so what is measured is the alias `UnresolvedStarWithColumns` builds
    # rather than a projection that never reaches it.
    df = spark.sql("SELECT * FROM VALUES (1, 'x'), (2, 'y') AS t(a, b)").withColumn("c", F.expr(expression))
    field = df.schema["c"]

    assert field.dataType.simpleString() == data_type
    assert field.nullable == nullable
    if inner is not None:
        # A container carries a second flag, for its element or its field, that the type string
        # renders no more than the root one.
        fields = getattr(field.dataType, "fields", None)
        actual = [x.nullable for x in fields] if fields else [field.dataType.containsNull]
        assert actual == [inner] * len(actual)


def _annotated(spark):
    """A column that the projection passes through, annotated with metadata."""
    return spark.sql("SELECT * FROM VALUES (1, 'x'), (2, 'y') AS t(a, b)").withMetadata("a", {"k": "v"})


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


def test_a_column_the_projection_reads_keeps_the_field_metadata_it_does_not_own(spark, tmp_path):
    # `withColumn` overrides the Spark metadata of the column it builds, defaulting to empty. The
    # field it reads may carry metadata that is not part of what Spark reports -- the comment of a
    # table column is stored as one -- and the override is added on top of what the field already
    # has rather than replacing it, so attaching it there makes the logical schema differ from the
    # physical one and the plan fails once it has to produce rows.
    location = tmp_path / "commented"
    spark.sql(f"CREATE TABLE commented (id INT, a STRING COMMENT 'hello') USING parquet LOCATION '{location}'")
    try:
        spark.sql("INSERT INTO commented VALUES (1, 'x')")
        replaced = spark.table("commented").withColumn("a", col("a"))
        added = spark.table("commented").withColumn("c", col("a"))

        # The column the projection replaces, and a new one built from the same field: whatever the
        # field underneath carries, the metadata Spark reports for either is empty.
        assert [row.asDict() for row in replaced.collect()] == [{"id": 1, "a": "x"}]
        assert replaced.schema["a"].metadata == {}
        assert [row.asDict() for row in added.collect()] == [{"id": 1, "a": "x", "c": "x"}]
        assert added.schema["c"].metadata == {}
    finally:
        spark.sql("DROP TABLE IF EXISTS commented")


def _join_carrying_field_metadata(spark):
    # The metadata comes from the input schema rather than from `withMetadata`, and a `USING` join
    # passes the field through, so nothing in the plan asked for metadata.
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("a", StringType(), metadata={"k": "v"}),
        ]
    )
    left = spark.createDataFrame([(1, "a1")], schema)
    right = spark.createDataFrame([(1, "b1")], "id int, b string")
    return left.join(right, "id")


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("a", [{"id": 1, "a": "a1", "b": "b1"}]),
        ("c", [{"id": 1, "a": "a1", "b": "b1", "c": "a1"}]),
    ],
)
def test_a_column_carrying_field_metadata_can_be_replaced_or_copied(spark, name, expected):
    # Whatever `withColumn` does with the metadata, the rows still have to come out: the override
    # rides on an alias over a plain column reference, and an override that reaches the schema but
    # not the physical projection makes the plan fail once it has to produce rows.
    df = _join_carrying_field_metadata(spark).withColumn(name, col("a"))

    assert [row.asDict() for row in df.collect()] == expected


def test_a_replacement_clears_the_metadata_a_using_join_passed_through(spark):
    # The same shape as `test_metadata_on_a_passed_through_column_reaches_collect`: the override
    # rides on an alias over a plain column reference and reaches the schema but not the physical
    # projection. Here nothing asked for metadata -- the alias carries only the empty override that
    # `withColumn` always sets -- so no part of this is covered by the cases above.
    df = _join_carrying_field_metadata(spark).withColumn("a", col("a"))

    assert df.schema["a"].metadata == {}


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
    ("metadata_then_select", "false", ["A"], ["{'A': 1}", "{'A': 1}", "{'A': 2}"]),
    ("metadata_then_group_by", "false", ["a", "count"], ["{'a': 1, 'count': 2}", "{'a': 2, 'count': 1}"]),
    ("metadata_then_group_by", "true", ["a", "count"], ["{'a': 1, 'count': 2}", "{'a': 2, 'count': 1}"]),
]

# (case, caseSensitive, error condition)
COMPOSITION_ERRORS = [
    _error_param("replaced_then_union", "true", "NUM_COLUMNS_MISMATCH"),
    _error_param("replaced_then_union_by_name", "true", "UNRESOLVED_COLUMN_AMONG_FIELD_NAMES"),
    # The client decides whether the name carries a plan id, and that is what selects between the
    # two conditions Spark raises, so an older client reaches this through
    # `CANNOT_RESOLVE_DATAFRAME_COLUMN` instead.
    pytest.param(
        *("renamed_then_group_by", "true", "UNRESOLVED_COLUMN.WITH_SUGGESTION"),
        marks=pytest.mark.skipif(
            pyspark_version() < (4, 1), reason="The client stops sending a plan ID for `groupBy` from PySpark 4.1 on"
        ),
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


# A `df["name"]` reference is resolved against the plan node tagged with the id rather than against
# its output, so it keeps working after the column it names has been replaced or renamed away. The
# operator above decides whether it can: Spark's pull-up walks down through a `UnaryNode`, so
# `Filter` and `Sort` both reach the attribute below the projection, while `Project` and `Aggregate`
# consume the output and reject it. The three cases below are the ones where the answer differs
# between reading the original column and reading the one that replaced it.


# The row the reference selects. Its negation is a different row, which is what makes the
# filtered result tell the original column apart from the one that replaced it.
_SELECTED = 2


def _negated(spark):
    """A frame whose replacement negates the column, so the two answers order and filter apart."""
    df = spark.sql("SELECT * FROM VALUES (1), (2), (3) AS t(a)")
    return df, df.withColumn("A", -col("a"))


def test_a_sort_by_a_replaced_column_reads_the_original(spark):
    # The reference names the pre-replacement column, so ascending order follows `a` (1, 2, 3) and
    # the rows come out as -1, -2, -3. Sorting by the *name* instead resolves against the output,
    # which is the replacement, and ascending order follows `A` the other way round. The pair is
    # what makes either answer wrong for the other query.
    df, replaced = _negated(spark)

    assert [tuple(row) for row in replaced.orderBy(df["a"].asc()).collect()] == [(-1,), (-2,), (-3,)]
    assert [tuple(row) for row in replaced.orderBy(col("a").asc()).collect()] == [(-3,), (-2,), (-1,)]


def test_a_filter_by_a_replaced_column_reads_the_original(spark):
    # `Filter` is a `UnaryNode` like `Sort`, so the same reference resolves there too. Keeping the
    # row where the original `a` is 2 keeps the row whose replacement is -2, which is the answer
    # that tells the original apart from the replacement: filtering on `A` would keep nothing.
    df, replaced = _negated(spark)

    assert [tuple(row) for row in replaced.filter(df["a"] == _SELECTED).collect()] == [(-_SELECTED,)]

    # The same replacement spelled with the case the column already has. That spelling took the
    # column out of the output under the old exact matching too, so it is the one that shows the
    # limitation is older than the rewrite rather than a consequence of it.
    same_case = df.withColumn("a", -col("a"))
    assert [tuple(row) for row in same_case.filter(df["a"] == _SELECTED).collect()] == [(-_SELECTED,)]


# A key a `USING` join hid stays reachable by its qualified name while the new expression is
# resolved, and stays out of the output. Measured on the Spark JVM.
_HIDDEN_JOIN_KEY = [
    ("z", "r.a", ["a", "b", "c", "z"], [(1, 11, 22, 1)]),
    ("z", "l.a", ["a", "b", "c", "z"], [(1, 11, 22, 1)]),
    ("z", "r.c", ["a", "b", "c", "z"], [(1, 11, 22, 22)]),
    ("z", "a", ["a", "b", "c", "z"], [(1, 11, 22, 1)]),
    ("a", "r.a + 5", ["a", "b", "c"], [(6, 11, 22)]),
]


@pytest.mark.parametrize(("name", "expression", "columns", "rows"), _HIDDEN_JOIN_KEY)
def test_with_column_reads_a_key_the_join_hid(spark, name, expression, columns, rows):
    left = spark.sql("SELECT 1 AS a, 11 AS b").alias("l")
    right = spark.sql("SELECT 1 AS a, 22 AS c").alias("r")

    df = left.join(right, "a").withColumn(name, expr(expression))
    assert df.columns == columns
    assert [tuple(row) for row in df.collect()] == rows
    # The hidden key is not a column of the output, so a star does not expand to it.
    assert df.select("*").columns == columns


def test_with_columns_reads_the_keys_of_both_sides(spark):
    left = spark.sql("SELECT 1 AS a, 11 AS b").alias("l")
    right = spark.sql("SELECT 1 AS a, 22 AS c").alias("r")

    df = left.join(right, "a").withColumns({"z": col("r.a"), "y": col("l.a")})
    assert df.columns == ["a", "b", "c", "z", "y"]
    assert [tuple(row) for row in df.collect()] == [(1, 11, 22, 1, 1)]


def test_a_filter_by_a_renamed_column_reads_it_under_its_old_name(spark):
    # The same rule for a rename, where the values survive and only the name is gone.
    df = spark.sql("SELECT * FROM VALUES (1), (2), (3) AS t(a)")
    renamed = df.withColumnRenamed("a", "Z")

    assert renamed.columns == ["Z"]
    assert [tuple(row) for row in renamed.filter(df["a"] == _SELECTED).collect()] == [(_SELECTED,)]


def test_a_filter_by_a_column_the_projection_keeps_needs_no_pull_up(spark):
    # The control the three above need: what `Filter` cannot do is reach an attribute the
    # projection dropped, not resolve a `df["col"]` reference at all. When the projection keeps the
    # column there is nothing to pull up and both engines agree, so a fix that made every plan-id
    # reference in a filter resolve would satisfy the xfails above and still be wrong here.
    df = spark.sql("SELECT * FROM VALUES (1), (2), (3) AS t(a)")
    kept = df.withColumn("b", lit(9))

    assert [tuple(row) for row in kept.filter(df["a"] == _SELECTED).collect()] == [(_SELECTED, 9)]


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
    (
        "expr-meta/column reference",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 2}"],
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
    (
        "expr-meta/arithmetic",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': 2}", "{'a': 2, 'b': 'y', 'c': 3}"],
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
    (
        "expr-meta/case when",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:string>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': 'small'}", "{'a': 2, 'b': 'y', 'c': 'big'}"],
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
    (
        "expr-meta/coalesce",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 2}"],
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
    (
        "expr-meta/nested field",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': 1}", "{'a': 2, 'b': 'y', 'c': 2}"],
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
    (
        "expr-meta/nondeterministic",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:int>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': 0}", "{'a': 2, 'b': 'y', 'c': 0}"],
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
    (
        "expr-meta/cast of itself",
        ["a", "b", "c"],
        "struct<a:int,b:string,c:string>",
        [{}, {}, {"k": "v"}],
        ["{'a': 1, 'b': 'x', 'c': '1'}", "{'a': 2, 'b': 'y', 'c': '2'}"],
    ),
]

# (case, error condition)
METADATA_ERRORS = [
    ("expr/aggregate function", "MISSING_GROUP_BY"),
    ("expr-replace/aggregate function", "MISSING_GROUP_BY"),
    ("expr-meta/aggregate function", "MISSING_GROUP_BY"),
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
    # Whitespace is part of a name rather than something to skip, so ` a` is not the column `a`.
    # The SQL identifier grammar drops it, and would resolve the wrong column without an error.
    "leading space": ("a", " a"),
}


def _parser_cases(spark):
    def named(name):
        # The rename passes the string through untouched, so the column is literally `name`.
        return spark.sql("SELECT 1 AS a").withColumnRenamed("a", name)

    cases = {name: (lambda c=column, p=probe: named(c).select(col(p))) for name, (column, probe) in PARSER.items()}
    cases["three parts"] = lambda: spark.sql("SELECT named_struct('b', named_struct('c', 1)) AS a").select(col("a.b.c"))
    # A dot still separates parts when a part is not a valid SQL identifier. The SQL grammar cannot
    # parse `a.b c`, and falling back to the whole string made it one column named `a.b c`.
    cases["space after a dot"] = lambda: spark.sql("SELECT named_struct('b c', 1) AS a").select(col("a.b c"))
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
    ("space after a dot", ["b c"], ["{'b c': 1}"]),
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
    ("leading space", "UNRESOLVED_COLUMN.WITH_SUGGESTION"),
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


# The matrix above compares one name against another with the resolver, which is one of the two
# rules Spark folds identifiers with. The duplicate check lowercases instead, and an attribute
# reference has to satisfy both. The cases below are the ones where the rules disagree, so they
# are the only ones that tell which rule ran.

# `İ` lowercases to two code points, `i` plus a combining dot, so `toLowerCase` does not make it
# `i` -- but `Character.toUpperCase` maps both to `İ`, so `equalsIgnoreCase` does. Every other
# character in the matrix above folds the same way under both rules.
_DOTTED_CAPITAL_I = "\u0130"


def test_the_resolver_and_the_duplicate_check_fold_a_name_differently(spark):
    # `withColumn` matches with the resolver alone, so `İ` replaces the column named `i` -- that is
    # the "dotted capital I" row above. The duplicate check `UnresolvedStarWithColumns` runs first
    # lowercases instead, and the two names do not collide there, so the same pair of names is
    # accepted by `withColumns` and both columns are added. A single fold cannot produce both
    # answers.
    _configure(spark, "false")
    try:
        added = spark.sql("SELECT 1 AS a, 2 AS b").withColumns({"i": lit(1), _DOTTED_CAPITAL_I: lit(2)})

        assert added.columns == ["a", "b", "i", _DOTTED_CAPITAL_I]
        assert [row.asDict() for row in added.collect()] == [{"a": 1, "b": 2, "i": 1, _DOTTED_CAPITAL_I: 2}]

        # The contrast: `ẞ` lowercases to `ß`, so there the two rules agree and the pair is a
        # duplicate. Without this half, a fold that rejected everything would also pass.
        with pytest.raises(Exception, match="COLUMN_ALREADY_EXISTS"):
            _ = spark.sql("SELECT 1 AS a").withColumns({"\u00df": lit(1), "\u1e9e": lit(2)}).collect()
    finally:
        _unconfigure(spark)


def test_an_attribute_reference_has_to_satisfy_both_folds(spark):
    # `withMetadata` names the column through `self[columnName]`, an attribute reference, which
    # Spark looks up in a map keyed by the lowercased name before filtering the candidates with the
    # resolver. `İ` passes the resolver and fails the lookup, so the name that `withColumn`
    # resolves is one that `withMetadata` cannot.
    _configure(spark, "false")
    try:
        df = spark.sql("SELECT 1 AS i")

        assert df.withColumn(_DOTTED_CAPITAL_I, lit(9)).columns == [_DOTTED_CAPITAL_I]
        with pytest.raises(Exception, match="CANNOT_RESOLVE_DATAFRAME_COLUMN"):
            _ = df.withMetadata(_DOTTED_CAPITAL_I, {"k": "v"}).collect()
    finally:
        _unconfigure(spark)


# Vithkuqi was added in Unicode 14, and the JDK that runs Spark ships Unicode 13, so it has no case
# mapping there and the two letters are simply different characters. A fold built on newer tables
# knows the pair and would merge them.
_VITHKUQI_CAPITAL_A = "\U00010570"
_VITHKUQI_SMALL_A = "\U00010597"


def test_a_case_pair_the_jdk_does_not_know_is_not_folded(spark):
    # Both names survive, under a resolver that is folding every pair it does know.
    _configure(spark, "false")
    try:
        df = spark.sql(f"SELECT 1 AS `{_VITHKUQI_CAPITAL_A}`")
        added = df.withColumn(_VITHKUQI_SMALL_A, lit(9))

        assert added.columns == [_VITHKUQI_CAPITAL_A, _VITHKUQI_SMALL_A]
        assert [tuple(row) for row in added.collect()] == [(1, 9)]

        # And the rename finds nothing to rename, rather than renaming the capital.
        assert df.withColumnRenamed(_VITHKUQI_SMALL_A, "z").columns == [_VITHKUQI_CAPITAL_A]
    finally:
        _unconfigure(spark)


# An input that already names two columns the same. `UnresolvedStarWithColumns.expandStar` maps over
# EVERY expanded column, so a name that matches both replaces both rather than being ambiguous, and
# `drop` and the renames filter the same way. What the repeated name does to an expression that
# READS it depends on whether the two columns are one attribute or two: `AttributeSeq.resolve` calls
# `candidates.distinct` before it looks for an ambiguity, and two projections of one attribute are
# equal there, so only genuinely different attributes are ambiguous. Measured on the Spark JVM.


def _repeated(spark):
    """One attribute projected twice, so `a` names two columns that are the same attribute."""
    return spark.sql("SELECT a, a, b FROM VALUES (1, 'x') AS t(a, b)")


def _repeated_by_join(spark):
    """Two columns named `a` that are different attributes, one from each side of a join."""
    left = spark.sql("SELECT * FROM VALUES (1, 'x') AS t(a, b)")
    right = spark.sql("SELECT * FROM VALUES (1, 'y') AS t(a, c)")
    return left.join(right, left.a == right.a)


# (case, columns, rows)
_REPEATED_INPUT_RESULTS = [
    ("replace_both", ["a", "a", "b"], [(9, 9, "x")]),
    ("add_a_new_name", ["a", "a", "b", "z"], [(1, 1, "x", 9)]),
    ("with_columns", ["a", "a", "b", "z"], [(9, 9, "x", 8)]),
    ("drop", ["b"], [("x",)]),
    ("rename", ["z", "z", "b"], [(1, 1, "x")]),
    ("rename_onto_an_existing_name", ["a", "a", "a"], [(1, 1, "x")]),
    ("join_replace_both", ["a", "b", "a", "c"], [(9, "x", 9, "y")]),
    ("join_drop", ["b", "c"], [("x", "y")]),
    ("join_rename", ["z", "b", "z", "c"], [(1, "x", 1, "y")]),
]


def _repeated_input_cases(spark):
    return {
        # Every column the name matches is replaced, so both `a` take the new value.
        "replace_both": lambda: _repeated(spark).withColumn("a", lit(9)),
        # A name that matches nothing is appended and the repeated pair is left alone.
        "add_a_new_name": lambda: _repeated(spark).withColumn("z", lit(9)),
        "with_columns": lambda: _repeated(spark).withColumns({"a": lit(9), "z": lit(8)}),
        # `drop` and the renames compare the name the same way, over every column.
        "drop": lambda: _repeated(spark).drop("a"),
        "rename": lambda: _repeated(spark).withColumnRenamed("a", "z"),
        # A rename is not checked for duplicates, so this leaves three columns named `a`.
        "rename_onto_an_existing_name": lambda: _repeated(spark).withColumnRenamed("b", "a"),
        # The same three shapes where the two columns are different attributes.
        "join_replace_both": lambda: _repeated_by_join(spark).withColumn("a", lit(9)),
        "join_drop": lambda: _repeated_by_join(spark).drop("a"),
        "join_rename": lambda: _repeated_by_join(spark).withColumnRenamed("a", "z"),
    }


@pytest.mark.parametrize("case_sensitive", ["false", "true"])
@pytest.mark.parametrize(("case", "columns", "rows"), _REPEATED_INPUT_RESULTS)
def test_a_repeated_input_name_is_replaced_dropped_and_renamed_everywhere(spark, case, case_sensitive, columns, rows):
    _configure(spark, case_sensitive)
    try:
        result = _repeated_input_cases(spark)[case]()

        assert result.columns == columns
        assert [tuple(row) for row in result.collect()] == rows
    finally:
        _unconfigure(spark)


@pytest.mark.parametrize(
    ("case_sensitive", "columns", "rows"),
    [
        # The resolver matches the capital, so both columns are replaced under the new name.
        ("false", ["A", "A", "b"], [(9, 9, "x")]),
        # It does not, so neither is replaced and the capital is appended instead.
        ("true", ["a", "a", "b", "A"], [(1, 1, "x", 9)]),
    ],
)
def test_a_repeated_input_name_is_replaced_by_the_case_the_resolver_matches(spark, case_sensitive, columns, rows):
    _configure(spark, case_sensitive)
    try:
        result = _repeated(spark).withColumn("A", lit(9))

        assert result.columns == columns
        assert [tuple(row) for row in result.collect()] == rows
    finally:
        _unconfigure(spark)


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(lambda df: df.withColumn("a", col("a") + 1), id="with-column"),
        pytest.param(lambda df: df.select(col("a") + 1), id="select"),
    ],
)
def test_a_name_that_two_columns_of_one_attribute_carry_is_ambiguous_by_join_only(spark, operation):
    _configure(spark, "false")
    try:
        # Two different attributes of the same name are ambiguous, however the name is read.
        with pytest.raises(Exception, match=re.escape("[AMBIGUOUS_REFERENCE]")):
            operation(_repeated_by_join(spark)).collect()
    finally:
        _unconfigure(spark)


# An expression that reads the name answers it, because the two columns are one candidate. The
# last case is `withMetadata`, which is `withColumn(name, col(name), metadata)` and so reads the
# name as well.
_ONE_ATTRIBUTE_TWICE = [
    ("with_column", ["a", "a", "b"], [(2, 2, "x")]),
    ("select", ["(a + 1)"], [(2,)]),
    ("with_metadata", ["a", "a", "b"], [(1, 1, "x")]),
]


@pytest.mark.parametrize(("case", "columns", "rows"), _ONE_ATTRIBUTE_TWICE)
def test_an_expression_reads_a_name_two_columns_of_one_attribute_carry(spark, case, columns, rows):
    _configure(spark, "false")
    try:
        df = _repeated(spark)
        result = {
            "with_column": lambda: df.withColumn("a", col("a") + 1),
            "select": lambda: df.select(col("a") + 1),
            "with_metadata": lambda: df.withMetadata("a", {"k": "v"}),
        }[case]()

        assert result.columns == columns
        assert [tuple(row) for row in result.collect()] == rows
    finally:
        _unconfigure(spark)


def test_with_metadata_on_a_name_two_attributes_carry(spark):
    _configure(spark, "false")
    try:
        # What is asserted is that the name is refused as ambiguous. Spark reports the plain
        # condition here while Sail reports the one for a column of a DataFrame, because
        # `withMetadata` is `withColumn(name, col(name), metadata)` and the column it builds
        # carries a plan ID. The pattern therefore admits both spellings and nothing else, rather
        # than freezing a condition that is not the one Spark raises.
        with pytest.raises(Exception, match=r"\[AMBIGUOUS(_COLUMN)?_REFERENCE\]"):
            _repeated_by_join(spark).withMetadata("a", {"k": "v"}).collect()
    finally:
        _unconfigure(spark)


# The rule has to hold however the two outputs of one attribute were made, not only for the one
# projection that made them here: through a chain of projections, through the operators that pass
# their input on, and for more than two of them. A rename is the discriminating negative, since it
# gives the column a new attribute and the name really is ambiguous afterwards. Measured on the
# Spark JVM.
_ONE_ATTRIBUTE_SHAPES = [
    ("select_twice", ["a", "a", "b"], [(2, 2, "x")]),
    ("three_outputs", ["a", "a", "a"], [(2, 2, 2)]),
    ("chained_projections", ["a", "a"], [(2, 2)]),
    ("through_a_sort", ["a", "a", "b"], [(2, 2, "x")]),
    ("through_a_limit", ["a", "a", "b"], [(2, 2, "x")]),
    ("through_an_alias", ["a", "a", "b"], [(2, 2, "x")]),
    ("after_a_group_by", ["a", "a"], [(2, 2)]),
]


def _one_attribute_shapes(spark):
    source = spark.sql("SELECT * FROM VALUES (1, 'x') AS t(a, b)")
    read = lambda df: df.withColumn("a", col("a") + 1)  # noqa: E731

    return {
        "select_twice": lambda: read(source.select("a", "a", "b")),
        "three_outputs": lambda: read(source.select("a", "a", "a")),
        "chained_projections": lambda: read(source.select("a", "a", "b").select("a", "a")),
        "through_a_sort": lambda: read(source.select("a", "a", "b").sort("b")),
        "through_a_limit": lambda: read(source.select("a", "a", "b").limit(5)),
        "through_an_alias": lambda: read(source.select("a", "a", "b").alias("q")),
        "after_a_group_by": lambda: read(source.groupBy("a").count().select("a", "a")),
    }


@pytest.mark.parametrize(("case", "columns", "rows"), _ONE_ATTRIBUTE_SHAPES)
def test_one_attribute_under_several_outputs_however_it_was_made(spark, case, columns, rows):
    _configure(spark, "false")
    try:
        result = _one_attribute_shapes(spark)[case]()

        assert result.columns == columns
        assert [tuple(row) for row in result.collect()] == rows
    finally:
        _unconfigure(spark)


def test_a_rename_onto_an_existing_name_makes_the_name_ambiguous(spark):
    _configure(spark, "false")
    try:
        # The rename gives `b` an attribute of its own, so the two columns named `a` are two
        # attributes and reading the name is ambiguous, unlike selecting one column twice.
        renamed = spark.sql("SELECT * FROM VALUES (1, 'x') AS t(a, b)").withColumnRenamed("b", "a")

        with pytest.raises(Exception, match=re.escape("[AMBIGUOUS_REFERENCE]")):
            renamed.withColumn("a", col("a") + 1).collect()
    finally:
        _unconfigure(spark)


# A `Filter` is a `UnaryNode`, so its condition reaches an attribute the projection under it
# dropped: the attribute is pulled up for the condition and projected away again
# (`ResolveMissingReferences`). These are the shapes around that pull-up. Measured on the Spark JVM.
_PULLED_UP_FILTER = [
    ("reads_a_column_the_projection_kept_too", ["A", "b"], [(-_SELECTED, "y")]),
    ("two_filters_each_pulling_up", ["A", "b"], [(-_SELECTED, "y")]),
    ("a_star_after_the_filter", ["A", "b"], [(-_SELECTED, "y")]),
    ("over_an_aggregate", ["a", "count"], [(_SELECTED, 1)]),
]


def _pulled_up_filter_cases(spark):
    df = spark.sql("SELECT * FROM VALUES (1, 'x'), (2, 'y'), (3, 'z') AS t(a, b)")
    replaced = df.withColumn("A", -col("a"))

    return {
        # The condition names one column the projection dropped and one it kept.
        "reads_a_column_the_projection_kept_too": lambda: replaced.filter((df["a"] == _SELECTED) & (col("b") == "y")),
        "two_filters_each_pulling_up": lambda: replaced.filter(df["a"] >= _SELECTED).filter(df["a"] <= _SELECTED),
        # The column the condition needed must not reach the output, under a star either.
        "a_star_after_the_filter": lambda: replaced.filter(df["a"] == _SELECTED).select("*"),
        # An aggregate under the filter instead of a projection.
        "over_an_aggregate": lambda: df.groupBy("a").count().filter(df["a"] == _SELECTED),
    }


@pytest.mark.parametrize(("case", "columns", "rows"), _PULLED_UP_FILTER)
def test_a_filter_pulls_up_the_column_its_condition_needs(spark, case, columns, rows):
    result = _pulled_up_filter_cases(spark)[case]()

    assert result.columns == columns
    assert [tuple(row) for row in result.collect()] == rows


_PULL_UP_DEPTH = [
    "another_projection_in_between",
    "a_limit_in_between",
    "a_sort_in_between",
    "a_filter_in_between",
    "a_drop_duplicates_in_between",
    "three_operators_deep",
]


def _pull_up_depth_cases(spark):
    df = spark.sql("SELECT * FROM VALUES (1, 'x'), (2, 'y'), (3, 'z') AS t(a, b)")
    replaced = df.withColumn("A", -col("a"))

    return {
        # The operators an attribute is carried through, each of which outputs what it reads.
        "another_projection_in_between": lambda: replaced.select("A", "b").filter(df["a"] == _SELECTED),
        "a_limit_in_between": lambda: replaced.limit(3).filter(df["a"] == _SELECTED),
        "a_sort_in_between": lambda: replaced.sort("b").filter(df["a"] == _SELECTED),
        "a_filter_in_between": lambda: replaced.filter(col("b") != "q").filter(df["a"] == _SELECTED),
        # `dropDuplicates` states the columns it reads, so one more column does not change which
        # rows survive and the attribute can cross it.
        "a_drop_duplicates_in_between": lambda: replaced.dropDuplicates(["A"]).filter(df["a"] == _SELECTED),
        "three_operators_deep": lambda: replaced.select("A", "b").limit(3).filter(df["a"] == _SELECTED),
    }


@pytest.mark.parametrize("case", _PULL_UP_DEPTH)
def test_a_filter_pulls_a_column_through_the_operators_that_output_what_they_read(spark, case):
    result = _pull_up_depth_cases(spark)[case]()

    assert result.columns[0] == "A"
    assert [next(iter(row)) for row in result.collect()] == [-_SELECTED]


def test_a_filter_over_an_aggregate_reads_a_grouping_key_the_projection_dropped(spark):
    # The key is still an output of the aggregate, so carrying it through the projection above it
    # is all this takes. An attribute the aggregate does not output is a different case, refused
    # by both engines below.
    df = spark.sql("SELECT * FROM VALUES (1, 'x'), (2, 'y'), (3, 'z') AS t(a, b)")
    result = df.groupBy("a").count().select("count").filter(df["a"] == _SELECTED)

    assert [tuple(row) for row in result.collect()] == [(1,)]


def _pull_up_refused_cases(spark):
    df = spark.sql("SELECT * FROM VALUES (1, 'x'), (2, 'y'), (3, 'z') AS t(a, b)")
    replaced = df.withColumn("A", -col("a"))

    return {
        # `A` names the replacement in the output and the dropped column below it, so the whole
        # condition cannot be resolved against the two of them at once.
        "the_condition_also_names_the_replacement": lambda: replaced.filter(
            (df["a"] == _SELECTED) & (col("A") == -_SELECTED)
        ),
        # A distinct reads every column it outputs, so one more column changes which rows survive.
        "a_distinct_in_between": lambda: replaced.select("A").distinct().filter(df["a"] == _SELECTED),
        # The two Spark refuses as well: a column would escape the qualifier of an alias, and an
        # attribute the aggregate does not output is not there to be carried.
        "a_subquery_alias_in_between": lambda: replaced.alias("q").filter(df["a"] == _SELECTED),
        "an_attribute_the_aggregate_does_not_output": lambda: (
            df.groupBy("a").count().select("count").filter(df["b"] == "y")
        ),
    }


# Where Sail stops and Spark does not. Each is refused rather than answered with the wrong rows,
# which is the trade a wider pull-up must never make.
@pytest.mark.parametrize(
    ("case", "rows"),
    [
        pytest.param(
            "the_condition_also_names_the_replacement",
            [-_SELECTED],
        ),
        pytest.param(
            "a_distinct_in_between",
            [-_SELECTED],
            marks=pytest.mark.xfail(
                not is_jvm_spark(),
                reason="Sail builds one node for `SELECT DISTINCT`, which Spark refuses to carry "
                "a column through, and for `DataFrame.distinct`, which it allows",
                strict=True,
            ),
        ),
    ],
)
def test_a_filter_that_the_pull_up_does_not_reach(spark, case, rows):
    result = _pull_up_refused_cases(spark)[case]()

    assert [next(iter(row)) for row in result.collect()] == rows


@_SPARK_4
@pytest.mark.parametrize("case", ["a_subquery_alias_in_between", "an_attribute_the_aggregate_does_not_output"])
def test_a_filter_that_neither_engine_resolves(spark, case):
    with pytest.raises(Exception, match=re.escape("[CANNOT_RESOLVE_DATAFRAME_COLUMN]")):
        _pull_up_refused_cases(spark)[case]().collect()


def test_a_filter_by_a_column_that_exists_nowhere_reports_the_output(spark):
    # The fallback must not change which failure is reported: a name that no schema has is still
    # an unresolved column, named against the output rather than against the wider input.
    df = spark.sql("SELECT * FROM VALUES (1, 'x') AS t(a, b)")

    with pytest.raises(Exception, match=re.escape("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")):
        df.withColumn("A", -col("a")).filter(col("nope") == _SELECTED).collect()


# What a rename does to the qualifier. `UnresolvedStarWithColumnsRenames.expandStar` builds an
# `Alias` for a column the resolver matched and passes every other column on as it is, so the
# matched one becomes an attribute of its own and loses the qualifier while the others keep theirs.
# A rename that matches the name the column already has still builds the alias, which is what tells
# "the resolver matched it" apart from "its name changed". Measured on the Spark JVM.
_RENAME_QUALIFIER = [
    ("untouched_column", "x.b", ["b"]),
    ("untouched_by_a_rename_of_nothing", "x.b", ["b"]),
    ("untouched_by_a_rename_to_itself", "x.b", ["b"]),
    ("untouched_by_a_rename_differing_in_case", "x.b", ["b"]),
    ("untouched_by_two_renames", "x.b", ["b"]),
    ("qualified_star", "x.*", ["b"]),
]


def _rename_qualifier_cases(spark):
    def aliased():
        return spark.sql("SELECT 1 AS a, 2 AS b").alias("x")

    return {
        "untouched_column": lambda: aliased().withColumnRenamed("a", "z"),
        "untouched_by_a_rename_of_nothing": lambda: aliased().withColumnRenamed("nope", "z"),
        "untouched_by_a_rename_to_itself": lambda: aliased().withColumnRenamed("a", "a"),
        "untouched_by_a_rename_differing_in_case": lambda: aliased().withColumnRenamed("A", "z"),
        "untouched_by_two_renames": lambda: aliased().withColumnsRenamed({"a": "z"}),
        "qualified_star": lambda: aliased().withColumnRenamed("a", "z"),
    }


@pytest.mark.parametrize(("case", "reference", "columns"), _RENAME_QUALIFIER)
def test_a_rename_keeps_the_qualifier_of_the_columns_it_did_not_match(spark, case, reference, columns):
    result = _rename_qualifier_cases(spark)[case]().select(reference)

    assert result.columns == columns


@pytest.mark.parametrize(
    ("case", "reference"),
    [
        # The renamed column is a new attribute, so the qualifier no longer reaches it.
        ("untouched_column", "x.z"),
        # The name did not change and the qualifier still went, because the resolver matched it.
        ("untouched_by_a_rename_to_itself", "x.a"),
    ],
)
def test_a_rename_takes_the_qualifier_from_the_column_it_matched(spark, case, reference):
    with pytest.raises(Exception, match=re.escape("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")):
        _rename_qualifier_cases(spark)[case]().select(reference).collect()


# What tells one attribute read twice from two attributes that lead to the same field. Spark
# compares attributes by their identity (`candidates.distinct` in `AttributeSeq.resolve`), and two
# things give a NEW identity that the field alone does not show: reading one plan twice, which is
# what a CTE joined with itself does, and renaming a column, which Spark builds as an `Alias`
# (`UnresolvedStarWithColumnsRenames.expandStar`). Each case below answers on Spark and is refused
# by it, and each of them is a query a narrower notion of identity would answer with the rows of
# whichever column came first. Measured on the Spark JVM.
_TWO_ATTRIBUTES = [
    # One plan read twice: the two sides hand out the same fields, and they are still two columns.
    ("WITH t AS (SELECT 1 AS a) SELECT a FROM t x CROSS JOIN t y"),
    ("WITH t AS (SELECT 1 AS a) SELECT a + 1 FROM t x CROSS JOIN t y"),
    ("WITH t AS (SELECT 1 AS a, 2 AS b) SELECT a FROM t x CROSS JOIN t y"),
    # A rename gives the column an attribute of its own, so the copy and the rename are two.
    ("SELECT x FROM (SELECT a AS x, a AS x FROM VALUES (1) AS t(a))"),
]


@pytest.mark.parametrize("query", _TWO_ATTRIBUTES)
def test_two_attributes_that_lead_to_one_field_are_still_ambiguous(spark, query):
    with pytest.raises(Exception, match=re.escape("[AMBIGUOUS_REFERENCE]")):
        spark.sql(query).collect()


def test_a_rename_gives_the_column_an_attribute_of_its_own(spark):
    # `toDF` renames every column, so the two outputs of the one attribute below become two
    # attributes and the name is ambiguous again -- unlike the same frame without the rename,
    # which the test above this one pins as resolvable.
    repeated = spark.sql("SELECT a, a FROM VALUES (1) AS t(a)")

    assert [tuple(row) for row in repeated.select(col("a") + 1).collect()] == [(2,)]
    with pytest.raises(Exception, match=re.escape("[AMBIGUOUS_REFERENCE]")):
        repeated.toDF("a", "A").select("a").collect()


def test_a_qualified_name_still_reads_one_side_of_a_plan_read_twice(spark):
    # The control for the four above: the qualifier says which side, so the name resolves.
    result = spark.sql("WITH t AS (SELECT 1 AS a) SELECT x.a FROM t x CROSS JOIN t y")

    assert [tuple(row) for row in result.collect()] == [(1,)]


def test_a_struct_field_keeps_the_metadata_its_alias_was_given(spark):
    # A field of a struct reports the metadata of a `NamedExpression` (`CreateNamedStruct`), and an
    # alias that was given metadata reports that metadata whatever it reads (`Alias.metadata`), so
    # there is nothing for the struct to hide.
    annotated = col("a").cast("bigint").alias("z", metadata={"m": "z"})
    result = spark.sql("SELECT 1 AS a").select(F.struct(annotated).alias("st"))

    assert result.schema["st"].dataType["z"].metadata == {"m": "z"}
    assert [tuple(row[0]) for row in result.collect()] == [(1,)]


# Reading a column through the DataFrame it came from (`df.c`) carries the plan ID of that
# DataFrame, and Spark answers it by walking the plan for a node tagged with that ID
# (`ColumnResolutionHelper.resolveDataFrameColumn`). What the walk finds decides everything below,
# and `spark.sql.analyzer.strictDataFrameColumnResolution` only governs the name-based fallback
# that runs once the node HAS been found and the column was not in its output. Every case here is
# one PySpark's own `test_column.py` pins, measured on the Spark JVM.
_STRICT_RESOLUTION = "spark.sql.analyzer.strictDataFrameColumnResolution"
_RANGE_ROWS = 10


@contextlib.contextmanager
def _resolution(spark, strict):
    previous = spark.conf.get(_STRICT_RESOLUTION, None)
    try:
        spark.conf.set(_STRICT_RESOLUTION, strict)
        yield
    finally:
        if previous is None:
            spark.conf.unset(_STRICT_RESOLUTION)
        else:
            spark.conf.set(_STRICT_RESOLUTION, previous)


# (case, the rows the reference reads)
_PLAN_ID_RESOLVES = [
    ("through_a_filter", [(1,), (2,)]),
    ("through_a_sort", [(1,), (2,)]),
    ("through_a_distinct", [(1,), (2,)]),
    ("after_a_group_by", [(1,), (2,)]),
    ("after_a_pivot", [(1,), (2,)]),
    ("after_an_intersect", [(2,)]),
]


def _plan_id_cases(spark):
    two = spark.sql("SELECT 1 AS c UNION ALL SELECT 2 AS c")
    pivoted = spark.sql("SELECT 1 AS c, 'a' AS k, 10 AS v UNION ALL SELECT 2 AS c, 'b' AS k, 20 AS v")
    other = spark.sql("SELECT 2 AS c UNION ALL SELECT 3 AS c")

    return {
        # The operators that pass their input on keep the identity of what they pass.
        "through_a_filter": lambda: two.filter(two.c > 0).select(two.c),
        "through_a_sort": lambda: two.sort(two.c).select(two.c),
        "through_a_distinct": lambda: two.distinct().select(two.c),
        # An aggregate keeps the identity of its grouping key, which is the same rule.
        "after_a_group_by": lambda: two.groupBy("c").count().select(two.c),
        "after_a_pivot": lambda: pivoted.groupBy("c").pivot("k").sum("v").select(pivoted.c),
        # An intersection is walked into, so its left side stays reachable.
        "after_an_intersect": lambda: two.intersect(other).select(two.c),
    }


@pytest.mark.parametrize(("case", "rows"), _PLAN_ID_RESOLVES)
def test_a_dataframe_column_resolves_through_the_operators_that_keep_its_identity(spark, case, rows):
    result = _plan_id_cases(spark)[case]()

    assert sorted(tuple(row) for row in result.collect()) == rows


def test_a_sort_names_a_column_the_projection_dropped(spark):
    # The reference is resolved below the projection and the column is carried up for the sort.
    df = spark.range(_RANGE_ROWS).withColumn("v", col("id") + 1)

    assert len(df.select(df.v).sort(df.id).collect()) == _RANGE_ROWS


def test_a_self_join_through_a_rename_is_not_ambiguous(spark):
    # The tagged node is found on both sides of the join, and the renamed side does not output
    # `a`, which is what tells the two candidates apart.
    df = spark.range(_RANGE_ROWS).withColumn("a", col("id"))
    renamed = df.withColumnRenamed("a", "b")

    assert len(df.join(renamed, df.a == renamed.b).select(df.a, renamed.b).collect()) == _RANGE_ROWS


_PLAN_ID_REFUSED = [
    "a_column_of_another_dataframe",
    "a_column_of_another_dataframe_inside_an_expression",
    "a_column_the_rename_took_away",
    "a_column_that_was_dropped",
]


def _plan_id_refused_cases(spark):
    other = spark.sql("SELECT 1 AS a, 2 AS b")
    source = spark.sql("SELECT 1 AS c")
    wider = spark.sql("SELECT 1 AS c, 2 AS d")

    return {
        "a_column_of_another_dataframe": lambda: source.select(other.a),
        "a_column_of_another_dataframe_inside_an_expression": lambda: source.withColumn("x", other.a + 1),
        "a_column_the_rename_took_away": lambda: source.withColumnRenamed("c", "c2").select(source.c),
        "a_column_that_was_dropped": lambda: wider.drop("c").select(wider.c),
    }


@pytest.mark.parametrize("strict", ["true", "false"])
@pytest.mark.parametrize("case", _PLAN_ID_REFUSED)
def test_a_dataframe_column_the_plan_does_not_hold_is_refused_in_both_modes(spark, case, strict):
    with _resolution(spark, strict), pytest.raises(Exception, match=re.escape("[CANNOT_RESOLVE_DATAFRAME_COLUMN]")):
        _plan_id_refused_cases(spark)[case]().collect()


# (case, the values the lenient mode reads off the current output)
_SHADOWED = [
    ("a_chain_of_with_column", [1]),
    ("a_select_with_an_alias", ["1"]),
    ("an_aggregate_with_an_alias", [1]),
]


def _shadowed_cases(spark):
    source = spark.sql("SELECT 1 AS c")

    return {
        "a_chain_of_with_column": lambda: (
            source.withColumn("c", col("c").cast("string")).withColumn("c", col("c").cast("int")).select(source.c)
        ),
        "a_select_with_an_alias": lambda: source.select(source.c.cast("string").alias("c")).select(source.c),
        "an_aggregate_with_an_alias": lambda: source.groupBy().agg(spark_sum("c").alias("c")).select(source.c),
    }


@pytest.mark.parametrize(("case", "values"), _SHADOWED)
def test_a_shadowed_dataframe_column_is_read_by_name_only_when_resolution_is_lenient(spark, case, values):
    # The node IS found and the column it held is gone, which is the one place the setting decides.
    with _resolution(spark, "true"), pytest.raises(Exception, match=re.escape("[CANNOT_RESOLVE_DATAFRAME_COLUMN]")):
        _shadowed_cases(spark)[case]().collect()

    with _resolution(spark, "false"):
        assert [next(iter(row)) for row in _shadowed_cases(spark)[case]().collect()] == values


# Where the plan-ID walk still differs from Spark. Each expectation below is the Spark JVM's,
# measured in BOTH resolution modes, so the day Sail closes the gap the case reports it instead of
# staying a note nobody reruns.


def test_a_dataframe_column_is_refused_through_a_union(spark):
    # Spark treats a `Union` as a leaf while walking for the plan ID
    # (`resolveDataFrameColumnRecursively`: `case _: Union => Seq.empty`), so the node below it is
    # never found and the reference is refused -- in both modes, since the setting only opens the
    # fallback once the node HAS been found.
    df1, df2 = spark.sql("SELECT 1 AS c"), spark.sql("SELECT 2 AS c")

    with _resolution(spark, "true"), pytest.raises(Exception, match=re.escape("[CANNOT_RESOLVE_DATAFRAME_COLUMN]")):
        df1.union(df2).select(df1.c).collect()

    # TODO: Sail finds the plan ID below the union, so the lenient fallback reads the name off the
    #   union's output and answers `[1, 2]` where Spark refuses. Telling the two apart needs the
    #   resolver to know which plan IDs sit under a union.
    with _resolution(spark, "false"):
        if is_jvm_spark():
            with pytest.raises(Exception, match=re.escape("[CANNOT_RESOLVE_DATAFRAME_COLUMN]")):
                df1.union(df2).select(df1.c).collect()
        else:
            assert sorted(next(iter(row)) for row in df1.union(df2).select(df1.c).collect()) == [1, 2]


@pytest.mark.xfail(
    not is_jvm_spark(),
    reason="A temp view does not carry the plan IDs of the plan it was created from",
    strict=True,
)
@pytest.mark.parametrize("strict", ["true", "false"])
def test_a_dataframe_column_read_back_through_a_temp_view(spark, strict):
    # The view keeps the plan it was created from, tags included, so the reference still finds its
    # node. Sail resolves each request on its own, and the plan of `df` does not travel with the
    # second one, so the ID is unknown there.
    view = f"v_{uuid.uuid4().hex}"
    df = spark.sql("SELECT 1 AS c")
    df.createOrReplaceTempView(view)
    try:
        with _resolution(spark, strict):
            assert [next(iter(row)) for row in spark.table(view).select(df.c).collect()] == [1]
    finally:
        spark.sql(f"DROP VIEW IF EXISTS {view}")


@pytest.mark.skipif(
    pyspark_version() < (4, 0),
    reason="`df['*']` is a star carrying a plan ID only from the Spark 4 client, which sends an "
    "`UnresolvedStar`; the 3.5 client sends a plain column named `*` instead, so the case is a "
    "different one there",
)
@pytest.mark.xfail(not is_jvm_spark(), reason="Sail does not implement a wildcard carrying a plan ID", strict=True)
def test_a_dataframe_star_expands_to_the_columns_of_that_dataframe(spark):
    # `df["*"]` is an unresolved star carrying df's plan ID, which the analyzer expands to the
    # output of the node it names.
    df = spark.sql("SELECT 'Books' AS c, 100 AS v UNION ALL SELECT 'Electronics' AS c, 200 AS v")

    assert sorted((row.c, row.v) for row in df.select(df["*"]).collect()) == [
        ("Books", 100),
        ("Electronics", 200),
    ]
