"""A negative integer literal must be typed by the widest of its own digits, the same as its
positive counterpart -- not by the positive magnitude first and the sign second.

Spark's grammar folds the sign into the literal itself (`SqlBaseParser.g4`'s `MINUS? INTEGER_VALUE`)
and evaluates the combined text as one `BigDecimal`, picking the narrowest integer type that value
fits (`AstBuilder.scala`'s `numericLiteral`). Sail's tokenizer never accepts a leading `-` inside a
`NumberLiteral` (`sail-sql-parser/src/ast/literal.rs`) -- doing that in the lexer risks swallowing a
real subtraction like `a-5` into one literal token, since the lexer has no notion of "this minus is
unary" -- so Sail instead recombines sign and magnitude one level up, in `from_ast_expression`
(`sail-sql-analyzer/src/expression.rs`): a `UnaryOperator::Minus` directly over a bare
`AtomExpr::NumberLiteral` re-parses the negated text through the same literal-typing ladder,
instead of building a generic unary-minus function call over the (already positive-typed) literal.
`-(2147483648)`, a parenthesized expression, is deliberately excluded -- it is `AtomExpr::Nested`,
not a bare literal, and Spark types it from `2147483648` alone (bigint) too.

Before this fix, `-2147483648` (`Int32::MIN`) was typed from `2147483648` alone, which overflows
`Int32` and promotes to `Int64` before the sign was ever considered; `typeof(-2147483648)` proved
it (`"int"` on Spark, `"bigint"` on Sail). The four minimum literals (`-128Y`, `-32768S`,
`-2147483648`, `-9223372036854775808L`) either mistyped or outright failed to parse, and anything
downstream that depends on the resulting type (CAST to BINARY, array/VALUES schema inference,
ANSI-off integer overflow arithmetic) inherited the wrong width.

The boundary cases one step further out (`2147483648`, `-2147483649`, etc., which Spark ALSO
promotes) are included alongside as a control group: they confirm Sail's ordinary literal
promotion (Int32 -> Int64 -> Decimal) was correct in general even before this fix, and the bug was
specifically about the sign/magnitude interaction at each type's minimum, not literal typing as a
whole.

Every expectation below was measured on the Spark 4.2 JVM over Spark Connect.
"""


def test_minimum_literal_keeps_its_narrow_type(spark):
    cases = [
        ("-128Y", "tinyint"),
        ("-32768S", "smallint"),
        ("-2147483648", "int"),
        ("-9223372036854775808L", "bigint"),
    ]
    for literal, expected_type in cases:
        row = spark.sql(f"SELECT typeof({literal}) AS t").collect()[0]
        assert row["t"] == expected_type, literal


def test_out_of_range_literal_promotes_on_both_sides_of_zero(spark):
    cases = [
        # One step past each signed 32/64-bit minimum: Spark promotes these too, on both
        # sides of zero, confirming Sail's ordinary literal-promotion ladder is unaffected.
        ("2147483648", "bigint"),
        ("-2147483649", "bigint"),
        ("9223372036854775808", "decimal(19,0)"),
        ("-9223372036854775809", "decimal(19,0)"),
        # An explicitly parenthesized negation is a real unary-minus expression on both
        # engines, not part of the literal -- Spark also types this from `2147483648` alone
        # (bigint), so it must stay that way rather than being folded into the literal too.
        ("-(2147483648)", "bigint"),
    ]
    for literal, expected_type in cases:
        row = spark.sql(f"SELECT typeof({literal}) AS t").collect()[0]
        assert row["t"] == expected_type, literal


def test_int_min_cast_to_binary_is_four_bytes(spark):
    spark.conf.set("spark.sql.ansi.enabled", "false")
    row = spark.sql("SELECT CAST(-2147483648 AS BINARY) AS v").collect()[0]
    assert bytes(row["v"]) == b"\x80\x00\x00\x00"


def test_int_min_in_array_keeps_int_type(spark):
    row = spark.sql("SELECT typeof(array(-2147483648)[0]) AS t").collect()[0]
    assert row["t"] == "int"


def test_int_min_in_values_infers_int_column(spark):
    rows = spark.sql("SELECT typeof(v) AS t FROM VALUES (-2147483648), (1) AS t(v)").collect()
    assert [row["t"] for row in rows] == ["int", "int"]


def test_int_min_negated_overflows_like_int_under_ansi_off(spark):
    # Int32::MIN negated overflows back to itself in two's complement (`-INT_MIN == INT_MIN`);
    # Spark's ANSI-off semantics keep that overflow, which only shows up once `-2147483648` is
    # actually typed INT (Int64 has no overflow to wrap at this magnitude).
    spark.conf.set("spark.sql.ansi.enabled", "false")
    row = spark.sql("SELECT -2147483648 * -1 AS v").collect()[0]
    assert row["v"] == -2147483648


def test_int_min_plus_one_keeps_int_type(spark):
    row = spark.sql("SELECT typeof(-2147483648 + 1) AS t").collect()[0]
    assert row["t"] == "int"


def test_int_min_with_space_before_digits_keeps_int_type(spark):
    # Spark's grammar accepts whitespace between the sign and the digits and still folds it
    # into the same literal-typing rule (`- 2147483648` types identically to `-2147483648`);
    # the fix (pattern-matching `UnaryOperator::Minus` directly wrapping a bare
    # `AtomExpr::NumberLiteral` in the AST, after parsing) does not depend on the two tokens
    # being adjacent, since whitespace is already skipped before the AST is built.
    row = spark.sql("SELECT typeof(- 2147483648) AS t").collect()[0]
    assert row["t"] == "int"


def test_ordinary_subtraction_is_unaffected(spark):
    # The fix only fires for `UnaryOperator::Minus` directly over a bare number literal; a
    # `BinaryOperator::Minus` (subtraction) must still work identically, with or without
    # whitespace around the operator, and regardless of which side is a literal.
    assert spark.sql("SELECT 5 - 3 AS v").collect()[0]["v"] == 2
    assert spark.sql("SELECT 5-3 AS v").collect()[0]["v"] == 2
    assert spark.sql("SELECT a - 5 AS v FROM VALUES (10) AS t(a)").collect()[0]["v"] == 5
    assert spark.sql("SELECT a-5 AS v FROM VALUES (10) AS t(a)").collect()[0]["v"] == 5


def test_double_negation_is_unaffected(spark):
    # `- -5`: the outer minus wraps another `UnaryOperator::Minus`, not a bare number literal,
    # so only the inner one is folded into its literal; the outer stays a real negation.
    row = spark.sql("SELECT - -5 AS v, typeof(- -5) AS t").collect()[0]
    assert row["v"] == 5
    assert row["t"] == "int"


def test_int_min_type_survives_union_coercion(spark):
    # Type coercion across a UNION's branches must see INT on both sides regardless of which
    # branch carries the bare literal -- if either side were still typed BIGINT, this would
    # coerce the whole UNION to BIGINT instead.
    for query in (
        "SELECT typeof(v) AS t FROM (SELECT -2147483648 AS v UNION SELECT CAST(1 AS INT) AS v)",
        "SELECT typeof(v) AS t FROM (SELECT CAST(1 AS INT) AS v UNION SELECT -2147483648 AS v)",
    ):
        assert spark.sql(query).collect()[0]["t"] == "int"


def test_int_min_type_survives_multi_row_values_with_other_values(spark):
    # A three-row VALUES list spanning INT_MIN, zero, and INT_MAX: the type must stay INT for
    # every row, not just the single-row case already covered above.
    rows = spark.sql(
        "SELECT typeof(v) AS t, v FROM VALUES (-2147483648), (0), (2147483647) AS t(v) ORDER BY v"
    ).collect()
    assert [row["t"] for row in rows] == ["int", "int", "int"]
    assert [row["v"] for row in rows] == [-2147483648, 0, 2147483647]


def test_int_min_type_survives_constant_folding_contexts(spark):
    # WHERE-clause comparison and a struct field extraction both push the literal through a
    # constant-folding-eligible path in the physical plan; the type (and value) must still be
    # exactly what the SQL text says, not silently promoted.
    assert (
        spark.sql("SELECT COUNT(*) AS v FROM VALUES (10) AS t(a) WHERE a > -2147483648")
        .collect()[0]["v"]
        == 1
    )
    row = spark.sql("SELECT typeof(-2147483648) AS t WHERE -2147483648 = -2147483648").collect()[0]
    assert row["t"] == "int"
    row = spark.sql("SELECT typeof(struct(-2147483648).col1) AS t").collect()[0]
    assert row["t"] == "int"


def test_tinyint_min_arithmetic_keeps_tinyint_type(spark):
    # Not just INT: the same fix must hold for the other three minimum-literal suffixes too,
    # combined with arithmetic (not just a bare `typeof()` on the literal alone).
    row = spark.sql("SELECT typeof(-128Y + 1Y) AS t").collect()[0]
    assert row["t"] == "tinyint"
