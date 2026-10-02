Feature: Negative integer literal typing

  # A negative integer literal must be typed by the widest of its own digits, the same as its
  # positive counterpart -- not by the positive magnitude first and the sign second.
  #
  # Spark's grammar folds the sign into the literal itself (`SqlBaseParser.g4`'s `MINUS?
  # INTEGER_VALUE`) and evaluates the combined text as one `BigDecimal`, picking the narrowest
  # integer type that value fits (`AstBuilder.scala`'s `numericLiteral`). Sail's tokenizer never
  # accepts a leading `-` inside a `NumberLiteral` (`sail-sql-parser/src/ast/literal.rs`) --
  # doing that in the lexer risks swallowing a real subtraction like `a-5` into one literal
  # token, since the lexer has no notion of "this minus is unary" -- so Sail instead recombines
  # sign and magnitude one level up, in `from_ast_expression`
  # (`sail-sql-analyzer/src/expression.rs`): a `UnaryOperator::Minus` directly over a bare
  # `AtomExpr::NumberLiteral` re-parses the negated text through the same literal-typing ladder,
  # instead of building a generic unary-minus function call over the (already positive-typed)
  # literal. `-(2147483648)`, a parenthesized expression, is deliberately excluded -- it is
  # `AtomExpr::Nested`, not a bare literal, and Spark types it from `2147483648` alone (bigint)
  # too.
  #
  # Before this fix, `-2147483648` (`Int32::MIN`) was typed from `2147483648` alone, which
  # overflows `Int32` and promotes to `Int64` before the sign was ever considered;
  # `typeof(-2147483648)` proved it ("int" on Spark, "bigint" on Sail). The four minimum
  # literals (`-128Y`, `-32768S`, `-2147483648`, `-9223372036854775808L`) either mistyped or
  # outright failed to parse, and anything downstream that depends on the resulting type (CAST
  # to BINARY, array/VALUES schema inference, ANSI-off integer overflow arithmetic) inherited
  # the wrong width.
  #
  # Every expectation below was measured on the Spark 4.2 JVM over Spark Connect.

  Rule: Minimum literal per integer width keeps its narrow type

    Scenario Outline: Minimum literal: <case>
      When query
        """
        SELECT typeof(<literal>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case              | literal                   | result   |
        | TINYINT minimum   | -128Y                     | tinyint  |
        | SMALLINT minimum  | -32768S                   | smallint |
        | INT minimum       | -2147483648               | int      |
        | BIGINT minimum    | -9223372036854775808L     | bigint   |

    # One step past each signed 32/64-bit minimum: Spark promotes these too, on both sides of
    # zero, confirming Sail's ordinary literal-promotion ladder (Int32 -> Int64 -> Decimal) is
    # correct in general and the bug was specifically about the sign/magnitude interaction at
    # each type's minimum, not literal typing as a whole.
    Scenario Outline: Literal one step past the minimum promotes on both sides of zero: <case>
      When query
        """
        SELECT typeof(<literal>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | literal               | result        |
        | one past INT max            | 2147483648            | bigint        |
        | one past INT min            | -2147483649           | bigint        |
        | one past BIGINT max         | 9223372036854775808   | decimal(19,0) |
        | one past BIGINT min         | -9223372036854775809  | decimal(19,0) |

    # An explicitly parenthesized negation is a real unary-minus expression on both engines, not
    # part of the literal -- Spark also types this from `2147483648` alone (bigint), so it must
    # stay that way rather than being folded into the literal too.
    Scenario: A parenthesized negation is not folded into the literal
      When query
        """
        SELECT typeof(-(2147483648)) AS result
        """
      Then query result
        | result |
        | bigint |

  Rule: The INT minimum literal's type propagates through downstream consumers

    Scenario: CAST to BINARY of the INT minimum literal is four bytes, not eight
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(-2147483648 AS BINARY) AS result
        """
      Then query result
        | result         |
        | [80 00 00 00]  |

    Scenario: The INT minimum literal keeps INT type inside an array
      When query
        """
        SELECT typeof(array(-2147483648)[0]) AS result
        """
      Then query result
        | result |
        | int    |

    Scenario: The INT minimum literal infers an INT VALUES column
      When query
        """
        SELECT typeof(v) AS result FROM VALUES (-2147483648), (1) AS t(v)
        """
      Then query result
        | result |
        | int    |
        | int    |

    # Int32::MIN negated overflows back to itself in two's complement (`-INT_MIN == INT_MIN`);
    # Spark's ANSI-off semantics keep that overflow, which only shows up once `-2147483648` is
    # actually typed INT (Int64 has no overflow to wrap at this magnitude).
    Scenario: The INT minimum literal negated overflows like INT under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT -2147483648 * -1 AS result
        """
      Then query result
        | result      |
        | -2147483648 |

    Scenario: The INT minimum literal plus one keeps INT type
      When query
        """
        SELECT typeof(-2147483648 + 1) AS result
        """
      Then query result
        | result |
        | int    |

    # Spark's grammar accepts whitespace between the sign and the digits and still folds it into
    # the same literal-typing rule (`- 2147483648` types identically to `-2147483648`); the fix
    # (pattern-matching `UnaryOperator::Minus` directly wrapping a bare `AtomExpr::NumberLiteral`
    # in the AST, after parsing) does not depend on the two tokens being adjacent, since
    # whitespace is already skipped before the AST is built.
    Scenario: The INT minimum literal keeps INT type with a space before the digits
      When query
        """
        SELECT typeof(- 2147483648) AS result
        """
      Then query result
        | result |
        | int    |

    Scenario: The TINYINT minimum literal keeps TINYINT type combined with arithmetic
      When query
        """
        SELECT typeof(-128Y + 1Y) AS result
        """
      Then query result
        | result  |
        | tinyint |

  Rule: Ordinary subtraction and double negation are unaffected

    # The fix only fires for `UnaryOperator::Minus` directly over a bare number literal; a
    # `BinaryOperator::Minus` (subtraction) must still work identically, with or without
    # whitespace around the operator, and regardless of which side is a literal.
    Scenario Outline: Subtraction is unaffected: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | 2      |

      Examples:
        | case                     | expr    |
        | literals with spaces     | 5 - 3   |
        | literals without spaces  | 5-3     |

    Scenario: Subtraction between a column and a literal is unaffected
      When query
        """
        SELECT a - 5 AS result FROM VALUES (10) AS t(a)
        """
      Then query result
        | result |
        | 5      |

    Scenario: Subtraction between a column and a literal without spaces is unaffected
      When query
        """
        SELECT a-5 AS result FROM VALUES (10) AS t(a)
        """
      Then query result
        | result |
        | 5      |

    # `- -5`: the outer minus wraps another `UnaryOperator::Minus`, not a bare number literal, so
    # only the inner one is folded into its literal; the outer stays a real negation.
    Scenario: Double negation of a literal is unaffected
      When query
        """
        SELECT - -5 AS result, typeof(- -5) AS result_type
        """
      Then query result
        | result | result_type |
        | 5      | int         |

  Rule: The INT minimum literal's type survives coercion and constant-folding contexts

    # Type coercion across a UNION's branches must see INT on both sides regardless of which
    # branch carries the bare literal -- if either side were still typed BIGINT, this would
    # coerce the whole UNION to BIGINT instead.
    Scenario Outline: The INT minimum literal survives UNION coercion: <case>
      When query
        """
        SELECT typeof(v) AS result FROM (<query>)
        """
      Then query result
        | result |
        | int    |
        | int    |

      Examples:
        | case                             | query                                                                         |
        | literal on the left branch       | SELECT -2147483648 AS v UNION SELECT CAST(1 AS INT) AS v                      |
        | literal on the right branch      | SELECT CAST(1 AS INT) AS v UNION SELECT -2147483648 AS v                      |

    # A three-row VALUES list spanning INT_MIN, zero, and INT_MAX: the type must stay INT for
    # every row, not just the single-row case already covered above.
    Scenario: The INT minimum literal's type survives a multi-row VALUES list with other values
      When query
        """
        SELECT typeof(v) AS result, v FROM VALUES (-2147483648), (0), (2147483647) AS t(v) ORDER BY v
        """
      Then query result
        | result | v           |
        | int    | -2147483648 |
        | int    | 0           |
        | int    | 2147483647  |

    # A WHERE-clause comparison pushes the literal through a constant-folding-eligible path in
    # the physical plan; the value must still be exactly what the SQL text says, not silently
    # promoted.
    Scenario: The INT minimum literal's value survives a WHERE-clause comparison
      When query
        """
        SELECT COUNT(*) AS result FROM VALUES (10) AS t(a) WHERE a > -2147483648
        """
      Then query result
        | result |
        | 1      |

    Scenario: The INT minimum literal's type survives a WHERE-clause self-comparison
      When query
        """
        SELECT typeof(-2147483648) AS result WHERE -2147483648 = -2147483648
        """
      Then query result
        | result |
        | int    |

    # A struct field extraction also pushes the literal through a constant-folding-eligible path.
    Scenario: The INT minimum literal's type survives a struct field extraction
      When query
        """
        SELECT typeof(struct(-2147483648).col1) AS result
        """
      Then query result
        | result |
        | int    |
