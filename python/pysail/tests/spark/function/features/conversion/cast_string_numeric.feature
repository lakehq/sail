Feature: CAST and the type constructors from STRING to a numeric type

  # Spark's non-ANSI string -> integral cast TRUNCATES a fractional part: `UTF8String.toLong`
  # / `toInt` are called with `allowDecimal = true`, which breaks at the '.' and returns the
  # integral part (opt/spark/common/unsafe/.../UTF8String.java:1665-1717, :1762-1818). Only the
  # ANSI and TRY paths use `toLongExact`/`toIntExact` with `allowDecimal = false` (:1861-1881),
  # which raise CAST_INVALID_INPUT and return NULL respectively.
  #
  # The type constructors (`int(x)`, `bigint(x)`, ...) are registered as `castAlias`
  # (FunctionRegistry.scala:994-1006), i.e. they are exactly `CAST(x AS T)` and must agree
  # with it on every input.
  #
  # All expected values measured against Spark 4.2.0.

  Rule: A non-ANSI STRING to integral cast truncates the fractional part

    # Sail routes Utf8 -> Int32 under ANSI off through `SparkCastStringToInt32`, which
    # implements the truncation, so this width already agrees with Spark. It is pinned
    # separately from the diverging widths below so that a future unification of the two
    # code paths cannot silently regress the half that works.
    Scenario: casting a fractional string to INT truncates toward zero
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('1.23' AS INT) AS a,
               CAST('-4.56' AS INT) AS b,
               CAST('  7.99  ' AS INT) AS c
        """
      Then query result
        | a | b  | c |
        | 1 | -4 | 7 |

    # The discriminating counterpart of the rule: TRY_CAST uses `allowDecimal = false`, so a
    # fractional string is NULL there. Without this row a "return NULL on a fractional string"
    # implementation would look correct for TRY and be indistinguishable from the plain CAST.
    Scenario: TRY_CAST does not truncate and returns NULL instead
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT TRY_CAST('1.23' AS BIGINT) AS a,
               TRY_CAST('1.23' AS INT) AS b
        """
      Then query result
        | a    | b    |
        | NULL | NULL |

    @sail-bug
    Scenario Outline: a fractional string truncates for every integral width: <case>
      # Sail plans every integral width other than Int32-under-ANSI-off as
      # `TRY_CAST(btrim(x) AS <width>)`; arrow's parser has no `allowDecimal`, so it yields
      # NULL where Spark yields the truncated value. This is a silent wrong answer, not an
      # error, and it makes `int('1.23')` disagree with `CAST('1.23' AS INT)` inside Sail
      # even though Spark defines them as the same expression.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS r
        """
      Then query result
        | r        |
        | <result> |

      Examples:
        | case                                    | expr                        | result |
        | the INT constructor                     | int('1.23')                 | 1      |
        | the BIGINT cast                         | CAST('1.23' AS BIGINT)      | 1      |
        | the BIGINT constructor, negative        | bigint('-4.56')             | -4     |
        | the SMALLINT cast                       | CAST('1.23' AS SMALLINT)    | 1      |
        | the TINYINT cast                        | CAST('1.23' AS TINYINT)     | 1      |

    @sail-bug
    Scenario: a fractional string column truncates for a non-INT width
      # The same gap on the column path rather than on a folded literal, so a constant-folding
      # change cannot make the literal row pass while the column row still returns NULL.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(v AS BIGINT) AS r
        FROM VALUES ('1.23'), ('-4.56') AS t(v)
        """
      Then query result
        | r  |
        | 1  |
        | -4 |

  Rule: An ANSI STRING to numeric cast raises CAST_INVALID_INPUT quoting the input

    # A blank input cannot live in an Examples table: Gherkin strips the cell's surrounding
    # whitespace, so the three spaces would reach the query as an empty string and the
    # scenario would stop testing the trim at all.
    @sail-bug
    Scenario: the ANSI cast error quotes the untrimmed value
      # Spark quotes the ORIGINAL string; Sail trims first and reports the value as ''.
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(v AS DECIMAL(10,2)) AS r FROM VALUES ('   ') AS t(v)
        """
      Then query error CAST_INVALID_INPUT.*The value '   ' of the type "STRING"

    @sail-bug
    Scenario Outline: the ANSI cast error names the class and the target type: <case>
      # Spark raises CAST_INVALID_INPUT naming the DECLARED target type. Sail leaks
      # DataFusion's own message for a non-blank malformed input, which carries no error
      # class and names the internal Decimal128(38, 10) rather than DECIMAL(10,2).
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(v AS <target>) AS r FROM VALUES ('<value>') AS t(v)
        """
      Then query error <pattern>

      Examples:
        | case                          | value | target        | pattern                                                          |
        | a malformed string to DECIMAL | abc   | DECIMAL(10,2) | CAST_INVALID_INPUT.*The value 'abc'.*cannot be cast to "DECIMAL |
        | a fractional string to INT    | 1.23  | INT           | CAST_INVALID_INPUT.*The value '1.23'.*cannot be cast to "INT"    |
        | a fractional string to BIGINT | 1.23  | BIGINT        | CAST_INVALID_INPUT.*The value '1.23'.*cannot be cast to "BIGINT" |

  Rule: Exponent notation is accepted by the decimal parser and rejected by the integral one
    # Spark's two string parsers do NOT share a grammar: `Decimal.apply(String)` goes through
    # BigDecimal and accepts an exponent, while `UTF8String.toInt`/`toLong` do not. So
    # `CAST('1e2' AS DECIMAL(10,2))` is 100.00 and `CAST('1e2' AS INT)` is NULL, in the same
    # engine and the same ANSI mode.
    #
    # These rows are the boundary that guards the truncation follow-up above: a fix that made
    # the integral path "parse as a double and truncate" would return 100 for `CAST('1e2' AS
    # INT)` and silently break this rule. Measured against Spark 4.2.0.

    Scenario: the integral parser rejects exponent notation
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('1e2' AS INT) AS a,
               CAST('1e2' AS BIGINT) AS b,
               CAST('1e2' AS DOUBLE) AS c
        """
      Then query result
        | a    | b    | c     |
        | NULL | NULL | 100.0 |

    @sail-bug
    Scenario Outline: the decimal parser accepts exponent notation: <case>
      # Sail routes the decimal target through arrow's parser, which has no exponent support,
      # so it returns NULL where Spark returns the value.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('<value>' AS DECIMAL(10,2)) AS r
        """
      Then query result
        | r        |
        | <result> |

      Examples:
        | case                            | value | result |
        | a positive exponent             | 1e2   | 100.00 |
        | a mantissa with a decimal point | 1.5e1 | 15.00  |
