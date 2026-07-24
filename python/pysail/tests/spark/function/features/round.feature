@round
Feature: round with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: round — the argument must be foldable

    @column_args
    Scenario: round with the argument as a literal
      When query
        """
        SELECT round(2.5, 0) AS result
        """
      Then query result ordered
        | result |
        | 3      |

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['3.0', 'NULL'].
    @column_args @sail-bug
    Scenario: round takes argument 2 from a column containing NULL
      When query
        """
        SELECT round(2.5, c) AS result FROM VALUES (1, 0), (2, NULL) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

    # Spark requires a foldable argument here; Sail accepts a column: Sail returns ['3.0', '3.0'].
    @column_args @sail-bug
    Scenario: round takes argument 2 from a column
      When query
        """
        SELECT round(2.5, c) AS result FROM VALUES (1, 0), (2, 0) AS t(i, c) ORDER BY i
        """
      Then query error NON_FOLDABLE_INPUT

  Rule: round preserves an integral input type

    # Spark's `Round.dataType` ends in `case t => t`, so a non-decimal input keeps its
    # type. DataFusion's signature coerces integral inputs to Float64, which silently
    # loses precision past 2^53. With a non-negative scale the call is the identity, so
    # the plan builder returns the argument untouched.

    Scenario: round of a bigint past 2^53 keeps every digit
      When query
        """
        SELECT round(CAST(9007199254740993 AS BIGINT), 0) AS r,
               typeof(round(CAST(9007199254740993 AS BIGINT), 0)) AS t
        """
      Then query result
        | r                | t      |
        | 9007199254740993 | bigint |

    Scenario: round of a bigint at a positive scale is the identity
      When query
        """
        SELECT round(CAST(9007199254740993 AS BIGINT), 2) AS r
        """
      Then query result
        | r                |
        | 9007199254740993 |

    Scenario: round keeps the narrower integral types
      When query
        """
        SELECT typeof(round(CAST(300 AS SMALLINT), 0)) AS s,
               typeof(round(CAST(25 AS INT), 0)) AS i,
               typeof(round(CAST(12 AS TINYINT), 0)) AS t
        """
      Then query result
        | s        | i   | t       |
        | smallint | int | tinyint |

    Scenario: round of an integral column keeps the type
      When query
        """
        SELECT round(v, 0) AS r, typeof(round(v, 0)) AS t
        FROM VALUES (CAST(9007199254740993 AS BIGINT)) AS t(v)
        """
      Then query result
        | r                | t      |
        | 9007199254740993 | bigint |

    Scenario: round of a NULL bigint is NULL
      When query
        """
        SELECT round(CAST(NULL AS BIGINT), 0) AS r
        """
      Then query result
        | r    |
        | NULL |

    # A NEGATIVE scale over an integer does round, and can overflow the input type —
    # Spark raises under ANSI and wraps otherwise. That needs the ANSI flag inside the
    # UDF plus an integral kernel, so it is still on the Float64 path.

    @sail-bug
    Scenario: round of a bigint at a negative scale keeps the type
      When query
        """
        SELECT round(CAST(25 AS BIGINT), -1) AS r,
               typeof(round(CAST(25 AS BIGINT), -1)) AS t
        """
      Then query result
        | r  | t      |
        | 30 | bigint |

    @sail-bug
    Scenario: round overflow at a negative scale raises under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT round(CAST(127 AS TINYINT), -1) AS r
        """
      Then query error ARITHMETIC_OVERFLOW

    @sail-bug
    Scenario: round overflow at a negative scale wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT round(CAST(127 AS TINYINT), -1) AS r
        """
      Then query result
        | r    |
        | -126 |
