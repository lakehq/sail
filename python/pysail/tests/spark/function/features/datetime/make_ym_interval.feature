Feature: make_ym_interval builds a year-month interval from years and months

  Rule: A NULL in any argument yields NULL (Spark MakeYMInterval is null-intolerant)
    Scenario Outline: NULL argument: <case>
      When query
        """
        SELECT make_ym_interval(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                            | args                 |
        | NULL months yields NULL         | 1, NULL              |
        | NULL years yields NULL          | NULL, 6              |
        | both arguments NULL yields NULL | NULL, NULL           |
        | typed NULL argument yields NULL | CAST(NULL AS INT), 6 |

    Scenario: NULL propagates per row over a column
      When query
        """
        SELECT make_ym_interval(y, m) AS result
        FROM VALUES (1, 6), (CAST(NULL AS INT), 3), (2, CAST(NULL AS INT)) AS t(y, m)
        """
      Then query result
        | result                       |
        | INTERVAL '1-6' YEAR TO MONTH |
        | NULL                         |
        | NULL                         |

  Rule: Non-NULL arguments build a year-month interval
    Scenario Outline: Build: <case>
      When query
        """
        SELECT make_ym_interval(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                     | args   | result                         |
        | years and months combine                                 | 1, 6   | INTERVAL '1-6' YEAR TO MONTH   |
        | zero years and months                                    | 0, 0   | INTERVAL '0-0' YEAR TO MONTH   |
        | negative years and months                                | -1, -6 | INTERVAL '-1-6' YEAR TO MONTH  |
        | months overflowing into years are normalized             | 2, 13  | INTERVAL '3-1' YEAR TO MONTH   |
        | negative years with positive months normalize below zero | -1, 1  | INTERVAL '-0-11' YEAR TO MONTH |

  Rule: Omitted arguments default to zero
    Scenario Outline: Omitted argument: <case>
      When query
        """
        SELECT make_ym_interval(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                         | args | result                       |
        | no arguments builds a zero interval          |      | INTERVAL '0-0' YEAR TO MONTH |
        | single argument builds a whole-year interval | 2    | INTERVAL '2-0' YEAR TO MONTH |
        | single NULL argument yields NULL             | NULL | NULL                         |

  Rule: More than two arguments is an error
    Scenario: three arguments is rejected
      When query
        """
        SELECT make_ym_interval(1, 2, 3) AS result
        """
      Then query error make_ym_interval

  Rule: Integer overflow is an error regardless of ANSI mode
    Scenario Outline: Integer overflow is an error regardless of ANSI mode: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_ym_interval(200000000, 0) AS result
        """
      Then query error INTERVAL_ARITHMETIC_OVERFLOW

      Examples:
        | case                               | ansi  |
        | overflow errors with ANSI enabled  | true  |
        | overflow errors with ANSI disabled | false |

  @function(nullability) @spark-4
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to make_ym_interval yields the schema Spark declares
      When query
        """
        SELECT make_ym_interval(1, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: interval year to month (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to make_ym_interval yields the schema Spark declares
      When query
        """
        SELECT make_ym_interval(CAST(id AS INT), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: interval year to month (nullable = false)
        """

    Scenario: a nullable column input to make_ym_interval stays nullable
      When query
        """
        SELECT make_ym_interval(c, 2) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: interval year to month (nullable = true)
        """

    Scenario: non-null string arguments are nullable, because Spark casts them to INT
      When query
        """
        SELECT make_ym_interval('1', '2') AS result
        """
      Then query schema
        """
        root
         |-- result: interval year to month (nullable = true)
        """

  @function(nullability) @spark-4
  Rule: Nullability through Spark's implicit casts
  # String -> * is force-nullable (Cast.scala:458)
  # Spark 4+: PySpark 3.5 renders the type as a bare `interval` in `treeString`.

    @sail-bug
    Scenario Outline: make_ym_interval without an implicit cast keeps its non-nullable schema
      When query
        """
        SELECT make_ym_interval(<input>, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: interval year to month (nullable = false)
        """

      Examples:
        | case    | input |
        | no cast | 1     |

    Scenario Outline: make_ym_interval through a force-nullable implicit cast: <case>
      When query
        """
        SELECT make_ym_interval(<input>, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: interval year to month (nullable = true)
        """

      Examples:
        | case          | input |
        | STRING -> INT | '1'   |

  # Spark 4.2.0 IntervalUtils.scala `makeYearMonthInterval`: Math.toIntExact(addExact(month,
  # multiplyExact(year, 12))), so the Int month range is fully usable up to its bounds.
  Rule: The Int month range is usable up to its bounds
    Scenario Outline: Bound: <case>
      When query
        """
        SELECT make_ym_interval(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | args            | result                                |
        | the maximum from years and months  | 178956970, 7    | INTERVAL '178956970-7' YEAR TO MONTH  |
        | the minimum from years and months  | -178956970, -8  | INTERVAL '-178956970-8' YEAR TO MONTH |
        | the maximum from months alone      | 0, 2147483647   | INTERVAL '178956970-7' YEAR TO MONTH  |

    Scenario: one month past the maximum overflows
      When query
        """
        SELECT make_ym_interval(178956970, 8) AS result
        """
      Then query error \[INTERVAL_ARITHMETIC_OVERFLOW\.WITHOUT_SUGGESTION\]

  # Spark 4.2.0 intervalExpressions.scala `MakeYMInterval` is ImplicitCastInputTypes over
  # (INT, INT): DECIMAL / STRING / BIGINT arguments are cast to INT first.
  Rule: Arguments are implicitly cast to INT

    @sail-bug
    Scenario Outline: make_ym_interval casts a non-integer years argument to INT: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_ym_interval(<years>, 0) AS result FROM VALUES (<v1>), (<v2>) AS t(y)
        """
      Then query result
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | case                                  | ansi  | years | v1  | v2   | r1                           | r2                            |
        | decimal column truncates toward zero  | true  | y     | 1.9 | -2.5 | INTERVAL '1-0' YEAR TO MONTH | INTERVAL '-2-0' YEAR TO MONTH |
        | decimal literal truncates toward zero | true  | 1.9   | 1.9 | -2.5 | INTERVAL '1-0' YEAR TO MONTH | INTERVAL '1-0' YEAR TO MONTH  |
        | malformed string is NULL              | false | y     | 'x' | '3'  | NULL                         | INTERVAL '3-0' YEAR TO MONTH  |

    @sail-bug
    Scenario Outline: make_ym_interval arguments that do not cast to INT: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_ym_interval(<years>, 0) AS result
        """
      Then query error <error>

      Examples:
        | case                                    | ansi  | years                     | error                                                |
        | malformed string under ANSI             | true  | 'x'                       | CAST_INVALID_INPUT                               |
        | BIGINT that does not fit INT under ANSI | true  | CAST(3000000000 AS BIGINT) | CAST_OVERFLOW                                    |
        | legacy cast wraps, then months overflow | false | CAST(3000000000 AS BIGINT) | INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION |
