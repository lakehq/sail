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

  # `MakeYMInterval` takes `ImplicitCastInputTypes(IntegerType, IntegerType)`
  # (`intervalExpressions.scala:570`), so every numeric or string argument goes through the
  # `Cast` to INT, with that cast's ANSI rules: a fraction truncates toward zero, a BIGINT or
  # DECIMAL out of range wraps with ANSI off, a DOUBLE saturates and NaN is 0, and a malformed
  # string is NULL with ANSI off.
  Rule: Arguments are cast to INT the way Spark casts them
    Scenario Outline: Cast argument with ANSI <ansi>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_ym_interval(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | ansi  | args                                       | result                        |
        | a DOUBLE truncates                  | true  | 1.9, 2                                     | INTERVAL '1-2' YEAR TO MONTH  |
        | a negative DOUBLE truncates to zero | false | CAST(-1.9 AS DOUBLE), 0                    | INTERVAL '-1-0' YEAR TO MONTH |
        | a DECIMAL truncates                 | true  | CAST(1.9 AS DECIMAL(3,1)), 2               | INTERVAL '1-2' YEAR TO MONTH  |
        | a FLOAT truncates                   | false | CAST(1.5 AS FLOAT), 0                      | INTERVAL '1-0' YEAR TO MONTH  |
        | a BIGINT in range                   | true  | CAST(1 AS BIGINT), CAST(2 AS DECIMAL(3,0)) | INTERVAL '1-2' YEAR TO MONTH  |
        | a string is parsed                  | true  | '1', '2'                                   | INTERVAL '1-2' YEAR TO MONTH  |
        | a string is trimmed                 | true  | ' 3 ', 0                                   | INTERVAL '3-0' YEAR TO MONTH  |
        | a BIGINT wraps with ANSI off        | false | CAST(4294967298 AS BIGINT), 0              | INTERVAL '2-0' YEAR TO MONTH  |
        | a DECIMAL wraps with ANSI off       | false | CAST(4294967298 AS DECIMAL(12,0)), 0       | INTERVAL '2-0' YEAR TO MONTH  |
        | NaN is zero with ANSI off           | false | CAST('NaN' AS DOUBLE), 0                   | INTERVAL '0-0' YEAR TO MONTH  |
        | a decimal string truncates          | false | '1.9', 0                                   | INTERVAL '1-0' YEAR TO MONTH  |
        | a malformed string is NULL          | false | 'x', 2                                     | NULL                          |
        | an out-of-range string is NULL      | false | '9999999999', 0                            | NULL                          |

    Scenario Outline: Cast argument with ANSI <ansi> is refused: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_ym_interval(<args>) AS result
        """
      Then query error <error>

      Examples:
        | case                                | ansi  | args                                 | error                        |
        | a BIGINT out of range               | true  | CAST(4294967298 AS BIGINT), 0        | CAST_OVERFLOW                |
        | a DOUBLE out of range               | true  | CAST(3e10 AS DOUBLE), 0              | CAST_OVERFLOW                |
        | NaN                                 | true  | CAST('NaN' AS DOUBLE), 0             | CAST_OVERFLOW                |
        | a DECIMAL out of range              | true  | CAST(4294967298 AS DECIMAL(12,0)), 0 | CAST_OVERFLOW                |
        | a malformed string                  | true  | 'x', 2                               | CAST_INVALID_INPUT           |
        | a decimal string                    | true  | '1.9', 0                             | CAST_INVALID_INPUT           |
        | an out-of-range string              | true  | '9999999999', 0                      | CAST_INVALID_INPUT           |
        | a saturated DOUBLE overflows months | false | CAST(3e10 AS DOUBLE), 0              | INTERVAL_ARITHMETIC_OVERFLOW |

    Scenario: String arguments from a column with ANSI off are cast per row
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_ym_interval(s) AS result FROM VALUES ('1'), ('x'), (NULL) AS t(s)
        """
      Then query result
        | result                       |
        | INTERVAL '1-0' YEAR TO MONTH |
        | NULL                         |
        | NULL                         |

  Rule: Arguments that are not numbers or strings are refused at analysis
    Scenario Outline: <case> is refused
      When query
        """
        SELECT make_ym_interval(<args>) AS result
        """
      Then query error <error>

      Examples:
        | case            | args                | error                                   |
        | a BOOLEAN       | true, 2             | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE |
        | a DATE          | DATE'2024-01-01', 2 | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE |
        | three arguments | 1, 2, 3             | WRONG_NUM_ARGS                          |

  @function(nullability) @spark-4
  Rule: Output schema

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

    # The implicit cast to INT is nullable from a STRING, a fraction or a DECIMAL whatever the
    # value, so the result is too; a BIGINT is not.
    Scenario Outline: a non-null <case> argument yields nullable <nullable>
      When query
        """
        SELECT make_ym_interval(<args>) AS result
        """
      Then query schema
        """
        root
         |-- result: interval year to month (nullable = <nullable>)
        """

      Examples:
        | case    | args                       | nullable |
        | BIGINT  | CAST(1 AS BIGINT), 2       | false    |
        | STRING  | '1', '2'                   | true     |
        | DOUBLE  | CAST(1.0 AS DOUBLE), 2     | true     |
        | DECIMAL | CAST(1 AS DECIMAL(3,0)), 2 | true     |
