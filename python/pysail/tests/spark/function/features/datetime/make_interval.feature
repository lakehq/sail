Feature: make_interval output schema

  @function(nullability) @spark-4
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to make_interval yields the schema Spark declares
      When query
        """
        SELECT make_interval(100, 11, 1, 1, 12, 30, 01.001001) AS result
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to make_interval yields the schema Spark declares
      When query
        """
        SELECT make_interval(CAST(id AS INT), 11, 1, 1, 12, 30, 01.001001) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = false)
        """

    Scenario: a nullable column input to make_interval stays nullable
      When query
        """
        SELECT make_interval(c, 11, 1, 1, 12, 30, 01.001001) AS result FROM VALUES (100), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = true)
        """

  Rule: Result values (migrated from test_make_interval.txt doctests)

    Scenario: make_interval doctest #1 (result)
      When query
        """
        SELECT y, m, w, d, h, mi, s, make_interval(y, m, w, d, h, mi, s) AS interval FROM VALUES (1, 2, 3, 4, 5, 6, CAST(7.5 AS DOUBLE)), (CAST(NULL AS INT), 2, 3, 4, 5, 6, CAST(7.5 AS DOUBLE)), (1, CAST(NULL AS INT), 3, 4, 5, 6, CAST(7.5 AS DOUBLE)), (1, 2, CAST(NULL AS INT), 4, 5, 6, CAST(7.5 AS DOUBLE)), (1, 2, 3, CAST(NULL AS INT), 5, 6, CAST(7.5 AS DOUBLE)), (1, 2, 3, 4, CAST(NULL AS INT), 6, CAST(7.5 AS DOUBLE)), (1, 2, 3, 4, 5, CAST(NULL AS INT), CAST(7.5 AS DOUBLE)), (1, 2, 3, 4, 5, 6, CAST(NULL AS DOUBLE)), (1, 1, 1, 1, 1, 1, CAST(1.0 AS DOUBLE)), (0, 0, 0, 0, 0, 0, CAST('NaN' AS DOUBLE)), (0, 0, 0, 0, 0, 0, CAST('Infinity' AS DOUBLE)), (0, 0, 0, 0, 0, 0, CAST('-Infinity' AS DOUBLE)) AS t(y, m, w, d, h, mi, s)
        """
      Then query result
        | y    | m    | w    | d    | h    | mi   | s         | interval                                               |
        | 1    | 2    | 3    | 4    | 5    | 6    | 7.5       | 1 years 2 months 25 days 5 hours 6 minutes 7.5 seconds |
        | NULL | 2    | 3    | 4    | 5    | 6    | 7.5       | NULL                                                   |
        | 1    | NULL | 3    | 4    | 5    | 6    | 7.5       | NULL                                                   |
        | 1    | 2    | NULL | 4    | 5    | 6    | 7.5       | NULL                                                   |
        | 1    | 2    | 3    | NULL | 5    | 6    | 7.5       | NULL                                                   |
        | 1    | 2    | 3    | 4    | NULL | 6    | 7.5       | NULL                                                   |
        | 1    | 2    | 3    | 4    | 5    | NULL | 7.5       | NULL                                                   |
        | 1    | 2    | 3    | 4    | 5    | 6    | NULL      | NULL                                                   |
        | 1    | 1    | 1    | 1    | 1    | 1    | 1.0       | 1 years 1 months 8 days 1 hours 1 minutes 1 seconds    |
        | 0    | 0    | 0    | 0    | 0    | 0    | NaN       | NULL                                                   |
        | 0    | 0    | 0    | 0    | 0    | 0    | Infinity  | NULL                                                   |
        | 0    | 0    | 0    | 0    | 0    | 0    | -Infinity | NULL                                                   |

  Rule: Valid argument combinations build interval

    Scenario: All zeros yields zero interval
      When query
      """
      SELECT CAST(make_interval(0, 0, 0, 0, 0, 0, 0) AS STRING) AS result
      """
      Then query result
      | result    |
      | 0 seconds |

    Scenario: Year only
      When query
      """
      SELECT CAST(make_interval(1) AS STRING) AS result
      """
      Then query result
      | result  |
      | 1 years |

    Scenario: Full args
      When query
      """
      SELECT CAST(make_interval(1, 2, 3, 4, 5, 6, 7.5) AS STRING) AS result
      """
      Then query result
      | result                                                 |
      | 1 years 2 months 25 days 5 hours 6 minutes 7.5 seconds |

    Scenario: NULL field propagates to NULL result
      When query
      """
      SELECT make_interval(1, NULL, 3) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Full args with all-ones
      When query
      """
      SELECT CAST(make_interval(1, 1, 1, 1, 1, 1, 1.0) AS STRING) AS result
      """
      Then query result
      | result                                              |
      | 1 years 1 months 8 days 1 hours 1 minutes 1 seconds |

  Rule: Per-row NULL propagation across all fields

    Scenario: Each field NULL in turn yields NULL row
      When query
      """
      SELECT CAST(make_interval(y, m, w, d, h, mi, sec) AS STRING) AS result FROM VALUES
        (CAST(NULL AS INT), 2, 3, 4, 5, 6, CAST(7.5 AS DOUBLE)),
        (1, CAST(NULL AS INT), 3, 4, 5, 6, CAST(7.5 AS DOUBLE)),
        (1, 2, CAST(NULL AS INT), 4, 5, 6, CAST(7.5 AS DOUBLE)),
        (1, 2, 3, CAST(NULL AS INT), 5, 6, CAST(7.5 AS DOUBLE)),
        (1, 2, 3, 4, CAST(NULL AS INT), 6, CAST(7.5 AS DOUBLE)),
        (1, 2, 3, 4, 5, CAST(NULL AS INT), CAST(7.5 AS DOUBLE)),
        (1, 2, 3, 4, 5, 6, CAST(NULL AS DOUBLE)) AS t(y, m, w, d, h, mi, sec)
      """
      Then query result
      | result |
      | NULL   |
      | NULL   |
      | NULL   |
      | NULL   |
      | NULL   |
      | NULL   |
      | NULL   |

  Rule: Non-finite seconds yield NULL

    Scenario: NaN seconds returns NULL
      When query
      """
      SELECT CAST(make_interval(0, 0, 0, 0, 0, 0, CAST('NaN' AS DOUBLE)) AS STRING) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Infinity seconds returns NULL
      When query
      """
      SELECT CAST(make_interval(0, 0, 0, 0, 0, 0, CAST('Infinity' AS DOUBLE)) AS STRING) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Overflow raises error

    @spark-4
    Scenario: Year overflow raises error
      When query
      """
      SELECT make_interval(2147483647)
      """
      Then query error overflow|month_day_nano_interval|ARITHMETIC|calendar_interval|Unsupported

  Rule: Argument count validation matches Spark

    @spark-4
    Scenario: No args raises WRONG_NUM_ARGS
      When query
      """
      SELECT make_interval()
      """
      Then query error WRONG_NUM_ARGS|UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION|month_day_nano_interval|calendar_interval|Unsupported

  # Spark 4.2.0 intervalExpressions.scala `MakeInterval`: inputs are implicitly cast to
  # (INT x6, DECIMAL(18, 6)); IntervalUtils.scala `makeInterval` builds the value with
  # Math.addExact / multiplyExact over (Int months, Int days, Long micros). An
  # ArithmeticException becomes ARITHMETIC_OVERFLOW when `failOnError` (= ANSI) is on, NULL
  # otherwise. Sail answers NULL even with ANSI on.
  Rule: Field overflow follows the ANSI flag

    @sail-bug
    Scenario Outline: make_interval field overflow raises under ANSI: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_interval(<args>) AS result FROM VALUES (1, 2), (3, 4), (2147483647, 0) AS t(y, m)
        """
      Then query error \[ARITHMETIC_OVERFLOW\]

      Examples:
        | case                       | args               |
        | literal years overflow     | 2147483647         |
        | literal negative years     | -2147483648        |
        | literal weeks overflow     | 0, 0, 306783379    |
        | column years overflow      | y, m               |

    Scenario: make_interval field overflow yields NULL per row without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_interval(y, m) AS result FROM VALUES (1, 2), (3, 4), (2147483647, 0) AS t(y, m)
        """
      Then query result
        | result           |
        | 1 years 2 months |
        | 3 years 4 months |
        | NULL             |

  # The microsecond field is a Long in Spark: Int.MaxValue hours plus Int.MaxValue minutes
  # (about 7.7e18 micros) still fits. Sail stores nanoseconds and overflows 1000x earlier,
  # answering NULL.
  Rule: The time part spans the full Long microsecond range

    @sail-bug
    Scenario Outline: make_interval keeps a time part that fits in Long microseconds: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_interval(0, 0, 0, 0, h, mi, s) AS result
        FROM VALUES (2147483647, 2147483647, 0), (1, 2, 99999999999.999999) AS t(h, mi, s)
        """
      Then query result
        | result                                                    |
        | 2183275041 hours 7 minutes                                |
        | 27777778 hours 48 minutes 39.999999 seconds               |

      Examples:
        | case       | ansi  |
        | ANSI on    | true  |
        | ANSI off   | false |

  # `secs` is implicitly cast to DECIMAL(18, 6): digits beyond the microsecond round HALF_UP,
  # and a value that does not fit DECIMAL(18, 6) fails the cast (ANSI) or becomes NULL.
  Rule: Seconds go through DECIMAL(18, 6)

    @sail-bug
    Scenario Outline: make_interval rounds sub-microsecond seconds half up: <case>
      When query
        """
        SELECT make_interval(0, 0, 0, 0, 0, 0, <secs>) AS result FROM VALUES (1.0000005), (2.0000004) AS t(s)
        """
      Then query result
        | result   |
        | <result> |
        | <other>  |

      Examples:
        | case                    | secs      | result           | other            |
        | literal rounds up       | 1.0000005 | 1.000001 seconds | 1.000001 seconds |
        | column rounds per row   | s         | 1.000001 seconds | 2 seconds        |

    @sail-bug
    Scenario Outline: make_interval seconds outside DECIMAL(18, 6) fail the cast under ANSI: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_interval(0, 0, 0, 0, 0, 0, <secs>) AS result
        FROM VALUES (CAST(1000000000000.5 AS DOUBLE)), (CAST(1.5 AS DOUBLE)) AS t(s)
        """
      Then query error \[NUMERIC_VALUE_OUT_OF_RANGE\.WITH_SUGGESTION\]

      Examples:
        | case           | secs            |
        | decimal literal | 1000000000000.5 |
        | double column  | s               |

    Scenario: make_interval seconds outside DECIMAL(18, 6) yield NULL without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_interval(0, 0, 0, 0, 0, 0, s) AS result
        FROM VALUES (CAST(1000000000000.5 AS DOUBLE)), (CAST(1.5 AS DOUBLE)) AS t(s)
        """
      Then query result
        | result      |
        | NULL        |
        | 1.5 seconds |

  # ImplicitCastInputTypes: a DECIMAL or STRING argument is cast to INT (truncating toward
  # zero; a malformed string is NULL without ANSI). Sail rejects the signature at planning.
  Rule: Integer fields accept implicitly cast arguments

    @sail-bug
    Scenario Outline: make_interval casts a non-integer years argument to INT: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_interval(y) AS result FROM VALUES (<v1>), (<v2>) AS t(y)
        """
      Then query result
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | case                          | ansi  | v1  | v2   | r1      | r2       |
        | decimal truncates toward zero | true  | 1.9 | -2.5 | 1 years | -2 years |
        | malformed string is NULL      | false | 'x' | '3'  | NULL    | 3 years  |
