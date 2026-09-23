Feature: make_dt_interval output schema

  @function(nullability) @spark-4
  Rule: Output schema

    Scenario: a non-null literal input to make_dt_interval yields the schema Spark declares
      When query
        """
        SELECT make_dt_interval(1, 12, 30, 01.001001) AS result
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = false)
        """

    Scenario: a non-null column input to make_dt_interval yields the schema Spark declares
      When query
        """
        SELECT make_dt_interval(CAST(id AS INT), 12, 30, 01.001001) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = false)
        """

    Scenario: a nullable column input to make_dt_interval stays nullable
      When query
        """
        SELECT make_dt_interval(c, 12, 30, 01.001001) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = true)
        """

  Rule: Result values (migrated from test_make_dt_interval.txt doctests)

    Scenario Outline: Doctest (derived column name): <case>
      When query
        """
        SELECT (make_dt_interval(<args>))
        """
      Then query result
        | <name>   |
        | <result> |

      Examples:
        | case                                  | args          | name                            | result                                |
        | make_dt_interval doctest #1 (result)  | null, 0, 0, 0 | make_dt_interval(NULL, 0, 0, 0) | NULL                                  |
        | make_dt_interval doctest #2 (result)  | 0, null, 0, 0 | make_dt_interval(0, NULL, 0, 0) | NULL                                  |
        | make_dt_interval doctest #3 (result)  | 0, 0, null, 0 | make_dt_interval(0, 0, NULL, 0) | NULL                                  |
        | make_dt_interval doctest #4 (result)  | 0, 0, 0, null | make_dt_interval(0, 0, 0, NULL) | NULL                                  |
        | make_dt_interval doctest #10 (result) | 0, 0, 0, 0    | make_dt_interval(0, 0, 0, 0)    | INTERVAL '0 00:00:00' DAY TO SECOND   |
        | make_dt_interval doctest #13 (result) | 0, 0, 0, 0.1  | make_dt_interval(0, 0, 0, 0.1)  | INTERVAL '0 00:00:00.1' DAY TO SECOND |

    Scenario Outline: Doctest (aliased): <case>
      When query
        """
        SELECT (make_dt_interval(<args>)) AS make_dt_interval
        """
      Then query result
        | make_dt_interval |
        | <result>         |

      Examples:
        | case                                 | args       | result                              |
        | make_dt_interval doctest #5 (result) |            | INTERVAL '0 00:00:00' DAY TO SECOND |
        | make_dt_interval doctest #6 (result) | 1          | INTERVAL '1 00:00:00' DAY TO SECOND |
        | make_dt_interval doctest #7 (result) | 1, 1       | INTERVAL '1 01:00:00' DAY TO SECOND |
        | make_dt_interval doctest #8 (result) | 1, 1, 1    | INTERVAL '1 01:01:00' DAY TO SECOND |
        | make_dt_interval doctest #9 (result) | 1, 1, 1, 1 | INTERVAL '1 01:01:01' DAY TO SECOND |

    Scenario Outline: Doctest (bare alias): <case>
      When query
        """
        SELECT (make_dt_interval(<args>)) <alias>
        """
      Then query result
        | <alias>  |
        | <result> |

      Examples:
        | case                                  | args         | alias | result                              |
        | make_dt_interval doctest #11 (result) | -1, 24, 0, 0 | df    | INTERVAL '0 00:00:00' DAY TO SECOND |
        | make_dt_interval doctest #12 (result) | 1, -24, 0, 0 | dt    | INTERVAL '0 00:00:00' DAY TO SECOND |

    Scenario: make_dt_interval doctest #14 (result)
      When query
        """
        SELECT day, hour, `min`, sec, make_dt_interval(day) AS r FROM VALUES (CAST(1 AS BIGINT), CAST(12 AS BIGINT), CAST(30 AS BIGINT), CAST(1.001001 AS DOUBLE)) AS t(day, hour, `min`, sec)
        """
      Then query result
        | day | hour | min | sec      | r                                   |
        | 1   | 12   | 30  | 1.001001 | INTERVAL '1 00:00:00' DAY TO SECOND |

    Scenario: make_dt_interval doctest #15 (result)
      When query
        """
        SELECT day, hour, `min`, sec, make_dt_interval(day, hour) AS r FROM VALUES (CAST(1 AS BIGINT), CAST(12 AS BIGINT), CAST(30 AS BIGINT), CAST(1.001001 AS DOUBLE)) AS t(day, hour, `min`, sec)
        """
      Then query result
        | day | hour | min | sec      | r                                   |
        | 1   | 12   | 30  | 1.001001 | INTERVAL '1 12:00:00' DAY TO SECOND |

    Scenario: make_dt_interval doctest #16 (result)
      When query
        """
        SELECT day, hour, `min`, sec, make_dt_interval(day, hour, `min`) AS r FROM VALUES (CAST(1 AS BIGINT), CAST(12 AS BIGINT), CAST(30 AS BIGINT), CAST(1.001001 AS DOUBLE)) AS t(day, hour, `min`, sec)
        """
      Then query result
        | day | hour | min | sec      | r                                   |
        | 1   | 12   | 30  | 1.001001 | INTERVAL '1 12:30:00' DAY TO SECOND |

    Scenario: make_dt_interval doctest #17 (result)
      When query
        """
        SELECT day, hour, `min`, sec, make_dt_interval(day, hour, `min`, sec) AS r FROM VALUES (CAST(1 AS BIGINT), CAST(12 AS BIGINT), CAST(30 AS BIGINT), CAST(1.001001 AS DOUBLE)) AS t(day, hour, `min`, sec)
        """
      Then query result
        | day | hour | min | sec      | r                                          |
        | 1   | 12   | 30  | 1.001001 | INTERVAL '1 12:30:01.001001' DAY TO SECOND |

  # Spark 4.2.0 intervalExpressions.scala `MakeDTInterval`: inputs are implicitly cast to
  # (INT, INT, INT, DECIMAL(18, 6)); IntervalUtils.scala `makeDayTimeInterval` sums Long
  # microseconds with Math.addExact / multiplyExact and turns any ArithmeticException into
  # INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION. That branch is NOT ANSI-gated.
  Rule: Microsecond overflow raises regardless of ANSI

    @sail-bug
    Scenario Outline: make_dt_interval overflow raises: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_dt_interval(<args>) AS result FROM VALUES (1), (106751992) AS t(d)
        """
      Then query error \[INTERVAL_ARITHMETIC_OVERFLOW\.WITHOUT_SUGGESTION\]

      Examples:
        | case                               | ansi  | args                       |
        | days beyond Long micros, ANSI on   | true  | 106751992                  |
        | days beyond Long micros, ANSI off  | false | 106751992                  |
        | one micro past the maximum         | true  | 106751991, 4, 0, 54.775808 |
        | one micro past the maximum, legacy | false | 106751991, 4, 0, 54.775808 |
        | column days, ANSI on               | true  | d                          |
        | column days, ANSI off              | false | d                          |

  # The full Long microsecond range is representable. Sail answers NULL for these (or fails
  # with "declared as non-nullable but contains null values" on a column).
  Rule: The full Long microsecond range is representable

    @sail-bug
    Scenario Outline: make_dt_interval builds a value near the Long bounds: <case>
      When query
        """
        SELECT CAST(make_dt_interval(<args>) AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                           | args                           | result                                              |
        | the maximum                    | 106751991, 4, 0, 54.775807     | INTERVAL '106751991 04:00:54.775807' DAY TO SECOND  |
        | the minimum                    | -106751991, -4, -0, -54.775808 | INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND |
        | Int.MaxValue hours and minutes | 0, 2147483647, 2147483647      | INTERVAL '90969793 09:07:00' DAY TO SECOND          |

    @sail-bug
    Scenario: make_dt_interval builds large hours and minutes from columns
      When query
        """
        SELECT make_dt_interval(0, h, mi) AS result
        FROM VALUES (2147483647, 2147483647), (1, 2) AS t(h, mi)
        """
      Then query result
        | result                                     |
        | INTERVAL '90969793 09:07:00' DAY TO SECOND |
        | INTERVAL '0 01:02:00' DAY TO SECOND        |

  Rule: Seconds go through DECIMAL(18, 6)

    Scenario: make_dt_interval rounds sub-microsecond seconds half up per row
      When query
        """
        SELECT make_dt_interval(0, 0, 0, s) AS result FROM VALUES (1.0000005), (2.0000004) AS t(s)
        """
      Then query result
        | result                                     |
        | INTERVAL '0 00:00:01.000001' DAY TO SECOND |
        | INTERVAL '0 00:00:02' DAY TO SECOND        |

    @sail-bug
    Scenario: make_dt_interval seconds outside DECIMAL(18, 6) fail the cast under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_dt_interval(0, 0, 0, 1000000000000.5) AS result
        """
      Then query error \[NUMERIC_VALUE_OUT_OF_RANGE\.WITH_SUGGESTION\]

    @sail-bug
    Scenario: make_dt_interval seconds outside DECIMAL(18, 6) yield NULL without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_dt_interval(0, 0, 0, s) AS result
        FROM VALUES (1000000000000.5), (2.5) AS t(s)
        """
      Then query result
        | result                                |
        | NULL                                  |
        | INTERVAL '0 00:00:02.5' DAY TO SECOND |

  # A DOUBLE -> DECIMAL cast is force-nullable (Spark 4.2.0 Cast.scala), so a DOUBLE `secs`
  # makes the result nullable even for non-null inputs.
  @function(nullability) @spark-4
  Rule: Output schema through the seconds cast

    @sail-bug
    Scenario Outline: a DOUBLE seconds argument makes make_dt_interval nullable: <case>
      When query
        """
        SELECT make_dt_interval(1, 2, 3, <secs>) AS result FROM VALUES (CAST(4.5 AS DOUBLE)) AS t(s)
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = true)
        """

      Examples:
        | case           | secs                   |
        | double literal | CAST(4.5 AS DOUBLE)    |
        | double column  | s                      |

  # ImplicitCastInputTypes: DECIMAL / STRING / BIGINT days are cast to INT. Sail rejects the
  # DECIMAL and STRING signatures at planning and reports its own cast error for BIGINT.
  Rule: Integer fields accept implicitly cast arguments

    @sail-bug
    Scenario Outline: make_dt_interval casts a non-integer days argument to INT: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_dt_interval(d) AS result FROM VALUES (<v1>), (<v2>) AS t(d)
        """
      Then query result
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | case                          | ansi  | v1  | v2   | r1                                  | r2                                   |
        | decimal truncates toward zero | true  | 1.9 | -2.5 | INTERVAL '1 00:00:00' DAY TO SECOND | INTERVAL '-2 00:00:00' DAY TO SECOND |
        | malformed string is NULL      | false | 'x' | '3'  | NULL                                | INTERVAL '3 00:00:00' DAY TO SECOND  |

    @sail-bug
    Scenario Outline: make_dt_interval BIGINT days that do not fit INT: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT make_dt_interval(CAST(3000000000 AS BIGINT)) AS result
        """
      Then query error <error>

      Examples:
        | case                                   | ansi  | error                                             |
        | ANSI cast overflow                     | true  | CAST_OVERFLOW                                 |
        | legacy cast wraps, then days overflow  | false | INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION |
