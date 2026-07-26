@next_day
Feature: next_day comprehensive tests

  Rule: Argument count validation

    Scenario Outline: Arity: <case>
      When query
        """
        SELECT next_day(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                            | args                             |
        | next_day zero arguments errors  |                                  |
        | next_day one argument errors    | DATE'2024-01-10'                 |
        | next_day three arguments errors | DATE'2024-01-10', 'Mon', 'extra' |

  Rule: NULL combinatorial

    Scenario Outline: NULL combinatorial: <case>
      When query
        """
        SELECT next_day(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                   | args                                     |
        | next_day NULL date     | CAST(NULL AS DATE), 'Monday'             |
        | next_day NULL day name | DATE'2024-01-10', NULL                   |
        | next_day both NULL     | CAST(NULL AS DATE), CAST(NULL AS STRING) |

  Rule: All days of week (from Wednesday 2024-01-10)

    Scenario Outline: Day of week: <case>
      When query
        """
        SELECT next_day(DATE'2024-01-10', <day>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                             | day         | result     |
        | next_day Monday                                  | 'Monday'    | 2024-01-15 |
        | next_day Tuesday                                 | 'Tuesday'   | 2024-01-16 |
        | next_day Wednesday (same day skips to next week) | 'Wednesday' | 2024-01-17 |
        | next_day Thursday                                | 'Thursday'  | 2024-01-11 |
        | next_day Friday                                  | 'Friday'    | 2024-01-12 |
        | next_day Saturday                                | 'Saturday'  | 2024-01-13 |
        | next_day Sunday                                  | 'Sunday'    | 2024-01-14 |

  Rule: Abbreviated day names

    Scenario Outline: Abbreviated: <case>
      When query
        """
        SELECT next_day(DATE'2024-01-10', <day>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case        | day  | result     |
        | next_day Mo | 'Mo' | 2024-01-15 |
        | next_day Tu | 'Tu' | 2024-01-16 |
        | next_day We | 'We' | 2024-01-17 |

  Rule: Case insensitive

    Scenario Outline: Case insensitive: <case>
      When query
        """
        SELECT next_day(DATE'2024-01-10', <day>) AS result
        """
      Then query result
        | result     |
        | 2024-01-15 |

      Examples:
        | case               | day      |
        | next_day lowercase | 'monday' |
        | next_day uppercase | 'MONDAY' |

    # Spark applies toUpperCase(Locale.ROOT), i.e. full Unicode case folding, so a
    # non-ASCII letter that upper-cases into a day-name letter is valid. Exactly two
    # codepoints are reachable: U+0131 (dotless i) -> 'I' and U+017F (long s) -> 'S'.
    # These two scenarios pin that behaviour. An ASCII-only compare
    # (eq_ignore_ascii_case) to save the parser's per-row allocation must therefore
    # stay behind an `is_ascii()` guard, or it silently diverges from Spark here.
    Scenario Outline: Unicode case folding: <case>
      When query
        """
        SELECT next_day(DATE'2024-01-10', <day>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                               | day      | result     |
        | next_day matches a day name containing a dotless i | 'frıday' | 2024-01-12 |
        | next_day matches a day name containing a long s    | 'ſunday' | 2024-01-14 |

    # Case folding is not normalisation: full-width letters do not fold to ASCII.
    Scenario: next_day rejects a full-width day name under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'ＭＯＮＤＡＹ') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Surrounding whitespace is not trimmed

    # Spark is case-insensitive but does NOT trim the day name: surrounding
    # whitespace makes it invalid, exactly like an unknown name.

    Scenario: next_day ANSI=true errors on a padded day name
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'Monday ') AS result
        """
      Then query error .*Illegal input for day of week.*

    Scenario: next_day ANSI=false returns NULL on a padded day name
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT next_day(DATE'2024-01-10', '  Monday  ') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: String date input coercion

    Scenario: next_day with string date
      When query
        """
        SELECT next_day('2024-01-10', 'Monday') AS result
        """
      Then query result
        | result     |
        | 2024-01-15 |

  Rule: Multi-row

    Scenario: next_day multi-row
      When query
        """
        SELECT next_day(d, day) AS result FROM VALUES (DATE'2024-01-10', 'Monday'), (DATE'2024-01-10', 'Friday'), (CAST(NULL AS DATE), 'Monday') AS t(d, day)
        """
      Then query result
        | result     |
        | 2024-01-15 |
        | 2024-01-12 |
        | NULL       |

  Rule: Error conditions

    Scenario: next_day multi-row with invalid day name errors
      When query
        """
        SELECT next_day(d, day) AS result FROM VALUES (DATE'2024-01-10', 'Monday'), (DATE'2024-01-10', 'InvalidDay') AS t(d, day)
        """
      Then query error .*Illegal input for day of week.*

    Scenario: next_day invalid day name errors
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'InvalidDay') AS result
        """
      Then query error .*Illegal input for day of week.*

  Rule: ANSI mode on invalid day name (Spark JVM parity)

    # ANSI=true → error with ILLEGAL_DAY_OF_WEEK (matches Spark strict mode).
    # ANSI=false → returns NULL (matches Spark lenient mode).
    # Bound at planning time via PlanConfig::ansi_mode; serialized in
    # SparkNextDayUdf for distributed execution.

    Scenario: next_day ANSI=true errors on invalid day name
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'InvalidDay') AS result
        """
      Then query error .*Illegal input for day of week.*

    Scenario: next_day ANSI=false returns NULL on invalid day name
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT next_day(DATE'2024-01-10', 'InvalidDay') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: next_day ANSI=false multi-row with mixed valid and invalid day names
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT next_day(d, day) AS result FROM VALUES
          (DATE'2024-01-10', 'Monday'),
          (DATE'2024-01-10', 'InvalidDay')
          AS t(d, day)
        """
      Then query result
        | result     |
        | 2024-01-15 |
        | NULL       |

  Rule: Timestamp implicit coercion to Date

    # Spark implicitly casts Timestamp / Timestamp_NTZ to Date before applying
    # next_day. Regression test for the same pattern as issue #1735 (last_day).

    Scenario Outline: Timestamp coercion: <case>
      When query
        """
        SELECT next_day(CAST(<input> AS <type>), <day>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                       | input                 | type          | day   | result     |
        | next_day accepts TIMESTAMP input (Spark casts to Date)     | '2024-01-15 10:30:00' | TIMESTAMP     | 'Mon' | 2024-01-22 |
        | next_day accepts TIMESTAMP_NTZ input (Spark casts to Date) | '2024-01-15 10:30:00' | TIMESTAMP_NTZ | 'Fri' | 2024-01-19 |
        | next_day on TIMESTAMP at year boundary                     | '2024-12-31 23:59:59' | TIMESTAMP     | 'Wed' | 2025-01-01 |

  Rule: next_day — the argument may come from a column

    @column_args
    Scenario: next_day with the argument as a literal
      When query
        """
        SELECT next_day('2015-01-14', 'TU') AS result
        """
      Then query result ordered
        | result     |
        | 2015-01-20 |

    @column_args
    Scenario Outline: Argument from a column: <case>
      When query
        """
        SELECT next_day(<date>, c) AS result FROM VALUES (1, <v1>), (2, <v2>) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | case                                                                 | date              | v1   | v2   | r1         | r2         |
        | next_day takes argument 2 from a column containing NULL              | '2015-01-14'      | 'TU' | NULL | 2015-01-20 | NULL       |
        | next_day takes argument 2 from a column                              | '2015-01-14'      | 'TU' | 'TU' | 2015-01-20 | 2015-01-20 |
        | next_day takes argument 2 from a column holding two different values | DATE '2015-01-14' | 'TU' | 'FR' | 2015-01-20 | 2015-01-16 |

  @spark_null
  Rule: Output schema

    Scenario: a non-null date literal yields a date
      When query
        """
        SELECT next_day(DATE '2024-01-15', 'MON') AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

    Scenario: a non-null date column yields a date
      When query
        """
        SELECT next_day(CAST(CAST(id AS TIMESTAMP) AS DATE), 'MON') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

    Scenario: a nullable date column stays nullable
      When query
        """
        SELECT next_day(c, 'MON') AS result FROM VALUES (DATE '2024-01-15'), (CAST(NULL AS DATE)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """
