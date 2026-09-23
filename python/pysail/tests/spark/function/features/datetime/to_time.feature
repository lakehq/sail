@spark-4.1
Feature: to_time (strict variant)
  Strict to_time that throws an error on invalid input.

  Rule: Valid input parses

    @sail-only
    Scenario Outline: Valid input: <case>
      When query
        """
        SELECT to_time(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case              | args                   | result          |
        | HH:MM:SS basic    | '10:30:45'             | 10:30:45        |
        | With microseconds | '10:30:45.123456'      | 10:30:45.123456 |
        | With format       | '10-30-45', 'HH-mm-ss' | 10:30:45        |

  Rule: Two-argument form is type-consistent with the one-argument form

    # Regression: in the 2-arg form a non-string first argument (TIME/TIMESTAMP)
    # must be coerced exactly as in the 1-arg form — cast straight to TIME with
    # the format ignored — not stringified and re-parsed through the format
    # (which would fail). See `time_with_try` in datetime.rs.

    @sail-only
    Scenario Outline: Two-arg: <case>
      When query
        """
        SELECT to_time(<arg>, <fmt>) AS result
        """
      Then query result
        | result   |
        | 10:30:45 |

      Examples:
        | case                                                              | arg                             | fmt        |
        | TIMESTAMP first argument ignores the format and extracts the time | TIMESTAMP '2024-01-15 10:30:45' | 'HH:mm:ss' |
        | TIME first argument ignores the format                            | TIME '10:30:45'                 | 'HH-mm-ss' |

    @sail-only
    Scenario: Unsupported first-argument type is rejected with a format too
      When query
        """
        SELECT to_time(123, 'HH:mm:ss')
        """
      Then query error (?i)STRING, TIME, TIMESTAMP or NULL|data type|Unsupported

  Rule: Wrong argument count is rejected

    @sail-only
    Scenario: Three arguments raise an error
      When query
        """
        SELECT to_time('10:30:45', 'HH:mm:ss', 'extra')
        """
      Then query error (?i)requires 1 or 2 arguments|arguments

  Rule: Invalid input throws

    @sail-only
    Scenario Outline: Invalid input: <case>
      When query
        """
        SELECT to_time(<arg>)
        """
      Then query error cannot parse|UNSUPPORTED_OPERATION|Unsupported|data type

      Examples:
        | case                           | arg          |
        | Garbage string raises error    | 'not-a-time' |
        | Out-of-range hour raises error | '25:00:00'   |

  Rule: NULL input propagates

    @sail-only
    Scenario: NULL input returns NULL
      When query
        """
        SELECT to_time(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Spark Java datetime pattern contract

    Background:
      Given config spark.sql.timeType.enabled = true

    Scenario: Java datetime pattern contract parses quoted literals and fractional widths with to_time
      When query
        """
        SELECT
          to_time(
            '10B30',
            concat('HH', chr(39), 'B', chr(39), 'mm')
          ) AS quoted_literal,
          to_time('10:30:45.1', 'HH:mm:ss.SSSSSS') AS short_fraction,
          to_time(CAST(NULL AS STRING), 'HH:mm:ss') AS null_input
        """
      Then query schema
        """
        root
         |-- quoted_literal: time(6) (nullable = true)
         |-- short_fraction: time(6) (nullable = true)
         |-- null_input: time(6) (nullable = true)
        """
      And query result
        | quoted_literal | short_fraction | null_input |
        | 10:30:00       | 10:30:45.1     | NULL       |

    Scenario Outline: Java datetime pattern contract makes to_time reject <case>
      When query
        """
        SELECT to_time(<value>, <format>)
        """
      Then query error <error>

      Examples:
        | case                                      | value                | format                         | error                                                             |
        | a fraction wider than the pattern         | '10:30:45.1234'      | 'HH:mm:ss.SSS'                  | (?i)CANNOT_PARSE_TIME\|datetime value does not match format        |
        | an unquoted restricted pattern letter     | '10B30'              | 'HHBmm'                         | (?i)INVALID_DATETIME_PATTERN\|invalid datetime pattern             |
        | a pattern with an unmatched literal quote | '10:30'              | concat('HH', chr(39), 'mm')     | (?i)INVALID_DATETIME_PATTERN\|invalid datetime pattern             |

  # Every expected value below was measured on Spark 4.2.0 (JVM, UTC). `ToTime` lives in
  # Spark 4.2.0 `timeExpressions.scala`: its input types are STRING only, and without a format
  # it parses with `DateTimeUtils.stringToTime` (the same lenient rules as CAST to TIME).
  Rule: Column input and Spark's lenient default parsing

    Scenario: to_time over a column with distinct rows
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_time(s) AS r FROM VALUES
          ('00:00:00'), ('09:05:03.5'), ('23:59:59.999999'), ('10:30:45.1234567'), (NULL) AS x(s)
        """
      Then query schema
        """
        root
         |-- r: time(6) (nullable = true)
        """
      And query result
        | r               |
        | 00:00:00        |
        | 09:05:03.5      |
        | 23:59:59.999999 |
        | 10:30:45.123456 |
        | NULL            |

    Scenario: to_time with a per-row format column
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_time(s, f) AS r FROM VALUES
          ('10-30-45', 'HH-mm-ss'), ('11.31', 'HH.mm'), ('10:30:45 PM', 'hh:mm:ss a'), (NULL, 'HH:mm') AS x(s, f)
        """
      Then query result
        | r        |
        | 10:30:45 |
        | 11:31:00 |
        | 22:30:45 |
        | NULL     |

    @sail-bug
    Scenario Outline: to_time without a format accepts <case>
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_time(<value>) AS r
        """
      Then query result
        | r        |
        | 10:30:45 |

      Examples:
        | case               | value        |
        | surrounding spaces | ' 10:30:45 ' |
        | a leading T        | 'T10:30:45'  |

  Rule: Parse failures raise CANNOT_PARSE_TIME in both ANSI modes

    @sail-bug
    Scenario Outline: to_time fails to parse <case> (ANSI <ansi>)
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_time(<args>) AS r
        """
      Then query error CANNOT_PARSE_TIME

      Examples:
        | case                               | ansi  | args                        |
        | garbage                            | true  | 'garbage'                   |
        | garbage                            | false | 'garbage'                   |
        | the hour 24                        | true  | '24:00:00'                  |
        | a bare number                      | true  | '12'                        |
        | a value that misses the format     | true  | '10:30', 'HH-mm'            |
        | a value that misses the format     | false | '10:30', 'HH-mm'            |
        | a format that needs a missing year | true  | '10:30:45', 'yyyy HH:mm:ss' |

    @sail-bug
    Scenario: to_time fails on the first row whose per-row format does not match
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_time(s, f) AS r FROM VALUES ('10-30-45', 'HH-mm-ss'), ('11.31', 'HH-mm') AS x(s, f)
        """
      Then query error CANNOT_PARSE_TIME

  Rule: The first argument must be a STRING

    @sail-bug
    Scenario Outline: to_time rejects a <case> first argument
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_time(<value>) AS r
        """
      Then query error UNEXPECTED_INPUT_TYPE

      Examples:
        | case | value             |
        | INT  | 10                |
        | DATE | DATE '2024-01-15' |
