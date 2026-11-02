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

  Rule: The first argument must be a STRING, in BOTH the one- and two-argument forms
    # Measured on Spark JVM 4.2.0 with spark.sql.timeType.enabled=true (the internal gate
    # whose default is `Utils.isTesting`, so TIME is unreachable in a normal session
    # unless the conf is set explicitly).
    #
    # Spark's ToTime declares the first parameter as STRING only and enforces it at
    # ANALYSIS time, including in the one-argument form. Sail instead coerces DATE /
    # TIMESTAMP / TIMESTAMP_NTZ / TIME straight to TIME and returns the time-of-day, so it
    # ACCEPTS every query below. That makes Sail a superset of Spark here, which is a
    # divergence rather than a feature: the @sail-only "Two-arg" outline earlier in this
    # file asserts Sail's behaviour as if it were correct, and because @sail-only is
    # skipped on the JVM it can never catch this.

    @sail-bug
    Scenario Outline: One-arg form rejects a non-STRING first argument: <case>
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_time(<input>) AS result
        """
      Then query error The first parameter requires the "STRING" type

      Examples:
        | case          | input                               |
        | INT           | 1                                   |
        | BOOLEAN       | true                                |
        | BINARY        | X'48656C6C6F'                       |
        | DATE          | DATE '2024-01-15'                   |
        | TIMESTAMP     | TIMESTAMP '2024-01-15 12:00:00'     |
        | TIMESTAMP_NTZ | TIMESTAMP_NTZ '2024-01-15 12:00:00' |
        | TIME          | TIME '12:30:00'                     |

    @sail-bug
    Scenario Outline: try_to_time rejects a non-STRING first argument too: <case>
      # The try_ variant swallows VALUE errors, not ANALYSIS errors, so the type check
      # still fires. That is what distinguishes an analysis-time guard from a runtime one.
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT try_to_time(<input>) AS result
        """
      Then query error The first parameter requires the "STRING" type

      Examples:
        | case      | input                           |
        | INT       | 1                               |
        | DATE      | DATE '2024-01-15'               |
        | TIMESTAMP | TIMESTAMP '2024-01-15 12:00:00' |
        | TIME      | TIME '12:30:00'                 |

  Rule: An unparseable value names the format it tried
    # Spark's CANNOT_PARSE_TIME message quotes the format, and for the one-argument form
    # that format is the literal 'HH:mm:ss.SSSSSS'. Asserting that substring is what
    # distinguishes Spark's message from Sail's own wording ("cannot parse '...' as time
    # with default formats"), which a `.*` assertion would let through.

    @sail-bug
    Scenario Outline: Unparseable input: <case>
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_time(<input>) AS result
        """
      Then query error cannot be parsed to a TIME value because it does not match to the datetime format

      Examples:
        | case             | input           |
        | garbage          | 'foo'           |
        | empty string     | ''              |
        | whitespace only  | '   '           |
        | trailing garbage | '2024-01-15xyz' |
