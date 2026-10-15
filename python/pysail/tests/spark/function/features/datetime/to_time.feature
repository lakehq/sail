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

  Rule: Both argument forms require string input

    Scenario Outline: Two-arg <function> rejects <case>
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT <function>(<arg>, 'HH:mm:ss') AS result
        """
      Then query error The first parameter requires the "STRING" type

      Examples:
        | function    | case           | arg                                     |
        | to_time     | TIMESTAMP      | TIMESTAMP '2024-01-15 10:30:45'          |
        | to_time     | TIME           | TIME '10:30:45'                          |
        | try_to_time | TIMESTAMP      | TIMESTAMP '2024-01-15 10:30:45'          |
        | try_to_time | TIME           | TIME '10:30:45'                          |
        | to_time     | NULL TIMESTAMP | CAST(NULL AS TIMESTAMP)                  |
        | try_to_time | NULL TIME      | CAST(NULL AS TIME)                       |

    Scenario: Unsupported first-argument type is rejected with a format too
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_time(123, 'HH:mm:ss')
        """
      Then query error The first parameter requires the "STRING" type

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
    # Spark validates STRING input during analysis, including for try_to_time.

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
