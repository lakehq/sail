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

  Rule: Sail also accepts temporal inputs

    @sail-only
    Scenario Outline: Two-arg <function> converts <case> without parsing its format
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(<function>(<arg>, 'HH-mm-ss') AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function    | case           | arg                                      | result   |
        | to_time     | TIMESTAMP      | TIMESTAMP '2024-01-15 10:30:45'           | 10:30:45 |
        | to_time     | TIMESTAMP_NTZ  | TIMESTAMP_NTZ '2024-01-15 10:30:45'       | 10:30:45 |
        | to_time     | TIME           | TIME '10:30:45'                           | 10:30:45 |
        | try_to_time | TIMESTAMP      | TIMESTAMP '2024-01-15 10:30:45'           | 10:30:45 |
        | try_to_time | TIME           | TIME '10:30:45'                           | 10:30:45 |
        | to_time     | NULL TIMESTAMP | CAST(NULL AS TIMESTAMP)                   | NULL     |
        | try_to_time | NULL TIME      | CAST(NULL AS TIME)                        | NULL     |

    Scenario: Unsupported first-argument type is rejected with a format too
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_time(123, 'HH:mm:ss')
        """
      Then query error (?i)STRING|unsupported|invalid.*type

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
      Then query error CANNOT_PARSE_TIME|UNSUPPORTED_OPERATION|Unsupported|data type

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

  Rule: Unsupported primitive types fail analysis

    Scenario Outline: One-arg form rejects a non-STRING first argument: <case>
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_time(<input>) AS result
        """
      Then query error (?i)STRING|unsupported|invalid.*type

      Examples:
        | case          | input                               |
        | INT           | 1                                   |
        | BOOLEAN       | true                                |
        | BINARY        | X'48656C6C6F'                       |
        | DATE          | DATE '2024-01-15'                   |

    Scenario Outline: try_to_time rejects a non-STRING first argument too: <case>
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT try_to_time(<input>) AS result
        """
      Then query error (?i)STRING|unsupported|invalid.*type

      Examples:
        | case      | input                           |
        | INT       | 1                               |
        | DATE      | DATE '2024-01-15'               |

  Rule: An unparseable value names the format it tried
    # Spark reports the quoted default format 'HH:mm:ss.SSSSSS' for the one-argument form.

    Scenario Outline: Unparseable input: <case>
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT to_time(<input>) AS result
        """
      Then query error \[CANNOT_PARSE_TIME\].*cannot be parsed to a TIME value because it does not match to the datetime format 'HH:mm:ss\.SSSSSS'\.

      Examples:
        | case             | input           |
        | garbage          | 'foo'           |
        | empty string     | ''              |
        | whitespace only  | '   '           |
        | trailing garbage | '2024-01-15xyz' |
