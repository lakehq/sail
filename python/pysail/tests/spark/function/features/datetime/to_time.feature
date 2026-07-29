@to_time @spark-4.1
Feature: to_time (strict variant)
  Strict to_time that throws an error on invalid input.

  NOTE: TIME type is a preview feature in Spark 4.x; the JVM raises
  `[UNSUPPORTED_TIME_TYPE] The data type TIME is not supported` for
  any TIME usage. The scenarios below reflect Sail's implementation.
  Once Spark stabilises TIME support, remove @sail-only and update
  expected values to match Spark JVM output. — owners to decide.

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
