@to_time @spark-4.1
Feature: to_time (strict variant)
  Strict to_time that throws an error on invalid input.

  NOTE: TIME type is a preview feature in Spark 4.1.1; the JVM raises
  `[UNSUPPORTED_TIME_TYPE] The data type TIME is not supported` for
  any TIME usage. The scenarios below reflect Sail's implementation.
  Once Spark stabilises TIME support, remove @sail-only and update
  expected values to match Spark JVM output. — owners to decide.

  Rule: Valid input parses

    @sail-only
    Scenario: HH:MM:SS basic
      When query
      """
      SELECT to_time('10:30:45') AS result
      """
      Then query result
      | result   |
      | 10:30:45 |

    @sail-only
    Scenario: With microseconds
      When query
      """
      SELECT to_time('10:30:45.123456') AS result
      """
      Then query result
      | result          |
      | 10:30:45.123456 |

    @sail-only
    Scenario: With format
      When query
      """
      SELECT to_time('10-30-45', 'HH-mm-ss') AS result
      """
      Then query result
      | result   |
      | 10:30:45 |

  Rule: Invalid input throws

    @sail-only
    Scenario: Garbage string raises error
      When query
      """
      SELECT to_time('not-a-time')
      """
      Then query error cannot parse|UNSUPPORTED_OPERATION|Unsupported|data type

    @sail-only
    Scenario: Out-of-range hour raises error
      When query
      """
      SELECT to_time('25:00:00')
      """
      Then query error cannot parse|UNSUPPORTED_OPERATION|Unsupported|data type

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
