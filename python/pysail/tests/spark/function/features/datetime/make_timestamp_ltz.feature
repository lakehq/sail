Feature: make_timestamp_ltz

  Rule: Invalid time zone validation

    Scenario: `try_make_timestamp_ltz` validates the time zone before invalid fields
      When query
        """
        SELECT try_make_timestamp_ltz(
          2024, 13, 1, 0, 0, 0, 'Not/AZone'
        ) AS result
        """
      Then query error INVALID_TIMEZONE

  Rule: Daylight saving time handling

  Background:
      Given config spark.sql.session.timeZone = America/Los_Angeles

    Scenario Outline: `make_timestamp_ltz` around daylight saving time transition
      When query
        """
        SELECT make_timestamp_ltz(DATE <date>, TIME <time>, <tz>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | date         | time       | tz                 | result              |
        | '2025-03-09' | '10:30:00' | 'Europe/Amsterdam' | 2025-03-09 01:30:00 |
        | '2025-03-09' | '11:30:00' | 'Europe/Amsterdam' | 2025-03-09 03:30:00 |

  # Spark 4.2.0 datetimeExpressions.scala, MakeTimestampLTZExpressionBuilder builds a
  # MakeTimestamp with dataType TIMESTAMP, so it shares its ANSI-gated validation.
  Rule: make_timestamp_ltz components honour ANSI mode

    Background:
      Given config spark.sql.session.timeZone = UTC

    @sail-bug
    Scenario: make_timestamp_ltz rejects month 13 under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_timestamp_ltz(2024, 13, 1, 0, 0, 0) AS result
        """
      Then query error \[DATETIME_FIELD_OUT_OF_BOUNDS

    @sail-bug
    Scenario Outline: make_timestamp_ltz returns NULL for an invalid component without ANSI: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_timestamp_ltz(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                      | args                               |
        | month 13                  | 2024, 13, 1, 0, 0, 0               |
        | leap day on non-leap year | 2023, 2, 29, 0, 0, 0, 'Asia/Tokyo' |

    Scenario Outline: make_timestamp_ltz interprets the fields in a region zone: <tz>
      When query
        """
        SELECT make_timestamp_ltz(2024, 1, 1, 0, 0, 0, '<tz>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | tz               | result              |
        | Asia/Tokyo       | 2023-12-31 15:00:00 |
        | America/New_York | 2024-01-01 05:00:00 |

    @sail-bug
    Scenario: make_timestamp_ltz interprets the fields in a zone offset
      When query
        """
        SELECT make_timestamp_ltz(2024, 1, 1, 0, 0, 0, '+05:30') AS result
        """
      Then query result
        | result              |
        | 2023-12-31 18:30:00 |

  # MakeTimestampLTZ resolves the fields in the session zone; MakeTimestampNTZ keeps the
  # wall-clock fields, so its epoch (read as UTC by to_unix_timestamp) never changes.
  Rule: make_timestamp_ltz versus make_timestamp_ntz across session time zones

    Scenario Outline: make_timestamp_ltz and make_timestamp_ntz in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT
          make_timestamp_ltz(2024, 1, 15, 10, 0, 0) AS ltz,
          unix_seconds(make_timestamp_ltz(2024, 1, 15, 10, 0, 0)) AS ltz_epoch,
          make_timestamp_ntz(2024, 1, 15, 10, 0, 0) AS ntz,
          to_unix_timestamp(make_timestamp_ntz(2024, 1, 15, 10, 0, 0)) AS ntz_epoch,
          make_timestamp_ltz(2024, 1, 1, 0, 0, 0, 'UTC') AS utc_midnight
        """
      Then query result
        | ltz                 | ltz_epoch   | ntz                 | ntz_epoch  | utc_midnight   |
        | 2024-01-15 10:00:00 | <ltz_epoch> | 2024-01-15 10:00:00 | 1705312800 | <utc_midnight> |

      Examples:
        | zone                | ltz_epoch  | utc_midnight        |
        | America/Los_Angeles | 1705341600 | 2023-12-31 16:00:00 |
        | Asia/Kolkata        | 1705293000 | 2024-01-01 05:30:00 |
        | Pacific/Chatham     | 1705263300 | 2024-01-01 13:45:00 |
        | Pacific/Pago_Pago   | 1705352400 | 2023-12-31 13:00:00 |

    @sail-bug
    Scenario: make_timestamp_ltz moves a session-zone DST gap forward
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT unix_seconds(make_timestamp_ltz(2024, 3, 10, 2, 30, 0)) AS result
        """
      Then query result
        | result     |
        | 1710066600 |

  # `MakeTimestamp` reads `failOnError = SQLConf.get.ansiEnabled` (`datetimeExpressions.scala:2886`)
  # and that same flag decides the SCHEMA, not only the value:
  # `nullable = if (failOnError) children.exists(_.nullable) else true` (:2921). So with ANSI on and
  # non-null arguments the column is declared NOT nullable, and with ANSI off it is nullable even
  # though nothing can be null. The Rules above already cover the value side of the flag; this is
  # the side a values-only test cannot see, and the pair is what tells the rule apart from
  # "always nullable".
  @function(nullability)
  Rule: ANSI decides the nullability of the result

    @sail-bug
    Scenario: with ANSI on and non-null arguments the result is not nullable
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_timestamp_ltz(2024, 3, 5, 6, 7, 8) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: with ANSI off the result is nullable even from non-null arguments
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_timestamp_ltz(2024, 3, 5, 6, 7, 8) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    # A nullable argument makes it nullable under ANSI too -- the guard against hardcoding `false`.
    Scenario: with ANSI on a nullable argument keeps the result nullable
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_timestamp_ltz(y, 3, 5, 6, 7, 8) AS result FROM VALUES (2024), (CAST(NULL AS INT)) AS t(y)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
