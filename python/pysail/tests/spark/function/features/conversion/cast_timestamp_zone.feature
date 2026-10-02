Feature: the session zone decides what a timestamp cast produces

  # Every test in this suite runs with the session zone pinned to UTC, and UTC is exactly the zone
  # where an engine that ignores the zone still looks right. These scenarios move the zone on
  # purpose, so that a cast which only ever converts through UTC is caught.
  # The instant is built from an epoch second rather than from a TIMESTAMP literal: a literal is
  # parsed AND printed in the session zone, so the two cancel out and nothing moves.
  # Measured on the Spark 4.2 JVM over Spark Connect.

  Rule: casting an instant to a local wall clock applies the session zone

    @sail-bug
    Scenario Outline: the epoch instant read as a wall clock in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query template
        """
        SELECT CAST(CAST(0L AS TIMESTAMP) AS TIMESTAMP_NTZ) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | zone                | result              |
        | America/Los_Angeles | 1969-12-31 16:00:00 |
        | Asia/Kolkata        | 1970-01-01 05:30:00 |
        | Australia/Lord_Howe | 1970-01-01 10:00:00 |

    Scenario: the epoch instant read as a wall clock in UTC
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(CAST(0L AS TIMESTAMP) AS TIMESTAMP_NTZ) AS result
        """
      Then query result collected
        | result              |
        | 1970-01-01 00:00:00 |

  Rule: printing an instant applies the session zone

    Scenario Outline: the epoch instant printed in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query template
        """
        SELECT CAST(CAST(0L AS TIMESTAMP) AS STRING) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | zone                | result              |
        | UTC                 | 1970-01-01 00:00:00 |
        | America/Los_Angeles | 1969-12-31 16:00:00 |
        | Asia/Kolkata        | 1970-01-01 05:30:00 |
        | Australia/Lord_Howe | 1970-01-01 10:00:00 |

    # The day itself changes, not just the clock: the instant falls on the previous day west of UTC.
    Scenario Outline: the day an instant belongs to in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query template
        """
        SELECT CAST(CAST(1709618828L AS TIMESTAMP) AS DATE) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | zone                | result     |
        | UTC                 | 2024-03-05 |
        | America/Los_Angeles | 2024-03-04 |
        | Asia/Kolkata        | 2024-03-05 |

  Rule: the epoch second of an instant does not depend on the session zone

    Scenario Outline: the epoch instant as a number in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query template
        """
        SELECT CAST(CAST(0L AS TIMESTAMP) AS BIGINT) AS result
        """
      Then query result collected
        | result |
        | 0      |

      Examples:
        | zone                |
        | UTC                 |
        | America/Los_Angeles |
        | Asia/Kolkata        |
