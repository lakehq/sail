Feature: make_timestamp_ltz

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
