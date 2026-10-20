Feature: extract output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to extract yields the schema Spark declares
      When query
        """
        SELECT extract(YEAR FROM TIMESTAMP '2019-08-12 01:00:00.123456') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

  Rule: Fields are read in the session time zone

    # Spark 4.2.0 Extract replaces itself with the matching field expression (DayOfMonth, Hour, ...),
    # each resolved in the session zone.

    Scenario: extract of the epoch in a fixed-offset session zone
      Given config spark.sql.session.timeZone = +05:45
      When query
        """
        SELECT
          extract(HOUR FROM TIMESTAMP '1970-01-01 00:00:00Z') AS h,
          extract(MINUTE FROM TIMESTAMP '1970-01-01 00:00:00Z') AS m
        """
      Then query result
        | h | m  |
        | 5 | 45 |

    @sail-bug
    Scenario: extract of an instant built with timestamp_millis in Kiritimati
      Given config spark.sql.session.timeZone = Pacific/Kiritimati
      When query
        """
        SELECT
          extract(DAY FROM timestamp_millis(1735646400000)) AS d,
          extract(YEAR FROM timestamp_millis(1735646400000)) AS y
        """
      Then query result
        | d | y    |
        | 1 | 2025 |
