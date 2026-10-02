Feature: dayofmonth output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to dayofmonth yields the schema Spark declares
      When query
        """
        SELECT dayofmonth('2009-07-30') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to dayofmonth yields the schema Spark declares
      When query
        """
        SELECT dayofmonth(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to dayofmonth stays nullable
      When query
        """
        SELECT dayofmonth(c) AS result FROM VALUES ('2009-07-30'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    # Spark 4.2.0 Cast.forceNullable: (TimestampNTZType, DateType) falls into `case (_, DateType)
    # => true`, so the implicit cast of a TIMESTAMP_NTZ to DATE makes the result nullable.
    @sail-bug
    Scenario: a non-null timestamp_ntz literal input to dayofmonth is nullable through the cast to DATE
      When query
        """
        SELECT dayofmonth(TIMESTAMP_NTZ '2024-01-01 00:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: The day is read in the session time zone

    # Spark 4.2.0 DayOfMonth takes a DATE; a TIMESTAMP reaches it through Cast(TimestampType,
    # DateType) in the session zone. The epoch is still 1969-12-31 in a -12:00 zone, and
    # 2024-12-31 12:00 UTC is already 2025-01-01 in Kiritimati (+14).

    Scenario: dayofmonth of the epoch in a -12:00 session zone
      Given config spark.sql.session.timeZone = -12:00
      When query
        """
        SELECT dayofmonth(TIMESTAMP '1970-01-01 00:00:00Z') AS result
        """
      Then query result
        | result |
        | 31     |

    @sail-bug
    Scenario: dayofmonth of an instant built with timestamp_micros in Kiritimati
      Given config spark.sql.session.timeZone = Pacific/Kiritimati
      When query
        """
        SELECT dayofmonth(timestamp_micros(1735646400000000)) AS result
        """
      Then query result
        | result |
        | 1      |
