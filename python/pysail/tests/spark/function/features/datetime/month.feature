Feature: month output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to month yields the schema Spark declares
      When query
        """
        SELECT month('2016-07-30') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to month yields the schema Spark declares
      When query
        """
        SELECT month(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to month stays nullable
      When query
        """
        SELECT month(c) AS result FROM VALUES ('2016-07-30'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    # Spark 4.2.0 Cast.forceNullable: (TimestampNTZType, DateType) falls into `case (_, DateType)
    # => true`, so the implicit cast of a TIMESTAMP_NTZ to DATE makes the result nullable.
    @sail-bug
    Scenario: a non-null timestamp_ntz literal input to month is nullable through the cast to DATE
      When query
        """
        SELECT month(TIMESTAMP_NTZ '2024-01-01 00:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
