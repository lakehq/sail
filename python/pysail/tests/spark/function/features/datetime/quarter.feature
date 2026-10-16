Feature: quarter output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to quarter yields the schema Spark declares
      When query
        """
        SELECT quarter('2016-08-31') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to quarter yields the schema Spark declares
      When query
        """
        SELECT quarter(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to quarter stays nullable
      When query
        """
        SELECT quarter(c) AS result FROM VALUES ('2016-08-31'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    # Spark 4.2.0 Cast.forceNullable: (TimestampNTZType, DateType) falls into `case (_, DateType)
    # => true`, so the implicit cast of a TIMESTAMP_NTZ to DATE makes the result nullable.
    @sail-bug
    Scenario: a non-null timestamp_ntz literal input to quarter is nullable through the cast to DATE
      When query
        """
        SELECT quarter(TIMESTAMP_NTZ '2024-01-01 00:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
