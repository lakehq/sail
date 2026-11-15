Feature: date_part / extract output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: date_part second of a non-null timestamp yields a non-nullable decimal
      When query
        """
        SELECT date_part('second', TIMESTAMP '2024-01-01 00:00:05') AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(8,6) (nullable = false)
        """

    @sail-bug
    Scenario: extract second of a non-null timestamp yields a non-nullable decimal
      When query
        """
        SELECT extract(SECOND FROM TIMESTAMP '2024-01-01 00:00:05') AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(8,6) (nullable = false)
        """

    Scenario: date_part second of a nullable timestamp stays nullable
      When query
        """
        SELECT date_part('second', c) AS result FROM VALUES (TIMESTAMP '2024-01-01 00:00:05'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: decimal(8,6) (nullable = true)
        """
