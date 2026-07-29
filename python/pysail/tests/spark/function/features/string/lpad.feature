@lpad
Feature: lpad output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to lpad yields the schema Spark declares
      When query
        """
        SELECT lpad('hi', 5, '??') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to lpad yields the schema Spark declares
      When query
        """
        SELECT lpad(CAST(id AS STRING), 5, '??') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to lpad stays nullable
      When query
        """
        SELECT lpad(c, 5, '??') AS result FROM VALUES ('hi'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
