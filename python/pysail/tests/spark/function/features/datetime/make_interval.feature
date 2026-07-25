@make_interval
Feature: make_interval output schema

  @spark_null
  Rule: Output schema

    @sail-bug
Scenario: a non-null literal input to make_interval yields the schema Spark declares
      When query
        """
        SELECT make_interval(100, 11, 1, 1, 12, 30, 01.001001) AS result
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = false)
        """

    @sail-bug
Scenario: a non-null column input to make_interval yields the schema Spark declares
      When query
        """
        SELECT make_interval(CAST(id AS INT), 11, 1, 1, 12, 30, 01.001001) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = false)
        """

    Scenario: a nullable column input to make_interval stays nullable
      When query
        """
        SELECT make_interval(c, 11, 1, 1, 12, 30, 01.001001) AS result FROM VALUES (100), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: interval (nullable = true)
        """
