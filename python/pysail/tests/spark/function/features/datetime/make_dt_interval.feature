@make_dt_interval
Feature: make_dt_interval output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to make_dt_interval yields the schema Spark declares
      When query
        """
        SELECT make_dt_interval(1, 12, 30, 01.001001) AS result
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = false)
        """

    Scenario: a non-null column input to make_dt_interval yields the schema Spark declares
      When query
        """
        SELECT make_dt_interval(CAST(id AS INT), 12, 30, 01.001001) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = false)
        """

    Scenario: a nullable column input to make_dt_interval stays nullable
      When query
        """
        SELECT make_dt_interval(c, 12, 30, 01.001001) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = true)
        """
