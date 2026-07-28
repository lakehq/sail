@find_in_set
Feature: find_in_set output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to find_in_set yields the schema Spark declares
      When query
        """
        SELECT find_in_set('ab','abc,b,ab,c,def') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to find_in_set yields the schema Spark declares
      When query
        """
        SELECT find_in_set(CAST(id AS STRING), 'abc,b,ab,c,def') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to find_in_set stays nullable
      When query
        """
        SELECT find_in_set(c, 'abc,b,ab,c,def') AS result FROM VALUES ('ab'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
