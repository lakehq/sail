@json_object_keys
Feature: json_object_keys output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to json_object_keys yields the schema Spark declares
      When query
        """
        SELECT json_object_keys('{}') AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """

    Scenario: a non-null column input to json_object_keys yields the schema Spark declares
      When query
        """
        SELECT json_object_keys(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """

    Scenario: a nullable column input to json_object_keys stays nullable
      When query
        """
        SELECT json_object_keys(c) AS result FROM VALUES ('{}'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """
