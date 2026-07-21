@json_array_length
Feature: json_array_length output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to json_array_length yields the schema Spark declares
      When query
        """
        SELECT json_array_length('[1,2,3,4]') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to json_array_length yields the schema Spark declares
      When query
        """
        SELECT json_array_length(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to json_array_length stays nullable
      When query
        """
        SELECT json_array_length(c) AS result FROM VALUES ('[1,2,3,4]'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
