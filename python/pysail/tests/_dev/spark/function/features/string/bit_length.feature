Feature: bit_length output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to bit_length yields the schema Spark declares
      When query
        """
        SELECT bit_length('Spark SQL') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column input to bit_length yields the schema Spark declares
      When query
        """
        SELECT bit_length(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to bit_length stays nullable
      When query
        """
        SELECT bit_length(c) AS result FROM VALUES ('Spark SQL'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """
