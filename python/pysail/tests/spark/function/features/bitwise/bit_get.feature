@bit_get
Feature: bit_get output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to bit_get yields the schema Spark declares
      When query
        """
        SELECT bit_get(11, 0) AS result
        """
      Then query schema
        """
        root
         |-- result: byte (nullable = false)
        """

    Scenario: a non-null column input to bit_get yields the schema Spark declares
      When query
        """
        SELECT bit_get(CAST(id AS INT), 0) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: byte (nullable = false)
        """

    Scenario: a nullable column input to bit_get stays nullable
      When query
        """
        SELECT bit_get(c, 0) AS result FROM VALUES (11), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: byte (nullable = true)
        """
