@if
Feature: if output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to if yields the schema Spark declares
      When query
        """
        SELECT if(1 < 2, 'a', 'b') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to if stays nullable
      When query
        """
        SELECT if(c, 'a', 'b') AS result FROM VALUES (1 < 2), (CAST(NULL AS BOOLEAN)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
