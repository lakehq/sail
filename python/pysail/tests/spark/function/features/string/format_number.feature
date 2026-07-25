@format_number
Feature: format_number output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to format_number yields the schema Spark declares
      When query
        """
        SELECT format_number(12332.123456, 4) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to format_number stays nullable
      When query
        """
        SELECT format_number(c, 4) AS result FROM VALUES (12332.123456), (CAST(NULL AS DECIMAL(11,6))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
