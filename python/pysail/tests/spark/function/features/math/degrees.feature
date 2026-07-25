@degrees
Feature: degrees output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to degrees yields the schema Spark declares
      When query
        """
        SELECT degrees(3.141592653589793) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to degrees stays nullable
      When query
        """
        SELECT degrees(c) AS result FROM VALUES (3.141592653589793), (CAST(NULL AS DECIMAL(16,15))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """
