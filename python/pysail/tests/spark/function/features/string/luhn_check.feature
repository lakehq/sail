Feature: luhn_check output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to luhn_check yields the schema Spark declares
      When query
        """
        SELECT luhn_check('8112189876') AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

    Scenario: a non-null column input to luhn_check yields the schema Spark declares
      When query
        """
        SELECT luhn_check(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

    Scenario: a nullable column input to luhn_check stays nullable
      When query
        """
        SELECT luhn_check(c) AS result FROM VALUES ('8112189876'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
