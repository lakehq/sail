@translate
Feature: translate output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to translate yields the schema Spark declares
      When query
        """
        SELECT translate('AaBbCc', 'abc', '123') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to translate yields the schema Spark declares
      When query
        """
        SELECT translate(CAST(id AS STRING), 'abc', '123') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to translate stays nullable
      When query
        """
        SELECT translate(c, 'abc', '123') AS result FROM VALUES ('AaBbCc'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
