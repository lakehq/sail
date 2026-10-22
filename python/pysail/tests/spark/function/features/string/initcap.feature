Feature: initcap output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to initcap yields the schema Spark declares
      When query
        """
        SELECT initcap('sPark sql') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to initcap yields the schema Spark declares
      When query
        """
        SELECT initcap(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to initcap stays nullable
      When query
        """
        SELECT initcap(c) AS result FROM VALUES ('sPark sql'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
