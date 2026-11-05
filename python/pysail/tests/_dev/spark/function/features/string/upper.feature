Feature: upper output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to upper yields the schema Spark declares
      When query
        """
        SELECT upper('SparkSql') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to upper yields the schema Spark declares
      When query
        """
        SELECT upper(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to upper stays nullable
      When query
        """
        SELECT upper(c) AS result FROM VALUES ('SparkSql'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    @sail-bug
    Scenario: distinct upper results remain non-nullable for non-null input
      When query
        """
        SELECT DISTINCT upper(p) AS s
        FROM VALUES ('x1.CSV'), ('x1.CSV') AS t(p)
        """
      Then query schema
        """
        root
         |-- s: string (nullable = false)
        """

  Rule: Collected results

    Scenario: distinct upper results can be collected
      When query
        """
        SELECT DISTINCT upper(p) AS s
        FROM VALUES ('x1.CSV'), ('x1.CSV') AS t(p)
        """
      Then query result collected
        | s      |
        | X1.CSV |
