@datediff
Feature: datediff output schema

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to datediff yields the schema Spark declares
      When query
        """
        SELECT datediff('2009-07-31', '2009-07-30') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to datediff yields the schema Spark declares
      When query
        """
        SELECT datediff(CAST(id AS STRING), '2009-07-30') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to datediff stays nullable
      When query
        """
        SELECT datediff(c, '2009-07-30') AS result FROM VALUES ('2009-07-31'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Return type

    Scenario: the two-argument form returns INT
      When query
        """
        SELECT datediff('2009-07-31', '2009-07-30') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: the three-argument form returns BIGINT, unlike the two-argument one
      When query
        """
        SELECT datediff(DAY, DATE '2024-01-01', DATE '2024-01-10') AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: the three-argument form with a time unit returns BIGINT
      When query
        """
        SELECT datediff(HOUR, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-02 03:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """
