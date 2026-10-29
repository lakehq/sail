Feature: substring_index output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to substring_index yields the schema Spark declares
      When query
        """
        SELECT substring_index('www.apache.org', '.', 2) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to substring_index yields the schema Spark declares
      When query
        """
        SELECT substring_index(CAST(id AS STRING), '.', 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to substring_index stays nullable
      When query
        """
        SELECT substring_index(c, '.', 2) AS result FROM VALUES ('www.apache.org'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    @sail-bug
    Scenario: distinct substring_index results remain non-nullable for non-null input
      When query
        """
        SELECT DISTINCT substring_index(p, '.', 1) AS s
        FROM VALUES ('x1.CSV'), ('x1.CSV') AS t(p)
        """
      Then query schema
        """
        root
         |-- s: string (nullable = false)
        """

  Rule: Collected results

    Scenario: substring_index preserves duplicate input rows
      When query
        """
        SELECT substring_index(p, '.', 1) AS s
        FROM VALUES ('x1.CSV'), ('x1.CSV') AS t(p)
        """
      Then query result collected
        | s  |
        | x1 |
        | x1 |

    Scenario: substring_index results can be collected after grouping
      When query
        """
        SELECT substring_index(p, '.', 1) AS s
        FROM VALUES ('x1.CSV'), ('x1.CSV') AS t(p)
        GROUP BY 1
        """
      Then query result collected
        | s  |
        | x1 |

    Scenario: distinct substring_index results can be collected
      When query
        """
        SELECT DISTINCT substring_index(p, '.', 1) AS s
        FROM VALUES ('x1.CSV'), ('x1.CSV') AS t(p)
        """
      Then query result collected
        | s  |
        | x1 |
