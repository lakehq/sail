Feature: percentile_cont output type

  Rule: Result type

    @sail-bug
    Scenario: percentile_cont returns DOUBLE over a CASE widened to FLOAT with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY CASE WHEN id = 0 THEN 1 ELSE CAST(2.5 AS FLOAT) END) AS result
        FROM range(3)
        """
      Then query result
        | result |
        | 2.5    |
      And query schema
        """
        root
         |-- result: double (nullable = true)
        """
