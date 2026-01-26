Feature: first_value and last_value window functions with sliding frames

  Rule: first_value with sliding window frame
    Scenario: first_value with ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
      When query
        """
        SELECT
          x,
          FIRST_VALUE(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS x_first
        FROM (SELECT explode(array(1, 2, 3, 4, 5)) AS x)
        """
      Then query result ordered
        | x | x_first |
        | 1 | 1       |
        | 2 | 1       |
        | 3 | 2       |
        | 4 | 3       |
        | 5 | 4       |

    Scenario: first_value with ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      When query
        """
        SELECT
          x,
          FIRST_VALUE(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS x_first
        FROM (SELECT explode(array(1, 2, 3, 4, 5)) AS x)
        """
      Then query result ordered
        | x | x_first |
        | 1 | 1       |
        | 2 | 1       |
        | 3 | 1       |
        | 4 | 2       |
        | 5 | 3       |

    Scenario: first_value with ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
      When query
        """
        SELECT
          x,
          FIRST_VALUE(x) OVER (ORDER BY x ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS x_first
        FROM (SELECT explode(array(1, 2, 3, 4, 5)) AS x)
        """
      Then query result ordered
        | x | x_first |
        | 1 | 1       |
        | 2 | 2       |
        | 3 | 3       |
        | 4 | 4       |
        | 5 | 5       |

  Rule: last_value with sliding window frame
    Scenario: last_value with ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
      When query
        """
        SELECT
          x,
          LAST_VALUE(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS x_last
        FROM (SELECT explode(array(1, 2, 3, 4, 5)) AS x)
        """
      Then query result ordered
        | x | x_last |
        | 1 | 1      |
        | 2 | 2      |
        | 3 | 3      |
        | 4 | 4      |
        | 5 | 5      |

    Scenario: last_value with ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
      When query
        """
        SELECT
          x,
          LAST_VALUE(x) OVER (ORDER BY x ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS x_last
        FROM (SELECT explode(array(1, 2, 3, 4, 5)) AS x)
        """
      Then query result ordered
        | x | x_last |
        | 1 | 2      |
        | 2 | 3      |
        | 3 | 4      |
        | 4 | 5      |
        | 5 | 5      |

    Scenario: last_value with ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      When query
        """
        SELECT
          x,
          LAST_VALUE(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS x_last
        FROM (SELECT explode(array(1, 2, 3, 4, 5)) AS x)
        """
      Then query result ordered
        | x | x_last |
        | 1 | 1      |
        | 2 | 2      |
        | 3 | 3      |
        | 4 | 4      |
        | 5 | 5      |

  Rule: first_value and last_value with partitioning
    Scenario: first_value with partition and sliding frame
      When query
        """
        SELECT
          grp,
          x,
          FIRST_VALUE(x) OVER (PARTITION BY grp ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS x_first
        FROM (
          SELECT 'a' AS grp, explode(array(1, 2, 3)) AS x
          UNION ALL
          SELECT 'b' AS grp, explode(array(4, 5, 6)) AS x
        )
        ORDER BY grp, x
        """
      Then query result ordered
        | grp | x | x_first |
        | a   | 1 | 1       |
        | a   | 2 | 1       |
        | a   | 3 | 2       |
        | b   | 4 | 4       |
        | b   | 5 | 4       |
        | b   | 6 | 5       |

  Rule: first_value and last_value with multiple columns (issue 1061 reproduction)
    Scenario: first and last with multiple order by columns
      When query
        """
        SELECT
          g,
          x,
          y,
          FIRST_VALUE(x) OVER (PARTITION BY g ORDER BY x, y ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS x_first,
          LAST_VALUE(x) OVER (PARTITION BY g ORDER BY x, y ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS x_last,
          FIRST_VALUE(y) OVER (PARTITION BY g ORDER BY x, y ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS y_first,
          LAST_VALUE(y) OVER (PARTITION BY g ORDER BY x, y ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS y_last
        FROM (
          SELECT 'a' AS g, 0 AS x, 3 AS y
          UNION ALL SELECT 'a', 1, 2
          UNION ALL SELECT 'a', 2, 0
          UNION ALL SELECT 'a', 3, 1
          UNION ALL SELECT 'a', 4, 1
        )
        ORDER BY g, x, y
        """
      Then query result ordered
        | g | x | y | x_first | x_last | y_first | y_last |
        | a | 0 | 3 | 0       | 0      | 3       | 3      |
        | a | 1 | 2 | 0       | 1      | 3       | 2      |
        | a | 2 | 0 | 1       | 2      | 2       | 0      |
        | a | 3 | 1 | 2       | 3      | 0       | 1      |
        | a | 4 | 1 | 3       | 4      | 1       | 1      |

  Rule: first_value and last_value with ignore nulls
    Scenario: first_value ignoring nulls
      When query
        """
        SELECT
          x,
          FIRST_VALUE(x, true) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS x_first
        FROM (
          SELECT 1 AS id, NULL AS x
          UNION ALL SELECT 2, 10
          UNION ALL SELECT 3, NULL
          UNION ALL SELECT 4, 20
        )
        """
      Then query result ordered
        | x    | x_first |
        | NULL | NULL    |
        | 10   | 10      |
        | NULL | 10      |
        | 20   | 10      |
