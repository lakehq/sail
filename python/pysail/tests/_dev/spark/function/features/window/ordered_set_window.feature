Feature: Ordered-set aggregate functions over a window

  # Spark allows `percentile_cont` and `percentile_disc` with WITHIN GROUP over a window only
  # when the window has no ORDER BY and covers the whole partition. `mode() WITHIN GROUP`
  # supports any window, where the ordering breaks ties (ASC: lowest, DESC: highest).
  Rule: percentile_cont and percentile_disc over the whole partition

    Scenario Outline: <function>(<percentile>) WITHIN GROUP (ORDER BY <ordering>) OVER (<window>)
      When query
        """
        SELECT id, <function>(<percentile>) WITHIN GROUP (ORDER BY <ordering>) OVER (<window>) AS p
        FROM VALUES
          (1, 1, 3.0D), (1, 2, 1.0D), (1, 3, NULL), (1, 4, 2.0D), (1, 5, 2.0D), (2, 6, 5.0D), (2, 7, 4.0D)
        AS t(g, id, x)
        ORDER BY id
        """
      Then query result ordered
        | id | p        |
        | 1  | <first>  |
        | 2  | <first>  |
        | 3  | <first>  |
        | 4  | <first>  |
        | 5  | <first>  |
        | 6  | <second> |
        | 7  | <second> |

      Examples:
        | function        | percentile | ordering | window         | first | second |
        | percentile_cont | 0.5        | x        | PARTITION BY g | 2.0   | 4.5    |
        | percentile_cont | 0.25       | x DESC   | PARTITION BY g | 2.25  | 4.75   |
        | percentile_disc | 0.5        | x        | PARTITION BY g | 2.0   | 4.0    |
        | percentile_disc | 0.25       | x DESC   |                | 4.0   | 4.0    |

    Scenario Outline: <function> WITHIN GROUP over a window with <case> is an error
      When query
        """
        SELECT <function>(0.5) WITHIN GROUP (ORDER BY x) OVER (<window>) AS p
        FROM VALUES (1, 1, 3.0D), (1, 2, 1.0D) AS t(g, id, x)
        """
      Then query error INVALID_WINDOW_SPEC_FOR_AGGREGATION_FUNC

      Examples:
        | function        | case            | window                                                                   |
        | percentile_cont | an ORDER BY     | PARTITION BY g ORDER BY id                                               |
        | percentile_cont | a sliding frame | PARTITION BY g ROWS BETWEEN 1 PRECEDING AND CURRENT ROW                  |
        | percentile_disc | an ORDER BY     | PARTITION BY g ORDER BY id                                               |
        | percentile_disc | a RANGE frame   | PARTITION BY g RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING |

    Scenario: percentile_cont over a window without WITHIN GROUP is an error
      When query
        """
        SELECT percentile_cont(0.5) OVER (PARTITION BY g) AS p FROM VALUES (1, 1, 3.0D) AS t(g, id, x)
        """
      Then query error WITHIN_GROUP_MISSING

  Rule: mode() WITHIN GROUP over any window

    Scenario Outline: mode() WITHIN GROUP (ORDER BY <ordering>) OVER (<window>)
      When query
        """
        SELECT id, mode() WITHIN GROUP (ORDER BY <ordering>) OVER (<window>) AS m
        FROM VALUES
          (1, 1, 3.0D), (1, 2, 1.0D), (1, 3, NULL), (1, 4, 2.0D), (1, 5, 2.0D), (2, 6, 5.0D), (2, 7, 4.0D)
        AS t(g, id, x)
        ORDER BY id
        """
      Then query result ordered
        | id | m    |
        | 1  | <m1> |
        | 2  | <m2> |
        | 3  | <m3> |
        | 4  | <m4> |
        | 5  | <m5> |
        | 6  | <m6> |
        | 7  | <m7> |

      Examples:
        | ordering | window                     | m1  | m2  | m3  | m4  | m5  | m6  | m7  |
        | x        | PARTITION BY g             | 2.0 | 2.0 | 2.0 | 2.0 | 2.0 | 4.0 | 4.0 |
        | x DESC   | PARTITION BY g             | 2.0 | 2.0 | 2.0 | 2.0 | 2.0 | 5.0 | 5.0 |
        | x        | PARTITION BY g ORDER BY id | 3.0 | 1.0 | 1.0 | 1.0 | 2.0 | 5.0 | 4.0 |
