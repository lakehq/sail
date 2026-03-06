Feature: percentile() window function computes running percentiles

  Rule: Running percentile with ORDER BY

    Scenario: running percentile 0.5 with unbounded preceding
      When query
      """
      SELECT
        x,
        percentile(x, 0.5) OVER (ORDER BY x) AS running_p50
      FROM (VALUES (0), (1), (2), (3), (4)) AS t(x)
      """
      Then query result ordered
      | x | running_p50 |
      | 0 | 0.0         |
      | 1 | 0.5         |
      | 2 | 1.0         |
      | 3 | 1.5         |
      | 4 | 2.0         |

    Scenario: running percentile 1.0 with explicit ROWS frame
      When query
      """
      SELECT
        x,
        percentile(x, 1.0) OVER (
          ORDER BY x
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_max
      FROM (VALUES (0), (1), (2), (3), (4), (5)) AS t(x)
      """
      Then query result ordered
      | x | running_max |
      | 0 | 0.0         |
      | 1 | 1.0         |
      | 2 | 2.0         |
      | 3 | 3.0         |
      | 4 | 4.0         |
      | 5 | 5.0         |

  Rule: Sliding window

    Scenario: sliding window with 2 preceding
      When query
      """
      SELECT
        x,
        percentile(x, 0.5) OVER (
          ORDER BY x
          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS sliding_p50
      FROM (VALUES (0), (1), (2), (3), (4), (5)) AS t(x)
      """
      Then query result ordered
      | x | sliding_p50 |
      | 0 | 0.0         |
      | 1 | 0.5         |
      | 2 | 1.0         |
      | 3 | 2.0         |
      | 4 | 3.0         |
      | 5 | 4.0         |

    Scenario: centered sliding window
      When query
      """
      SELECT
        x,
        percentile(x, 0.5) OVER (
          ORDER BY x
          ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        ) AS centered_p50
      FROM (VALUES (0), (10), (20), (30), (40)) AS t(x)
      """
      Then query result ordered
      | x  | centered_p50 |
      | 0  | 5.0          |
      | 10 | 10.0         |
      | 20 | 20.0         |
      | 30 | 30.0         |
      | 40 | 35.0         |

    Scenario: forward looking window
      When query
      """
      SELECT
        x,
        percentile(x, 0.5) OVER (
          ORDER BY x
          ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
        ) AS forward_p50
      FROM (VALUES (0), (1), (2), (3), (4)) AS t(x)
      """
      Then query result ordered
      | x | forward_p50 |
      | 0 | 1.0         |
      | 1 | 2.0         |
      | 2 | 3.0         |
      | 3 | 3.5         |
      | 4 | 4.0         |

  Rule: PARTITION BY support

    Scenario: running percentile with partition by
      When query
      """
      SELECT
        grp,
        x,
        percentile(x, 0.5) OVER (
          PARTITION BY grp
          ORDER BY x
        ) AS running_p50
      FROM (VALUES ('A', 1), ('A', 2), ('A', 3), ('B', 10), ('B', 20), ('B', 30)) AS t(grp, x)
      ORDER BY grp, x
      """
      Then query result ordered
      | grp | x  | running_p50 |
      | A   | 1  | 1.0         |
      | A   | 2  | 1.5         |
      | A   | 3  | 2.0         |
      | B   | 10 | 10.0        |
      | B   | 20 | 15.0        |
      | B   | 30 | 20.0        |

    Scenario: sliding window with partition by
      When query
      """
      SELECT
        grp,
        x,
        percentile(x, 0.5) OVER (
          PARTITION BY grp
          ORDER BY x
          ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS sliding_p50
      FROM (VALUES ('A', 1), ('A', 2), ('A', 3), ('A', 4), ('B', 10), ('B', 20), ('B', 30)) AS t(grp, x)
      ORDER BY grp, x
      """
      Then query result ordered
      | grp | x  | sliding_p50 |
      | A   | 1  | 1.0         |
      | A   | 2  | 1.5         |
      | A   | 3  | 2.5         |
      | A   | 4  | 3.5         |
      | B   | 10 | 10.0        |
      | B   | 20 | 15.0        |
      | B   | 30 | 25.0        |

  Rule: Multiple percentiles

    Scenario: multiple percentiles over same window
      When query
      """
      SELECT
        x,
        percentile(x, 0.25) OVER (ORDER BY x) AS p25,
        percentile(x, 0.50) OVER (ORDER BY x) AS p50,
        percentile(x, 0.75) OVER (ORDER BY x) AS p75
      FROM (VALUES (0), (10), (20), (30), (40)) AS t(x)
      """
      Then query result ordered
      | x  | p25  | p50  | p75  |
      | 0  | 0.0  | 0.0  | 0.0  |
      | 10 | 2.5  | 5.0  | 7.5  |
      | 20 | 5.0  | 10.0 | 15.0 |
      | 30 | 7.5  | 15.0 | 22.5 |
      | 40 | 10.0 | 20.0 | 30.0 |

  Rule: NULL handling in windows

    Scenario: window percentile with NULLs
      When query
      """
      SELECT
        x,
        percentile(x, 0.5) OVER (
          ORDER BY x
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_median
      FROM (VALUES (0), (NULL), (10), (NULL), (20), (30)) AS t(x)
      ORDER BY x NULLS FIRST
      """
      Then query result ordered
      | x    | running_median |
      | NULL | NULL           |
      | NULL | NULL           |
      | 0    | 0.0            |
      | 10   | 5.0            |
      | 20   | 10.0           |
      | 30   | 15.0           |

  Rule: Multiple partitions with different percentiles

    Scenario: partition by with p50 and p90
      When query
      """
      SELECT
        category,
        value,
        percentile(value, 0.5) OVER (PARTITION BY category ORDER BY value) AS p50,
        percentile(value, 0.9) OVER (PARTITION BY category ORDER BY value) AS p90
      FROM (VALUES
        ('A', 1), ('A', 2), ('A', 3), ('A', 4), ('A', 5),
        ('B', 10), ('B', 20), ('B', 30)
      ) AS t(category, value)
      ORDER BY category, value
      """
      Then query result ordered
      | category | value | p50  | p90  |
      | A        | 1     | 1.0  | 1.0  |
      | A        | 2     | 1.5  | 1.9  |
      | A        | 3     | 2.0  | 2.8  |
      | A        | 4     | 2.5  | 3.7  |
      | A        | 5     | 3.0  | 4.6  |
      | B        | 10    | 10.0 | 10.0 |
      | B        | 20    | 15.0 | 19.0 |
      | B        | 30    | 20.0 | 28.0 |
