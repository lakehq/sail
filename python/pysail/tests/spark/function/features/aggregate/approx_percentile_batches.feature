Feature: Approximate percentiles retain values across batches and partial states

  Scenario Outline: Nullable <type> observations merge across input partitions
    When query
      """
      SELECT g, p[0] = g AND p[1] = g + 48 AND p[2] = g + 96 AS correct
      FROM (
        SELECT g, percentile_approx(v, array(0D, 0.5D, 1D), 1000000) AS p
        FROM (
          SELECT CAST(id % 4 AS INT) AS g,
                 CASE WHEN id % 11 = 0 OR id % 4 = 3 THEN NULL
                      ELSE CAST(id % 100 AS <type>) END AS v
          FROM range(0, 20000, 1, 4)
        ) t
        GROUP BY g
      ) q ORDER BY g
      """
    Then query result
      | g | correct |
      | 0 | true    |
      | 1 | true    |
      | 2 | true    |
      | 3 | NULL    |

    Examples:
      | type     |
      | TINYINT  |
      | SMALLINT |
      | INT      |
      | BIGINT   |
      | FLOAT    |
      | DOUBLE   |

  @sail-bug
  Scenario: Compare percentile arrays with arrays of grouping expressions
    # The shared comparison planner does not reconcile array element nullability
    # after grouping. This also fails with collect_list on the PR merge-base.
    When query
      """
      SELECT id % 2 AS g,
             percentile_approx(id, array(0.5D)) = array(id % 2 + 4) AS correct
      FROM range(10)
      GROUP BY id % 2 ORDER BY g
      """
    Then query result
      | g | correct |
      | 0 | true    |
      | 1 | true    |

  Scenario: Bounded percentile windows skip nulls when adding and retracting observations
    When query
      """
      SELECT id, approx_percentile(v, array(0D, 0.5D, 1D)) OVER (
        ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      ) AS p
      FROM VALUES (0, CAST(NULL AS DOUBLE)), (1, 1D), (2, 2D),
                  (3, CAST(NULL AS DOUBLE)), (4, 4D), (5, 5D) t(id, v)
      ORDER BY id
      """
    Then query result
      | id | p               |
      | 0  | NULL            |
      | 1  | [1.0, 1.0, 1.0] |
      | 2  | [1.0, 1.0, 2.0] |
      | 3  | [1.0, 1.0, 2.0] |
      | 4  | [2.0, 2.0, 4.0] |
      | 5  | [4.0, 4.0, 5.0] |
