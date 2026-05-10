Feature: hll_union_agg merges binary HLL sketches across rows

  Rule: hll_union_agg merges sketches with the same lgConfigK

    Scenario: hll_union_agg of independently-built sketches
      When query
        """
        SELECT hll_sketch_estimate(hll_union_agg(sketch)) AS result
        FROM (
          SELECT hll_sketch_agg(col) AS sketch FROM VALUES (1), (2), (3) AS t(col)
          UNION ALL
          SELECT hll_sketch_agg(col) AS sketch FROM VALUES (4), (5), (6) AS t(col)
        )
        """
      Then query result
        | result |
        | 6      |

  Rule: hll_union_agg returns binary type

    Scenario: hll_union_agg returns binary type
      When query
        """
        SELECT typeof(hll_union_agg(sketch)) AS result
        FROM (SELECT hll_sketch_agg(col) AS sketch FROM VALUES (1) AS t(col))
        """
      Then query result
        | result |
        | binary |

  Rule: hll_union_agg works as a window function

    Scenario: hll_union_agg over an expanding window uses the default allowDifferentLgConfigK
      When query
        """
        SELECT id, hll_sketch_estimate(hll_union_agg(sketch) OVER (
          ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS result
        FROM (
          SELECT 1 AS id, hll_sketch_agg(col) AS sketch FROM VALUES (1), (2) AS t(col)
          UNION ALL
          SELECT 2 AS id, hll_sketch_agg(col) AS sketch FROM VALUES (2), (3) AS t(col)
          UNION ALL
          SELECT 3 AS id, hll_sketch_agg(col) AS sketch FROM VALUES (4) AS t(col)
        )
        ORDER BY id
        """
      Then query result
        | id | result |
        | 1  | 2      |
        | 2  | 3      |
        | 3  | 4      |

  Rule: hll_union_agg with allowDifferentLgConfigK

    Scenario: hll_union_agg rejects different lgConfigK by default
      When query
        """
        SELECT hll_sketch_estimate(hll_union_agg(sketch)) AS result
        FROM (
          SELECT hll_sketch_agg(col, 10) AS sketch FROM VALUES (1) AS t(col)
          UNION ALL
          SELECT hll_sketch_agg(col, 12) AS sketch FROM VALUES (2) AS t(col)
        )
        """
      Then query error HLL sketches have different lgConfigK

    Scenario: hll_union_agg with allowDifferentLgConfigK true succeeds
      When query
        """
        SELECT hll_sketch_estimate(hll_union_agg(sketch, true)) AS result
        FROM (
          SELECT hll_sketch_agg(col, 10) AS sketch FROM VALUES (1) AS t(col)
          UNION ALL
          SELECT hll_sketch_agg(col, 12) AS sketch FROM VALUES (1) AS t(col)
        )
        """
      Then query result
        | result |
        | 1      |
