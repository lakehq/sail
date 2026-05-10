Feature: hll_union merges two HLL sketches

  Rule: hll_union merges two sketches with the same lgConfigK

    Scenario: hll_union of disjoint sketches
      When query
        """
        SELECT hll_sketch_estimate(
          hll_union(
            hll_sketch_agg(col1),
            hll_sketch_agg(col2)
          )
        ) AS result
        FROM VALUES (1, 4), (2, 5), (3, 6) AS tab(col1, col2)
        """
      Then query result
        | result |
        | 6      |

    Scenario: hll_union of overlapping sketches
      When query
        """
        SELECT hll_sketch_estimate(
          hll_union(
            hll_sketch_agg(col1),
            hll_sketch_agg(col2)
          )
        ) AS result
        FROM VALUES (1, 1), (2, 2), (3, 3) AS tab(col1, col2)
        """
      Then query result
        | result |
        | 3      |

  Rule: hll_union with allowDifferentLgConfigK

    Scenario: hll_union rejects different lgConfigK by default
      When query
        """
        SELECT hll_sketch_estimate(
          hll_union(
            (SELECT hll_sketch_agg(col, 10) FROM VALUES (1) AS t(col)),
            (SELECT hll_sketch_agg(col, 12) FROM VALUES (1) AS t(col))
          )
        ) AS result
        """
      Then query error HLL sketches have different lgConfigK

    Scenario: hll_union with allowDifferentLgConfigK true succeeds
      When query
        """
        SELECT hll_sketch_estimate(
          hll_union(
            (SELECT hll_sketch_agg(col, 10) FROM VALUES (1) AS t(col)),
            (SELECT hll_sketch_agg(col, 12) FROM VALUES (1) AS t(col)),
            true
          )
        ) AS result
        """
      Then query result
        | result |
        | 1      |
