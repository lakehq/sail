Feature: DataSketches functions

  Rule: HLL sketch functions estimate distinct values

    Scenario: hll_sketch_agg estimates distinct integer values
      When query
        """
        SELECT hll_sketch_estimate(hll_sketch_agg(col)) AS result
        FROM VALUES (1), (1), (2), (2), (3) AS tab(col)
        """
      Then query result
        | result |
        | 3      |

    Scenario: hll_union merges two sketches
      When query
        """
        SELECT hll_sketch_estimate(hll_union(hll_sketch_agg(col1), hll_sketch_agg(col2))) AS result
        FROM VALUES (1, 4), (1, 4), (2, 5), (2, 5), (3, 6) AS tab(col1, col2)
        """
      Then query result
        | result |
        | 6      |

    Scenario: hll_union_agg merges sketch rows with different lgConfigK values when allowed
      When query
        """
        SELECT hll_sketch_estimate(hll_union_agg(sketch, true)) AS result
        FROM (
          SELECT hll_sketch_agg(col) AS sketch FROM VALUES (1) AS tab(col)
          UNION ALL
          SELECT hll_sketch_agg(col, 20) AS sketch FROM VALUES (1) AS tab(col)
        ) AS sketches
        """
      Then query result
        | result |
        | 1      |

  Rule: count_min_sketch returns Spark-compatible binary sketches

    Scenario: count_min_sketch serializes integer counts in Spark format
      When query
        """
        SELECT hex(count_min_sketch(col, 0.5d, 0.5d, 1)) AS result
        FROM VALUES (1), (2), (1) AS tab(col)
        """
      Then query result
        | result                                                                                                                       |
        | 0000000100000000000000030000000100000004000000005D8D6AB90000000000000000000000000000000200000000000000010000000000000000 |

  Rule: DataSketches functions return Spark-compatible types

    Scenario: HLL and count-min sketch functions return binary and bigint values
      When query
        """
        SELECT
          typeof(hll_sketch_agg(col)) AS hll_type,
          typeof(hll_sketch_estimate(hll_sketch_agg(col))) AS estimate_type,
          typeof(count_min_sketch(col, 0.5d, 0.5d, 1)) AS count_min_type
        FROM VALUES (1) AS tab(col)
        """
      Then query result
        | hll_type | estimate_type | count_min_type |
        | binary   | bigint        | binary         |
