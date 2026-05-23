Feature: hll_sketch_estimate returns the cardinality from a binary HLL sketch

  Rule: hll_sketch_estimate returns the distinct count

    Scenario: hll_sketch_estimate of a small distinct set
      When query
        """
        SELECT hll_sketch_estimate(hll_sketch_agg(col)) AS result
        FROM VALUES (1), (1), (2), (3) AS tab(col)
        """
      Then query result
        | result |
        | 3      |

  Rule: hll_sketch_estimate returns a long value

    Scenario: hll_sketch_estimate returns bigint type
      When query
        """
        SELECT typeof(hll_sketch_estimate(hll_sketch_agg(col))) AS result
        FROM VALUES (1) AS tab(col)
        """
      Then query result
        | result |
        | bigint |

  Rule: hll_sketch_estimate handles null inputs

    Scenario: hll_sketch_estimate of NULL returns NULL
      When query
        """
        SELECT hll_sketch_estimate(CAST(NULL AS BINARY)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: hll_sketch_estimate rejects invalid sketch input

    Scenario: hll_sketch_estimate of arbitrary binary fails
      When query
        """
        SELECT hll_sketch_estimate(X'00112233') AS result
        """
      Then query error invalid sketch
