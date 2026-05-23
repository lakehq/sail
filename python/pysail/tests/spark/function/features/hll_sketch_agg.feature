Feature: hll_sketch_agg builds an HLL sketch and supports cardinality estimation

  Rule: hll_sketch_agg with default lgConfigK estimates distinct count

    Scenario: hll_sketch_agg over a small distinct value set
      When query
        """
        SELECT hll_sketch_estimate(hll_sketch_agg(col)) AS result
        FROM VALUES (1), (1), (2), (2), (3) AS tab(col)
        """
      Then query result
        | result |
        | 3      |

    Scenario: hll_sketch_agg with explicit lgConfigK
      When query
        """
        SELECT hll_sketch_estimate(hll_sketch_agg(col, 12)) AS result
        FROM VALUES (1), (2), (3), (3) AS tab(col)
        """
      Then query result
        | result |
        | 3      |

    Scenario: hll_sketch_agg with smaller lgConfigK
      When query
        """
        SELECT hll_sketch_estimate(hll_sketch_agg(col, 10)) AS result
        FROM VALUES (1), (2), (3) AS tab(col)
        """
      Then query result
        | result |
        | 3      |

  Rule: hll_sketch_agg returns a binary value

    Scenario: hll_sketch_agg returns binary type
      When query
        """
        SELECT typeof(hll_sketch_agg(col)) AS result
        FROM VALUES (1) AS tab(col)
        """
      Then query result
        | result |
        | binary |

  Rule: hll_sketch_agg works as a window function

    Scenario: hll_sketch_agg over an expanding window uses the default lgConfigK
      When query
        """
        SELECT id, hll_sketch_estimate(hll_sketch_agg(col) OVER (
          ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS result
        FROM VALUES (1, 1), (2, 1), (3, 2), (4, 3) AS tab(id, col)
        ORDER BY id
        """
      Then query result
        | id | result |
        | 1  | 1      |
        | 2  | 1      |
        | 3  | 2      |
        | 4  | 3      |

  Rule: hll_sketch_agg handles null values

    Scenario: hll_sketch_agg ignores null input values
      When query
        """
        SELECT hll_sketch_estimate(hll_sketch_agg(col)) AS result
        FROM VALUES (1), (CAST(NULL AS INT)), (2), (CAST(NULL AS INT)), (3) AS tab(col)
        """
      Then query result
        | result |
        | 3      |

    Scenario: hll_sketch_agg on all-null input returns 0 distinct count
      When query
        """
        SELECT hll_sketch_estimate(hll_sketch_agg(col)) AS result
        FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS tab(col)
        """
      Then query result
        | result |
        | 0      |

  Rule: hll_sketch_agg rejects invalid lgConfigK values

    Scenario: hll_sketch_agg with lgConfigK below the minimum
      When query
        """
        SELECT hll_sketch_agg(col, 3) FROM VALUES (1) AS tab(col)
        """
      Then query error lgConfigK must be in
