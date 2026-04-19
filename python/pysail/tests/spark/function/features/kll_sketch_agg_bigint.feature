Feature: kll_sketch_agg_bigint builds a KLL sketch from bigint values

  Rule: kll_sketch_agg_bigint returns non-empty binary

    Scenario: kll_sketch_agg_bigint with basic input
      When query
        """
        SELECT length(hex(kll_sketch_agg_bigint(col))) > 0 AS result
        FROM VALUES (1), (2), (3), (4), (5) AS tab(col)
        """
      Then query result
        | result |
        | true   |

    Scenario: kll_sketch_agg_bigint with k parameter
      When query
        """
        SELECT length(hex(kll_sketch_agg_bigint(col, 400))) > 0 AS result
        FROM VALUES (1), (2), (3), (4), (5) AS tab(col)
        """
      Then query result
        | result |
        | true   |

    Scenario: kll_sketch_agg_bigint with single value
      When query
        """
        SELECT length(hex(kll_sketch_agg_bigint(col))) > 0 AS result
        FROM VALUES (42) AS tab(col)
        """
      Then query result
        | result |
        | true   |

  Rule: kll_sketch_agg_bigint returns binary type

    Scenario: kll_sketch_agg_bigint returns binary type
      When query
        """
        SELECT typeof(kll_sketch_agg_bigint(col)) AS result
        FROM VALUES (1) AS tab(col)
        """
      Then query result
        | result |
        | binary |

  Rule: kll_sketch_agg_bigint validates k

    Scenario: kll_sketch_agg_bigint rejects out-of-range k
      When query
        """
        SELECT hex(kll_sketch_agg_bigint(col, 7)) AS result
        FROM VALUES (1), (2), (3) AS tab(col)
        """
      Then query error kll_sketch_agg requires k to be in the range 8 to 65535, got 7

    Scenario: kll_sketch_agg_bigint rejects non-literal k
      When query
        """
        SELECT hex(kll_sketch_agg_bigint(col, k)) AS result
        FROM VALUES (1, 200), (2, 200), (3, 200) AS tab(col, k)
        """
      Then query error kll_sketch_agg requires a non-null integer literal for k

  Rule: kll_sketch_agg_bigint handles null values

    Scenario: kll_sketch_agg_bigint ignores null input values
      When query
        """
        SELECT length(hex(kll_sketch_agg_bigint(col))) > 0 AS result
        FROM VALUES (1), (CAST(NULL AS BIGINT)), (3) AS tab(col)
        """
      Then query result
        | result |
        | true   |

    Scenario: kll_sketch_agg_bigint with all null inputs returns a sketch
      When query
        """
        SELECT length(hex(kll_sketch_agg_bigint(col))) > 0 AS result
        FROM VALUES (CAST(NULL AS BIGINT)), (CAST(NULL AS BIGINT)) AS tab(col)
        """
      Then query result
        | result |
        | true   |

    Scenario: kll_sketch_agg_bigint on empty input returns a sketch
      When query
        """
        SELECT length(hex(kll_sketch_agg_bigint(col))) > 0 AS result
        FROM (SELECT CAST(1 AS BIGINT) AS col WHERE false) AS tab
        """
      Then query result
        | result |
        | true   |

  Rule: kll_sketch_agg_bigint with negative values

    Scenario: kll_sketch_agg_bigint with negative input values
      When query
        """
        SELECT length(hex(kll_sketch_agg_bigint(col))) > 0 AS result
        FROM VALUES (-1), (-2), (-3) AS tab(col)
        """
      Then query result
        | result |
        | true   |

  Rule: kll_sketch_agg_bigint as a window function

    Scenario: kll_sketch_agg_bigint over window
      When query
        """
        SELECT
          id,
          length(hex(kll_sketch_agg_bigint(col) OVER (
            ORDER BY id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ))) > 0 AS result
        FROM VALUES (1, 10), (2, 20), (3, 30) AS tab(id, col)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | true   |
        | 2  | true   |
        | 3  | true   |
