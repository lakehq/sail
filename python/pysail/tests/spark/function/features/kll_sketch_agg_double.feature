Feature: kll_sketch_agg_double builds a KLL sketch from double values

  Rule: kll_sketch_agg_double returns non-empty binary

    Scenario: kll_sketch_agg_double with basic input
      When query
        """
        SELECT length(hex(kll_sketch_agg_double(col))) > 0 AS result
        FROM VALUES (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE)), (CAST(3.0 AS DOUBLE)) AS tab(col)
        """
      Then query result
        | result |
        | true   |

    Scenario: kll_sketch_agg_double with k parameter
      When query
        """
        SELECT length(hex(kll_sketch_agg_double(col, 400))) > 0 AS result
        FROM VALUES (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE)), (CAST(3.0 AS DOUBLE)) AS tab(col)
        """
      Then query result
        | result |
        | true   |

    Scenario: kll_sketch_agg_double with single value
      When query
        """
        SELECT length(hex(kll_sketch_agg_double(col))) > 0 AS result
        FROM VALUES (CAST(42.5 AS DOUBLE)) AS tab(col)
        """
      Then query result
        | result |
        | true   |

  Rule: kll_sketch_agg_double returns binary type

    Scenario: kll_sketch_agg_double returns binary type
      When query
        """
        SELECT typeof(kll_sketch_agg_double(col)) AS result
        FROM VALUES (CAST(1.0 AS DOUBLE)) AS tab(col)
        """
      Then query result
        | result |
        | binary |

  Rule: kll_sketch_agg_double handles null values

    Scenario: kll_sketch_agg_double ignores null input values
      When query
        """
        SELECT length(hex(kll_sketch_agg_double(col))) > 0 AS result
        FROM VALUES (CAST(1.0 AS DOUBLE)), (CAST(NULL AS DOUBLE)), (CAST(3.0 AS DOUBLE)) AS tab(col)
        """
      Then query result
        | result |
        | true   |

    Scenario: kll_sketch_agg_double with all null inputs returns a sketch
      When query
        """
        SELECT length(hex(kll_sketch_agg_double(col))) > 0 AS result
        FROM VALUES (CAST(NULL AS DOUBLE)), (CAST(NULL AS DOUBLE)) AS tab(col)
        """
      Then query result
        | result |
        | true   |

    Scenario: kll_sketch_agg_double on empty input returns a sketch
      When query
        """
        SELECT length(hex(kll_sketch_agg_double(col))) > 0 AS result
        FROM (SELECT CAST(1.0 AS DOUBLE) AS col WHERE false) AS tab
        """
      Then query result
        | result |
        | true   |

  Rule: kll_sketch_agg_double with negative values

    Scenario: kll_sketch_agg_double with negative input values
      When query
        """
        SELECT length(hex(kll_sketch_agg_double(col))) > 0 AS result
        FROM VALUES (CAST(-1.0 AS DOUBLE)), (CAST(-2.0 AS DOUBLE)), (CAST(-3.0 AS DOUBLE)) AS tab(col)
        """
      Then query result
        | result |
        | true   |

  Rule: kll_sketch_agg_double as a window function

    Scenario: kll_sketch_agg_double over window
      When query
        """
        SELECT
          id,
          length(hex(kll_sketch_agg_double(col) OVER (
            ORDER BY id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ))) > 0 AS result
        FROM VALUES (1, CAST(10.0 AS DOUBLE)), (2, CAST(20.0 AS DOUBLE)), (3, CAST(30.0 AS DOUBLE)) AS tab(id, col)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | true   |
        | 2  | true   |
        | 3  | true   |
