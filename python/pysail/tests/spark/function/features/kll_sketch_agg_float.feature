Feature: kll_sketch_agg_float builds a KLL sketch from float values

  Rule: kll_sketch_agg_float returns non-empty binary

    Scenario: kll_sketch_agg_float with basic input
      When query
        """
        SELECT length(hex(kll_sketch_agg_float(col))) > 0 AS result
        FROM VALUES (CAST(1.0 AS FLOAT)), (CAST(2.0 AS FLOAT)), (CAST(3.0 AS FLOAT)) AS tab(col)
        """
      Then query result
        | result |
        | true   |

    Scenario: kll_sketch_agg_float with k parameter
      When query
        """
        SELECT length(hex(kll_sketch_agg_float(col, 400))) > 0 AS result
        FROM VALUES (CAST(1.0 AS FLOAT)), (CAST(2.0 AS FLOAT)), (CAST(3.0 AS FLOAT)) AS tab(col)
        """
      Then query result
        | result |
        | true   |

    Scenario: kll_sketch_agg_float with single value
      When query
        """
        SELECT length(hex(kll_sketch_agg_float(col))) > 0 AS result
        FROM VALUES (CAST(42.5 AS FLOAT)) AS tab(col)
        """
      Then query result
        | result |
        | true   |

  Rule: kll_sketch_agg_float returns binary type

    Scenario: kll_sketch_agg_float returns binary type
      When query
        """
        SELECT typeof(kll_sketch_agg_float(col)) AS result
        FROM VALUES (CAST(1.0 AS FLOAT)) AS tab(col)
        """
      Then query result
        | result |
        | binary |

  Rule: kll_sketch_agg_float handles null values

    Scenario: kll_sketch_agg_float ignores null input values
      When query
        """
        SELECT length(hex(kll_sketch_agg_float(col))) > 0 AS result
        FROM VALUES (CAST(1.0 AS FLOAT)), (CAST(NULL AS FLOAT)), (CAST(3.0 AS FLOAT)) AS tab(col)
        """
      Then query result
        | result |
        | true   |

    Scenario: kll_sketch_agg_float with all null inputs returns a sketch
      When query
        """
        SELECT length(hex(kll_sketch_agg_float(col))) > 0 AS result
        FROM VALUES (CAST(NULL AS FLOAT)), (CAST(NULL AS FLOAT)) AS tab(col)
        """
      Then query result
        | result |
        | true   |

    Scenario: kll_sketch_agg_float on empty input returns a sketch
      When query
        """
        SELECT length(hex(kll_sketch_agg_float(col))) > 0 AS result
        FROM (SELECT CAST(1.0 AS FLOAT) AS col WHERE false) AS tab
        """
      Then query result
        | result |
        | true   |

  Rule: kll_sketch_agg_float with negative values

    Scenario: kll_sketch_agg_float with negative input values
      When query
        """
        SELECT length(hex(kll_sketch_agg_float(col))) > 0 AS result
        FROM VALUES (CAST(-1.0 AS FLOAT)), (CAST(-2.0 AS FLOAT)), (CAST(-3.0 AS FLOAT)) AS tab(col)
        """
      Then query result
        | result |
        | true   |

  Rule: kll_sketch_agg_float as a window function

    Scenario: kll_sketch_agg_float over window
      When query
        """
        SELECT
          id,
          length(hex(kll_sketch_agg_float(col) OVER (
            ORDER BY id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ))) > 0 AS result
        FROM VALUES (1, CAST(10.0 AS FLOAT)), (2, CAST(20.0 AS FLOAT)), (3, CAST(30.0 AS FLOAT)) AS tab(id, col)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | true   |
        | 2  | true   |
        | 3  | true   |
