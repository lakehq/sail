Feature: Theta sketch functions

  Rule: theta_sketch_agg builds compact theta sketches

    Scenario: theta_sketch_agg estimates distinct integer values
      When query
        """
        SELECT theta_sketch_estimate(theta_sketch_agg(col)) AS result
        FROM VALUES (1), (1), (2), (2), (3) AS tab(col)
        """
      Then query result
        | result |
        | 3      |

    Scenario: theta_sketch_agg accepts an explicit lgNomEntries value
      When query
        """
        SELECT theta_sketch_estimate(theta_sketch_agg(col, 15)) AS result
        FROM VALUES (1), (1), (2), (2), (3) AS tab(col)
        """
      Then query result
        | result |
        | 3      |

    Scenario: theta_sketch_agg ignores null input values
      When query
        """
        SELECT theta_sketch_estimate(theta_sketch_agg(col)) AS result
        FROM VALUES (1), (CAST(NULL AS INT)), (2), (2) AS tab(col)
        """
      Then query result
        | result |
        | 2      |

  Rule: theta sketch set operations combine sketches

    Scenario: theta_union merges two sketches
      When query
        """
        SELECT theta_sketch_estimate(theta_union(theta_sketch_agg(col1), theta_sketch_agg(col2))) AS result
        FROM VALUES (1, 4), (1, 4), (2, 5), (2, 5), (3, 6) AS tab(col1, col2)
        """
      Then query result
        | result |
        | 6      |

    Scenario: theta_intersection intersects two sketches
      When query
        """
        SELECT theta_sketch_estimate(theta_intersection(theta_sketch_agg(col1), theta_sketch_agg(col2))) AS result
        FROM VALUES (5, 4), (1, 4), (2, 5), (2, 5), (3, 1) AS tab(col1, col2)
        """
      Then query result
        | result |
        | 2      |

    Scenario: theta_difference subtracts sketches
      When query
        """
        SELECT theta_sketch_estimate(theta_difference(theta_sketch_agg(col1), theta_sketch_agg(col2))) AS result
        FROM VALUES (5, 4), (1, 4), (2, 5), (2, 5), (3, 1) AS tab(col1, col2)
        """
      Then query result
        | result |
        | 2      |

  Rule: theta sketch aggregate set operations combine sketch rows

    Scenario: theta_union_agg merges sketch rows
      When query
        """
        SELECT theta_sketch_estimate(theta_union_agg(sketch)) AS result
        FROM (
          SELECT theta_sketch_agg(col) AS sketch FROM VALUES (1), (2), (2), (3) AS tab(col)
          UNION ALL
          SELECT theta_sketch_agg(col) AS sketch FROM VALUES (4), (5), (5), (6) AS tab(col)
        ) AS sketches
        """
      Then query result
        | result |
        | 6      |

    Scenario: theta_intersection_agg intersects sketch rows
      When query
        """
        SELECT theta_sketch_estimate(theta_intersection_agg(sketch)) AS result
        FROM (
          SELECT theta_sketch_agg(col) AS sketch FROM VALUES (1), (2), (2), (3) AS tab(col)
          UNION ALL
          SELECT theta_sketch_agg(col) AS sketch FROM VALUES (2), (3), (3), (4) AS tab(col)
        ) AS sketches
        """
      Then query result
        | result |
        | 2      |

  Rule: theta sketch functions return Spark-compatible types

    Scenario: theta sketch functions return binary and bigint values
      When query
        """
        SELECT
          typeof(theta_sketch_agg(col)) AS sketch_type,
          typeof(theta_sketch_estimate(theta_sketch_agg(col))) AS estimate_type
        FROM VALUES (1) AS tab(col)
        """
      Then query result
        | sketch_type | estimate_type |
        | binary      | bigint        |
