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

    Scenario: theta_sketch_agg follows Spark array null-element hashing
      When query
        """
        SELECT theta_sketch_estimate(theta_sketch_agg(col)) AS result
        FROM VALUES (array(CAST(NULL AS INT))), (array(0)) AS tab(col)
        """
      Then query result
        | result |
        | 1      |

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

    Scenario: theta_union accepts null arguments in three-argument form
      When query
        """
        SELECT
          theta_sketch_estimate(NULL) AS estimate,
          theta_union(NULL, sketch, 12) IS NULL AS left_null,
          theta_union(sketch, NULL, 12) IS NULL AS right_null,
          theta_union(sketch, sketch, NULL) IS NULL AS config_null,
          theta_intersection(NULL, sketch) IS NULL AS intersection_null,
          theta_difference(sketch, NULL) IS NULL AS difference_null
        FROM (
          SELECT theta_sketch_agg(col) AS sketch FROM VALUES (1) AS tab(col)
        ) AS sketches
        """
      Then query result
        | estimate | left_null | right_null | config_null | intersection_null | difference_null |
        | NULL     | true      | true       | true        | true              | true            |

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

    Scenario: theta sketch outputs use Spark compressed serialization when applicable
      When query
        """
        WITH sketches AS (
          SELECT
            theta_sketch_agg(col1) AS left_sketch,
            theta_sketch_agg(col2) AS right_sketch
          FROM VALUES (1, 1), (2, 2), (3, 4), (5, 4) AS tab(col1, col2)
        )
        SELECT
          substr(hex(left_sketch), 3, 2) AS agg_version,
          substr(hex(theta_union(left_sketch, right_sketch)), 3, 2) AS union_version,
          substr(hex(theta_intersection(left_sketch, right_sketch)), 3, 2) AS intersection_version,
          substr(hex(theta_difference(left_sketch, right_sketch)), 3, 2) AS difference_version
        FROM sketches
        """
      Then query result
        | agg_version | union_version | intersection_version | difference_version |
        | 04          | 04            | 04                   | 04                 |

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

    Scenario: theta_union_agg accepts untyped null sketch inputs
      When query
        """
        SELECT theta_sketch_estimate(theta_union_agg(NULL)) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: theta_intersection_agg rejects untyped null sketch inputs
      When query
        """
        SELECT theta_sketch_estimate(theta_intersection_agg(NULL)) AS result
        """
      Then query error theta_intersection_agg cannot produce a result without any non-null input sketches

    Scenario: theta_intersection_agg rejects typed null sketch inputs
      When query
        """
        SELECT theta_sketch_estimate(theta_intersection_agg(CAST(NULL AS BINARY))) AS result
        """
      Then query error theta_intersection_agg cannot produce a result without any non-null input sketches

    Scenario: theta_intersection_agg rejects empty inputs
      When query
        """
        SELECT theta_sketch_estimate(theta_intersection_agg(CAST(col AS BINARY))) AS result
        FROM VALUES (CAST(NULL AS BINARY)) AS tab(col)
        WHERE false
        """
      Then query error theta_intersection_agg cannot produce a result without any non-null input sketches

    Scenario: theta_intersection_agg skips null-only partial sketch states
      When query
        """
        SELECT theta_sketch_estimate(theta_intersection_agg(sketch)) AS result
        FROM (
          SELECT CAST(NULL AS BINARY) AS sketch FROM range(0, 2, 1, 1)
          UNION ALL
          SELECT theta_sketch_agg(col) AS sketch FROM VALUES (1), (2) AS tab(col)
        ) AS sketches
        """
      Then query result
        | result |
        | 2      |

    Scenario: theta sketch aggregates work as window functions with default arguments
      When query
        """
        WITH input AS (
          SELECT * FROM VALUES (1, 1), (2, 1), (3, 2) AS tab(id, col)
        ),
        sketches AS (
          SELECT 1 AS id, theta_sketch_agg(col) AS sketch FROM VALUES (1), (2) AS tab(col)
          UNION ALL
          SELECT 2 AS id, theta_sketch_agg(col) AS sketch FROM VALUES (2), (3) AS tab(col)
        )
        SELECT 'sketch' AS fn, id,
          theta_sketch_estimate(theta_sketch_agg(col) OVER (
            ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )) AS result
        FROM input
        UNION ALL
        SELECT 'union' AS fn, id,
          theta_sketch_estimate(theta_union_agg(sketch) OVER (
            ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )) AS result
        FROM sketches
        ORDER BY fn, id
        """
      Then query result ordered
        | fn     | id | result |
        | sketch | 1  | 1      |
        | sketch | 2  | 1      |
        | sketch | 3  | 2      |
        | union  | 1  | 2      |
        | union  | 2  | 3      |

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
