@function(sketch)
Feature: Theta sketch functions

  Rule: theta_sketch_agg builds compact theta sketches

    Scenario Outline: Sketch agg: <case>
      When query
        """
        SELECT theta_sketch_estimate(theta_sketch_agg(<args>)) AS result
        FROM VALUES <values> AS tab(col)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                      | args    | values                                 | result |
        | theta_sketch_agg estimates distinct integer values        | col     | (1), (1), (2), (2), (3)                | 3      |
        | theta_sketch_agg accepts an explicit lgNomEntries value   | col, 15 | (1), (1), (2), (2), (3)                | 3      |
        | theta_sketch_agg ignores null input values                | col     | (1), (CAST(NULL AS INT)), (2), (2)     | 2      |
        | theta_sketch_agg follows Spark array null-element hashing | col     | (array(CAST(NULL AS INT))), (array(0)) | 1      |

  Rule: theta sketch set operations combine sketches

    Scenario Outline: Set operation: <case>
      When query
        """
        SELECT theta_sketch_estimate(<op>(theta_sketch_agg(col1), theta_sketch_agg(col2))) AS result
        FROM VALUES <values> AS tab(col1, col2)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                       | op                 | values                                 | result |
        | theta_union merges two sketches            | theta_union        | (1, 4), (1, 4), (2, 5), (2, 5), (3, 6) | 6      |
        | theta_intersection intersects two sketches | theta_intersection | (5, 4), (1, 4), (2, 5), (2, 5), (3, 1) | 2      |
        | theta_difference subtracts sketches        | theta_difference   | (5, 4), (1, 4), (2, 5), (2, 5), (3, 1) | 2      |

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
      Then query error (infinite set|without any non-null input sketches)

    Scenario: theta_intersection_agg rejects typed null sketch inputs
      When query
        """
        SELECT theta_sketch_estimate(theta_intersection_agg(CAST(NULL AS BINARY))) AS result
        """
      Then query error (infinite set|without any non-null input sketches)

    Scenario: theta_intersection_agg rejects empty inputs
      When query
        """
        SELECT theta_sketch_estimate(theta_intersection_agg(CAST(col AS BINARY))) AS result
        FROM VALUES (CAST(NULL AS BINARY)) AS tab(col)
        WHERE false
        """
      Then query error (infinite set|without any non-null input sketches)

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

  Rule: theta_sketch_agg normalizes floating-point special values

    Scenario Outline: Float special: <case>
      When query
        """
        SELECT theta_sketch_estimate(theta_sketch_agg(CAST(c AS <type>))) AS result
        FROM VALUES (<v1>), (<v2>) AS tab(c)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                  | type   | v1                | v2                 | result |
        | theta_sketch_agg accepts float input and treats all NaNs as one value | FLOAT  | float('NaN')      | float('NaN')       | 1      |
        | theta_sketch_agg treats positive and negative zero as equal           | FLOAT  | float('0.0')      | float('-0.0')      | 1      |
        | theta_sketch_agg keeps positive and negative infinity distinct        | FLOAT  | float('Infinity') | float('-Infinity') | 2      |
        | theta_sketch_agg accepts double input and normalizes NaN              | DOUBLE | double('NaN')     | double('NaN')      | 1      |

  Rule: theta_sketch_agg validates input types and lgNomEntries bounds

    Scenario: theta_sketch_agg accepts an array of bigint values
      When query
        """
        SELECT theta_sketch_estimate(theta_sketch_agg(c)) AS result
        FROM VALUES (array(CAST(1 AS BIGINT))) AS tab(c)
        """
      Then query result
        | result |
        | 1      |

    Scenario: theta_sketch_agg rejects decimal input
      When query
        """
        SELECT theta_sketch_agg(CAST(c AS DECIMAL(10,2))) FROM VALUES (1.0) AS tab(c)
        """
      Then query error (UNEXPECTED_INPUT_TYPE|does not support input type)

    Scenario: theta_sketch_agg accepts the valid lgNomEntries boundaries
      When query
        """
        SELECT
          theta_sketch_estimate(theta_sketch_agg(col, 4)) AS lo,
          theta_sketch_estimate(theta_sketch_agg(col, 26)) AS hi
        FROM VALUES (1) AS tab(col)
        """
      Then query result
        | lo | hi |
        | 1  | 1  |

    # Spark: [SKETCH_INVALID_LG_NOM_ENTRIES] Invalid call to `theta_sketch_agg`; the
    # `lgNomEntries` value must be between 4 and 26, inclusive: <n>. Sail rejects it too but
    # with its own wording.
    @sail-bug
    Scenario Outline: lgNomEntries bound: <case>
      When query
        """
        SELECT theta_sketch_agg(col, <n>) FROM VALUES (1) AS tab(col)
        """
      Then query error \[SKETCH_INVALID_LG_NOM_ENTRIES\].*must be between 4 and 26, inclusive: <n>

      Examples:
        | case                                                        | n  |
        | theta_sketch_agg rejects lgNomEntries below the valid range | 3  |
        | theta_sketch_agg rejects lgNomEntries above the valid range | 27 |
