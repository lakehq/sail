Feature: nullif output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to nullif yields the schema Spark declares
      When query
        """
        SELECT nullif(2, 2) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to nullif yields the schema Spark declares
      When query
        """
        SELECT nullif(CAST(id AS INT), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to nullif stays nullable
      When query
        """
        SELECT nullif(c, 2) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Floating-point comparison

    # Spark compares floats with `-0.0` equal to `0.0` and NaN equal to NaN (SQLOrderingUtil),
    # and returns the first argument as it is when the arguments are not equal.
    Scenario: nullif treats -0.0 and 0.0 as equal for DOUBLE columns
      When query
        """
        SELECT k, nullif(x, y) AS r
        FROM VALUES
          (1, -0.0D, 0.0D), (2, 0.0D, -0.0D), (3, -0.0D, -0.0D),
          (4, CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE)), (5, -0.0D, 1.0D), (6, 1.0D, 2.0D)
        AS t(k, x, y)
        ORDER BY k
        """
      Then query result ordered
        | k | r    |
        | 1 | NULL |
        | 2 | NULL |
        | 3 | NULL |
        | 4 | NULL |
        | 5 | -0.0 |
        | 6 | 1.0  |

    Scenario: nullif treats -0.0 and 0.0 as equal for literals
      When query
        """
        SELECT nullif(-0.0D, 0.0D) AS a, nullif(0.0D, -0.0D) AS b, nullif(-0.0F, 0.0F) AS c, nullif(-0.0D, 1.0D) AS d
        """
      Then query result
        | a    | b    | c    | d    |
        | NULL | NULL | NULL | -0.0 |

    Scenario: nullif treats -0.0 and 0.0 as equal for a column and a literal
      When query
        """
        SELECT nullif(x, 0.0D) AS a, nullif(0.0D, x) AS b FROM VALUES (-0.0D), (1.0D) AS t(x)
        """
      Then query result
        | a    | b    |
        | NULL | NULL |
        | 1.0  | 0.0  |

    Scenario: nullif treats -0.0 and 0.0 as equal for FLOAT columns
      When query
        """
        SELECT nullif(x, y) AS r FROM VALUES (-0.0F, 0.0F), (0.0F, -0.0F) AS t(x, y)
        """
      Then query result
        | r    |
        | NULL |
        | NULL |

    Scenario: nullif treats -0.0 and 0.0 as equal in nested values
      When query
        """
        SELECT
          nullif(array(x), array(y)) AS a,
          nullif(named_struct('f', x), named_struct('f', y)) AS b,
          nullif(array(named_struct('f', x)), array(named_struct('f', y))) AS c
        FROM VALUES (-0.0D, 0.0D) AS t(x, y)
        """
      Then query result
        | a    | b    | c    |
        | NULL | NULL | NULL |

    Scenario: nullif returns a nested value with -0.0 as it is
      When query
        """
        SELECT nullif(array(x), array(y)) AS a FROM VALUES (-0.0D, 1.0D) AS t(x, y)
        """
      Then query result
        | a      |
        | [-0.0] |
