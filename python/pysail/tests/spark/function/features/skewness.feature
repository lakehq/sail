@skewness
Feature: skewness returns the skewness of the values in a group

  # `skewness` is a Spark SQL aggregate function, so these scenarios run against
  # both Sail and JVM Spark. Float results are rounded to keep the rendered value
  # stable across engines. Expected values validated against Spark JVM 4.1.

  Rule: skewness over numeric values

    Scenario: skewness over double values
      When query
        """
        SELECT round(skewness(v), 10) AS result
        FROM VALUES (1.0D), (2.0D), (3.0D), (4.0D), (100.0D) AS t(v)
        """
      Then query result
        | result       |
        | 1.4975367033 |

    Scenario: skewness over integer values
      When query
        """
        SELECT round(skewness(v), 10) AS result
        FROM VALUES (1), (2), (3), (4), (100) AS t(v)
        """
      Then query result
        | result       |
        | 1.4975367033 |

    Scenario: skewness skips null values
      When query
        """
        SELECT round(skewness(v), 10) AS result
        FROM VALUES (1.0D), (CAST(NULL AS DOUBLE)), (3.0D), (5.0D) AS t(v)
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: skewness over numeric strings via implicit cast
      When query
        """
        SELECT round(skewness(v), 10) AS result
        FROM VALUES ('1'), ('2'), ('3'), ('100') AS t(v)
        """
      Then query result
        | result       |
        | 1.1537390558 |

    Scenario: skewness over decimal values
      When query
        """
        SELECT round(skewness(CAST(v AS DECIMAL(10, 2))), 10) AS result
        FROM VALUES (1), (2), (3), (4), (100) AS t(v)
        """
      Then query result
        | result       |
        | 1.4975367033 |

  Rule: skewness edge cases

    Scenario: skewness of a single value is NULL
      When query
        """
        SELECT skewness(v) AS result FROM VALUES (5.0D) AS t(v)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: skewness over an empty group is NULL
      When query
        """
        SELECT skewness(v) AS result FROM VALUES (1.0D) AS t(v) WHERE v > 100
        """
      Then query result
        | result |
        | NULL   |

  Rule: skewness with grouping and windows

    Scenario: skewness computed per group
      When query
        """
        SELECT g, round(skewness(v), 10) AS result
        FROM VALUES ('a', 1.0D), ('a', 2.0D), ('a', 3.0D), ('a', 4.0D), ('a', 100.0D),
                    ('b', 1.0D), ('b', 2.0D), ('b', 3.0D), ('b', 4.0D), ('b', 5.0D) AS t(g, v)
        GROUP BY g
        """
      Then query result
        | g | result       |
        | a | 1.4975367033 |
        | b | 0.0          |

    Scenario: skewness as a window function over a partition
      When query
        """
        SELECT g, v, round(skewness(v) OVER (PARTITION BY g), 10) AS result
        FROM VALUES ('a', 1.0D), ('a', 2.0D), ('a', 3.0D), ('a', 4.0D), ('a', 100.0D) AS t(g, v)
        ORDER BY v
        """
      Then query result ordered
        | g | v     | result       |
        | a | 1.0   | 1.4975367033 |
        | a | 2.0   | 1.4975367033 |
        | a | 3.0   | 1.4975367033 |
        | a | 4.0   | 1.4975367033 |
        | a | 100.0 | 1.4975367033 |

  Rule: skewness extreme cases

    Scenario: skewness of identical values (zero variance) is NULL
      When query
        """
        SELECT skewness(v) AS result FROM VALUES (2.0D), (2.0D), (2.0D), (2.0D) AS t(v)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: skewness of exactly two values is zero
      When query
        """
        SELECT round(skewness(v), 10) AS result FROM VALUES (1.0D), (2.0D) AS t(v)
        """
      Then query result
        | result |
        | 0.0    |

    Scenario: skewness flips sign under sign negation
      When query
        """
        SELECT round(skewness(v), 10) AS result
        FROM VALUES (-1.0D), (-2.0D), (-3.0D), (-4.0D), (-100.0D) AS t(v)
        """
      Then query result
        | result        |
        | -1.4975367033 |

  Rule: skewness input type validation

    # Spark rejects boolean input with DATATYPE_MISMATCH; Sail rejects it from
    # `coerce_types` with an unsupported-type error. The regex matches both.
    Scenario: skewness rejects boolean input
      When query
        """
        SELECT skewness(v) AS result FROM VALUES (true), (false), (true) AS t(v)
        """
      Then query error (DATATYPE_MISMATCH|numeric type)
