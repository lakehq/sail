@lambda_hof
@aggregate
Feature: aggregate higher-order function

  Rule: Array aggregation with lambda functions

    Scenario: aggregate sums integer array with identity finish
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 6      |

    Scenario: aggregate applies explicit finish lambda
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10) AS result
        """
      Then query result
        | result |
        | 60     |

    Scenario: aggregate computes average with struct accumulator
      When query
        """
        SELECT aggregate(
          array(1, 2, 3, 4),
          named_struct('sum', 0, 'cnt', 0),
          (acc, x) -> named_struct('sum', acc.sum + x, 'cnt', acc.cnt + 1),
          acc -> acc.sum / acc.cnt
        ) AS avg
        """
      Then query result
        | avg |
        | 2.5 |

    Scenario: aggregate applies finish to initial value for empty array
      When query
        """
        SELECT aggregate(CAST(array() AS ARRAY<INT>), 0, (acc, x) -> acc + x, acc -> acc * 10) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: aggregate returns NULL for NULL array
      When query
        """
        SELECT aggregate(CAST(NULL AS ARRAY<INT>), 0, (acc, x) -> acc + x, acc -> acc * 10) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: aggregate handles NULL elements through merge lambda
      When query
        """
        SELECT aggregate(array(1, NULL, 3), 0, (acc, x) -> acc + coalesce(x, 0)) AS result
        """
      Then query result
        | result |
        | 4      |

    Scenario: aggregate can capture outer columns per row
      When query
        """
        SELECT aggregate(arr, base, (acc, x) -> acc + x) AS result
        FROM VALUES
          (array(1, 2), 10),
          (array(3), 20)
        AS t(arr, base)
        """
      Then query result
        | result |
        | 13     |
        | 23     |

    Scenario: aggregate supports struct accumulator and finish conversion
      When query
        """
        SELECT aggregate(
          array(
            CAST(20.0 AS DOUBLE),
            CAST(4.0 AS DOUBLE),
            CAST(2.0 AS DOUBLE),
            CAST(6.0 AS DOUBLE),
            CAST(10.0 AS DOUBLE)
          ),
          named_struct('count', 0, 'sum', CAST(0.0 AS DOUBLE)),
          (acc, x) -> named_struct('count', acc.count + 1, 'sum', acc.sum + x),
          acc -> acc.sum / acc.count
        ) AS result
        """
      Then query result
        | result |
        | 8.4    |

    Scenario: aggregate merge can reference the element without the accumulator
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> x) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: aggregate merge references the element and a captured column
      When query
        """
        SELECT aggregate(arr, 0, (acc, x) -> x + base) AS result
        FROM VALUES
          (array(1, 2), 10),
          (array(3), 20)
        AS t(arr, base)
        """
      Then query result
        | result |
        | 12     |
        | 23     |

    Scenario: reduce is an alias for aggregate
      When query
        """
        SELECT reduce(array(1, 2, 3), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 6      |

    Scenario: reduce applies an explicit finish lambda
      When query
        """
        SELECT reduce(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10) AS result
        """
      Then query result
        | result |
        | 60     |

    Scenario: reduce merge can reference the element without the accumulator
      When query
        """
        SELECT reduce(array(1, 2, 3), 0, (acc, x) -> x) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: reduce returns NULL for NULL array
      When query
        """
        SELECT reduce(CAST(NULL AS ARRAY<INT>), 0, (acc, x) -> acc + x, acc -> acc * 10) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Float extremes and all-NULL elements propagate through the fold

    Scenario: aggregate propagates positive infinity
      When query
        """
        SELECT aggregate(array(CAST('Infinity' AS DOUBLE), 1.0), CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: aggregate propagates negative infinity
      When query
        """
        SELECT aggregate(array(CAST('-Infinity' AS DOUBLE), 1.0), CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) AS result
        """
      Then query result
        | result    |
        | -Infinity |

    Scenario: aggregate propagates NaN
      When query
        """
        SELECT aggregate(array(CAST('NaN' AS DOUBLE), 1.0), CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: aggregate of positive and negative infinity is NaN
      When query
        """
        SELECT aggregate(array(CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE)), CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: aggregate over an all-NULL-element array propagates NULL
      When query
        """
        SELECT aggregate(CAST(array(NULL, NULL) AS ARRAY<INT>), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: aggregate mixing normal, NULL, infinity and NaN propagates NULL
      When query
        """
        SELECT aggregate(array(1.0, CAST(NULL AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE)), CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: aggregate mixing normal, NULL, infinity and NaN with coalesce yields NaN
      When query
        """
        SELECT aggregate(array(1.0, CAST(NULL AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE)), CAST(0.0 AS DOUBLE), (acc, x) -> acc + coalesce(x, 0)) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: aggregate mixing normal, infinity and NaN yields NaN
      When query
        """
        SELECT aggregate(array(1.0, CAST('Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE)), CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: aggregate mixing normal, NULL and infinity with coalesce yields infinity
      When query
        """
        SELECT aggregate(array(1.0, CAST(NULL AS DOUBLE), CAST('Infinity' AS DOUBLE)), CAST(0.0 AS DOUBLE), (acc, x) -> acc + coalesce(x, 0)) AS result
        """
      Then query result
        | result   |
        | Infinity |

  Rule: ANSI arithmetic inside the merge lambda

    @sail-bug
    Scenario: integer overflow inside merge errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT aggregate(array(2000000000, 2000000000), 0, (acc, x) -> acc + x) AS result
        """
      Then query error .*

    Scenario: integer overflow inside merge wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT aggregate(array(2000000000, 2000000000), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result     |
        | -294967296 |

  Rule: Lambda arity is validated against Spark

    Scenario: aggregate rejects a merge lambda with fewer than 2 parameters
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, x -> x) AS result
        """
      Then query error (?i)lambda function

    Scenario: aggregate rejects a merge lambda with more than 2 parameters
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (a, b, c) -> a) AS result
        """
      Then query error (?i)lambda function

    Scenario: aggregate rejects a finish lambda with more than 1 parameter
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x, (a, b) -> a) AS result
        """
      Then query error (?i)lambda function

    Scenario: reduce rejects a merge lambda with fewer than 2 parameters
      When query
        """
        SELECT reduce(array(1, 2, 3), 0, acc -> acc + 1) AS result
        """
      Then query error (?i)lambda function
