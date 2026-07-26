@lambda_hof
@aggregate
Feature: aggregate higher-order function

  Rule: Array aggregation with lambda functions

    Scenario Outline: Lambda: <case>
      When query
        """
        SELECT aggregate(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                              | args                                                                 | result |
        | aggregate sums integer array with identity finish                 | array(1, 2, 3), 0, (acc, x) -> acc + x                               | 6      |
        | aggregate applies explicit finish lambda                          | array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10              | 60     |
        | aggregate applies finish to initial value for empty array         | CAST(array() AS ARRAY<INT>), 0, (acc, x) -> acc + x, acc -> acc * 10 | 0      |
        | aggregate returns NULL for NULL array                             | CAST(NULL AS ARRAY<INT>), 0, (acc, x) -> acc + x, acc -> acc * 10    | NULL   |
        | aggregate handles NULL elements through merge lambda              | array(1, NULL, 3), 0, (acc, x) -> acc + coalesce(x, 0)               | 4      |
        | aggregate merge can reference the element without the accumulator | array(1, 2, 3), 0, (acc, x) -> x                                     | 3      |

    Scenario Outline: reduce: <case>
      When query
        """
        SELECT reduce(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                           | args                                                              | result |
        | reduce is an alias for aggregate                               | array(1, 2, 3), 0, (acc, x) -> acc + x                            | 6      |
        | reduce applies an explicit finish lambda                       | array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10           | 60     |
        | reduce merge can reference the element without the accumulator | array(1, 2, 3), 0, (acc, x) -> x                                  | 3      |
        | reduce returns NULL for NULL array                             | CAST(NULL AS ARRAY<INT>), 0, (acc, x) -> acc + x, acc -> acc * 10 | NULL   |

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

  Rule: Float extremes and all-NULL elements propagate through the fold

    Scenario Outline: aggregate <case>
      When query
        """
        SELECT aggregate(<arr>, <init>, (acc, x) -> <merge>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                           | arr                                                                                 | init                | merge                | result    |
        | propagates positive infinity                                   | array(CAST('Infinity' AS DOUBLE), 1.0)                                              | CAST(0.0 AS DOUBLE) | acc + x              | Infinity  |
        | propagates negative infinity                                   | array(CAST('-Infinity' AS DOUBLE), 1.0)                                             | CAST(0.0 AS DOUBLE) | acc + x              | -Infinity |
        | propagates NaN                                                 | array(CAST('NaN' AS DOUBLE), 1.0)                                                   | CAST(0.0 AS DOUBLE) | acc + x              | NaN       |
        | of positive and negative infinity is NaN                       | array(CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE))                      | CAST(0.0 AS DOUBLE) | acc + x              | NaN       |
        | over an all-NULL-element array propagates NULL                 | CAST(array(NULL, NULL) AS ARRAY<INT>)                                               | 0                   | acc + x              | NULL      |
        | mixing normal, NULL, infinity and NaN propagates NULL          | array(1.0, CAST(NULL AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE)) | CAST(0.0 AS DOUBLE) | acc + x              | NULL      |
        | mixing normal, NULL, infinity and NaN with coalesce yields NaN | array(1.0, CAST(NULL AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE)) | CAST(0.0 AS DOUBLE) | acc + coalesce(x, 0) | NaN       |
        | mixing normal, infinity and NaN yields NaN                     | array(1.0, CAST('Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE))                       | CAST(0.0 AS DOUBLE) | acc + x              | NaN       |
        | mixing normal, NULL and infinity with coalesce yields infinity | array(1.0, CAST(NULL AS DOUBLE), CAST('Infinity' AS DOUBLE))                        | CAST(0.0 AS DOUBLE) | acc + coalesce(x, 0) | Infinity  |

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

    Scenario Outline: <fn> rejects <case>
      When query
        """
        SELECT <fn>(array(1, 2, 3), 0, <lambdas>) AS result
        """
      Then query error (?i)lambda function

      Examples:
        | fn        | case                                        | lambdas                          |
        | aggregate | a merge lambda with fewer than 2 parameters | x -> x                           |
        | aggregate | a merge lambda with more than 2 parameters  | (a, b, c) -> a                   |
        | aggregate | a finish lambda with more than 1 parameter  | (acc, x) -> acc + x, (a, b) -> a |

    Scenario: reduce rejects a merge lambda with fewer than 2 parameters
      When query
        """
        SELECT reduce(array(1, 2, 3), 0, acc -> acc + 1) AS result
        """
      Then query error (?i)lambda function
