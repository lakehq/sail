@function(lambda)
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

    Scenario: aggregate merge can reference the element without the accumulator
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> x) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: reduce is an alias for aggregate
      When query
        """
        SELECT reduce(array(1, 2, 3), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 6      |

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

  Rule: Basic reduce — fold array to scalar

    Scenario: Sum integers
      When query
        """
        SELECT reduce(array(1, 2, 3, 4, 5), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 15     |

    Scenario: Product of integers
      When query
        """
        SELECT reduce(array(1, 2, 3, 4, 5), 1, (acc, x) -> acc * x) AS result
        """
      Then query result
        | result |
        | 120    |

    Scenario: Single element array
      When query
        """
        SELECT reduce(array(42), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: Maximum value using greatest
      When query
        """
        SELECT reduce(array(3, 1, 4, 1, 5, 9, 2, 6), 0, (acc, x) -> greatest(acc, x)) AS result
        """
      Then query result
        | result |
        | 9      |

    Scenario: Concatenate strings
      When query
        """
        SELECT reduce(array('a', 'b', 'c'), '', (acc, x) -> concat(acc, x)) AS result
        """
      Then query result
        | result |
        | abc    |

    Scenario: Count array elements using reduce
      When query
        """
        SELECT reduce(array('x', 'y', 'z'), 0, (acc, x) -> acc + 1) AS result
        """
      Then query result
        | result |
        | 3      |

  Rule: With finish lambda — transform accumulator after fold

    Scenario: Sum then double with finish
      When query
        """
        SELECT aggregate(array(1, 2, 3, 4), 0, (acc, x) -> acc + x, acc -> acc * 2) AS result
        """
      Then query result
        | result |
        | 20     |

    Scenario: Sum then offset with finish
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc + 100) AS result
        """
      Then query result
        | result |
        | 106    |

    Scenario: Sum then negate with finish
      When query
        """
        SELECT aggregate(array(1, 2, 3, 4, 5), 0, (acc, x) -> acc + x, acc -> -acc) AS result
        """
      Then query result
        | result |
        | -15    |

    Scenario: Integer division in finish for average
      When query
        """
        SELECT aggregate(array(10, 20, 30, 40), 0, (acc, x) -> acc + x, acc -> acc / 4) AS result
        """
      Then query result
        | result |
        | 25.0   |

  Rule: aggregate is an alias for reduce

    Scenario: aggregate with 3 arguments matches reduce
      When query
        """
        SELECT
          reduce(array(10, 20, 30), 0, (acc, x) -> acc + x) AS r,
          aggregate(array(10, 20, 30), 0, (acc, x) -> acc + x) AS a
        """
      Then query result
        | r  | a  |
        | 60 | 60 |

    Scenario: aggregate with 4 arguments applies finish
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10) AS result
        """
      Then query result
        | result |
        | 60     |

  Rule: Empty array returns initial value (with finish applied)

    Scenario: reduce on empty int array returns zero value
      When query
        """
        SELECT reduce(x, 99, (acc, e) -> acc + e) AS result
        FROM VALUES (CAST(array() AS array<int>)) AS t(x)
        """
      Then query result
        | result |
        | 99     |

    Scenario: aggregate on empty array applies finish to zero
      When query
        """
        SELECT aggregate(x, 42, (acc, e) -> acc + e, acc -> acc * 2) AS result
        FROM VALUES (CAST(array() AS array<int>)) AS t(x)
        """
      Then query result
        | result |
        | 84     |

    Scenario: reduce on empty string array returns initial string
      When query
        """
        SELECT reduce(x, 'start', (acc, e) -> concat(acc, e)) AS result
        FROM VALUES (CAST(array() AS array<string>)) AS t(x)
        """
      Then query result
        | result |
        | start  |

  Rule: NULL array input returns NULL without invoking lambdas

    Scenario: NULL array returns NULL
      When query
        """
        SELECT reduce(CAST(NULL AS array<int>), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL array with finish still returns NULL
      When query
        """
        SELECT aggregate(CAST(NULL AS array<int>), 0, (acc, x) -> acc + x, acc -> acc * 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL array from VALUES table
      When query
        """
        SELECT reduce(x, 0, (acc, e) -> acc + e) AS result
        FROM VALUES (CAST(NULL AS array<int>)) AS t(x)
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL elements propagate through the merge lambda

    Scenario: NULL element causes NULL result via addition
      When query
        """
        SELECT reduce(array(1, NULL, 3), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: coalesce in merge skips NULL elements
      When query
        """
        SELECT reduce(array(1, NULL, 3), 0, (acc, x) -> acc + coalesce(x, 0)) AS result
        """
      Then query result
        | result |
        | 4      |

    Scenario: NULL element in string concat causes NULL
      When query
        """
        SELECT reduce(array('a', NULL, 'c'), '', (acc, x) -> concat(acc, x)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: coalesce in merge skips NULL strings
      When query
        """
        SELECT reduce(array('a', NULL, 'c'), '', (acc, x) -> concat(acc, coalesce(x, ''))) AS result
        """
      Then query result
        | result |
        | ac     |

    Scenario: NULL element propagates to finish
      When query
        """
        SELECT aggregate(array(1, NULL, 3), 0, (acc, x) -> acc + x, acc -> acc * 2) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL initial value propagates through merge

    Scenario: NULL zero with strict addition returns NULL
      When query
        """
        SELECT reduce(array(1, 2, 3), CAST(NULL AS int), (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: coalesce in merge can recover from NULL zero
      When query
        """
        SELECT reduce(array(1, 2, 3), CAST(NULL AS int), (acc, x) -> coalesce(acc, 0) + x) AS result
        """
      Then query result
        | result |
        | 6      |

  Rule: Different data types

    Scenario: Reduce BIGINT array
      When query
        """
        SELECT reduce(array(1L, 2L, 3L, 4L), 0L, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 10     |

    Scenario: Reduce DOUBLE array
      When query
        """
        SELECT reduce(array(1.5, 2.5, 3.0), CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 7.0    |

    Scenario: Reduce boolean array with logical OR
      When query
        """
        SELECT reduce(array(false, false, true, false), false, (acc, x) -> acc OR x) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Reduce boolean array with logical AND — all true
      When query
        """
        SELECT reduce(array(true, true, true), true, (acc, x) -> acc AND x) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Reduce boolean array with logical AND — one false
      When query
        """
        SELECT reduce(array(true, false, true), true, (acc, x) -> acc AND x) AS result
        """
      Then query result
        | result |
        | false  |

  Rule: Multiple rows (batch processing)

    Scenario: reduce applied to multiple rows
      When query
        """
        SELECT id, reduce(arr, 0, (acc, x) -> acc + x) AS total
        FROM VALUES
          (1, array(1, 2, 3)),
          (2, array(10, 20)),
          (3, array(5))
        AS t(id, arr)
        ORDER BY id
        """
      Then query result ordered
        | id | total |
        | 1  | 6     |
        | 2  | 30    |
        | 3  | 5     |

    Scenario: reduce with NULL arrays mixed in batch
      When query
        """
        SELECT id, reduce(arr, 0, (acc, x) -> acc + x) AS total
        FROM VALUES
          (1, array(1, 2, 3)),
          (2, CAST(NULL AS array<int>)),
          (3, array(7, 8))
        AS t(id, arr)
        ORDER BY id
        """
      Then query result ordered
        | id | total |
        | 1  | 6     |
        | 2  | NULL  |
        | 3  | 15    |

    Scenario: aggregate with finish applied to each row
      When query
        """
        SELECT id, aggregate(arr, 0, (acc, x) -> acc + x, acc -> acc * mult) AS result
        FROM VALUES
          (1, array(1, 2, 3), 2),
          (2, array(4, 5), 10),
          (3, array(7), 1)
        AS t(id, arr, mult)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 12     |
        | 2  | 90     |
        | 3  | 7      |

  Rule: External column references in lambda body

    Scenario: Merge lambda references outer column
      When query
        """
        SELECT reduce(arr, 0, (acc, x) -> acc + x + bonus) AS result
        FROM VALUES (array(1, 2, 3), 10) AS t(arr, bonus)
        """
      Then query result
        | result |
        | 36     |

    Scenario: Finish lambda references outer column
      When query
        """
        SELECT aggregate(arr, 0, (acc, x) -> acc + x, acc -> acc + bonus) AS result
        FROM VALUES (array(1, 2, 3), 100) AS t(arr, bonus)
        """
      Then query result
        | result |
        | 106    |

  Rule: Complex lambda expressions

    Scenario: Running minimum using IF
      When query
        """
        SELECT reduce(array(3, 1, 4, 1, 5, 9, 2, 6), 2147483647, (acc, x) -> if(x < acc, x, acc)) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: Count even elements
      When query
        """
        SELECT reduce(array(1, 2, 3, 4, 5, 6), 0, (acc, x) -> acc + if(x % 2 = 0, 1, 0)) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: Find first element greater than threshold
      When query
        """
        SELECT reduce(array(3, 7, 2, 9, 4), -1, (acc, x) -> if(acc = -1 AND x > 5, x, acc)) AS result
        """
      Then query result
        | result |
        | 7      |

    Scenario: Build string with separator
      When query
        """
        SELECT reduce(array('b', 'c', 'd'), 'a', (acc, x) -> concat(acc, ',', x)) AS result
        """
      Then query result
        | result  |
        | a,b,c,d |

  Rule: Column alias named reduce is valid

    Scenario: reduce result aliased as reduce
      When query
        """
        SELECT reduce(array(1, 2, 3), 0, (acc, x) -> acc + x) AS reduce
        """
      Then query result
        | reduce |
        | 6      |

  Rule: Empty double array with explicit zero returns initial value

    Scenario: reduce on empty double array with CAST initial value returns zero
      When query
        """
        SELECT reduce(CAST(array() AS array<double>), CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 0.0    |

  Rule: finish lambda is applied even when accumulator is NULL

    Scenario: aggregate NULL initial value with coalesce finish returns finish result
      When query
        """
        SELECT aggregate(array(1, 2, 3), CAST(NULL AS int), (acc, x) -> acc + x, acc -> coalesce(acc, -999)) AS result
        """
      Then query result
        | result |
        | -999   |

  Rule: Non-lambda expression in place of a lambda

    @sail-bug
    Scenario Outline: Non-lambda argument: <case>
      When query
        """
        SELECT aggregate(array(1, 2), 0, <rest>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                   | rest                        | result |
        | a constant merge       | 9                           | 9      |
        | a constant finish      | (acc, x) -> acc + x, 99     | 99     |
        | both lambdas constant  | 9, 99                       | 99     |

    # The mixed [merge non-lambda, finish lambda] form: the wrapped merge takes a
    # two-parameter set (acc, element) while the real finish takes one (acc), so
    # this is the shape where positional param-set alignment would regress if the
    # wrapped and real lambdas were bound out of order.
    @sail-bug
    Scenario: A constant merge with a real finish lambda
      When query
        """
        SELECT aggregate(array(1, 2), 0, 9, acc -> acc) AS result
        """
      Then query result
        | result |
        | 9      |

    @sail-bug
    Scenario: reduce (the aggregate alias) accepts a non-lambda merge
      When query
        """
        SELECT reduce(array(1, 2), 0, 9) AS result
        """
      Then query result
        | result |
        | 9      |

    @sail-bug
    Scenario: A merge lambda that only references an outer column
      When query
        """
        SELECT aggregate(array(1, 2), 0, v) AS result FROM (SELECT 7 AS v) t
        """
      Then query result
        | result |
        | 7      |

    @sail-bug
    Scenario: The zero wins over a constant merge lambda on an empty array
      When query
        """
        SELECT aggregate(array(), 0, 9) AS result
        """
      Then query result
        | result |
        | 0      |

    @sail-bug
    Scenario: A constant merge lambda over a NULL array
      When query
        """
        SELECT aggregate(CAST(NULL AS ARRAY<INT>), 0, 9) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: A constant merge lambda over an array column resolves per row
      When query
        """
        SELECT aggregate(c, 0, 9) AS result
        FROM VALUES (array(1, 2)), (array()), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query result ordered
        | result |
        | 9      |
        | 0      |
        | NULL   |

    @sail-bug
    Scenario: A merge lambda whose type does not match the accumulator is still an error
      When query
        """
        SELECT aggregate(array(1, 2), 0, 'x') AS result
        """
      Then query error The third parameter requires the "INT" type

    @sail-bug
    Scenario: the merge type is validated at analysis, even inside an unreachable IF branch
      When query
        """
        SELECT IF(false, aggregate(array(1), 0, 'x'), 0) AS result
        """
      Then query error The third parameter requires the "INT" type

  Rule: Untyped NULL body

    @sail-bug
    Scenario: An untyped NULL merge lambda body
      When query
        """
        SELECT aggregate(array(1, 2), 0, (acc, x) -> NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: An untyped NULL in place of the merge lambda
      When query
        """
        SELECT aggregate(array(1, 2), 0, NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: The zero survives an untyped NULL merge lambda on an empty array
      When query
        """
        SELECT aggregate(array(), 0, (acc, x) -> NULL) AS result
        """
      Then query result
        | result |
        | 0      |

    # Spark's type coercion replaces a NULL-typed merge body (here `assert_true`,
    # whose type is VOID) with a constant NULL of the accumulator's type, so the
    # body never runs and the fold collapses to NULL. Sail keeps and evaluates the
    # body, so the side effect still raises. Same class as the exists/forall/filter
    # erasure; `array_sort` differs — it rejects a VOID comparator at analysis.
    @sail-bug
    Scenario: a side-effecting NULL-typed merge lambda is erased rather than evaluated
      When query
        """
        SELECT aggregate(array(1, 0), 0, (acc, x) -> assert_true(x <> 0)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Subquery expressions are rejected in a value argument

    @sail-bug
    Scenario: a subquery in the array argument is rejected
      When query
        """
        SELECT aggregate((SELECT array(1, 2)), 0, (a, x) -> a + x) AS result
        """
      Then query error Subquery expressions are not supported within higher-order functions

    @sail-bug
    Scenario: a subquery in the zero argument is rejected
      When query
        """
        SELECT aggregate(array(1, 2), (SELECT 0), (a, x) -> a + x) AS result
        """
      Then query error Subquery expressions are not supported within higher-order functions
