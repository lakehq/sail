@lambda_hof
@filter
Feature: array filter with lambda

  Rule: Filter array elements using lambda predicates

    Scenario: Filter integers greater than a value
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x > 2) AS result
        """
      Then query result
        | result    |
        | [3, 4, 5] |

    Scenario: Filter integers less than a value
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x < 3) AS result
        """
      Then query result
        | result |
        | [1, 2] |

    Scenario: Filter integers greater than or equal to a value
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x >= 3) AS result
        """
      Then query result
        | result    |
        | [3, 4, 5] |

    Scenario: Filter integers less than or equal to a value
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x <= 2) AS result
        """
      Then query result
        | result |
        | [1, 2] |

    Scenario: Filter where all elements match
      When query
        """
        SELECT filter(array(10, 20, 30), x -> x > 5) AS result
        """
      Then query result
        | result       |
        | [10, 20, 30] |

    Scenario: Filter where no elements match
      When query
        """
        SELECT filter(array(1, 2, 3), x -> x > 10) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: Filter with reversed comparison
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> 3 < x) AS result
        """
      Then query result
        | result |
        | [4, 5] |

  Rule: Filter with index argument

    Scenario: Filter using element index - keep elements at even indices
      When query
        """
        SELECT filter(array(10, 20, 30, 40, 50), (x, i) -> i % 2 = 0) AS result
        """
      Then query result
        | result         |
        | [10, 30, 50]   |

    Scenario: Filter using element and index - element greater than index
      When query
        """
        SELECT filter(array(0, 5, 1, 10, 2), (x, i) -> x > i) AS result
        """
      Then query result
        | result    |
        | [5, 10]   |

  Rule: Filter with complex expressions

    Scenario: Filter with AND condition
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x > 1 AND x < 5) AS result
        """
      Then query result
        | result      |
        | [2, 3, 4]   |

    Scenario: Filter with OR condition
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x = 1 OR x = 5) AS result
        """
      Then query result
        | result |
        | [1, 5] |

    Scenario: Filter with modulo function
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5, 6), x -> x % 2 = 0) AS result
        """
      Then query result
        | result      |
        | [2, 4, 6]   |

  Rule: Filter with external column references

    Scenario: Filter using external column as threshold
      When query
        """
        SELECT filter(arr, x -> x > threshold) AS result
        FROM (SELECT array(1, 2, 3, 4, 5) AS arr, 2 AS threshold)
        """
      Then query result
        | result    |
        | [3, 4, 5] |

    Scenario: Filter using multiple external columns
      When query
        """
        SELECT filter(arr, x -> x > min_val AND x < max_val) AS result
        FROM (SELECT array(1, 2, 3, 4, 5) AS arr, 1 AS min_val, 5 AS max_val)
        """
      Then query result
        | result      |
        | [2, 3, 4]   |

    Scenario: Filter with varying thresholds per row
      When query
        """
        SELECT filter(arr, x -> x > threshold) AS result
        FROM VALUES
          (array(1, 2, 3, 4, 5), 2),
          (array(10, 20, 30), 15)
        AS t(arr, threshold)
        """
      Then query result
        | result       |
        | [3, 4, 5]    |
        | [20, 30]     |

  Rule: Filter with null handling

    Scenario: Filter array containing nulls - nulls are excluded by predicate
      When query
        """
        SELECT filter(array(1, NULL, 3, NULL, 5), x -> x > 2) AS result
        """
      Then query result
        | result |
        | [3, 5] |

    Scenario: Filter with IS NOT NULL predicate
      When query
        """
        SELECT filter(array(1, NULL, 3, NULL, 5), x -> x IS NOT NULL) AS result
        """
      Then query result
        | result    |
        | [1, 3, 5] |

    Scenario: Filter with IS NULL predicate
      When query
        """
        SELECT filter(array(1, NULL, 3, NULL, 5), x -> x IS NULL) AS result
        """
      Then query result
        | result       |
        | [NULL, NULL] |

  Rule: Filter with different data types

    Scenario: Filter empty array
      # Explicitly typed so the test asserts filter behavior on an empty array,
      # not engine-specific inference of the untyped `array()` literal.
      When query
        """
        SELECT filter(CAST(array() AS ARRAY<INT>), x -> x > 0) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: Filter string array
      When query
        """
        SELECT filter(array('apple', 'banana', 'cherry'), x -> x > 'b') AS result
        """
      Then query result
        | result             |
        | [banana, cherry]   |

    Scenario: Filter string array with length condition
      When query
        """
        SELECT filter(array('a', 'bb', 'ccc', 'dddd'), x -> length(x) > 2) AS result
        """
      Then query result
        | result      |
        | [ccc, dddd] |

    Scenario: Filter with negative numbers
      When query
        """
        SELECT filter(array(-5, -2, 0, 3, 7), x -> x >= 0) AS result
        """
      Then query result
        | result   |
        | [0, 3, 7] |

    Scenario: Filter double array
      When query
        """
        SELECT filter(array(1.5, 2.7, 3.2, 4.8), x -> x > 2.5) AS result
        """
      Then query result
        | result          |
        | [2.7, 3.2, 4.8] |

  Rule: Filter with equality and other operators

    Scenario: Filter with equality
      When query
        """
        SELECT filter(array(1, 2, 3, 2, 1), x -> x = 2) AS result
        """
      Then query result
        | result |
        | [2, 2] |

    Scenario: Filter with not equal
      When query
        """
        SELECT filter(array(1, 2, 3, 2, 1), x -> x <> 2) AS result
        """
      Then query result
        | result    |
        | [1, 3, 1] |

    Scenario: Filter with NOT condition
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> NOT (x > 3)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

  Rule: Filter with operators

    Scenario: Filter with BETWEEN
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x BETWEEN 2 AND 4) AS result
        """
      Then query result
        | result    |
        | [2, 3, 4] |

  Rule: Filter with functions in predicate

    Scenario: Filter with function call in predicate
      When query
        """
        SELECT filter(array('a', 'bb', 'ccc'), x -> length(x) > 1) AS result
        """
      Then query result
        | result      |
        | [bb, ccc]   |

    Scenario: Filter with arithmetic in predicate
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x * 2 > 5) AS result
        """
      Then query result
        | result    |
        | [3, 4, 5] |

  Rule: Filter with index and external columns combined

    Scenario: Filter using both index and external column
      When query
        """
        SELECT filter(arr, (x, i) -> x > threshold AND i > 0) AS result
        FROM (SELECT array(1, 5, 2, 8, 3) AS arr, 2 AS threshold)
        """
      Then query result
        | result   |
        | [5, 8, 3] |

    Scenario: Filter with index less than value and external threshold
      When query
        """
        SELECT filter(arr, (x, i) -> x > threshold AND i < 3) AS result
        FROM (SELECT array(1, 5, 2, 8, 3) AS arr, 2 AS threshold)
        """
      Then query result
        | result |
        | [5]    |

  Rule: Filter with multiple rows (batch processing)

    Scenario: Filter multiple rows without external columns
      When query
        """
        SELECT filter(arr, x -> x > 2) AS result
        FROM VALUES
          (array(1, 2, 3)),
          (array(4, 5, 6)),
          (array(1, 1, 1))
        AS t(arr)
        """
      Then query result
        | result    |
        | [3]       |
        | [4, 5, 6] |
        | []        |

    Scenario: Filter multiple rows with different array sizes
      When query
        """
        SELECT filter(arr, x -> x % 2 = 0) AS result
        FROM VALUES
          (array(1, 2)),
          (array(1, 2, 3, 4, 5)),
          (array(7))
        AS t(arr)
        """
      Then query result
        | result |
        | [2]    |
        | [2, 4] |
        | []     |

  Rule: Filter with null array input

    Scenario: Filter a null array returns null
      When query
        """
        SELECT filter(CAST(NULL AS ARRAY<INT>), x -> x > 0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Filter null array of strings returns null
      When query
        """
        SELECT filter(CAST(NULL AS ARRAY<STRING>), x -> x IS NOT NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Filter multi-row table with null array row
      When query
        """
        SELECT filter(arr, x -> x > 0) AS result
        FROM VALUES (array(1, 2, 3)), (CAST(NULL AS ARRAY<INT>)), (array(4, 5))
        AS t(arr)
        """
      Then query result
        | result    |
        | [1, 2, 3] |
        | NULL      |
        | [4, 5]    |

    Scenario: Two-param lambda with null array row returns null
      When query
        """
        SELECT filter(arr, (x, i) -> i = 0) AS result
        FROM VALUES (array(10, 20)), (CAST(NULL AS ARRAY<INT>)), (array(30, 40, 50))
        AS t(arr)
        """
      Then query result
        | result |
        | [10]   |
        | NULL   |
        | [30]   |

  Rule: Filter with null elements - predicate returns null treated as false

    Scenario: NULL elements excluded when predicate returns NULL
      When query
        """
        SELECT filter(array(1, 2, 3, NULL), x -> x > 1) AS result
        """
      Then query result
        | result |
        | [2, 3] |

    Scenario: All-null array filtered with IS NOT NULL predicate yields empty array
      When query
        """
        SELECT filter(array(NULL, NULL), x -> x IS NOT NULL) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: Filter with boolean arrays

    Scenario: Filter boolean array keeping true values
      When query
        """
        SELECT filter(array(true, false, true, false), x -> x = true) AS result
        """
      Then query result
        | result       |
        | [true, true] |

    Scenario: Filter boolean array keeping false values
      When query
        """
        SELECT filter(array(true, false, true, false), x -> x = false) AS result
        """
      Then query result
        | result         |
        | [false, false] |

  Rule: Filter with nested arrays

    Scenario: Filter nested array by first element of inner array
      When query
        """
        SELECT filter(array(array(1, 2), array(3, 4), array(5, 6)), x -> x[0] > 2) AS result
        """
      Then query result
        | result           |
        | [[3, 4], [5, 6]] |

    Scenario: Filter nested array - all inner arrays have size greater than 1
      When query
        """
        SELECT filter(array(array(1, 2), array(3, 4), array(5, 6)), x -> size(x) > 1) AS result
        """
      Then query result
        | result                   |
        | [[1, 2], [3, 4], [5, 6]] |

  Rule: Filter with constant predicates

    Scenario: Constant true predicate keeps all elements
      When query
        """
        SELECT filter(array(1, 2, 3), x -> true) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: Constant false predicate empties the array
      When query
        """
        SELECT filter(array(1, 2, 3), x -> false) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: Constant NULL predicate empties the array
      When query
        """
        SELECT filter(array(1, 2, 3), x -> CAST(NULL AS BOOLEAN)) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: Constant true predicate keeps null array row as NULL
      When query
        """
        SELECT filter(arr, x -> true) AS result
        FROM VALUES (array(1, 2)), (CAST(NULL AS ARRAY<INT>)) AS t(arr)
        """
      Then query result
        | result |
        | [1, 2] |
        | NULL   |

  Rule: Filter with predicate using only outer columns

    Scenario: Predicate references only an external boolean column
      When query
        """
        SELECT filter(arr, x -> flag) AS result
        FROM VALUES (array(1, 2), true), (array(3, 4), false) AS t(arr, flag)
        """
      Then query result
        | result |
        | [1, 2] |
        | []     |

  Rule: Filter with struct elements

    Scenario: Filter array of structs by field access
      When query
        """
        SELECT filter(array(named_struct('a', 1), named_struct('a', 5)), x -> x.a > 2) AS result
        """
      Then query result
        | result |
        | [{5}]  |

  Rule: Nested filter calls

    Scenario: Filter applied to the result of another filter
      When query
        """
        SELECT filter(filter(array(1, 2, 3, 4), x -> x > 1), y -> y < 4) AS result
        """
      Then query result
        | result |
        | [2, 3] |

    Scenario: Filter inside the lambda body of another filter
      When query
        """
        SELECT filter(array(array(1, 2), array(3)), x -> size(filter(x, y -> y > 2)) > 0) AS result
        """
      Then query result
        | result |
        | [[3]]  |

    Scenario: Nested lambda parameter shadows the outer parameter
      When query
        """
        SELECT filter(array(array(1, 2), array(3, 4)), x -> size(filter(x, x -> x > 3)) > 0) AS result
        """
      Then query result
        | result   |
        | [[3, 4]] |

  Rule: Lambda parameter name resolution

    Scenario: Lambda parameter reference is case-insensitive
      When query
        """
        SELECT filter(array(1, 2, 3), x -> X > 1) AS result
        """
      Then query result
        | result |
        | [2, 3] |

    Scenario: Lambda parameter shadows a column with the same name
      # Both `x` references must resolve to the lambda parameter, not the
      # column `x = 2`; mixing them up would yield [3, 4, 5] instead of [].
      When query
        """
        SELECT filter(arr, x -> x > x) AS result
        FROM (SELECT array(1, 2, 3, 4, 5) AS arr, 2 AS x)
        """
      Then query result
        | result |
        | []     |

  Rule: Lambda body honors ANSI mode

    # `filter` itself has no ANSI-specific semantics (Spark's ArrayFilter in
    # higherOrderFunctions.scala has no ansiEnabled branch), but expressions
    # inside the lambda body inherit the ANSI mode of the session.

    Scenario: Division by zero inside the lambda errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT filter(array(1, 0, 2), x -> 10 / x > 4) AS result
        """
      Then query error (?i)by zero

    Scenario: Division by zero inside the lambda yields NULL predicate under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT filter(array(1, 0, 2), x -> 10 / x > 4) AS result
        """
      Then query result
        | result |
        | [1, 2] |

  Rule: Filter with an erroring predicate and null array rows

    # Regression: a NULL array row must yield NULL WITHOUT evaluating the
    # (potentially erroring) lambda over the row's elements. `clear_null_values()`
    # (default true on the HOF) clears null sublists before invoke, so a div-by-zero
    # predicate cannot fire on a null row. No non-null row has a 0, so there is no
    # legitimate error either.
    Scenario: Erroring predicate with a null array row under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT filter(arr, x -> 10 / x > 4) AS result
        FROM VALUES (array(2, 5)), (CAST(NULL AS ARRAY<INT>)), (array(1)) AS t(arr)
        """
      Then query result
        | result |
        | [2]    |
        | NULL   |
        | [1]    |

  Rule: Filter inside WHERE and ORDER BY clauses

    Scenario: Filter in a WHERE predicate keeps matching rows
      When query
        """
        SELECT arr AS result
        FROM VALUES (array(1, 2, 3)), (array(1, 1)) AS t(arr)
        WHERE size(filter(arr, x -> x > 2)) > 0
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: Filter in an ORDER BY sorts rows
      When query
        """
        SELECT arr AS result
        FROM VALUES (array(5, 6)), (array(1)), (array(9, 9, 9)) AS t(arr)
        ORDER BY size(filter(arr, x -> x > 0)) DESC
        """
      Then query result ordered
        | result    |
        | [9, 9, 9] |
        | [5, 6]    |
        | [1]       |

  Rule: Filter with constant predicate preserves null elements

    Scenario: Constant true predicate keeps null elements
      When query
        """
        SELECT filter(array(1, NULL, 3), x -> true) AS result
        """
      Then query result
        | result       |
        | [1, NULL, 3] |

    Scenario: Constant false predicate drops null elements
      When query
        """
        SELECT filter(array(1, NULL, 3), x -> false) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: Filter with additional predicate forms

    Scenario: Filter with IN predicate
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x IN (1, 3, 5)) AS result
        """
      Then query result
        | result    |
        | [1, 3, 5] |

    Scenario: Filter with CASE WHEN in the predicate
      When query
        """
        SELECT filter(array(1, 2, 3, 4), x -> CASE WHEN x % 2 = 0 THEN true ELSE false END) AS result
        """
      Then query result
        | result |
        | [2, 4] |

    Scenario: Filter with coalesce in the predicate over null elements
      When query
        """
        SELECT filter(array(1, NULL, 3), x -> coalesce(x, 0) > 1) AS result
        """
      Then query result
        | result |
        | [3]    |

  Rule: Filter with bigint arrays

    Scenario: Filter bigint array with arithmetic in the predicate
      When query
        """
        SELECT filter(array(1L, 2L, 3L, 4L), x -> x * 2 > 4) AS result
        """
      Then query result
        | result |
        | [3, 4] |

  Rule: Filter with a declared but unused index parameter

    Scenario: Two-param lambda that uses only the element
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), (x, i) -> x > 2) AS result
        """
      Then query result
        | result    |
        | [3, 4, 5] |

  Rule: Filter with decimal and date arrays

    Scenario: Filter decimal array
      When query
        """
        SELECT filter(array(1.5BD, 2.7BD, 3.2BD), x -> x > 2.0BD) AS result
        """
      Then query result
        | result     |
        | [2.7, 3.2] |

    Scenario: Filter date array
      When query
        """
        SELECT filter(array(DATE '2020-01-01', DATE '2021-06-15', DATE '2019-03-03'), x -> x > DATE '2020-01-01') AS result
        """
      Then query result
        | result         |
        | [2021-06-15]   |

  Rule: Filter with index and a nested higher-order function

    Scenario: Index parameter used inside a nested filter
      When query
        """
        SELECT filter(array(array(1, 2, 3), array(0), array(5, 6)), (x, i) -> size(filter(x, y -> y > i)) > 1) AS result
        """
      Then query result
        | result             |
        | [[1, 2, 3], [5, 6]] |

  Rule: Filter inside a join condition

    # @sail-bug: a lambda that captures a column from the OTHER join side hits
    # DataFusion's `join_proj_push_down` rule, which cannot rewrite through
    # `LambdaExpr` ("unable to unwrap lambda from filter"). Fails in all modes;
    # Spark returns the row. Likely an upstream DataFusion fix, not a Sail one.
    # If the lambda does not capture the other side, the HOF is pushed below the
    # join and works.
    @sail-bug
    Scenario: Lambda capturing a column from the other join side
      When query
        """
        SELECT b.id AS result
        FROM (SELECT array(1, 2) AS arr) a
        JOIN (SELECT 1 AS id) b
        ON size(filter(a.arr, x -> x > b.id)) > 0
        """
      Then query result
        | result |
        | 1      |

  Rule: Invalid lambda functions

    Scenario: Lambda with three parameters is rejected
      When query
        """
        SELECT filter(array(1, 2, 3), (x, i, z) -> true) AS result
        """
      Then query error .*

    Scenario: Lambda with non-boolean result is rejected
      When query
        """
        SELECT filter(array(1, 2, 3), x -> x + 1) AS result
        """
      Then query error .*

    Scenario: Lambda with duplicate parameter names is rejected
      When query
        """
        SELECT filter(array(1, 2, 3), (x, x) -> x > 1) AS result
        """
      Then query error .*

    Scenario: Lambda with case-insensitive duplicate parameter names is rejected
      When query
        """
        SELECT filter(array(1, 2, 3), (x, X) -> x > 1) AS result
        """
      Then query error .*

    Scenario: Filter over a non-array first argument is rejected
      When query
        """
        SELECT filter(42, x -> x > 0) AS result
        """
      Then query error .*

    Scenario: Filter over a map first argument is rejected
      When query
        """
        SELECT filter(map('a', 1), x -> x > 0) AS result
        """
      Then query error .*

  Rule: Non-lambda expression in place of the lambda

    @sail-bug
    Scenario: A constant true predicate keeps every element
      When query
        """
        SELECT filter(array(1, 2), true) AS result
        """
      Then query result
        | result |
        | [1, 2] |

    @sail-bug
    Scenario: A constant false predicate drops every element
      When query
        """
        SELECT filter(array(1, 2), false) AS result
        """
      Then query result
        | result |
        | []     |

    @sail-bug
    Scenario: A constant NULL predicate drops every element
      When query
        """
        SELECT filter(array(1, 2), CAST(NULL AS BOOLEAN)) AS result
        """
      Then query result
        | result |
        | []     |

    @sail-bug
    Scenario: A predicate that only references an outer column
      When query
        """
        SELECT filter(array(1, 2), v > 0) AS result FROM (SELECT 5 AS v) t
        """
      Then query result
        | result |
        | [1, 2] |

    @sail-bug
    Scenario: A constant predicate over an empty array
      When query
        """
        SELECT filter(array(), true) AS result
        """
      Then query result
        | result |
        | []     |

    @sail-bug
    Scenario: A constant predicate over a NULL array
      When query
        """
        SELECT filter(CAST(NULL AS ARRAY<INT>), true) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: A constant predicate over an array column resolves per row
      When query
        """
        SELECT filter(c, true) AS result
        FROM VALUES (array(1, 2)), (array()), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query result ordered
        | result |
        | [1, 2] |
        | []     |
        | NULL   |

    @sail-bug
    Scenario: A non-boolean constant is still a type error
      When query
        """
        SELECT filter(array(1, 2), 1) AS result
        """
      Then query error The second parameter requires the "BOOLEAN" type
