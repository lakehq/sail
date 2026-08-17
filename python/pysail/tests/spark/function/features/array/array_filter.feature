@function(lambda)
Feature: array filter with lambda

  Rule: Filter array elements using lambda predicates

    Scenario Outline: Comparison predicate: <case>
      When query
        """
        SELECT filter(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                             | arr                  | pred   | result       |
        | Filter integers greater than a value             | array(1, 2, 3, 4, 5) | x > 2  | [3, 4, 5]    |
        | Filter integers less than a value                | array(1, 2, 3, 4, 5) | x < 3  | [1, 2]       |
        | Filter integers greater than or equal to a value | array(1, 2, 3, 4, 5) | x >= 3 | [3, 4, 5]    |
        | Filter integers less than or equal to a value    | array(1, 2, 3, 4, 5) | x <= 2 | [1, 2]       |
        | Filter where all elements match                  | array(10, 20, 30)    | x > 5  | [10, 20, 30] |
        | Filter where no elements match                   | array(1, 2, 3)       | x > 10 | []           |
        | Filter with reversed comparison                  | array(1, 2, 3, 4, 5) | 3 < x  | [4, 5]       |

  Rule: Filter with index argument

    Scenario Outline: Index argument: <case>
      When query
        """
        SELECT filter(<arr>, (x, i) -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                        | arr                       | pred      | result       |
        | Filter using element index - keep elements at even indices  | array(10, 20, 30, 40, 50) | i % 2 = 0 | [10, 30, 50] |
        | Filter using element and index - element greater than index | array(0, 5, 1, 10, 2)     | x > i     | [5, 10]      |

  Rule: Filter with complex expressions

    Scenario Outline: Complex expression: <case>
      When query
        """
        SELECT filter(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | arr                     | pred            | result    |
        | Filter with AND condition   | array(1, 2, 3, 4, 5)    | x > 1 AND x < 5 | [2, 3, 4] |
        | Filter with OR condition    | array(1, 2, 3, 4, 5)    | x = 1 OR x = 5  | [1, 5]    |
        | Filter with modulo function | array(1, 2, 3, 4, 5, 6) | x % 2 = 0       | [2, 4, 6] |

  Rule: Filter with external column references

    Scenario Outline: External column: <case>
      When query
        """
        SELECT filter(arr, x -> <pred>) AS result
        FROM (SELECT array(1, 2, 3, 4, 5) AS arr, <cols>)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | pred                        | cols                       | result    |
        | Filter using external column as threshold | x > threshold               | 2 AS threshold             | [3, 4, 5] |
        | Filter using multiple external columns    | x > min_val AND x < max_val | 1 AS min_val, 5 AS max_val | [2, 3, 4] |

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
        | result    |
        | [3, 4, 5] |
        | [20, 30]  |

  Rule: Filter with null handling

    Scenario Outline: Null handling: <case>
      When query
        """
        SELECT filter(array(1, NULL, 3, NULL, 5), x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                            | pred          | result       |
        | Filter array containing nulls - nulls are excluded by predicate | x > 2         | [3, 5]       |
        | Filter with IS NOT NULL predicate                               | x IS NOT NULL | [1, 3, 5]    |
        | Filter with IS NULL predicate                                   | x IS NULL     | [NULL, NULL] |

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

    Scenario Outline: Data type: <case>
      When query
        """
        SELECT filter(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | arr                                | pred          | result           |
        | Filter string array                       | array('apple', 'banana', 'cherry') | x > 'b'       | [banana, cherry] |
        | Filter string array with length condition | array('a', 'bb', 'ccc', 'dddd')    | length(x) > 2 | [ccc, dddd]      |
        | Filter with negative numbers              | array(-5, -2, 0, 3, 7)             | x >= 0        | [0, 3, 7]        |
        | Filter double array                       | array(1.5, 2.7, 3.2, 4.8)          | x > 2.5       | [2.7, 3.2, 4.8]  |

  Rule: Filter with equality and other operators

    Scenario Outline: Equality operator: <case>
      When query
        """
        SELECT filter(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                      | arr                  | pred        | result    |
        | Filter with equality      | array(1, 2, 3, 2, 1) | x = 2       | [2, 2]    |
        | Filter with not equal     | array(1, 2, 3, 2, 1) | x <> 2      | [1, 3, 1] |
        | Filter with NOT condition | array(1, 2, 3, 4, 5) | NOT (x > 3) | [1, 2, 3] |

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

    Scenario Outline: Function in predicate: <case>
      When query
        """
        SELECT filter(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                   | arr                     | pred          | result    |
        | Filter with function call in predicate | array('a', 'bb', 'ccc') | length(x) > 1 | [bb, ccc] |
        | Filter with arithmetic in predicate    | array(1, 2, 3, 4, 5)    | x * 2 > 5     | [3, 4, 5] |

  Rule: Filter with index and external columns combined

    Scenario Outline: Index and external column: <case>
      When query
        """
        SELECT filter(arr, (x, i) -> <pred>) AS result
        FROM (SELECT array(1, 5, 2, 8, 3) AS arr, 2 AS threshold)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                     | pred                    | result    |
        | Filter using both index and external column              | x > threshold AND i > 0 | [5, 8, 3] |
        | Filter with index less than value and external threshold | x > threshold AND i < 3 | [5]       |

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

    Scenario Outline: Null array literal: <case>
      When query
        """
        SELECT filter(<arr>, <lambda>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                      | arr                         | lambda             |
        | Filter a null array returns null          | CAST(NULL AS ARRAY<INT>)    | x -> x > 0         |
        | Filter null array of strings returns null | CAST(NULL AS ARRAY<STRING>) | x -> x IS NOT NULL |

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

    Scenario Outline: Null element: <case>
      When query
        """
        SELECT filter(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                  | arr                  | pred          | result |
        | NULL elements excluded when predicate returns NULL                    | array(1, 2, 3, NULL) | x > 1         | [2, 3] |
        | All-null array filtered with IS NOT NULL predicate yields empty array | array(NULL, NULL)    | x IS NOT NULL | []     |

  Rule: Filter with boolean arrays

    Scenario Outline: Boolean array: <case>
      When query
        """
        SELECT filter(array(true, false, true, false), x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | pred      | result         |
        | Filter boolean array keeping true values  | x = true  | [true, true]   |
        | Filter boolean array keeping false values | x = false | [false, false] |

  Rule: Filter with nested arrays

    Scenario Outline: Nested array: <case>
      When query
        """
        SELECT filter(array(array(1, 2), array(3, 4), array(5, 6)), x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                            | pred        | result                   |
        | Filter nested array by first element of inner array             | x[0] > 2    | [[3, 4], [5, 6]]         |
        | Filter nested array - all inner arrays have size greater than 1 | size(x) > 1 | [[1, 2], [3, 4], [5, 6]] |

  Rule: Filter with constant predicates

    Scenario Outline: Constant predicate: <case>
      When query
        """
        SELECT filter(array(1, 2, 3), x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                       | pred                  | result    |
        | Constant true predicate keeps all elements | true                  | [1, 2, 3] |
        | Constant false predicate empties the array | false                 | []        |
        | Constant NULL predicate empties the array  | CAST(NULL AS BOOLEAN) | []        |

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

    Scenario Outline: Constant predicate over null elements: <case>
      When query
        """
        SELECT filter(array(1, NULL, 3), x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                         | pred  | result       |
        | Constant true predicate keeps null elements  | true  | [1, NULL, 3] |
        | Constant false predicate drops null elements | false | []           |

  Rule: Filter with additional predicate forms

    Scenario Outline: Additional predicate form: <case>
      When query
        """
        SELECT filter(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                     | arr                  | pred                                         | result    |
        | Filter with IN predicate                                 | array(1, 2, 3, 4, 5) | x IN (1, 3, 5)                               | [1, 3, 5] |
        | Filter with CASE WHEN in the predicate                   | array(1, 2, 3, 4)    | CASE WHEN x % 2 = 0 THEN true ELSE false END | [2, 4]    |
        | Filter with coalesce in the predicate over null elements | array(1, NULL, 3)    | coalesce(x, 0) > 1                           | [3]       |

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

    Scenario Outline: Decimal and date array: <case>
      When query
        """
        SELECT filter(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                 | arr                                                            | pred                  | result       |
        | Filter decimal array | array(1.5BD, 2.7BD, 3.2BD)                                     | x > 2.0BD             | [2.7, 3.2]   |
        | Filter date array    | array(DATE '2020-01-01', DATE '2021-06-15', DATE '2019-03-03') | x > DATE '2020-01-01' | [2021-06-15] |

  Rule: Filter with index and a nested higher-order function

    Scenario: Index parameter used inside a nested filter
      When query
        """
        SELECT filter(array(array(1, 2, 3), array(0), array(5, 6)), (x, i) -> size(filter(x, y -> y > i)) > 1) AS result
        """
      Then query result
        | result              |
        | [[1, 2, 3], [5, 6]] |

  Rule: Filter inside a join condition

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

    Scenario Outline: Invalid lambda: <case>
      When query
        """
        SELECT filter(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                                                               | args                              |
        | Lambda with three parameters is rejected                           | array(1, 2, 3), (x, i, z) -> true |
        | Lambda with non-boolean result is rejected                         | array(1, 2, 3), x -> x + 1        |
        | Lambda with duplicate parameter names is rejected                  | array(1, 2, 3), (x, x) -> x > 1   |
        | Lambda with case-insensitive duplicate parameter names is rejected | array(1, 2, 3), (x, X) -> x > 1   |
        | Filter over a non-array first argument is rejected                 | 42, x -> x > 0                    |
        | Filter over a map first argument is rejected                       | map('a', 1), x -> x > 0           |
