@transform
Feature: transform higher-order function

  Rule: Basic 1-param transform — integer arithmetic

    Scenario: Transform integers by multiplying by 2
      When query
        """
        SELECT transform(array(1, 2, 3), x -> x * 2) AS result
        """
      Then query result
        | result    |
        | [2, 4, 6] |

    Scenario: Transform integers by adding a constant
      When query
        """
        SELECT transform(array(10, 20, 30), x -> x + 100) AS result
        """
      Then query result
        | result            |
        | [110, 120, 130]   |

    Scenario: Transform integers by negating
      When query
        """
        SELECT transform(array(1, 2, 3), x -> -x) AS result
        """
      Then query result
        | result       |
        | [-1, -2, -3] |

    Scenario: Transform integers with modulo
      When query
        """
        SELECT transform(array(1, 2, 3, 4, 5), x -> x % 3) AS result
        """
      Then query result
        | result         |
        | [1, 2, 0, 1, 2] |

    Scenario: Transform integers multiplied by zero
      When query
        """
        SELECT transform(array(5, 10, 15), x -> x * 0) AS result
        """
      Then query result
        | result      |
        | [0, 0, 0]   |

    Scenario: Transform single-element array
      When query
        """
        SELECT transform(array(42), x -> x + 1) AS result
        """
      Then query result
        | result |
        | [43]   |

  Rule: Basic 2-param transform — element and index

    Scenario: Transform with index — add index to element
      When query
        """
        SELECT transform(array(10, 20, 30), (x, i) -> x + i) AS result
        """
      Then query result
        | result       |
        | [10, 21, 32] |

    Scenario: Transform with index — multiply element by index plus one
      When query
        """
        SELECT transform(array(10, 20, 30), (x, i) -> x * (i + 1)) AS result
        """
      Then query result
        | result       |
        | [10, 40, 90] |

    Scenario: Transform with index — return index only (0-based)
      When query
        """
        SELECT transform(array(100, 200, 300), (x, i) -> i) AS result
        """
      Then query result
        | result    |
        | [0, 1, 2] |

    Scenario: Transform with index from sequence — multiply element by index
      When query
        """
        SELECT transform(sequence(1, 5), (x, i) -> x * i) AS result
        """
      Then query result
        | result              |
        | [0, 2, 6, 12, 20]  |

    Scenario: Transform with index from sequence — return index only
      When query
        """
        SELECT transform(sequence(1, 3), (x, i) -> i) AS result
        """
      Then query result
        | result    |
        | [0, 1, 2] |

  Rule: Type coercion — different output types

    Scenario: Transform integers to strings via cast
      When query
        """
        SELECT transform(array(1, 2, 3), x -> cast(x as string)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: Transform integers to bigint
      When query
        """
        SELECT transform(array(1, 2, 3), x -> cast(x as bigint)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: Transform integers to double
      When query
        """
        SELECT transform(array(1, 2, 3), x -> cast(x as double)) AS result
        """
      Then query result
        | result          |
        | [1.0, 2.0, 3.0] |

    Scenario: Transform integers to booleans using comparison
      When query
        """
        SELECT transform(array(1, 2, 3, 4), x -> x > 2) AS result
        """
      Then query result
        | result                     |
        | [false, false, true, true] |

    Scenario: Transform booleans by negation
      When query
        """
        SELECT transform(array(true, false, true), x -> NOT x) AS result
        """
      Then query result
        | result              |
        | [false, true, false] |

    Scenario: Transform booleans to integers
      When query
        """
        SELECT transform(array(true, false, true), x -> cast(x as int)) AS result
        """
      Then query result
        | result    |
        | [1, 0, 1] |

  Rule: String transformations

    Scenario: Transform string array to uppercase
      When query
        """
        SELECT transform(arr, x -> upper(x)) AS result
        FROM VALUES (array("hello", "world")) AS t(arr)
        """
      Then query result
        | result          |
        | [HELLO, WORLD]  |

    Scenario: Transform string array to length of each string
      When query
        """
        SELECT transform(arr, x -> length(x)) AS result
        FROM VALUES (array("a", "bb", "ccc")) AS t(arr)
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: Transform string array with index — concat element and index
      When query
        """
        SELECT transform(arr, (x, i) -> concat(x, cast(i as string))) AS result
        FROM VALUES (array("a", "b", "c")) AS t(arr)
        """
      Then query result
        | result          |
        | [a0, b1, c2]    |

    Scenario: Transform string array with index — concat longer strings and index
      When query
        """
        SELECT transform(arr, (x, i) -> concat(x, cast(i as string))) AS result
        FROM VALUES (array("apple", "banana")) AS t(arr)
        """
      Then query result
        | result            |
        | [apple0, banana1] |

    Scenario: Transform empty string array
      When query
        """
        SELECT transform(arr, x -> upper(x)) AS result
        FROM VALUES (array("")) AS t(arr)
        """
      Then query result
        | result |
        | []     |

  Rule: Null handling

    Scenario: Transform array containing null — null propagates through arithmetic
      When query
        """
        SELECT transform(array(1, NULL, 3), x -> x * 2) AS result
        """
      Then query result
        | result       |
        | [2, NULL, 6] |

    Scenario: Transform array with null — null plus constant propagates
      When query
        """
        SELECT transform(array(1, NULL, 3), x -> x + 10) AS result
        """
      Then query result
        | result          |
        | [11, NULL, 13]  |

    Scenario: Transform null elements with coalesce — substitute null with default
      When query
        """
        SELECT transform(array(1, NULL, 3), x -> coalesce(x, -1)) AS result
        """
      Then query result
        | result     |
        | [1, -1, 3] |

    Scenario: Transform all-null array — all elements remain null
      When query
        """
        SELECT transform(array(NULL, NULL), x -> x * 2) AS result
        """
      Then query result
        | result       |
        | [NULL, NULL] |

    Scenario: Transform single-null element array
      When query
        """
        SELECT transform(array(NULL), x -> x + 1) AS result
        """
      Then query result
        | result |
        | [NULL] |

    Scenario: Transform null array itself returns null
      When query
        """
        SELECT transform(CAST(NULL AS ARRAY<INT>), x -> x * 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Transform typed null array of strings returns null
      When query
        """
        SELECT transform(CAST(NULL AS ARRAY<STRING>), x -> upper(x)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Empty array

    Scenario: Transform empty integer array returns empty array
      When query
        """
        SELECT transform(array(), x -> x * 2) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: Outer column references in lambda

    Scenario: Transform using outer column as addend
      When query
        """
        SELECT transform(array(1, 2, 3), x -> x + id) AS result
        FROM VALUES (10) AS t(id)
        """
      Then query result
        | result         |
        | [11, 12, 13]   |

    Scenario: Transform with varying outer column per row
      When query
        """
        SELECT transform(arr, x -> x + offset) AS result
        FROM VALUES (array(1, 2, 3), 10), (array(4, 5), 20) AS t(arr, offset)
        """
      Then query result
        | result       |
        | [11, 12, 13] |
        | [24, 25]     |

  Rule: Multi-row with mixed null arrays

    Scenario: Transform multiple rows including null array row
      When query
        """
        SELECT transform(a, x -> x + 1) AS result
        FROM VALUES (array(1, 2, 3)), (array(10, 20)), (CAST(NULL AS ARRAY<INT>)) AS t(a)
        """
      Then query result
        | result     |
        | [2, 3, 4]  |
        | [11, 21]   |
        | NULL       |

    Scenario: Two-param transform across multiple rows with null array row
      When query
        """
        SELECT transform(arr, (x, i) -> x + i) AS result
        FROM VALUES (array(10, 20)), (CAST(NULL AS ARRAY<INT>)), (array(30, 40, 50)) AS t(arr)
        """
      Then query result
        | result        |
        | [10, 21]      |
        | NULL          |
        | [30, 41, 52]  |

  Rule: Arrays of arrays

    Scenario: Transform array of arrays — return inner array size
      When query
        """
        SELECT transform(array(array(1, 2), array(3, 4)), x -> size(x)) AS result
        """
      Then query result
        | result |
        | [2, 2] |

    Scenario: Transform array of arrays — inner size plus constant
      When query
        """
        SELECT transform(array(array(1, 2), array(3, 4)), x -> size(x) + 10) AS result
        """
      Then query result
        | result    |
        | [12, 12]  |

    Scenario: Nested transform — transform within transform
      When query
        """
        SELECT transform(array(1, 2, 3), x -> transform(array(x, x + 1), y -> y * 2)) AS result
        """
      Then query result
        | result                     |
        | [[2, 4], [4, 6], [6, 8]]   |

  Rule: Struct output from lambda

    Scenario: Transform to struct output
      When query
        """
        SELECT transform(array(1, 2, 3), x -> struct(x, x * 2)) AS result
        """
      Then query result
        | result                     |
        | [{1, 2}, {2, 4}, {3, 6}]   |

    Scenario: Transform string array with index into struct of index and value
      When query
        """
        SELECT transform(arr, (v, i) -> struct(i, v)) AS result
        FROM VALUES (array("x", "y", "z")) AS t(arr)
        """
      Then query result
        | result                     |
        | [{0, x}, {1, y}, {2, z}]   |

  Rule: Chaining with other higher-order functions

    Scenario: Transform then filter — double elements then keep those greater than 4
      When query
        """
        SELECT filter(transform(array(1, 2, 3, 4), x -> x * 2), x -> x > 4) AS result
        """
      Then query result
        | result |
        | [6, 8] |

  Rule: Large boundary values

    Scenario: Transform INT_MAX and INT_MIN with identity
      When query
        """
        SELECT transform(array(2147483647, -2147483648), x -> x + 0) AS result
        """
      Then query result
        | result                         |
        | [2147483647, -2147483648]       |

  Rule: Lambda body honors ANSI mode

    # `transform` itself has no ANSI-specific semantics (Spark's ArrayTransform in
    # higherOrderFunctions.scala has no ansiEnabled branch), but arithmetic inside
    # the lambda body inherits the ANSI mode of the session.

    # @sail-bug: NOT transform-specific. Sail's integer `+` does not honor
    # `spark.sql.ansi.enabled` overflow checking — plain `SELECT 2147483647 + 1`
    # also wraps to -2147483648 under ANSI on instead of erroring (CAST does
    # error, arithmetic does not). The lambda body merely inherits this
    # pre-existing general gap. Spark errors with ARITHMETIC_OVERFLOW.
    @sail-bug
    Scenario: Arithmetic overflow inside the lambda errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT transform(array(2147483647), x -> x + 1) AS result
        """
      Then query error (?i)overflow

    Scenario: Arithmetic overflow inside the lambda wraps under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT transform(array(2147483647), x -> x + 1) AS result
        """
      Then query result
        | result        |
        | [-2147483648] |

  Rule: Invalid arguments are rejected

    Scenario: Transform over a non-array first argument is rejected
      When query
        """
        SELECT transform(42, x -> x + 1) AS result
        """
      Then query error .*

    Scenario: Transform over a map first argument is rejected
      When query
        """
        SELECT transform(map("a", 1), x -> x) AS result
        """
      Then query error .*

    Scenario: Lambda with three parameters is rejected
      When query
        """
        SELECT transform(array(1, 2, 3), (x, i, z) -> x) AS result
        """
      Then query error .*

  Rule: Lambda body that ignores its parameters

    Scenario: Constant lambda body broadcasts to every element
      When query
        """
        SELECT transform(array(1, 2, 3), x -> 42) AS result
        """
      Then query result
        | result       |
        | [42, 42, 42] |

  Rule: Index-only lambda used inside an expression (index-first rewrite)

    # The element parameter is unused while the index is used in an expression.
    # This exercises the planner's index-first rewrite — the lambda is rewritten
    # to `i -> ...` over the index-first UDF instance — beyond the bare
    # `(x, i) -> i` case covered above.

    Scenario: Index multiplied by a constant
      When query
        """
        SELECT transform(array(10, 20, 30), (x, i) -> i * 10) AS result
        """
      Then query result
        | result      |
        | [0, 10, 20] |

    Scenario: Index plus a constant
      When query
        """
        SELECT transform(array(10, 20, 30), (x, i) -> i + 100) AS result
        """
      Then query result
        | result          |
        | [100, 101, 102] |

  Rule: Two-parameter lambda over an empty array

    Scenario: Empty array with element-and-index lambda returns empty array
      When query
        """
        SELECT transform(array(), (x, i) -> x + i) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: Lambda returning a complex type

    Scenario: Lambda returns an array — result is an array of arrays
      When query
        """
        SELECT transform(array(1, 2, 3), x -> array(x, x * 2)) AS result
        """
      Then query result
        | result                   |
        | [[1, 2], [2, 4], [3, 6]] |

  Rule: Lambda body produces conditional nulls

    Scenario: IF expression yields NULL for some elements
      When query
        """
        SELECT transform(array(1, 2, 3, 4), x -> if(x > 2, x, NULL)) AS result
        """
      Then query result
        | result             |
        | [NULL, NULL, 3, 4] |

  Rule: Element referenced multiple times

    Scenario: Square each element
      When query
        """
        SELECT transform(array(1, 2, 3, 4), x -> x * x) AS result
        """
      Then query result
        | result        |
        | [1, 4, 9, 16] |

  Rule: Nested transform capturing the outer index

    Scenario: Inner lambda captures the outer 0-based index
      When query
        """
        SELECT transform(array(1, 2), (x, i) -> transform(array(10, 20), (y, j) -> i * 100 + y + j)) AS result
        """
      Then query result
        | result                 |
        | [[10, 21], [110, 121]] |

  Rule: Transform over an array of structs

    Scenario: Project a struct field from each element
      When query
        """
        SELECT transform(array(named_struct('k', 1, 'v', 10), named_struct('k', 2, 'v', 20)), x -> x.v) AS result
        """
      Then query result
        | result   |
        | [10, 20] |

  Rule: Chaining — filter then transform

    Scenario: Keep even elements then multiply by ten
      When query
        """
        SELECT transform(filter(array(1, 2, 3, 4, 5), x -> x % 2 = 0), x -> x * 10) AS result
        """
      Then query result
        | result   |
        | [20, 40] |

  Rule: Decimal element arithmetic

    # `decimal(2,1) * int` widens to `decimal(4,1)` in Spark, so the result
    # renders with the `.0` suffix. If Sail diverges here it is the pre-existing
    # decimal×int coercion gap, not a `transform` issue.

    Scenario: Multiply decimal elements by an integer
      When query
        """
        SELECT transform(array(1.5, 2.5, 3.5), x -> x * 2) AS result
        """
      Then query result
        | result          |
        | [3.0, 5.0, 7.0] |
