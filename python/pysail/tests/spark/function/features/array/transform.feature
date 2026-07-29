@lambda_hof
@transform
Feature: transform higher-order function

  Rule: Basic 1-param transform — integer arithmetic

    Scenario Outline: 1-param: <case>
      When query
        """
        SELECT transform(<arr>, x -> <expr>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                    | arr                  | expr    | result          |
        | Transform integers by multiplying by 2  | array(1, 2, 3)       | x * 2   | [2, 4, 6]       |
        | Transform integers by adding a constant | array(10, 20, 30)    | x + 100 | [110, 120, 130] |
        | Transform integers by negating          | array(1, 2, 3)       | -x      | [-1, -2, -3]    |
        | Transform integers with modulo          | array(1, 2, 3, 4, 5) | x % 3   | [1, 2, 0, 1, 2] |
        | Transform integers multiplied by zero   | array(5, 10, 15)     | x * 0   | [0, 0, 0]       |
        | Transform single-element array          | array(42)            | x + 1   | [43]            |

  Rule: Basic 2-param transform — element and index

    Scenario Outline: 2-param: <case>
      When query
        """
        SELECT transform(<arr>, (x, i) -> <expr>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                           | arr                  | expr        | result            |
        | Transform with index — add index to element                    | array(10, 20, 30)    | x + i       | [10, 21, 32]      |
        | Transform with index — multiply element by index plus one      | array(10, 20, 30)    | x * (i + 1) | [10, 40, 90]      |
        | Transform with index — return index only (0-based)             | array(100, 200, 300) | i           | [0, 1, 2]         |
        | Transform with index from sequence — multiply element by index | sequence(1, 5)       | x * i       | [0, 2, 6, 12, 20] |
        | Transform with index from sequence — return index only         | sequence(1, 3)       | i           | [0, 1, 2]         |

  Rule: Type coercion — different output types

    Scenario Outline: Type coercion: <case>
      When query
        """
        SELECT transform(<arr>, x -> <expr>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                            | arr                      | expr                           | result                     |
        | Transform integers to strings via cast          | array(1, 2, 3)           | concat(cast(x as string), "s") | [1s, 2s, 3s]               |
        | Transform integers to bigint                    | array(1, 2, 3)           | cast(x as bigint)              | [1, 2, 3]                  |
        | Transform integers to double                    | array(1, 2, 3)           | cast(x as double)              | [1.0, 2.0, 3.0]            |
        | Transform integers to booleans using comparison | array(1, 2, 3, 4)        | x > 2                          | [false, false, true, true] |
        | Transform booleans by negation                  | array(true, false, true) | NOT x                          | [false, true, false]       |
        | Transform booleans to integers                  | array(true, false, true) | cast(x as int)                 | [1, 0, 1]                  |

  Rule: String transformations

    Scenario Outline: String: <case>
      When query
        """
        SELECT transform(arr, <lambda>) AS result
        FROM VALUES (<values>) AS t(arr)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                | lambda                                 | values                   | result            |
        | Transform string array to uppercase                                 | x -> upper(x)                          | array("hello", "world")  | [HELLO, WORLD]    |
        | Transform string array to length of each string                     | x -> length(x)                         | array("a", "bb", "ccc")  | [1, 2, 3]         |
        | Transform string array with index — concat element and index        | (x, i) -> concat(x, cast(i as string)) | array("a", "b", "c")     | [a0, b1, c2]      |
        | Transform string array with index — concat longer strings and index | (x, i) -> concat(x, cast(i as string)) | array("apple", "banana") | [apple0, banana1] |

    Scenario: Transform array containing an empty string preserves length
      When query
        """
        SELECT size(transform(arr, x -> upper(x))) AS result
        FROM VALUES (array("")) AS t(arr)
        """
      Then query result
        | result |
        | 1      |

  Rule: Null handling

    Scenario Outline: Null handling: <case>
      When query
        """
        SELECT transform(<arr>, <lambda>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                 | arr                         | lambda               | result         |
        | Transform array containing null — null propagates through arithmetic | array(1, NULL, 3)           | x -> x * 2           | [2, NULL, 6]   |
        | Transform array with null — null plus constant propagates            | array(1, NULL, 3)           | x -> x + 10          | [11, NULL, 13] |
        | Transform null elements with coalesce — substitute null with default | array(1, NULL, 3)           | x -> coalesce(x, -1) | [1, -1, 3]     |
        | Transform all-null array — all elements remain null                  | array(NULL, NULL)           | x -> x * 2           | [NULL, NULL]   |
        | Transform single-null element array                                  | array(NULL)                 | x -> x + 1           | [NULL]         |
        | Transform null array itself returns null                             | CAST(NULL AS ARRAY<INT>)    | x -> x * 2           | NULL           |
        | Transform typed null array of strings returns null                   | CAST(NULL AS ARRAY<STRING>) | x -> upper(x)        | NULL           |

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
        | result       |
        | [11, 12, 13] |

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
        | result    |
        | [2, 3, 4] |
        | [11, 21]  |
        | NULL      |

    Scenario: Two-param transform across multiple rows with null array row
      When query
        """
        SELECT transform(arr, (x, i) -> x + i) AS result
        FROM VALUES (array(10, 20)), (CAST(NULL AS ARRAY<INT>)), (array(30, 40, 50)) AS t(arr)
        """
      Then query result
        | result       |
        | [10, 21]     |
        | NULL         |
        | [30, 41, 52] |

  Rule: Arrays of arrays

    Scenario Outline: Arrays of arrays: <case>
      When query
        """
        SELECT transform(<arr>, <lambda>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                 | arr                             | lambda                                      | result                   |
        | Transform array of arrays — return inner array size  | array(array(1, 2), array(3, 4)) | x -> size(x)                                | [2, 2]                   |
        | Transform array of arrays — inner size plus constant | array(array(1, 2), array(3, 4)) | x -> size(x) + 10                           | [12, 12]                 |
        | Nested transform — transform within transform        | array(1, 2, 3)                  | x -> transform(array(x, x + 1), y -> y * 2) | [[2, 4], [4, 6], [6, 8]] |

  Rule: Struct output from lambda

    Scenario: Transform to struct output
      When query
        """
        SELECT transform(array(1, 2, 3), x -> struct(x, x * 2)) AS result
        """
      Then query result
        | result                   |
        | [{1, 2}, {2, 4}, {3, 6}] |

    Scenario: Transform string array with index into struct of index and value
      When query
        """
        SELECT transform(arr, (v, i) -> struct(i, v)) AS result
        FROM VALUES (array("x", "y", "z")) AS t(arr)
        """
      Then query result
        | result                   |
        | [{0, x}, {1, y}, {2, z}] |

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
        | result                    |
        | [2147483647, -2147483648] |

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

    Scenario Outline: Invalid argument: <case>
      When query
        """
        SELECT transform(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                                                  | args                           |
        | Transform over a non-array first argument is rejected | 42, x -> x + 1                 |
        | Transform over a map first argument is rejected       | map("a", 1), x -> x            |
        | Lambda with three parameters is rejected              | array(1, 2, 3), (x, i, z) -> x |

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

    Scenario Outline: Index-first: <case>
      When query
        """
        SELECT transform(array(10, 20, 30), (x, i) -> <expr>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                           | expr    | result          |
        | Index multiplied by a constant | i * 10  | [0, 10, 20]     |
        | Index plus a constant          | i + 100 | [100, 101, 102] |

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
