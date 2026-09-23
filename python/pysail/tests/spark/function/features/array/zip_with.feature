Feature: zip_with higher-order function

  Rule: Basic behavior with equal-length arrays

    @sail-bug
    Scenario: Add corresponding integer elements
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> x + y) AS result
        """
      Then query result
        | result    |
        | [5, 7, 9] |

    @sail-bug
    Scenario: Subtract corresponding elements
      When query
        """
        SELECT zip_with(array(10, 20, 30), array(1, 2, 3), (x, y) -> x - y) AS result
        """
      Then query result
        | result      |
        | [9, 18, 27] |

    @sail-bug
    Scenario: Multiply corresponding elements
      When query
        """
        SELECT zip_with(array(2, 3, 4), array(5, 6, 7), (x, y) -> x * y) AS result
        """
      Then query result
        | result       |
        | [10, 18, 28] |

    @sail-bug
    Scenario: Single-element arrays
      When query
        """
        SELECT zip_with(array(42), array(58), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | [100]  |

    @sail-bug
    Scenario: Concatenate corresponding string elements
      When query
        """
        SELECT zip_with(array('a', 'b', 'c'), array('x', 'y', 'z'), (x, y) -> concat(x, y)) AS result
        """
      Then query result
        | result         |
        | [ax, by, cz]   |

    @sail-bug
    Scenario: Concatenate multi-character strings
      When query
        """
        SELECT zip_with(array('hello', 'world'), array(' foo', ' bar'), (x, y) -> concat(x, y)) AS result
        """
      Then query result
        | result               |
        | [hello foo, world bar] |

    @sail-bug
    Scenario: Boolean comparison lambda
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(3, 2, 1), (x, y) -> x > y) AS result
        """
      Then query result
        | result               |
        | [false, false, true] |

    @sail-bug
    Scenario: Conditional if in lambda
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(1, 3, 2), (x, y) -> if(x > y, 'left', 'right')) AS result
        """
      Then query result
        | result               |
        | [right, right, left] |

    @sail-bug
    Scenario: Lambda produces struct output
      When query
        """
        SELECT zip_with(array(1, 2), array(3, 4), (x, y) -> named_struct('sum', x + y, 'prod', x * y)) AS result
        """
      Then query result
        | result             |
        | [{4, 3}, {6, 8}]   |

    @sail-bug
    Scenario: Lambda uses only the first parameter
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> x * 2) AS result
        """
      Then query result
        | result    |
        | [2, 4, 6] |

    @sail-bug
    Scenario: Lambda uses only the second parameter
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> y + 10) AS result
        """
      Then query result
        | result         |
        | [14, 15, 16]   |

    @sail-bug
    Scenario: Lambda returns a constant
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> 99) AS result
        """
      Then query result
        | result         |
        | [99, 99, 99]   |

    @sail-bug
    Scenario: Lambda returns null always
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> null) AS result
        """
      Then query result
        | result               |
        | [NULL, NULL, NULL]   |

    @sail-bug
    Scenario: Lambda returns first argument unchanged
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> x) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

  Rule: Unequal-length arrays are padded with NULL

    @sail-bug
    Scenario: First array longer than second - missing second elements become NULL
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5), (x, y) -> x + y) AS result
        """
      Then query result
        | result         |
        | [5, 7, NULL]   |

    @sail-bug
    Scenario: Second array longer than first - missing first elements become NULL
      When query
        """
        SELECT zip_with(array(1, 2), array(4, 5, 6), (x, y) -> x + y) AS result
        """
      Then query result
        | result         |
        | [5, 7, NULL]   |

    @sail-bug
    Scenario: Large length difference - padded positions pass NULL to lambda
      When query
        """
        SELECT zip_with(array(1), array(1, 2, 3, 4, 5), (x, y) -> x + y) AS result
        """
      Then query result
        | result                      |
        | [2, NULL, NULL, NULL, NULL] |

    @sail-bug
    Scenario: Padded null position is typed NULL available to lambda
      When query
        """
        SELECT zip_with(array(1, 2), array(10, 20, 30), (x, y) -> nvl(cast(x AS string), 'was_null')) AS result
        """
      Then query result
        | result                  |
        | [1, 2, was_null]        |

    @sail-bug
    Scenario: Padded null position in first array is accessible in lambda
      When query
        """
        SELECT zip_with(array(10, 20, 30), array(1, 2), (x, y) -> nvl(cast(y AS string), 'was_null')) AS result
        """
      Then query result
        | result                  |
        | [1, 2, was_null]        |

  Rule: Empty arrays

    @sail-bug
    Scenario: Both arrays empty - returns empty array
      When query
        """
        SELECT zip_with(array(), array(), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | []     |

    @sail-bug
    Scenario: Both typed empty arrays - returns empty array
      When query
        """
        SELECT zip_with(CAST(array() AS ARRAY<INT>), CAST(array() AS ARRAY<INT>), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | []     |

    @sail-bug
    Scenario: Empty first array, non-empty second - all second elements become paired with NULL
      When query
        """
        SELECT zip_with(CAST(array() AS ARRAY<INT>), array(1, 2, 3), (x, y) -> x + y) AS result
        """
      Then query result
        | result               |
        | [NULL, NULL, NULL]   |

    @sail-bug
    Scenario: Non-empty first array, empty second - all first elements become paired with NULL
      When query
        """
        SELECT zip_with(array(1, 2, 3), CAST(array() AS ARRAY<INT>), (x, y) -> x + y) AS result
        """
      Then query result
        | result               |
        | [NULL, NULL, NULL]   |

  Rule: NULL array inputs return NULL

    @sail-bug
    Scenario: NULL first array returns NULL
      When query
        """
        SELECT zip_with(CAST(NULL AS ARRAY<INT>), array(1, 2, 3), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: NULL second array returns NULL
      When query
        """
        SELECT zip_with(array(1, 2, 3), CAST(NULL AS ARRAY<INT>), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: Both NULL arrays return NULL
      When query
        """
        SELECT zip_with(CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL elements within arrays are propagated through the lambda

    @sail-bug
    Scenario: NULL element in first array propagates through addition
      When query
        """
        SELECT zip_with(array(1, null, 3), array(4, 5, 6), (x, y) -> x + y) AS result
        """
      Then query result
        | result         |
        | [5, NULL, 9]   |

    @sail-bug
    Scenario: NULL element in second array propagates through addition
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, null, 6), (x, y) -> x + y) AS result
        """
      Then query result
        | result         |
        | [5, NULL, 9]   |

    @sail-bug
    Scenario: NULL elements in both arrays both propagate
      When query
        """
        SELECT zip_with(array(1, null, 3), array(null, 5, 6), (x, y) -> x + y) AS result
        """
      Then query result
        | result               |
        | [NULL, NULL, 9]      |

    @sail-bug
    Scenario: NULL element in string array propagates through concat
      When query
        """
        SELECT zip_with(array('a', null, 'c'), array('x', 'y', 'z'), (x, y) -> concat(x, y)) AS result
        """
      Then query result
        | result           |
        | [ax, NULL, cz]   |

  Rule: Type coercion between array element types

    @sail-bug
    Scenario: Integer and double arrays - elements coerced to double
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(1.5, 2.5, 3.5), (x, y) -> x + y) AS result
        """
      Then query result
        | result           |
        | [2.5, 4.5, 6.5]  |

    @sail-bug
    Scenario: Long and int arrays - elements coerced to long
      When query
        """
        SELECT zip_with(array(1L, 2L), array(1, 2), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | [2, 4] |

    @sail-bug
    Scenario: Double arrays with decimal addition
      When query
        """
        SELECT zip_with(array(1.1, 2.2), array(3.3, 4.4), (x, y) -> x + y) AS result
        """
      Then query result
        | result       |
        | [4.4, 6.6]   |

  Rule: Outer column references inside the lambda

    @sail-bug
    Scenario: Lambda captures a column from the outer query
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> x + y + v) AS result
        FROM (SELECT 10 AS v) t
        """
      Then query result
        | result         |
        | [15, 17, 19]   |

  Rule: Nested arrays as elements

    @sail-bug
    Scenario: Zip two arrays of arrays, concatenating each pair of inner arrays
      When query
        """
        SELECT zip_with(array(array(1, 2), array(3, 4)), array(array(5, 6), array(7, 8)), (x, y) -> concat(x, y)) AS result
        """
      Then query result
        | result                       |
        | [[1, 2, 5, 6], [3, 4, 7, 8]] |

  Rule: Multi-row queries

    @sail-bug
    Scenario: zip_with applied to rows with different array lengths
      When query
        """
        SELECT zip_with(a, b, (x, y) -> x + y) AS result
        FROM VALUES
          (array(1, 2), array(3, 4)),
          (array(10, 20, 30), array(1, 2, 3)),
          (array(5), array(6, 7))
        AS t(a, b)
        """
      Then query result
        | result       |
        | [4, 6]       |
        | [11, 22, 33] |
        | [11, NULL]   |

    @sail-bug
    Scenario: zip_with with NULL array rows returns NULL for those rows
      When query
        """
        SELECT zip_with(col1, col2, (x, y) -> x + y) AS result
        FROM VALUES
          (array(1, 2), array(3, 4)),
          (CAST(NULL AS ARRAY<INT>), array(5, 6)),
          (array(7, 8), CAST(NULL AS ARRAY<INT>))
        AS t(col1, col2)
        """
      Then query result
        | result |
        | [4, 6] |
        | NULL   |
        | NULL   |

    @sail-bug
    Scenario: zip_with from table with varying-length arrays
      When query
        """
        SELECT zip_with(col1, col2, (x, y) -> x * y) AS result
        FROM VALUES
          (array(1, 2, 3), array(4, 5, 6)),
          (array(10), array(3)),
          (array(2, 4), array(3, 5))
        AS t(col1, col2)
        """
      Then query result
        | result      |
        | [4, 10, 18] |
        | [30]        |
        | [6, 20]     |

  Rule: Composing zip_with with other functions

    @sail-bug
    Scenario: size() of the result array
      When query
        """
        SELECT size(zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> x + y)) AS result
        """
      Then query result
        | result |
        | 3      |

    @sail-bug
    Scenario: Nested zip_with calls
      When query
        """
        SELECT zip_with(
          zip_with(array(1, 2), array(3, 4), (a, b) -> a + b),
          zip_with(array(5, 6), array(7, 8), (a, b) -> a * b),
          (x, y) -> x + y
        ) AS result
        """
      Then query result
        | result    |
        | [39, 54]  |

    @sail-bug
    Scenario: zip_with on sequence-generated arrays
      When query
        """
        SELECT zip_with(sequence(1, 5), sequence(6, 10), (x, y) -> x + y) AS result
        """
      Then query result
        | result              |
        | [7, 9, 11, 13, 15]  |

  Rule: No aliases exist for zip_with

    Scenario: arrays_zip_with is not a valid function name
      When query
        """
        SELECT arrays_zip_with(array(1, 2), array(3, 4), (x, y) -> x + y) AS result
        """
      Then query error .*

  Rule: Invalid usage produces errors

    Scenario: Too few arguments - 2 instead of 3
      When query
        """
        SELECT zip_with(array(1, 2), array(3, 4)) AS result
        """
      Then query error .*

    Scenario: Too many arguments - 4 instead of 3
      When query
        """
        SELECT zip_with(array(1, 2), array(3, 4), (x, y) -> x + y, 99) AS result
        """
      Then query error .*

    Scenario: First argument is not an array
      When query
        """
        SELECT zip_with(42, array(3, 4), (x, y) -> x + y) AS result
        """
      Then query error .*

    Scenario: Second argument is not an array
      When query
        """
        SELECT zip_with(array(3, 4), 'hello', (x, y) -> x + y) AS result
        """
      Then query error .*

    Scenario: Lambda with only one parameter when two are required
      When query
        """
        SELECT zip_with(array(1, 2), array(3, 4), x -> x) AS result
        """
      Then query error .*

    Scenario: Lambda with three parameters when two are required
      When query
        """
        SELECT zip_with(array(1, 2), array(3, 4), (x, y, z) -> x + y + z) AS result
        """
      Then query error .*

  Rule: Basic element-wise application on equal-length arrays

    @sail-bug
    Scenario: Sum pairs of integers
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(4, 5, 6), (x, y) -> x + y) AS result
        """
      Then query result
        | result    |
        | [5, 7, 9] |

    @sail-bug
    Scenario: Multiply pairs
      When query
        """
        SELECT zip_with(array(2, 3, 4), array(10, 20, 30), (x, y) -> x * y) AS result
        """
      Then query result
        | result         |
        | [20, 60, 120]  |

    @sail-bug
    Scenario: Concatenate string pairs
      When query
        """
        SELECT zip_with(array('a', 'b', 'c'), array('x', 'y', 'z'), (x, y) -> concat(x, y)) AS result
        """
      Then query result
        | result          |
        | [ax, by, cz]    |

    @sail-bug
    Scenario: Single element arrays
      When query
        """
        SELECT zip_with(array(10), array(20), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | [30]   |

    @sail-bug
    Scenario: Return boolean comparison
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(1, 3, 2), (x, y) -> x = y) AS result
        """
      Then query result
        | result               |
        | [true, false, false] |

  Rule: Unequal-length arrays pad shorter with NULL

    @sail-bug
    Scenario: Left array longer than right
      When query
        """
        SELECT zip_with(array(1, 2, 3, 4), array(10, 20), (x, y) -> x + y) AS result
        """
      Then query result
        | result              |
        | [11, 22, NULL, NULL] |

    @sail-bug
    Scenario: Right array longer than left
      When query
        """
        SELECT zip_with(array(1, 2), array(10, 20, 30, 40), (x, y) -> x + y) AS result
        """
      Then query result
        | result                |
        | [11, 22, NULL, NULL]  |

    @sail-bug
    Scenario: Right array empty left has elements
      When query
        """
        SELECT zip_with(array(1, 2, 3), CAST(array() AS array<int>), (x, y) -> coalesce(x, 0) + coalesce(y, 0)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    @sail-bug
    Scenario: Left empty right has elements
      When query
        """
        SELECT zip_with(CAST(array() AS array<int>), array(10, 20), (x, y) -> coalesce(x, 0) + coalesce(y, 0)) AS result
        """
      Then query result
        | result    |
        | [10, 20]  |

    @sail-bug
    Scenario: Unequal lengths with coalesce in lambda
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(10), (x, y) -> coalesce(x, 0) + coalesce(y, 0)) AS result
        """
      Then query result
        | result    |
        | [11, 2, 3] |

  Rule: Empty arrays return empty array

    @sail-bug
    Scenario: Both arrays empty returns empty
      When query
        """
        SELECT zip_with(CAST(array() AS array<int>), CAST(array() AS array<int>), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: NULL array input returns NULL

    @sail-bug
    Scenario: Left array NULL returns NULL
      When query
        """
        SELECT zip_with(CAST(NULL AS array<int>), array(1, 2, 3), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: Right array NULL returns NULL
      When query
        """
        SELECT zip_with(array(1, 2, 3), CAST(NULL AS array<int>), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: Both arrays NULL returns NULL
      When query
        """
        SELECT zip_with(CAST(NULL AS array<int>), CAST(NULL AS array<int>), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: NULL array from VALUES table
      When query
        """
        SELECT zip_with(a, b, (x, y) -> x + y) AS result
        FROM VALUES (CAST(NULL AS array<int>), array(1, 2)) AS t(a, b)
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL elements propagate through the lambda

    @sail-bug
    Scenario: NULL element in left array
      When query
        """
        SELECT zip_with(array(1, NULL, 3), array(10, 20, 30), (x, y) -> x + y) AS result
        """
      Then query result
        | result         |
        | [11, NULL, 33] |

    @sail-bug
    Scenario: NULL element in right array
      When query
        """
        SELECT zip_with(array(1, 2, 3), array(10, NULL, 30), (x, y) -> x + y) AS result
        """
      Then query result
        | result         |
        | [11, NULL, 33] |

    @sail-bug
    Scenario: coalesce handles NULL elements
      When query
        """
        SELECT zip_with(array(1, NULL, 3), array(10, 20, NULL), (x, y) -> coalesce(x, 0) + coalesce(y, 0)) AS result
        """
      Then query result
        | result      |
        | [11, 20, 3] |

  Rule: Multiple rows batch processing

    @sail-bug
    Scenario: zip_with applied to multiple rows
      When query
        """
        SELECT id, zip_with(a, b, (x, y) -> x + y) AS result
        FROM VALUES
          (1, array(1, 2, 3), array(10, 20, 30)),
          (2, array(4, 5), array(40, 50)),
          (3, array(7), array(70))
        AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | result         |
        | 1  | [11, 22, 33]   |
        | 2  | [44, 55]       |
        | 3  | [77]           |

    @sail-bug
    Scenario: zip_with with NULL arrays mixed in batch
      When query
        """
        SELECT id, zip_with(a, b, (x, y) -> x + y) AS result
        FROM VALUES
          (1, array(1, 2), array(10, 20)),
          (2, CAST(NULL AS array<int>), array(3, 4)),
          (3, array(5, 6), array(50, 60))
        AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | result    |
        | 1  | [11, 22]  |
        | 2  | NULL      |
        | 3  | [55, 66]  |

    @sail-bug
    Scenario: zip_with with unequal lengths in batch
      When query
        """
        SELECT id, zip_with(a, b, (x, y) -> coalesce(x, 0) + coalesce(y, 0)) AS result
        FROM VALUES
          (1, array(1, 2, 3), array(10)),
          (2, array(4), array(40, 50, 60)),
          (3, array(7, 8), array(70, 80))
        AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | result      |
        | 1  | [11, 2, 3]  |
        | 2  | [44, 50, 60] |
        | 3  | [77, 88]    |

  Rule: Outer column references in lambda body

    @sail-bug
    Scenario: Lambda references outer column
      When query
        """
        SELECT zip_with(a, b, (x, y) -> x + y + bonus) AS result
        FROM VALUES (array(1, 2, 3), array(10, 20, 30), 5) AS t(a, b, bonus)
        """
      Then query result
        | result         |
        | [16, 27, 38]   |

  Rule: Complex lambda expressions

    @sail-bug
    Scenario: zip_with with IF expression
      When query
        """
        SELECT zip_with(array(1, 2, 3, 4), array(3, 2, 1, 5), (x, y) -> if(x > y, x, y)) AS result
        """
      Then query result
        | result      |
        | [3, 2, 3, 5] |

    @sail-bug
    Scenario: zip_with building struct-like string
      When query
        """
        SELECT zip_with(array('key1', 'key2', 'key3'), array('val1', 'val2', 'val3'), (k, v) -> concat(k, '=', v)) AS result
        """
      Then query result
        | result                          |
        | [key1=val1, key2=val2, key3=val3] |

  Rule: Different element types

    @sail-bug
    Scenario: BIGINT arrays
      When query
        """
        SELECT zip_with(array(1L, 2L, 3L), array(10L, 20L, 30L), (x, y) -> x + y) AS result
        """
      Then query result
        | result       |
        | [11, 22, 33] |

    @sail-bug
    Scenario: DOUBLE arrays
      When query
        """
        SELECT zip_with(array(1.5, 2.5), array(0.5, 1.5), (x, y) -> x + y) AS result
        """
      Then query result
        | result      |
        | [2.0, 4.0]  |
