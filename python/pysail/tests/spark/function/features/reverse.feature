@reverse
Feature: reverse function

  Rule: String reversal — basic usage
    Scenario: Reverse a simple ASCII string
      When query
        """
        SELECT reverse('Spark SQL') AS result
        """
      Then query result
        | result    |
        | LQS krapS |

    Scenario: Reverse a single character
      When query
        """
        SELECT reverse('a') AS result
        """
      Then query result
        | result |
        | a      |

    Scenario: Reverse an empty string
      When query
        """
        SELECT reverse('') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: Reverse a string of only whitespace preserves length
      When query
        """
        SELECT length(reverse('   ')) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: Reverse a longer ASCII string
      When query
        """
        SELECT reverse('abcde') AS result
        """
      Then query result
        | result |
        | edcba  |

  Rule: String reversal — NULL input
    Scenario: Untyped NULL returns NULL
      When query
        """
        SELECT reverse(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL string returns NULL
      When query
        """
        SELECT reverse(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: String reversal — implicit type coercion to string
    Scenario: Integer is implicitly cast to string before reversing
      When query
        """
        SELECT reverse(CAST(1 AS INT)) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: Multi-digit integer reversed as string
      When query
        """
        SELECT reverse(CAST(12345 AS INT)) AS result
        """
      Then query result
        | result |
        | 54321  |

    Scenario: Negative integer reversed as string includes minus sign
      When query
        """
        SELECT reverse(CAST(-12345 AS INT)) AS result
        """
      Then query result
        | result  |
        | 54321-  |

    Scenario: BIGINT is implicitly cast to string before reversing
      When query
        """
        SELECT reverse(CAST(12345 AS BIGINT)) AS result
        """
      Then query result
        | result |
        | 54321  |

    Scenario: TINYINT is implicitly cast to string before reversing
      When query
        """
        SELECT reverse(CAST(42 AS TINYINT)) AS result
        """
      Then query result
        | result |
        | 24     |

    Scenario: DECIMAL is implicitly cast to string before reversing
      When query
        """
        SELECT reverse(CAST(1.23 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | 32.1   |

    Scenario: DOUBLE is implicitly cast to string before reversing
      When query
        """
        SELECT reverse(CAST(12345.678 AS DOUBLE)) AS result
        """
      Then query result
        | result    |
        | 876.54321 |

    Scenario: Float is implicitly cast to string before reversing
      When query
        """
        SELECT reverse(CAST(1.0 AS FLOAT)) AS result
        """
      Then query result
        | result |
        | 0.1    |

    Scenario: FLOAT NaN is implicitly cast to string before reversing
      When query
        """
        SELECT reverse(CAST('NaN' AS FLOAT)) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: FLOAT Infinity is implicitly cast to string before reversing
      When query
        """
        SELECT reverse(CAST('Infinity' AS FLOAT)) AS result
        """
      Then query result
        | result    |
        | ytinifnI  |

    Scenario: FLOAT negative Infinity is implicitly cast to string before reversing
      When query
        """
        SELECT reverse(CAST('-Infinity' AS FLOAT)) AS result
        """
      Then query result
        | result    |
        | ytinifnI- |

    Scenario: Boolean true is implicitly cast to string before reversing
      When query
        """
        SELECT reverse(true) AS result
        """
      Then query result
        | result |
        | eurt   |

    Scenario: Boolean false is implicitly cast to string before reversing
      When query
        """
        SELECT reverse(false) AS result
        """
      Then query result
        | result |
        | eslaf  |

    Scenario: Date is implicitly cast to string before reversing
      When query
        """
        SELECT reverse(DATE '2024-10-15') AS result
        """
      Then query result
        | result     |
        | 51-01-4202 |

    Scenario: TIMESTAMP is implicitly cast to string before reversing
      When query
        """
        SELECT reverse(TIMESTAMP '2024-01-15 12:30:45') AS result
        """
      Then query result
        | result              |
        | 54:03:21 51-10-4202 |

    Scenario: TIMESTAMP_NTZ is implicitly cast to string before reversing
      When query
        """
        SELECT reverse(TIMESTAMP_NTZ '2024-01-15 12:30:45') AS result
        """
      Then query result
        | result              |
        | 54:03:21 51-10-4202 |

  Rule: Array reversal — basic usage
    Scenario: Reverse an integer array from the official example
      When query
        """
        SELECT reverse(array(2, 1, 4, 3)) AS result
        """
      Then query result
        | result       |
        | [3, 4, 1, 2] |

    Scenario: Reverse a string array
      When query
        """
        SELECT reverse(array('c', 'b', 'a')) AS result
        """
      Then query result
        | result    |
        | [a, b, c] |

    Scenario: Reverse a single-element array
      When query
        """
        SELECT reverse(array(42)) AS result
        """
      Then query result
        | result |
        | [42]   |

    Scenario: Reverse an empty array
      When query
        """
        SELECT reverse(array()) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: Array reversal — NULL handling
    Scenario: NULL array (typed) returns NULL
      When query
        """
        SELECT reverse(CAST(NULL AS ARRAY<INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Array with all NULL elements reverses element order
      When query
        """
        SELECT reverse(array(NULL, NULL)) AS result
        """
      Then query result
        | result       |
        | [NULL, NULL] |

    Scenario: Array with mixed NULLs and non-NULLs preserves NULL positions in reverse
      When query
        """
        SELECT reverse(array(1, NULL, 3)) AS result
        """
      Then query result
        | result       |
        | [3, NULL, 1] |

    Scenario: Array of typed NULLs at boundaries
      When query
        """
        SELECT reverse(array(NULL, 1, NULL)) AS result
        """
      Then query result
        | result          |
        | [NULL, 1, NULL] |

  Rule: Array reversal — complex element types
    Scenario: Reverse an array of integer arrays (nested)
      When query
        """
        SELECT reverse(array(array(1, 2), array(3, 4))) AS result
        """
      Then query result
        | result           |
        | [[3, 4], [1, 2]] |

    Scenario: Reverse an array of maps
      When query
        """
        SELECT reverse(array(map('a', 1), map('b', 2))) AS result
        """
      Then query result
        | result               |
        | [{b -> 2}, {a -> 1}] |

    Scenario: Reverse an array of structs
      When query
        """
        SELECT reverse(array(named_struct('x', 1), named_struct('x', 2))) AS result
        """
      Then query result
        | result     |
        | [{2}, {1}] |

  Rule: Multi-row vectorized — strings
    Scenario: Reverse multiple string rows including a NULL
      When query
        """
        SELECT reverse(s) AS result FROM VALUES ('abc'), (NULL), ('xyz') AS t(s)
        """
      Then query result
        | result |
        | cba    |
        | NULL   |
        | zyx    |

  Rule: Multi-row vectorized — arrays
    Scenario: Reverse multiple array rows including a NULL
      When query
        """
        SELECT reverse(a) AS result FROM VALUES (array(1,2,3)), (NULL), (array(4,5)) AS t(a)
        """
      Then query result
        | result    |
        | [3, 2, 1] |
        | NULL      |
        | [5, 4]    |

  Rule: Binary input
    Scenario: BINARY value is reversed as bytes and returned as string
      When query
        """
        SELECT reverse(X'48656C6C6F') AS result
        """
      Then query result
        | result |
        | olleH  |

    Scenario: Empty BINARY reversed returns empty string
      When query
        """
        SELECT reverse(X'') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: NULL BINARY returns NULL
      When query
        """
        SELECT reverse(CAST(NULL AS BINARY)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Type rejection
    Scenario: MAP type is rejected
      When query
        """
        SELECT reverse(map('a', 1)) AS result
        """
      Then query error .*DATATYPE_MISMATCH.*

    Scenario: STRUCT type is rejected
      When query
        """
        SELECT reverse(named_struct('a', 1)) AS result
        """
      Then query error .*DATATYPE_MISMATCH.*

  Rule: Arity enforcement
    Scenario: Zero arguments is rejected
      When query
        """
        SELECT reverse() AS result
        """
      Then query error .*

    Scenario: Two arguments is rejected
      When query
        """
        SELECT reverse('hello', 'world') AS result
        """
      Then query error .*
