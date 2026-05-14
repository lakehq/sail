@concat
Feature: concat function

  Rule: Basic concatenation

    Scenario: concat two integer arrays
      When query
        """
        SELECT concat(array(1, 2, 3), array(4, 5)) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

    Scenario: concat two string arrays
      When query
        """
        SELECT concat(array('a', 'b'), array('c')) AS result
        """
      Then query result
        | result    |
        | [a, b, c] |

  Rule: Empty array handling

    Scenario: concat empty array with typed array
      When query
        """
        SELECT concat(array(), array(1, 2, 3)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: concat typed array with empty array
      When query
        """
        SELECT concat(array(1, 2), array()) AS result
        """
      Then query result
        | result |
        | [1, 2] |

  Rule: Null propagation

    Scenario: concat array with null returns null
      When query
        """
        SELECT concat(array(1, 2), CAST(NULL AS ARRAY<INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: concat null with array returns null
      When query
        """
        SELECT concat(CAST(NULL AS ARRAY<INT>), array(1, 2)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: String concatenation

    Scenario: basic string concatenation
      When query
        """
        SELECT concat('Spark', 'SQL') AS result
        """
      Then query result
        | result   |
        | SparkSQL |

    Scenario: three string arguments
      When query
        """
        SELECT concat('Hello', ', ', 'World') AS result
        """
      Then query result
        | result        |
        | Hello, World  |

    Scenario: single string argument
      When query
        """
        SELECT concat('hello') AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: zero arguments returns empty string
      When query
        """
        SELECT concat() AS result
        """
      Then query result
        | result |
        |        |

    Scenario: empty strings
      When query
        """
        SELECT concat('', '') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: empty string with non-empty string
      When query
        """
        SELECT concat('', 'hello') AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: whitespace strings
      When query
        """
        SELECT concat('  ', '  ') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: many string arguments
      When query
        """
        SELECT concat('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h') AS result
        """
      Then query result
        | result   |
        | abcdefgh |

  Rule: String NULL propagation

    Scenario: NULL only returns NULL
      When query
        """
        SELECT concat(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL with string returns NULL
      When query
        """
        SELECT concat(NULL, 'b') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: string with NULL returns NULL
      When query
        """
        SELECT concat('a', NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL and NULL returns NULL
      When query
        """
        SELECT concat(NULL, NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: string NULL string returns NULL
      When query
        """
        SELECT concat('a', NULL, 'b') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL string returns NULL
      When query
        """
        SELECT concat(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL with string returns NULL
      When query
        """
        SELECT concat(CAST(NULL AS STRING), 'b') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Type coercion to string

    Scenario: TINYINT coerced to string
      When query
        """
        SELECT concat(CAST(1 AS TINYINT)) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: INT coerced to string
      When query
        """
        SELECT concat(1) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: INT and INT concatenated as strings
      When query
        """
        SELECT concat(1, 2) AS result
        """
      Then query result
        | result |
        | 12     |

    Scenario: DOUBLE coerced to string
      When query
        """
        SELECT concat(1.0) AS result
        """
      Then query result
        | result |
        | 1.0    |

    Scenario: DECIMAL coerced to string
      When query
        """
        SELECT concat(CAST(1.0 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result |
        | 1.00   |

    Scenario: BOOLEAN coerced to string
      When query
        """
        SELECT concat(true) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: DATE coerced to string
      When query
        """
        SELECT concat(DATE '2024-01-15') AS result
        """
      Then query result
        | result     |
        | 2024-01-15 |

    Scenario: TIMESTAMP coerced to string
      When query
        """
        SELECT concat(TIMESTAMP '2024-01-15 12:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:00:00 |

    Scenario: string with INT coercion
      When query
        """
        SELECT concat('hello', 1) AS result
        """
      Then query result
        | result |
        | hello1 |

  Rule: Binary concatenation

    Scenario: basic binary concatenation
      When query
        """
        SELECT concat(X'4865', X'6C6C6F') AS result
        """
      Then query result
        | result           |
        | [48 65 6C 6C 6F] |

    Scenario: single binary argument
      When query
        """
        SELECT concat(X'48656C6C6F') AS result
        """
      Then query result
        | result           |
        | [48 65 6C 6C 6F] |

    Scenario: three binary arguments
      When query
        """
        SELECT concat(X'48', X'65', X'6C6C6F') AS result
        """
      Then query result
        | result           |
        | [48 65 6C 6C 6F] |

    Scenario: empty binary with binary
      When query
        """
        SELECT concat(X'', X'4865') AS result
        """
      Then query result
        | result  |
        | [48 65] |

    Scenario: binary with string coercion
      When query
        """
        SELECT concat(X'48656C6C6F', 'world') AS result
        """
      Then query result
        | result     |
        | Helloworld |

    Scenario: string with binary coercion
      When query
        """
        SELECT concat('hello', X'48656C6C6F') AS result
        """
      Then query result
        | result     |
        | helloHello |

  Rule: Binary NULL propagation

    Scenario: NULL typed binary returns NULL
      When query
        """
        SELECT concat(CAST(NULL AS BINARY)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL binary with binary returns NULL
      When query
        """
        SELECT concat(CAST(NULL AS BINARY), X'48') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: binary with NULL binary returns NULL
      When query
        """
        SELECT concat(X'48', CAST(NULL AS BINARY)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: two NULL binaries returns NULL
      When query
        """
        SELECT concat(CAST(NULL AS BINARY), CAST(NULL AS BINARY)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Array concatenation

    Scenario: concat two boolean arrays
      When query
        """
        SELECT concat(array(true), array(false)) AS result
        """
      Then query result
        | result        |
        | [true, false] |

    Scenario: concat nested arrays
      When query
        """
        SELECT concat(array(array(1, 2)), array(array(3, 4))) AS result
        """
      Then query result
        | result           |
        | [[1, 2], [3, 4]] |

    Scenario: concat array with null elements
      When query
        """
        SELECT concat(array(1, NULL, 3), array(4)) AS result
        """
      Then query result
        | result          |
        | [1, NULL, 3, 4] |

    Scenario: concat three arrays
      When query
        """
        SELECT concat(array(1, 2, 3), array(4, 5), array(6)) AS result
        """
      Then query result
        | result             |
        | [1, 2, 3, 4, 5, 6] |

    Scenario: concat two empty arrays
      When query
        """
        SELECT concat(array(), array()) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: NULL typed array returns NULL
      When query
        """
        SELECT concat(CAST(NULL AS ARRAY<INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: typed NULL array with typed NULL array returns NULL
      When query
        """
        SELECT concat(CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Multi-row behavior

    Scenario: string concat from table with NULLs
      When query
        """
        SELECT concat(a, b) AS result
        FROM VALUES ('hello', ' world'), (NULL, 'x'), ('a', NULL) AS t(a, b)
        """
      Then query result
        | result      |
        | hello world |
        | NULL        |
        | NULL        |

    Scenario: array concat from table with NULLs
      When query
        """
        SELECT concat(a, b) AS result
        FROM VALUES (array(1, 2), array(3)), (NULL, array(4)), (array(5), NULL) AS t(a, b)
        """
      Then query result
        | result    |
        | [1, 2, 3] |
        | NULL      |
        | NULL      |

  Rule: Error cases

    Scenario: MAP type is rejected
      When query
        """
        SELECT concat(map('a', 1)) AS result
        """
      Then query error .*

    Scenario: STRUCT type is rejected
      When query
        """
        SELECT concat(named_struct('a', 1)) AS result
        """
      Then query error .*

    Scenario: untyped NULL mixed with typed array is rejected
      When query
        """
        SELECT concat(NULL, array(1, 2)) AS result
        """
      Then query error .*

    Scenario: typed array mixed with untyped NULL is rejected
      When query
        """
        SELECT concat(array(1, 2), NULL) AS result
        """
      Then query error .*

  Rule: Timestamp coercion to string

    Scenario: TIMESTAMP coerced to string with microseconds
      When query
        """
        SELECT concat(TIMESTAMP '2024-01-15 12:00:00.123456') AS result
        """
      Then query result
        | result                     |
        | 2024-01-15 12:00:00.123456 |

    Scenario: TIMESTAMP concatenated with string suffix
      When query
        """
        SELECT concat(TIMESTAMP '2024-01-15 12:00:00', '_suffix') AS result
        """
      Then query result
        | result                      |
        | 2024-01-15 12:00:00_suffix  |

    Scenario: string prefix concatenated with TIMESTAMP
      When query
        """
        SELECT concat('prefix_', TIMESTAMP '2024-01-15 12:00:00') AS result
        """
      Then query result
        | result                      |
        | prefix_2024-01-15 12:00:00  |

    Scenario: two TIMESTAMPs cast to string concatenated with separator
      When query
        """
        SELECT concat(
          CAST(TIMESTAMP '2024-01-15 12:00:00' AS STRING),
          '_',
          CAST(TIMESTAMP '2024-01-16 13:14:15' AS STRING)
        ) AS result
        """
      Then query result
        | result                                    |
        | 2024-01-15 12:00:00_2024-01-16 13:14:15  |

    Scenario: TIMESTAMP NULL with string returns NULL
      When query
        """
        SELECT concat(CAST(NULL AS TIMESTAMP), 'x') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: TIMESTAMP_NTZ coerced to string
      When query
        """
        SELECT concat(TIMESTAMP_NTZ '2024-01-15 12:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:00:00 |

    Scenario: TIMESTAMP_NTZ coerced to string with microseconds
      When query
        """
        SELECT concat(TIMESTAMP_NTZ '2024-01-15 12:00:00.123456') AS result
        """
      Then query result
        | result                     |
        | 2024-01-15 12:00:00.123456 |

    Scenario: TIMESTAMP_NTZ concatenated with string suffix
      When query
        """
        SELECT concat(TIMESTAMP_NTZ '2024-01-15 12:00:00', '_end') AS result
        """
      Then query result
        | result                  |
        | 2024-01-15 12:00:00_end |

    Scenario: TIMESTAMP_NTZ NULL with string returns NULL
      When query
        """
        SELECT concat(CAST(NULL AS TIMESTAMP_NTZ), 'x') AS result
        """
      Then query result
        | result |
        | NULL   |
