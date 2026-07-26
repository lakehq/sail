@concat
Feature: concat function

  Rule: Basic concatenation

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT concat(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                      | args                        | result          |
        | concat two integer arrays | array(1, 2, 3), array(4, 5) | [1, 2, 3, 4, 5] |
        | concat two string arrays  | array('a', 'b'), array('c') | [a, b, c]       |

  Rule: Empty array handling

    Scenario Outline: Empty array: <case>
      When query
        """
        SELECT concat(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | args                    | result    |
        | concat empty array with typed array | array(), array(1, 2, 3) | [1, 2, 3] |
        | concat typed array with empty array | array(1, 2), array()    | [1, 2]    |

  Rule: Null propagation

    Scenario Outline: Array null propagation: <case>
      When query
        """
        SELECT concat(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                | args                                  |
        | concat array with null returns null | array(1, 2), CAST(NULL AS ARRAY<INT>) |
        | concat null with array returns null | CAST(NULL AS ARRAY<INT>), array(1, 2) |

  Rule: String concatenation

    Scenario Outline: String: <case>
      When query
        """
        SELECT concat(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | args                                   | result       |
        | basic string concatenation         | 'Spark', 'SQL'                         | SparkSQL     |
        | three string arguments             | 'Hello', ', ', 'World'                 | Hello, World |
        | single string argument             | 'hello'                                | hello        |
        | empty string with non-empty string | '', 'hello'                            | hello        |
        | many string arguments              | 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h' | abcdefgh     |

    Scenario Outline: String with empty result: <case>
      When query
        """
        SELECT concat(<args>) AS result
        """
      Then query result
        | result |
        |        |

      Examples:
        | case                                | args       |
        | zero arguments returns empty string |            |
        | empty strings                       | '', ''     |
        | whitespace strings                  | '  ', '  ' |

  Rule: String NULL propagation

    Scenario Outline: String NULL propagation: <case>
      When query
        """
        SELECT concat(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                | args                      |
        | NULL only returns NULL              | NULL                      |
        | NULL with string returns NULL       | NULL, 'b'                 |
        | string with NULL returns NULL       | 'a', NULL                 |
        | NULL and NULL returns NULL          | NULL, NULL                |
        | string NULL string returns NULL     | 'a', NULL, 'b'            |
        | typed NULL string returns NULL      | CAST(NULL AS STRING)      |
        | typed NULL with string returns NULL | CAST(NULL AS STRING), 'b' |

  Rule: Type coercion to string

    Scenario Outline: Type coercion: <case>
      When query
        """
        SELECT concat(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | args                            | result              |
        | TINYINT coerced to string           | CAST(1 AS TINYINT)              | 1                   |
        | INT coerced to string               | 1                               | 1                   |
        | INT and INT concatenated as strings | 1, 2                            | 12                  |
        | DOUBLE coerced to string            | 1.0                             | 1.0                 |
        | DECIMAL coerced to string           | CAST(1.0 AS DECIMAL(10,2))      | 1.00                |
        | BOOLEAN coerced to string           | true                            | true                |
        | DATE coerced to string              | DATE '2024-01-15'               | 2024-01-15          |
        | TIMESTAMP coerced to string         | TIMESTAMP '2024-01-15 12:00:00' | 2024-01-15 12:00:00 |
        | string with INT coercion            | 'hello', 1                      | hello1              |

  Rule: Binary concatenation

    Scenario Outline: Binary: <case>
      When query
        """
        SELECT concat(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | args                    | result           |
        | basic binary concatenation  | X'4865', X'6C6C6F'      | [48 65 6C 6C 6F] |
        | single binary argument      | X'48656C6C6F'           | [48 65 6C 6C 6F] |
        | three binary arguments      | X'48', X'65', X'6C6C6F' | [48 65 6C 6C 6F] |
        | empty binary with binary    | X'', X'4865'            | [48 65]          |
        | binary with string coercion | X'48656C6C6F', 'world'  | Helloworld       |
        | string with binary coercion | 'hello', X'48656C6C6F'  | helloHello       |

  Rule: Binary NULL propagation

    Scenario Outline: Binary NULL propagation: <case>
      When query
        """
        SELECT concat(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                 | args                                       |
        | NULL typed binary returns NULL       | CAST(NULL AS BINARY)                       |
        | NULL binary with binary returns NULL | CAST(NULL AS BINARY), X'48'                |
        | binary with NULL binary returns NULL | X'48', CAST(NULL AS BINARY)                |
        | two NULL binaries returns NULL       | CAST(NULL AS BINARY), CAST(NULL AS BINARY) |

  Rule: Array concatenation

    Scenario Outline: Array: <case>
      When query
        """
        SELECT concat(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                | args                                               | result             |
        | concat two boolean arrays                           | array(true), array(false)                          | [true, false]      |
        | concat nested arrays                                | array(array(1, 2)), array(array(3, 4))             | [[1, 2], [3, 4]]   |
        | concat array with null elements                     | array(1, NULL, 3), array(4)                        | [1, NULL, 3, 4]    |
        | concat three arrays                                 | array(1, 2, 3), array(4, 5), array(6)              | [1, 2, 3, 4, 5, 6] |
        | concat two empty arrays                             | array(), array()                                   | []                 |
        | NULL typed array returns NULL                       | CAST(NULL AS ARRAY<INT>)                           | NULL               |
        | typed NULL array with typed NULL array returns NULL | CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>) | NULL               |

  Rule: Multi-row behavior

    Scenario Outline: Multi-row: <case>
      When query
        """
        SELECT concat(a, b) AS result
        FROM VALUES <values> AS t(a, b)
        """
      Then query result
        | result  |
        | <first> |
        | NULL    |
        | NULL    |

      Examples:
        | case                                | values                                                      | first       |
        | string concat from table with NULLs | ('hello', ' world'), (NULL, 'x'), ('a', NULL)               | hello world |
        | array concat from table with NULLs  | (array(1, 2), array(3)), (NULL, array(4)), (array(5), NULL) | [1, 2, 3]   |

  Rule: Error cases

    Scenario Outline: Error: <case>
      When query
        """
        SELECT concat(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                                            | args                 |
        | MAP type is rejected                            | map('a', 1)          |
        | STRUCT type is rejected                         | named_struct('a', 1) |
        | untyped NULL mixed with typed array is rejected | NULL, array(1, 2)    |
        | typed array mixed with untyped NULL is rejected | array(1, 2), NULL    |

  Rule: Timestamp coercion to string

    Scenario Outline: Timestamp: <case>
      When query
        """
        SELECT concat(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                              | args                                        | result                     |
        | TIMESTAMP coerced to string with microseconds     | TIMESTAMP '2024-01-15 12:00:00.123456'      | 2024-01-15 12:00:00.123456 |
        | TIMESTAMP concatenated with string suffix         | TIMESTAMP '2024-01-15 12:00:00', '_suffix'  | 2024-01-15 12:00:00_suffix |
        | string prefix concatenated with TIMESTAMP         | 'prefix_', TIMESTAMP '2024-01-15 12:00:00'  | prefix_2024-01-15 12:00:00 |
        | TIMESTAMP NULL with string returns NULL           | CAST(NULL AS TIMESTAMP), 'x'                | NULL                       |
        | TIMESTAMP_NTZ coerced to string                   | TIMESTAMP_NTZ '2024-01-15 12:00:00'         | 2024-01-15 12:00:00        |
        | TIMESTAMP_NTZ coerced to string with microseconds | TIMESTAMP_NTZ '2024-01-15 12:00:00.123456'  | 2024-01-15 12:00:00.123456 |
        | TIMESTAMP_NTZ concatenated with string suffix     | TIMESTAMP_NTZ '2024-01-15 12:00:00', '_end' | 2024-01-15 12:00:00_end    |
        | TIMESTAMP_NTZ NULL with string returns NULL       | CAST(NULL AS TIMESTAMP_NTZ), 'x'            | NULL                       |

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
        | result                                  |
        | 2024-01-15 12:00:00_2024-01-16 13:14:15 |

  Rule: Nested concat

    Scenario: nested concat with string constants folds to single value
      When query
        """
        SELECT concat(concat('a', 'b'), 'c') AS result
        """
      Then query result
        | result |
        | abc    |

    Scenario: nested concat with column flattens and returns correct result
      When query
        """
        SELECT concat(concat(v, 'b'), 'c') AS result
        FROM VALUES ('a') AS t(v)
        """
      Then query result
        | result |
        | abc    |

  Rule: Single-argument simplify edge cases

    Scenario Outline: Single argument: <case>
      When query
        """
        SELECT concat(<expr>) AS result
        FROM VALUES (<value>) AS t(<col>)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                      | expr               | value                                  | col | result                     |
        | single timestamp column coerced to string                 | ts                 | TIMESTAMP '2024-01-15 12:00:00'        | ts  | 2024-01-15 12:00:00        |
        | single timestamp column with microseconds                 | ts                 | TIMESTAMP '2024-01-15 12:00:00.123456' | ts  | 2024-01-15 12:00:00.123456 |
        | single timestamp column explicit cast matches bare column | CAST(ts AS STRING) | TIMESTAMP '2024-01-15 12:00:00'        | ts  | 2024-01-15 12:00:00        |
        | single INT column coerced to string                       | n                  | 42                                     | n   | 42                         |
        | single DOUBLE column coerced to string                    | d                  | 3.14                                   | d   | 3.14                       |
        | single BOOLEAN column coerced to string                   | b                  | true                                   | b   | true                       |
        | single NULL timestamp column returns NULL                 | ts                 | CAST(NULL AS TIMESTAMP)                | ts  | NULL                       |
        | single TIMESTAMP_NTZ column coerced to string             | ts                 | TIMESTAMP_NTZ '2024-01-15 12:00:00'    | ts  | 2024-01-15 12:00:00        |

  @spark_null
  Rule: Output schema

    Scenario: non-null string literals yield a non-nullable string
      When query
        """
        SELECT concat('a', 'b') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a non-null string column yields a non-nullable string
      When query
        """
        SELECT concat(CAST(id AS STRING), 'x') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT concat(c, 'x') AS result FROM VALUES ('a'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
