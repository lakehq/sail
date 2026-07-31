@reverse
Feature: reverse function

  Rule: String reversal

    Scenario Outline: String: <case>
      When query
        """
        SELECT reverse(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                | arg         | result    |
        | simple ASCII string | 'Spark SQL' | LQS krapS |
        | single character    | 'a'         | a         |
        | longer ASCII string | 'abcde'     | edcba     |

    Scenario Outline: String with empty result: <case>
      When query
        """
        SELECT reverse(<arg>) AS result
        """
      Then query result
        | result |
        |        |

      Examples:
        | case         | arg |
        | empty string | ''  |

    Scenario: reversing a whitespace-only string preserves its length
      When query
        """
        SELECT length(reverse('   ')) AS result
        """
      Then query result
        | result |
        | 3      |

  Rule: String NULL propagation

    Scenario Outline: String NULL: <case>
      When query
        """
        SELECT reverse(<arg>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                      | arg                  |
        | untyped NULL returns NULL | NULL                 |
        | NULL string returns NULL  | CAST(NULL AS STRING) |

  Rule: Type coercion to string

    Scenario Outline: Type coercion: <case>
      When query
        """
        SELECT reverse(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | arg                                 | result              |
        | INT is cast to string             | CAST(1 AS INT)                      | 1                   |
        | multi-digit INT                   | CAST(12345 AS INT)                  | 54321               |
        | negative INT keeps the minus sign | CAST(-12345 AS INT)                 | 54321-              |
        | BIGINT is cast to string          | CAST(12345 AS BIGINT)               | 54321               |
        | TINYINT is cast to string         | CAST(42 AS TINYINT)                 | 24                  |
        | DECIMAL is cast to string         | CAST(1.23 AS DECIMAL(10,2))         | 32.1                |
        | DOUBLE is cast to string          | CAST(12345.678 AS DOUBLE)           | 876.54321           |
        | FLOAT is cast to string           | CAST(1.0 AS FLOAT)                  | 0.1                 |
        | FLOAT NaN                         | CAST('NaN' AS FLOAT)                | NaN                 |
        | FLOAT Infinity                    | CAST('Infinity' AS FLOAT)           | ytinifnI            |
        | FLOAT negative Infinity           | CAST('-Infinity' AS FLOAT)          | ytinifnI-           |
        | boolean true                      | true                                | eurt                |
        | boolean false                     | false                               | eslaf               |
        | DATE is cast to string            | DATE '2024-10-15'                   | 51-01-4202          |
        | TIMESTAMP is cast to string       | TIMESTAMP '2024-01-15 12:30:45'     | 54:03:21 51-10-4202 |
        | TIMESTAMP_NTZ is cast to string   | TIMESTAMP_NTZ '2024-01-15 12:30:45' | 54:03:21 51-10-4202 |

  Rule: Array reversal

    Scenario Outline: Array: <case>
      When query
        """
        SELECT reverse(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                 | arg                  | result       |
        | integer array        | array(2, 1, 4, 3)    | [3, 4, 1, 2] |
        | string array         | array('c', 'b', 'a') | [a, b, c]    |
        | single-element array | array(42)            | [42]         |
        | empty array          | array()              | []           |

  Rule: Array NULL handling

    Scenario Outline: Array NULL: <case>
      When query
        """
        SELECT reverse(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                  | arg                  | result          |
        | all-NULL array reverses element order | array(NULL, NULL)    | [NULL, NULL]    |
        | mixed NULLs keep their positions      | array(1, NULL, 3)    | [3, NULL, 1]    |
        | typed NULLs at the boundaries         | array(NULL, 1, NULL) | [NULL, 1, NULL] |

    Scenario Outline: Array NULL propagation: <case>
      When query
        """
        SELECT reverse(<arg>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                          | arg                      |
        | typed NULL array returns NULL | CAST(NULL AS ARRAY<INT>) |

  Rule: Array reversal — complex element types

    Scenario Outline: Complex element: <case>
      When query
        """
        SELECT reverse(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case             | arg                                               | result               |
        | array of arrays  | array(array(1, 2), array(3, 4))                   | [[3, 4], [1, 2]]     |
        | array of maps    | array(map('a', 1), map('b', 2))                   | [{b -> 2}, {a -> 1}] |
        | array of structs | array(named_struct('x', 1), named_struct('x', 2)) | [{2}, {1}]           |

  # Spark reverses binary bytewise and keeps the binary type since 4.2. Up to
  # 4.1 it cast BINARY to STRING and reversed by character, so `reverse(x'CAFE')`
  # was a no-op there (0xCA is a UTF-8 lead byte). Sail follows 4.2.
  @spark-4.2
  Rule: Binary input

    Scenario Outline: Binary: <case>
      When query
        """
        SELECT reverse(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                             | arg           | result           |
        | BINARY is reversed bytewise      | X'48656C6C6F' | [6F 6C 6C 65 48] |
        | empty BINARY reverses to empty   | X''           | []               |

    Scenario Outline: Binary stays binary: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                    | expr                                   | result |
        | reversing BINARY yields BINARY          | typeof(reverse(X'48656C6C6F'))         | binary |
        | non-UTF-8 bytes survive the reversal    | hex(reverse(X'FF00FE'))                | FE00FF |
        | the reversed bytes decode back as UTF-8 | CAST(reverse(X'48656C6C6F') AS STRING) | olleH  |

    Scenario: multiple BINARY rows including a NULL
      When query
        """
        SELECT reverse(b) AS result FROM VALUES (X'0102'), (NULL), (X'414243') AS t(b)
        """
      Then query result
        | result     |
        | [02 01]    |
        | NULL       |
        | [43 42 41] |

    Scenario Outline: Binary NULL propagation: <case>
      When query
        """
        SELECT reverse(<arg>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                     | arg                  |
        | NULL BINARY returns NULL | CAST(NULL AS BINARY) |

  Rule: Multi-row behavior

    Scenario Outline: Multi-row: <case>
      When query
        """
        SELECT reverse(c) AS result FROM VALUES <values> AS t(c)
        """
      Then query result
        | result  |
        | <first> |
        | NULL    |
        | <third> |

      Examples:
        | case                    | values                               | first     | third  |
        | strings with a NULL row | ('abc'), (NULL), ('xyz')             | cba       | zyx    |
        | arrays with a NULL row  | (array(1,2,3)), (NULL), (array(4,5)) | [3, 2, 1] | [5, 4] |

  Rule: Type rejection

    Scenario Outline: Rejected type: <case>
      When query
        """
        SELECT reverse(<arg>) AS result
        """
      Then query error .*DATATYPE_MISMATCH.*

      Examples:
        | case               | arg                  |
        | MAP is rejected    | map('a', 1)          |
        | STRUCT is rejected | named_struct('a', 1) |

  Rule: Arity enforcement

    Scenario: zero arguments is rejected
      When query
        """
        SELECT reverse() AS result
        """
      Then query error .*

    Scenario: two arguments is rejected
      When query
        """
        SELECT reverse('hello', 'world') AS result
        """
      Then query error .*

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to reverse yields the schema Spark declares
      When query
        """
        SELECT reverse('Spark SQL') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to reverse yields the schema Spark declares
      When query
        """
        SELECT reverse(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to reverse stays nullable
      When query
        """
        SELECT reverse(c) AS result FROM VALUES ('Spark SQL'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
