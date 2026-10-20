Feature: array output schema

  Rule: Void columns preserve rows

    Scenario Outline: Void columns in array(<arguments>)
      When query
        """
        SELECT array(<arguments>) AS result FROM VALUES (NULL), (NULL) AS t(v)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: void (containsNull = true)
        """
      Then query result
        | result   |
        | <result> |
        | <result> |

      Examples:
        | arguments | result       |
        | v         | [NULL]       |
        | v, v      | [NULL, NULL] |
        | v, NULL   | [NULL, NULL] |

  Rule: Array subscripts use zero-based INT indices

    Scenario Outline: Array subscript values and bounds with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <array>[<index>] AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | array                          | index                  | result |
        | array(10, 20, 30)              | -1                     | NULL   |
        | array(10, 20, 30)              | -2                     | NULL   |
        | array(10, 20, 30)              | -3                     | NULL   |
        | array(10, 20, 30)              | -4                     | NULL   |
        | array(10, 20, 30)              | 0                      | 10     |
        | array(10, 20, 30)              | 2                      | 30     |
        | array(10, 20, 30)              | 3                      | NULL   |
        | CAST(array() AS ARRAY<INT>)    | -2                     | NULL   |
        | CAST(NULL AS ARRAY<INT>)       | -2                     | NULL   |
        | array(10, 20, 30)              | -4294967296L           | 10     |
        | array(10, 20, 30)              | -2147483649L           | NULL   |
        | array(10, 20, 30)              | -9223372036854775808L  | 10     |

    Scenario Outline: ANSI array subscripts report the narrowed index
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT array(10, 20, 30)[<index>] AS result
        """
      Then query error (?i)INVALID_ARRAY_INDEX.*<narrowed>

      Examples:
        | index         | narrowed   |
        | -1            | -1         |
        | -2            | -2         |
        | -4            | -4         |
        | -2147483649L | 2147483647 |

    Scenario Outline: Valid and NULL array subscripts with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <array>[<index>] AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | array                    | index                  | result |
        | array(10, 20, 30)        | 0                      | 10     |
        | CAST(NULL AS ARRAY<INT>) | -2                     | NULL   |
        | array(10, 20, 30)        | -4294967296L           | 10     |
        | array(10, 20, 30)        | -9223372036854775808L  | 10     |

    Scenario: A negative subscript returns NULL for runtime arrays with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT id, a[-2] AS result
        FROM (
          SELECT id, IF(id = 0, array(10, 20, 30), CAST(NULL AS ARRAY<INT>)) AS a
          FROM range(2)
        ) AS t
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 0  | NULL   |
        | 1  | NULL   |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to array yields the schema Spark declares
      When query
        """
        SELECT array(1, 2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: a non-null column input to array yields the schema Spark declares
      When query
        """
        SELECT array(CAST(id AS INT), 2, 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: a nullable column input to array stays nullable
      When query
        """
        SELECT array(c, 2, 3) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """
