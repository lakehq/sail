Feature: map output schema

  Rule: ANSI mode controls string coercion

    Scenario Outline: ANSI map merges values in argument order: <first>, <second>, <third>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT map(1, <first>, 2, <second>, 3, <third>) AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: integer
         |    |-- value: <type> (valueContainsNull = true)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | first | second | third | type          | result                         |
        | 1     | '2'    | 3.0   | decimal(21,1) | {1 -> 1.0, 2 -> 2.0, 3 -> 3.0} |
        | '2'   | 1      | 3.0   | decimal(21,1) | {1 -> 2.0, 2 -> 1.0, 3 -> 3.0} |
        | 1     | 3.0    | '2'   | double        | {1 -> 1.0, 2 -> 3.0, 3 -> 2.0} |
        | 3.0   | 1      | '2'   | double        | {1 -> 3.0, 2 -> 1.0, 3 -> 2.0} |
        | 3.0   | '2'    | 1     | double        | {1 -> 3.0, 2 -> 2.0, 3 -> 1.0} |
        | '2'   | 3.0    | 1     | double        | {1 -> 2.0, 2 -> 3.0, 3 -> 1.0} |

    Scenario: ANSI map retains integral precision after string promotion
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT map(1, id + 9007199254740993L, 2, '2', 3, 3.0) AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: integer
         |    |-- value: decimal(21,1) (valueContainsNull = true)
        """
      Then query result
        | result                                        |
        | {1 -> 9007199254740993.0, 2 -> 2.0, 3 -> 3.0} |
        | {1 -> 9007199254740994.0, 2 -> 2.0, 3 -> 3.0} |

    Scenario: ANSI map keeps distinct large integral keys after string promotion
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT map(9007199254740992L, 'a', '2', 'b', 9007199254740993BD, 'c') AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: decimal(20,0)
         |    |-- value: string (valueContainsNull = false)
        """
      Then query result
        | result                                                 |
        | {9007199254740992 -> a, 2 -> b, 9007199254740993 -> c} |

    Scenario: ANSI mode promotes mixed integral and string keys and values to BIGINT
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT map(1, 10, '02', '20') AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: long
         |    |-- value: long (valueContainsNull = true)
        """
      Then query result
        | result             |
        | {1 -> 10, 2 -> 20} |

    Scenario: ANSI mode promotes mixed Decimal and string values to DOUBLE
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT map(1, 1.25, 2, '2.5') AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: integer
         |    |-- value: double (valueContainsNull = true)
        """
      Then query result
        | result               |
        | {1 -> 1.25, 2 -> 2.5} |

    Scenario Outline: ANSI mode rejects malformed numeric strings in <argument>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT map(<arguments>) AS result
        """
      Then query error .*invalid.*

      Examples:
        | argument | arguments                |
        | keys     | 1, 10, 'invalid', 20      |
        | values   | 1, 10, 2, 'invalid'       |

    Scenario: ANSI mode detects duplicate keys after numeric conversion
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT map(1, 10, '01', 20) AS result
        """
      Then query error .*\[DUPLICATED_MAP_KEY\].*

    Scenario: ANSI mode applies LAST_WIN after numeric conversion
      Given config spark.sql.ansi.enabled = true
      Given config spark.sql.mapKeyDedupPolicy = LAST_WIN
      When query
        """
        SELECT map(1, 10, '01', 20) AS result
        """
      Then query result
        | result    |
        | {1 -> 20} |

    Scenario: Non-ANSI mode keeps mixed keys and values as strings
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT map(1, 10, '01', 'invalid') AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: string
         |    |-- value: string (valueContainsNull = false)
        """
      Then query result
        | result                    |
        | {1 -> 10, 01 -> invalid}   |

  Rule: Duplicate key policy

    Scenario: duplicate keys raise an error under the default EXCEPTION policy
      When query
        """
        SELECT map(1, 'a', 1, 'b') AS result
        """
      Then query error .*\[DUPLICATED_MAP_KEY\].*

    Scenario: LAST_WIN keeps the final value at the first key position
      Given config spark.sql.mapKeyDedupPolicy = LAST_WIN
      When query
        """
        SELECT map(1, 'a', 2, 'b', 1, 'c') AS result
        """
      Then query result
        | result           |
        | {1 -> c, 2 -> b} |

  Rule: Void values preserve rows

    Scenario Outline: Void columns in <expression>
      When query
        """
        SELECT <expression> AS result FROM VALUES (1, NULL), (2, NULL) AS t(id, v)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: integer
         |    |-- value: void (valueContainsNull = true)
        """
      Then query result
        | result   |
        | <first>  |
        | <second> |

      Examples:
        | expression               | first                    | second                   |
        | map(id, v)               | {1 -> NULL}              | {2 -> NULL}              |
        | map(id, v, id + 2, v)    | {1 -> NULL, 3 -> NULL}   | {2 -> NULL, 4 -> NULL}   |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to map yields the schema Spark declares
      When query
        """
        SELECT map(1.0, '2', 3.0, '4') AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: decimal(2,1)
         |    |-- value: string (valueContainsNull = false)
        """

    Scenario: nullable keys do not make the map itself nullable
      When query
        """
        SELECT map(c, '2', 3.0, '4') AS result FROM VALUES (1.0), (CAST(NULL AS DECIMAL(2,1))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: decimal(2,1)
         |    |-- value: string (valueContainsNull = false)
        """

    Scenario: NULL values preserve a non-null map with nullable values
      When query
        """
        SELECT map(1, v) AS result FROM VALUES ('a'), (CAST(NULL AS STRING)) AS t(v)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: integer
         |    |-- value: string (valueContainsNull = true)
        """
      Then query result
        | result      |
        | {1 -> a}    |
        | {1 -> NULL} |
