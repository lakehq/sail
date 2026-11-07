Feature: array() type coercion with mixed element types

  Spark's non-ANSI semantics coerce mixed string/numeric arrays to string,
  not to numeric. DataFusion 54 changed `comparison_coercion` to prefer
  numeric, which would otherwise reject `array('a', 1)` at runtime.

  Rule: Homogeneous arrays preserve their element type

    Scenario Outline: array of <case>
      When query
        """
        SELECT array(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case     | args          | result          |
        | integers | 1, 2, 3       | [1, 2, 3]       |
        | strings  | 'a', 'b', 'c' | [a, b, c]       |
        | doubles  | 1.0, 2.5, 3.5 | [1.0, 2.5, 3.5] |

  Rule: Mixed numeric types coerce to the widest numeric type

    Scenario: integer and double coerce to double
      When query
        """
        SELECT array(1, 2.5) AS result
        """
      Then query result
        | result     |
        | [1.0, 2.5] |

  Rule: Mixed string and numeric types coerce to string (Spark non-ANSI)

    Scenario Outline: <case> coerce to string
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT array(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                          | args             | result         |
        | string and integer            | 'a', 1           | [a, 1]         |
        | string and double             | 'a', 1.5         | [a, 1.5]       |
        | multiple strings and numerics | 'a', 1, 2.5, 'b' | [a, 1, 2.5, b] |

    # With ANSI on, the common type is numeric rather than string, so Spark tries to cast the
    # string element and fails at runtime. Sail also fails, but with a different error message.
    @sail-bug
    Scenario Outline: <case> raises under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT array(<args>) AS result
        """
      Then query error \[CAST_INVALID_INPUT\] The value 'a' of the type "STRING" cannot be cast to "<target>"

      Examples:
        | case                          | args             | target |
        | string and integer            | 'a', 1           | BIGINT |
        | string and double             | 'a', 1.5         | DOUBLE |
        | multiple strings and numerics | 'a', 1, 2.5, 'b' | DOUBLE |

  Rule: NULL elements are preserved during coercion

    Scenario: string, numeric and NULL coerce to string with NULL preserved
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT array('a', 1, NULL, 1.0) AS result
        """
      Then query result
        | result            |
        | [a, 1, NULL, 1.0] |

    @sail-bug
    Scenario: string, numeric and NULL raise under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT array('a', 1, NULL, 1.0) AS result
        """
      Then query error \[CAST_INVALID_INPUT\] The value 'a' of the type "STRING" cannot be cast to "DECIMAL\(21,1\)"

  Rule: Strings promote to the other atomic type under ANSI

    Scenario Outline: array(<args>) under ANSI is an array of <type>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT array(<args>) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: <type> (containsNull = true)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | args                                                 | type      | result                                     |
        | '1', CAST(2 AS TINYINT)                              | long      | [1, 2]                                     |
        | '1', 2                                               | long      | [1, 2]                                     |
        | '1', 2L                                              | long      | [1, 2]                                     |
        | '1', CAST(2.5 AS DECIMAL(3,1))                       | double    | [1.0, 2.5]                                 |
        | '1', CAST(2.5 AS FLOAT)                              | double    | [1.0, 2.5]                                 |
        | '1', 2.5D                                            | double    | [1.0, 2.5]                                 |
        | '1', 2L, 1.5D                                        | double    | [1.0, 2.0, 1.5]                            |
        | 'true', false                                        | boolean   | [true, false]                              |
        | '2020-01-02', DATE'2020-01-01'                       | date      | [2020-01-02, 2020-01-01]                   |
        | '2020-01-02 03:04:05', TIMESTAMP'2020-01-01 00:00:00' | timestamp | [2020-01-02 03:04:05, 2020-01-01 00:00:00] |

  Rule: Strings win over other atomic types without ANSI

    Scenario Outline: array(<args>) without ANSI is an array of strings
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT array(<args>) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = false)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | args                           | result                   |
        | '1', CAST(2 AS TINYINT)        | [1, 2]                   |
        | '1', 2                         | [1, 2]                   |
        | '1', 2L                        | [1, 2]                   |
        | '1', CAST(2.5 AS DECIMAL(3,1)) | [1, 2.5]                 |
        | '1', CAST(2.5 AS FLOAT)        | [1, 2.5]                 |
        | '1', 2.5D                      | [1, 2.5]                 |
        | '2020-01-02', DATE'2020-01-01' | [2020-01-02, 2020-01-01] |

    # Spark rejects BOOLEAN or BINARY with STRING without ANSI (DATA_DIFF_TYPES). Sail accepts it
    # and coerces to strings, as it did before.
    @sail-only
    Scenario Outline: array(<args>) without ANSI is an array of strings in Sail
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT array(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | args          | result        |
        | 'true', false | [true, false] |
        | 'b', X'61'    | [b, a]        |

    Scenario Outline: array(<args>) under ANSI is an array of binary
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT array(<args>) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: binary (containsNull = <contains_null>)
        """

      Examples:
        | args             | contains_null |
        | X'61', 'b'       | false         |
        | 'é', X'80', NULL | true          |

    Scenario: array of strings and binary values that are not valid UTF-8 under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT hex(a[0]) AS h0, hex(a[1]) AS h1, a[2] IS NULL AS n2
        FROM (SELECT array(s, b, NULL) AS a FROM VALUES ('é', X'80') AS t(s, b))
        """
      Then query result
        | h0   | h1 | n2   |
        | C3A9 | 80 | true |

  Rule: DECIMAL with FLOAT or DOUBLE coerces to DOUBLE

    Scenario: array of decimal and floating-point values
      When query
        """
        SELECT
          array(CAST(1 AS DECIMAL(10,2)), 1.5D) AS a,
          array(CAST(1 AS DECIMAL(38,10)), CAST(1.5 AS FLOAT), 2) AS b,
          array(2, CAST(1 AS DECIMAL(5,1)), CAST('NaN' AS DOUBLE)) AS c
        """
      Then query result
        | a          | b               | c               |
        | [1.0, 1.5] | [1.0, 1.5, 2.0] | [2.0, 1.0, NaN] |
