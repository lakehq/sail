Feature: zeroifnull output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to zeroifnull yields the schema Spark declares
      When query
        """
        SELECT zeroifnull(NULL) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

  Rule: Result values (migrated from test_zeroifnull.txt doctests)

    Scenario Outline: Result values: <case>
      When query
        """
        SELECT zeroifnull(<arg>), typeof(zeroifnull(<arg>))
        """
      Then query result
        | zeroifnull(<arg>) | typeof(zeroifnull(<arg>)) |
        | <value>           | <type>                    |

      Examples:
        | case                            | arg                   | value | type   |
        | zeroifnull doctest #1 (result)  | NULL                  | 0     | int    |
        | zeroifnull doctest #2 (result)  | 2                     | 2     | int    |
        | zeroifnull doctest #6 (result)  | CAST(NULL AS BIGINT)  | 0     | bigint |
        | zeroifnull doctest #9 (result)  | CAST(NULL AS DOUBLE)  | 0.0   | double |
        | zeroifnull doctest #10 (result) | CAST(2.718 AS DOUBLE) | 2.718 | double |
        | zeroifnull doctest #13 (result) | 0                     | 0     | int    |

    Scenario Outline: Result values with type promotion against the zero literal: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT zeroifnull(<arg>), typeof(zeroifnull(<arg>))
        """
      Then query result
        | zeroifnull(<arg>) | typeof(zeroifnull(<arg>)) |
        | <value>           | <type>                    |

      Examples:
        | case                            | arg                          | value             | type          |
        | zeroifnull doctest #3 (result)  | CAST(NULL AS TINYINT)        | 0                 | int           |
        | zeroifnull doctest #4 (result)  | CAST(5 AS TINYINT)           | 5                 | int           |
        | zeroifnull doctest #5 (result)  | CAST(NULL AS SMALLINT)       | 0                 | int           |
        | zeroifnull doctest #7 (result)  | CAST(NULL AS FLOAT)          | 0.0               | double        |
        | zeroifnull doctest #8 (result)  | CAST(3.14 AS FLOAT)          | 3.140000104904175 | double        |
        | zeroifnull doctest #11 (result) | CAST(NULL AS DECIMAL(10,2))  | 0.00              | decimal(12,2) |
        | zeroifnull doctest #12 (result) | CAST(42.99 AS DECIMAL(10,2)) | 42.99             | decimal(12,2) |

    # Kept separate because the derived column name repeats the SQL argument, so the two
    # cannot share one Examples slot.
    Scenario: zeroifnull doctest #14 (result)
      When query
        """
        SELECT zeroifnull(-5), typeof(zeroifnull(-5))
        """
      Then query result
        | zeroifnull(-5) | typeof(zeroifnull(-5)) |
        | -5             | int                    |

  Rule: Column types and non-nullable results

    Scenario Outline: Numeric column coercion for <input_type> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT zeroifnull(CAST(value AS <input_type>)) AS result
        FROM VALUES (1), (2), (0), (NULL) AS t(value)
        """
      Then query schema
        """
        root
         |-- result: <output_type> (nullable = false)
        """
      And query result collected
        | result |
        | <one>  |
        | <two>  |
        | <zero> |
        | <zero> |

      Examples:
        | ansi  | input_type    | output_type   | one  | two  | zero |
        | true  | TINYINT       | integer       | 1    | 2    | 0    |
        | false | TINYINT       | integer       | 1    | 2    | 0    |
        | true  | SMALLINT      | integer       | 1    | 2    | 0    |
        | false | SMALLINT      | integer       | 1    | 2    | 0    |
        | true  | FLOAT         | double        | 1.0  | 2.0  | 0.0  |
        | false | FLOAT         | float         | 1.0  | 2.0  | 0.0  |
        | true  | DECIMAL(10,2) | decimal(12,2) | 1.00 | 2.00 | 0.00 |
        | false | DECIMAL(10,2) | decimal(12,2) | 1.00 | 2.00 | 0.00 |

    Scenario Outline: Decimal column precision for <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      Given config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = false
      When query
        """
        SELECT zeroifnull(value) AS result
        FROM VALUES
          (CAST('<value>' AS <input_type>)),
          (CAST(NULL AS <input_type>))
        AS t(value)
        """
      Then query schema
        """
        root
         |-- result: <output_type> (nullable = false)
        """
      And query result collected
        | result   |
        | <result> |
        | <zero>   |

      Examples:
        | case                  | ansi  | input_type     | value                                    | output_type    | result                                | zero  |
        | integer above 2^53    | true  | DECIMAL(20,0)  | 9007199254740993                          | decimal(20,0)  | 9007199254740993                       | 0     |
        | integer above 2^53    | false | DECIMAL(20,0)  | 9007199254740993                          | decimal(20,0)  | 9007199254740993                       | 0     |
        | scale 38 rounding    | true  | DECIMAL(38,38) | 0.12345678901234567890123456789012345678   | decimal(38,28) | 0.1234567890123456789012345679          | 0E-28 |
        | scale 38 rounding    | false | DECIMAL(38,38) | 0.12345678901234567890123456789012345678   | decimal(38,28) | 0.1234567890123456789012345679          | 0E-28 |
        | rounding carries one | true  | DECIMAL(38,38) | 0.99999999999999999999999999999999999999   | decimal(38,28) | 1.0000000000000000000000000000          | 0E-28 |
        | rounding carries one | false | DECIMAL(38,38) | 0.99999999999999999999999999999999999999   | decimal(38,28) | 1.0000000000000000000000000000          | 0E-28 |
        | scale 30 rounding    | true  | DECIMAL(38,30) | 12345678.123456789012345678901234567890    | decimal(38,28) | 12345678.1234567890123456789012345679   | 0E-28 |
        | scale 30 rounding    | false | DECIMAL(38,30) | 12345678.123456789012345678901234567890    | decimal(38,28) | 12345678.1234567890123456789012345679   | 0E-28 |

  Rule: String coercion follows ANSI mode

    Scenario Outline: Integer strings and NULL with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT zeroifnull(value) AS result
        FROM VALUES ('00'), ('2147483648'), ('9007199254740993'), (NULL) AS t(value)
        """
      Then query schema
        """
        root
         |-- result: <output_type> (nullable = false)
        """
      And query result collected
        | result           |
        | <double_zero>    |
        | 2147483648       |
        | 9007199254740993 |
        | 0                |

      Examples:
        | ansi  | output_type | double_zero |
        | true  | long        | 0           |
        | false | string      | 00          |

    Scenario: Legacy mode preserves fractional and invalid numeric strings
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT zeroifnull(value) AS result
        FROM VALUES ('0.5'), ('abc'), (CAST(NULL AS STRING)) AS t(value)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
      And query result collected
        | result |
        | 0.5    |
        | abc    |
        | 0      |

    Scenario Outline: ANSI mode rejects the non-integer string <value>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT zeroifnull(value) AS result FROM VALUES ('<value>') AS t(value)
        """
      Then query error (?i)(cast|convert|parse|invalid)

      Examples:
        | value |
        | 0.5   |
        | abc   |

  Rule: Unsupported argument types

    Scenario Outline: Reject <input> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT zeroifnull(value) AS result FROM VALUES (<input>) AS t(value)
        """
      Then query error (?i)(type|coerc|cast)

      Examples:
        | ansi  | input             |
        | true  | TRUE              |
        | false | TRUE              |
        | true  | DATE '2024-01-02' |
        | false | DATE '2024-01-02' |
        | true  | array(0, 1)       |
        | false | array(0, 1)       |
