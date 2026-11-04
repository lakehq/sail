Feature: nullifzero output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to nullifzero yields the schema Spark declares
      When query
        """
        SELECT nullifzero(0) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to nullifzero yields the schema Spark declares
      When query
        """
        SELECT nullifzero(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to nullifzero stays nullable
      When query
        """
        SELECT nullifzero(c) AS result FROM VALUES (0), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Result values (migrated from test_nullifzero.txt doctests)

    Scenario Outline: Result values: <case>
      When query
        """
        SELECT nullifzero(<arg>), typeof(nullifzero(<arg>))
        """
      Then query result
        | nullifzero(<arg>) | typeof(nullifzero(<arg>)) |
        | <value>           | <type>                    |

      Examples:
        | case                            | arg                         | value | type          |
        | nullifzero doctest #1 (result)  | 0                           | NULL  | int           |
        | nullifzero doctest #2 (result)  | 2                           | 2     | int           |
        | nullifzero doctest #3 (result)  | CAST(0 AS TINYINT)          | NULL  | tinyint       |
        | nullifzero doctest #4 (result)  | CAST(2 AS TINYINT)          | 2     | tinyint       |
        | nullifzero doctest #5 (result)  | CAST(0 AS SMALLINT)         | NULL  | smallint      |
        | nullifzero doctest #6 (result)  | CAST(0 AS BIGINT)           | NULL  | bigint        |
        | nullifzero doctest #7 (result)  | CAST(0.0 AS FLOAT)          | NULL  | float         |
        | nullifzero doctest #8 (result)  | CAST(1.5 AS FLOAT)          | 1.5   | float         |
        | nullifzero doctest #9 (result)  | CAST(0.0 AS DOUBLE)         | NULL  | double        |
        | nullifzero doctest #10 (result) | CAST(2.5 AS DOUBLE)         | 2.5   | double        |
        | nullifzero doctest #11 (result) | CAST(0.00 AS DECIMAL(10,2)) | NULL  | decimal(10,2) |
        | nullifzero doctest #12 (result) | CAST(5.25 AS DECIMAL(10,2)) | 5.25  | decimal(10,2) |

    # An untyped NULL literal keeps the NullType through `nullifzero`, so Spark reports `void`.
    Scenario: nullifzero doctest #14 (result)
      When query
        """
        SELECT nullifzero(NULL), typeof(nullifzero(NULL))
        """
      Then query result
        | nullifzero(NULL) | typeof(nullifzero(NULL)) |
        | NULL             | void                     |

    # Kept separate because the derived column name repeats the SQL argument, so the two
    # cannot share one Examples slot.
    Scenario: nullifzero doctest #13 (result)
      When query
        """
        SELECT nullifzero(-1), typeof(nullifzero(-1))
        """
      Then query result
        | nullifzero(-1) | typeof(nullifzero(-1)) |
        | -1             | int                    |

  Rule: Decimal columns preserve their type and precision

    Scenario Outline: Preserve <input_type> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT nullifzero(value) AS result
        FROM VALUES
          (CAST('<value>' AS <input_type>)),
          (CAST(NULL AS <input_type>))
        AS t(value)
        """
      Then query schema
        """
        root
         |-- result: <output_type> (nullable = true)
        """
      And query result collected
        | result   |
        | <value>  |
        | NULL     |

      Examples:
        | ansi  | input_type     | output_type    | value                                  |
        | true  | DECIMAL(20,0)  | decimal(20,0)  | 9007199254740993                        |
        | false | DECIMAL(20,0)  | decimal(20,0)  | 9007199254740993                        |
        | true  | DECIMAL(38,38) | decimal(38,38) | 0.12345678901234567890123456789012345678 |
        | false | DECIMAL(38,38) | decimal(38,38) | 0.12345678901234567890123456789012345678 |
        | true  | DECIMAL(38,30) | decimal(38,30) | 12345678.123456789012345678901234567890  |
        | false | DECIMAL(38,30) | decimal(38,30) | 12345678.123456789012345678901234567890  |

  Rule: String comparisons preserve the original string result

    Scenario Outline: Integer string columns with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT nullifzero(value) AS result
        FROM VALUES ('00'), ('2147483648'), ('9007199254740993'), (NULL) AS t(value)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
      And query result collected
        | result           |
        | NULL             |
        | 2147483648       |
        | 9007199254740993 |
        | NULL             |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario: Legacy string comparison truncates fractions and tolerates failed casts
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT nullifzero(value) AS result
        FROM VALUES ('0.5'), ('abc'), ('2147483648'), (CAST(NULL AS STRING)) AS t(value)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
      And query result collected
        | result     |
        | NULL       |
        | abc        |
        | 2147483648 |
        | NULL       |

    Scenario Outline: ANSI mode rejects the non-integer string <value>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT nullifzero(value) AS result FROM VALUES ('<value>') AS t(value)
        """
      Then query error (?i)(cast|convert|parse|invalid)

      Examples:
        | value |
        | 0.5   |
        | abc   |

  Rule: Boolean comparison is available only in legacy mode

    Scenario: Legacy Boolean columns preserve TRUE and replace FALSE with NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT nullifzero(value) AS result
        FROM VALUES (TRUE), (FALSE), (CAST(NULL AS BOOLEAN)) AS t(value)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
      And query result collected
        | result |
        | true   |
        | NULL   |
        | NULL   |

    Scenario Outline: Reject <input> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT nullifzero(value) AS result FROM VALUES (<input>) AS t(value)
        """
      Then query error (?i)(type|coerc|cast|compar)

      Examples:
        | ansi  | input             |
        | true  | TRUE              |
        | true  | FALSE             |
        | true  | DATE '2024-01-02' |
        | false | DATE '2024-01-02' |
        | true  | array(0, 1)       |
        | false | array(0, 1)       |
