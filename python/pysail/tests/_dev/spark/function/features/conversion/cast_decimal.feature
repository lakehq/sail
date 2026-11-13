Feature: Numeric casts to Decimal handle precision overflow

  Rule: Non-ANSI numeric overflow returns NULL

    Scenario Outline: Numeric cast: <value> to <target>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(<value> AS <target>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | value                         | target        | result |
        | 83.14D                        | DECIMAL(4,3)  | NULL   |
        | 97.111D                       | DECIMAL(4,3)  | NULL   |
        | 83.14D                        | DECIMAL(5,3)  | 83.140 |
        | -83.14D                       | DECIMAL(4,3)  | NULL   |
        | 100L                          | DECIMAL(2,0)  | NULL   |
        | -99L                          | DECIMAL(2,0)  | -99    |
        | 99L                           | DECIMAL(2,0)  | 99     |
        | 1.25D                         | DECIMAL(2,1)  | 1.3    |
        | -1.25D                        | DECIMAL(2,1)  | -1.3   |
        | 9.99                          | DECIMAL(2,1)  | NULL   |
        | CAST('NaN' AS DOUBLE)          | DECIMAL(4,3)  | NULL   |
        | CAST('Infinity' AS DOUBLE)     | DECIMAL(4,3)  | NULL   |
        | CAST('-Infinity' AS DOUBLE)    | DECIMAL(4,3)  | NULL   |
        | CAST(NULL AS DOUBLE)          | DECIMAL(4,3)  | NULL   |

    Scenario: A numeric column has a nullable Decimal result
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(id AS DECIMAL(2,0)) AS result FROM range(98, 102)
        """
      Then query schema
        """
        root
         |-- result: decimal(2,0) (nullable = true)
        """
      Then query result
        | result |
        | 98     |
        | 99     |
        | NULL   |
        | NULL   |

  Rule: Safe Decimal conversions preserve input nullability

    Scenario Outline: Non-null <source> safely widens to <target>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(CAST(id AS <source>) AS <target>) AS result FROM range(1)
        """
      Then query schema
        """
        root
         |-- result: <target> (nullable = false)
        """
      Then query result
        | result |
        | 0      |

      Examples:
        | source        | target        |
        | TINYINT       | decimal(3,0)  |
        | SMALLINT      | decimal(5,0)  |
        | INT           | decimal(10,0) |
        | BIGINT        | decimal(20,0) |
        | DECIMAL(20,0) | decimal(21,0) |

    Scenario: BIGINT extrema fit a non-null Decimal result
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(v AS DECIMAL(20,0)) AS result
        FROM VALUES (-9223372036854775807L - 1L), (9223372036854775807L) AS t(v)
        """
      Then query schema
        """
        root
         |-- result: decimal(20,0) (nullable = false)
        """
      Then query result
        | result               |
        | -9223372036854775808 |
        | 9223372036854775807  |

    Scenario: An extra integer digit makes scale reduction safe
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(v AS DECIMAL(3,1)) AS result FROM VALUES (9.99), (1.23) AS t(v)
        """
      Then query schema
        """
        root
         |-- result: decimal(3,1) (nullable = false)
        """
      Then query result
        | result |
        | 10.0   |
        | 1.2    |

    Scenario: Scale reduction without an extra integer digit can overflow
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(v AS DECIMAL(2,1)) AS result FROM VALUES (9.99), (1.23) AS t(v)
        """
      Then query schema
        """
        root
         |-- result: decimal(2,1) (nullable = true)
        """
      Then query result
        | result |
        | NULL   |
        | 1.2    |

    Scenario: Safe widening preserves nullable input
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(v AS DECIMAL(20,0)) AS result
        FROM VALUES (1L), (CAST(NULL AS BIGINT)) AS t(v)
        """
      Then query schema
        """
        root
         |-- result: decimal(20,0) (nullable = true)
        """
      Then query result
        | result |
        | 1      |
        | NULL   |

  Rule: ANSI overflow raises and TRY_CAST returns NULL

    Scenario: ANSI numeric overflow raises
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(83.14D AS DECIMAL(4,3)) AS result
        """
      Then query error .*

    Scenario Outline: TRY_CAST handles overflow with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT TRY_CAST(83.14D AS DECIMAL(4,3)) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | ansi  |
        | true  |
        | false |

  Rule: FLOAT and DOUBLE values round half up from their decimal form

    Scenario Outline: CAST <value> AS <target> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(<value> AS <target>) AS result, TRY_CAST(<value> AS <target>) AS try_result
        """
      Then query result
        | result   | try_result |
        | <result> | <result>   |

      Examples:
        | ansi  | value                       | target         | result                     |
        | false | 0.145D                      | DECIMAL(10,2)  | 0.15                       |
        | true  | 0.145D                      | DECIMAL(10,2)  | 0.15                       |
        | false | -0.145D                     | DECIMAL(10,2)  | -0.15                      |
        | true  | 1.005D                      | DECIMAL(10,2)  | 1.01                       |
        | false | 0.14755D                    | DECIMAL(38,4)  | 0.1476                     |
        | true  | 0.00015D                    | DECIMAL(38,4)  | 0.0002                     |
        | false | 295161.0D                   | DECIMAL(31,15) | 295161.000000000000000     |
        | true  | 1.1E20D                     | DECIMAL(38,4)  | 110000000000000000000.0000 |
        | false | CAST(0.1 AS FLOAT)          | DECIMAL(38,18) | 0.100000001490116120       |
        | true  | CAST('NaN' AS DOUBLE)       | DECIMAL(10,2)  | NULL                       |
        | true  | CAST('-Infinity' AS DOUBLE) | DECIMAL(10,2)  | NULL                       |
        | true  | CAST('NaN' AS FLOAT)        | DECIMAL(10,2)  | NULL                       |

    Scenario: A DOUBLE column rounds like a DOUBLE literal
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(d AS DECIMAL(10,2)) AS result
        FROM VALUES (0.145D), (-0.145D), (9.995D), (CAST(NULL AS DOUBLE)) AS t(d)
        """
      Then query schema
        """
        root
         |-- result: decimal(10,2) (nullable = true)
        """
      Then query result ordered
        | result |
        | 0.15   |
        | -0.15  |
        | 10.00  |
        | NULL   |

    Scenario: A cast over a DOUBLE to DECIMAL cast keeps the column name
      When dataframe for cast of a double to decimal to string
      Then dataframe schema
        """
        root
         |-- d: string (nullable = true)
        """

    Scenario Outline: Rounding up past the precision of <target> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT TRY_CAST(<value> AS <target>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | ansi  | value   | target       |
        | false | 9.995D  | DECIMAL(3,2) |
        | true  | -9.995D | DECIMAL(3,2) |

    Scenario: Rounding up past the precision raises with ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(9.995D AS DECIMAL(3,2)) AS result
        """
      Then query error NUMERIC_VALUE_OUT_OF_RANGE.*9.995 cannot be represented as Decimal\(3, 2\)

    Scenario: Rounding up past the precision returns NULL with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(9.995D AS DECIMAL(3,2)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: String casts to Decimal follow ANSI mode

    Scenario Outline: ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(<value> AS <target>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case               | value                | target        | result |
        | valid string       | '1.25'               | DECIMAL(10,2) | 1.25   |
        | malformed string   | 'abc'                | DECIMAL(10,2) | NULL   |
        | empty string       | ''                   | DECIMAL(10,2) | NULL   |
        | overflowing string | '12345.6'            | DECIMAL(4,1)  | NULL   |
        | NULL string        | CAST(NULL AS STRING) | DECIMAL(10,2) | NULL   |

    Scenario Outline: ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(<value> AS <target>) AS result
        """
      Then query error .*

      Examples:
        | case               | value     | target        |
        | malformed string   | 'abc'     | DECIMAL(10,2) |
        | empty string       | ''        | DECIMAL(10,2) |
        | overflowing string | '12345.6' | DECIMAL(4,1)  |

    Scenario: A string column with malformed values casts to NULL with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(v AS DECIMAL(10,2)) AS result FROM VALUES ('1.5'), ('abc'), ('2') AS t(v)
        """
      Then query result ordered
        | result |
        | 1.50   |
        | NULL   |
        | 2.00   |
