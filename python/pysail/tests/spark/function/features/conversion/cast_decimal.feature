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
