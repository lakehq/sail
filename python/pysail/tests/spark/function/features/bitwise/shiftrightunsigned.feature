Feature: shiftrightunsigned output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to shiftrightunsigned yields the schema Spark declares
      When query
        """
        SELECT shiftrightunsigned(4, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to shiftrightunsigned yields the schema Spark declares
      When query
        """
        SELECT shiftrightunsigned(CAST(id AS INT), 1) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to shiftrightunsigned stays nullable
      When query
        """
        SELECT shiftrightunsigned(c, 1) AS result FROM VALUES (4), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Implicit casts

    Scenario Outline: shiftrightunsigned preserves supported decimal CASE magnitudes
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT id, shiftrightunsigned(
          CASE WHEN id = 0 THEN 8 ELSE CAST(<value> AS DECIMAL(11,1)) END, 1
        ) AS result
        FROM VALUES (0), (1) AS t(id)
        ORDER BY id
        """
      Then query result
        | id | result   |
        | 0  | 4        |
        | 1  | <result> |

      Examples:
        | value        | result     |
        | 3000000000   | 1500000000 |
        | -3000000000  | 647483648  |
        | 4294967295   | 2147483647 |
        | -4294967295  | 0          |
        | -4294967296  | 0          |
        | 3000000000.9 | 1500000000 |

    Scenario Outline: shiftrightunsigned truncates negative decimal fractions before checking their sign with ANSI <ansi_mode>
      Given config spark.sql.ansi.enabled = <ansi_mode>
      When query
        """
        SELECT id, shiftrightunsigned(
          CASE WHEN id = 0 THEN 1 ELSE value END, 1
        ) AS result
        FROM VALUES
          (0, CAST(0 AS DECIMAL(12,2))),
          (1, CAST(-0.5 AS DECIMAL(12,2))),
          (2, CAST(-1.5 AS DECIMAL(12,2))),
          (3, CAST(NULL AS DECIMAL(12,2)))
        AS t(id, value)
        ORDER BY id
        """
      Then query result
        | id | result     |
        | 0  | 0          |
        | 1  | 0          |
        | 2  | 2147483647 |
        | 3  | NULL       |

      Examples:
        | ansi_mode |
        | false     |
        | true      |

    @sail-bug
    Scenario: shiftrightunsigned saturates an out-of-range DOUBLE input with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT id, shiftrightunsigned(
          CASE WHEN id = 0 THEN 8 ELSE CAST(3000000000 AS DOUBLE) END, 1
        ) AS result
        FROM VALUES (0), (1) AS t(id)
        ORDER BY id
        """
      Then query result
        | id | result     |
        | 0  | 4          |
        | 1  | 1073741823 |

    Scenario Outline: shiftrightunsigned preserves numeric CASE inputs: <kind> with ANSI <ansi_mode>
      Given config spark.sql.ansi.enabled = <ansi_mode>
      When query
        """
        SELECT id, result, typeof(result) AS result_type
        FROM (
          SELECT id, shiftrightunsigned(
            CASE
              WHEN id = 0 THEN 8
              WHEN id = 1 THEN <other>
              WHEN id = 2 THEN -8
              ELSE -<other>
            END, 1
          ) AS result
          FROM VALUES (0), (1), (2), (3) AS t(id)
        ) AS q
        ORDER BY id
        """
      Then query result
        | id | result           | result_type   |
        | 0  | 4                | <result_type> |
        | 1  | <positive_other> | <result_type> |
        | 2  | <negative_eight> | <result_type> |
        | 3  | <negative_other> | <result_type> |

      Examples:
        | kind    | ansi_mode | other                     | result_type | positive_other | negative_eight      | negative_other      |
        | DECIMAL | false     | 2.5                       | int         | 1              | 2147483644          | 2147483647          |
        | DECIMAL | true      | 2.5                       | int         | 1              | 2147483644          | 2147483647          |
        | FLOAT   | false     | CAST(2.5 AS FLOAT)         | int         | 1              | 2147483644          | 2147483647          |
        | FLOAT   | true      | CAST(2.5 AS FLOAT)         | int         | 1              | 2147483644          | 2147483647          |
        | DOUBLE  | false     | CAST(2.5 AS DOUBLE)        | int         | 1              | 2147483644          | 2147483647          |
        | DOUBLE  | true      | CAST(2.5 AS DOUBLE)        | int         | 1              | 2147483644          | 2147483647          |
        | BIGINT  | false     | CAST(3000000000 AS BIGINT) | bigint      | 1500000000     | 9223372036854775804 | 9223372035354775808 |
        | BIGINT  | true      | CAST(3000000000 AS BIGINT) | bigint      | 1500000000     | 9223372036854775804 | 9223372035354775808 |

    Scenario Outline: shiftrightunsigned accepts the INT minimum from numeric CASE inputs: <kind> with ANSI <ansi_mode>
      Given config spark.sql.ansi.enabled = <ansi_mode>
      When query
        """
        SELECT id, result, typeof(result) AS result_type
        FROM (
          SELECT id, shiftrightunsigned(
            CASE WHEN id = 0
                 THEN CAST(-2147483648 AS INT)
                 ELSE <other>
            END, 1
          ) AS result
          FROM VALUES (0), (1) AS t(id)
        ) AS q
        ORDER BY id
        """
      Then query result
        | id | result     | result_type |
        | 0  | 1073741824 | int         |
        | 1  | 1073741824 | int         |

      Examples:
        | kind    | ansi_mode | other                                 |
        | DECIMAL | false     | CAST(-2147483648 AS DECIMAL(11,1))     |
        | DECIMAL | true      | CAST(-2147483648 AS DECIMAL(11,1))     |
        | FLOAT   | false     | CAST(-2147483648 AS FLOAT)             |
        | FLOAT   | true      | CAST(-2147483648 AS FLOAT)             |
        | DOUBLE  | false     | CAST(-2147483648 AS DOUBLE)            |
        | DOUBLE  | true      | CAST(-2147483648 AS DOUBLE)            |

    @sail-bug
    Scenario: shiftrightunsigned wraps decimals beyond the supported unsigned range
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT id, shiftrightunsigned(
          CASE WHEN id = 0 THEN 8 ELSE CAST(4294967296 AS DECIMAL(11,1)) END, 1
        ) AS result
        FROM VALUES (0), (1) AS t(id)
        ORDER BY id
        """
      Then query result
        | id | result |
        | 0  | 4      |
        | 1  | 0      |

  Rule: Shift counts

    @sail-bug
    Scenario Outline: shiftrightunsigned masks the shift count for <kind> inputs
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT shiftrightunsigned(CAST(<value> AS <kind>), <shift>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | kind   | value | shift | result |
        | INT    | 8     | 32    | 8      |
        | BIGINT | -1    | -1    | 1      |

    @sail-bug
    Scenario Outline: shiftrightunsigned preserves the sign bit when shifting <kind> by zero
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT shiftrightunsigned(CAST(-1 AS <kind>), 0) AS result
        """
      Then query result
        | result |
        | -1     |

      Examples:
        | kind   |
        | INT    |
        | BIGINT |
