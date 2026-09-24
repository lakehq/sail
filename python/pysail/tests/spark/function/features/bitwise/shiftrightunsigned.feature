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
