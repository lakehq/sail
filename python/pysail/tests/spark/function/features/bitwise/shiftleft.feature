Feature: shiftleft output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to shiftleft yields the schema Spark declares
      When query
        """
        SELECT shiftleft(2, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column input to shiftleft yields the schema Spark declares
      When query
        """
        SELECT shiftleft(CAST(id AS INT), 1) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to shiftleft stays nullable
      When query
        """
        SELECT shiftleft(c, 1) AS result FROM VALUES (2), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Shift counts

    Scenario Outline: shiftleft keeps the INT type for a widened CASE shift count with ANSI <ansi_mode>
      Given config spark.sql.ansi.enabled = <ansi_mode>
      When query
        """
        SELECT id, result, typeof(result) AS result_type
        FROM (
          SELECT id, shiftleft(1, CASE WHEN id = 0 THEN 1 ELSE id + 1 END) AS result
          FROM range(2)
        ) AS q
        ORDER BY id
        """
      Then query result
        | id | result | result_type |
        | 0  | 2      | int         |
        | 1  | 4      | int         |

      Examples:
        | ansi_mode |
        | false     |
        | true      |
