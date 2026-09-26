Feature: shiftright output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to shiftright yields the schema Spark declares
      When query
        """
        SELECT shiftright(4, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a non-null column input to shiftright yields the schema Spark declares
      When query
        """
        SELECT shiftright(CAST(id AS INT), 1) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a nullable column input to shiftright stays nullable
      When query
        """
        SELECT shiftright(c, 1) AS result FROM VALUES (4), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Shift counts

    Scenario Outline: shiftright keeps the INT type for a widened IF shift count with ANSI <ansi_mode>
      Given config spark.sql.ansi.enabled = <ansi_mode>
      When query
        """
        SELECT id, result, typeof(result) AS result_type
        FROM (SELECT id, -64 >> IF(id = 0, 2, id) AS result FROM range(2)) AS q
        ORDER BY id
        """
      Then query result
        | id | result | result_type |
        | 0  | -16    | int         |
        | 1  | -32    | int         |

      Examples:
        | ansi_mode |
        | false     |
        | true      |
