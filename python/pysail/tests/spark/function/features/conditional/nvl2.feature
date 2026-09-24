Feature: nvl2 output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to nvl2 yields the schema Spark declares
      When query
        """
        SELECT nvl2(NULL, 2, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: nvl2 preserves the declared nullability of its result branches
      When query
        """
        SELECT nvl2(x, x, 0) AS result
        FROM VALUES (1), (CAST(NULL AS INT)) AS t(x)
        ORDER BY result
        """
      Then query result
        | result |
        | 0      |
        | 1      |
      And query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: nvl2 preserves non-nullability when legacy temporal branches become strings
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT nvl2(NULL, DATE '2024-01-01', '2024-02-03') AS result
        """
      Then query result
        | result     |
        | 2024-02-03 |
      And query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: nvl2 preserves nullable temporal branches when converting them to strings
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT nvl2(id, d, '2024-02-03') AS result
        FROM VALUES
          (1, CAST(NULL AS DATE)),
          (CAST(NULL AS INT), DATE '2024-01-01')
        AS t(id, d)
        """
      Then query result
        | result     |
        | NULL       |
        | 2024-02-03 |
      And query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Result type

    Scenario: nvl2 is typed by its result arguments when the tested argument is a widened CASE
      When query
        """
        SELECT
          id,
          nvl2(CASE WHEN id = 0 THEN 1 WHEN id = 1 THEN CAST(2 AS BIGINT) END, 1, 0) AS result,
          typeof(nvl2(CASE WHEN id = 0 THEN 1 WHEN id = 1 THEN CAST(2 AS BIGINT) END, 1, 0)) AS result_type
        FROM VALUES (0), (1), (2) AS t(id)
        """
      Then query result
        | id | result | result_type |
        | 0  | 1      | int         |
        | 1  | 1      | int         |
        | 2  | 0      | int         |

    Scenario Outline: nvl2 exposes its common nonnumeric result type: <case>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(nvl2(NULL, <first_branch>, <second_branch>)) AS result_type
        """
      Then query result
        | result_type   |
        | <result_type> |

      Examples:
        | case                  | ansi  | first_branch                       | second_branch                      | result_type   |
        | DATE and TIMESTAMP_NTZ | false | DATE '2024-01-01'                  | TIMESTAMP_NTZ '2024-02-03 04:05:06' | timestamp_ntz |
        | DATE and TIMESTAMP_NTZ | true  | DATE '2024-01-01'                  | TIMESTAMP_NTZ '2024-02-03 04:05:06' | timestamp_ntz |
        | TIMESTAMP_NTZ and LTZ  | false | TIMESTAMP_NTZ '2024-01-01 00:00:00' | TIMESTAMP_LTZ '2024-02-03 04:05:06' | timestamp     |
        | TIMESTAMP_NTZ and LTZ  | true  | TIMESTAMP_NTZ '2024-01-01 00:00:00' | TIMESTAMP_LTZ '2024-02-03 04:05:06' | timestamp     |
        | STRING and BINARY     | true  | 'a'                                | X'62'                              | binary        |

    Scenario: nvl2 declares the common timestamp type in its output schema
      When query
        """
        SELECT nvl2(
          NULL,
          TIMESTAMP_NTZ '2024-01-01 00:00:00',
          TIMESTAMP_LTZ '2024-02-03 04:05:06'
        ) AS result
        """
      Then query result
        | result              |
        | 2024-02-03 04:05:06 |
      And query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """
