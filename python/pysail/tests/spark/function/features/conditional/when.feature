Feature: when output schema

  Rule: Numeric CASE branches share a type before downstream planning

    Scenario Outline: CASE widens numeric branches in either order with ANSI <ansi>: <type>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT result, reversed, typeof(result) AS result_type, typeof(reversed) AS reversed_type
        FROM (
          SELECT CASE WHEN p THEN a ELSE b END AS result,
                 CASE WHEN p THEN b ELSE a END AS reversed
          FROM VALUES (true, <left>, <right>), (false, <left>, <right>), (NULL, NULL, NULL) AS t(p, a, b)
        )
        """
      Then query result collected
        | result        | reversed      | result_type | reversed_type |
        | <left_value>  | <right_value> | <type>      | <type>        |
        | <right_value> | <left_value>  | <type>      | <type>        |
        | NULL          | NULL          | <type>      | <type>        |
      And query schema
        """
        root
         |-- result: <schema_type> (nullable = true)
         |-- reversed: <schema_type> (nullable = true)
         |-- result_type: string (nullable = false)
         |-- reversed_type: string (nullable = false)
        """

      Examples:
        | ansi  | left   | right       | left_value | right_value | type          | schema_type   |
        | false | 1      | 4294967296L | 1          | 4294967296  | bigint        | long          |
        | true  | 1      | 4294967296L | 1          | 4294967296  | bigint        | long          |
        | false | 1      | 1.5D        | 1.0        | 1.5         | double        | double        |
        | true  | 1      | 1.5D        | 1.0        | 1.5         | double        | double        |
        | false | 1      | 1.5BD       | 1.0        | 1.5         | decimal(11,1) | decimal(11,1) |
        | true  | 1      | 1.5BD       | 1.0        | 1.5         | decimal(11,1) | decimal(11,1) |
        | false | 1      | 1.5F        | 1.0        | 1.5         | float         | float         |
        | true  | 1      | 1.5F        | 1.0        | 1.5         | double        | double        |
        | false | 1.25BD | 1.5D        | 1.25       | 1.5         | double        | double        |
        | true  | 1.25BD | 1.5D        | 1.25       | 1.5         | double        | double        |
        | false | 1.25BD | 1.5F        | 1.25       | 1.5         | double        | double        |
        | true  | 1.25BD | 1.5F        | 1.25       | 1.5         | double        | double        |

    Scenario Outline: CASE considers every numeric result branch and preserves nulls with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          CASE WHEN id = 0 THEN 1 WHEN id = 1 THEN 4294967296L WHEN id = 2 THEN 2 ELSE NULL END AS multiple,
          CASE WHEN id = 0 THEN 1 WHEN id = 1 THEN 4294967296L END AS omitted,
          CASE WHEN id = 0 THEN NULL WHEN id = 1 THEN 1 ELSE 4294967296L END AS null_first,
          CASE id WHEN 0 THEN 1 WHEN 1 THEN if(id = 1, 4294967296L, 2) ELSE NULL END AS nested
        FROM VALUES (0), (1), (2), (3) AS t(id)
        """
      Then query result collected
        | multiple   | omitted    | null_first | nested     |
        | 1          | 1          | NULL       | 1          |
        | 4294967296 | 4294967296 | 1          | 4294967296 |
        | 2          | NULL       | 4294967296 | NULL       |
        | NULL       | NULL       | 4294967296 | NULL       |
      And query schema
        """
        root
         |-- multiple: long (nullable = true)
         |-- omitted: long (nullable = true)
         |-- null_first: long (nullable = true)
         |-- nested: long (nullable = true)
        """

      Examples:
        | ansi  |
        | false |
        | true  |

    Scenario Outline: Generators collect sequences with widened conditional bounds: <generator>, <stop>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT c, i FROM (
          SELECT c, <generator>(sequence(0, <stop>)) AS <aliases>
          FROM VALUES (-1L), (1L), (3L) AS t(c)
        )
        """
      Then query result collected
        | c  | i |
        | -1 | 0 |
        | 1  | 0 |
        | 3  | 0 |
        | 3  | 1 |
        | 3  | 2 |

      Examples:
        | ansi  | generator     | stop                                      | aliases  |
        | false | explode       | CASE WHEN c <= 0 THEN 1 ELSE c END - 1     | i        |
        | true  | explode       | CASE WHEN c <= 0 THEN 1 ELSE c END - 1     | i        |
        | false | posexplode    | CASE WHEN c <= 0 THEN 1 ELSE c END - 1     | (pos, i) |
        | true  | posexplode    | CASE WHEN c <= 0 THEN 1 ELSE c END - 1     | (pos, i) |
        | false | explode_outer | if(c <= 0, 1, c) - 1                      | i        |
        | true  | explode_outer | if(c <= 0, 1, c) - 1                      | i        |
        | false | explode       | CASE WHEN c <= 0 THEN 0 ELSE c - 1 END     | i        |
        | true  | explode       | CASE WHEN c <= 0 THEN 0 ELSE c - 1 END     | i        |

  Rule: Spark-compatible coercion for mixed string and temporal branches

    Scenario: CASE coerces date branches to string and remains usable by to_date when ANSI is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          id,
          CASE WHEN use_override THEN '2026-03-31' ELSE period_end END AS mapped,
          to_date(CASE WHEN use_override THEN '2026-03-31' ELSE period_end END) AS parsed,
          typeof(CASE WHEN use_override THEN '2026-03-31' ELSE period_end END) AS mapped_type
        FROM VALUES
          (1, true, DATE '2026-02-20'),
          (2, false, DATE '2025-12-01'),
          (3, false, CAST(NULL AS DATE))
        AS t(id, use_override, period_end)
        ORDER BY id
        """
      Then query result
        | id | mapped     | parsed     | mapped_type |
        | 1  | 2026-03-31 | 2026-03-31 | string      |
        | 2  | 2025-12-01 | 2025-12-01 | string      |
        | 3  | NULL       | NULL       | string      |

    Scenario: CASE exposes the Spark string schema for mixed string and date branches
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CASE WHEN use_override THEN '2026-03-31' ELSE period_end END AS result
        FROM VALUES
          (true, DATE '2026-02-20'),
          (false, CAST(NULL AS DATE))
        AS t(use_override, period_end)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario Outline: ANSI CASE widens mixed temporal branches independently of branch order: <case>
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT CASE
          WHEN id = 0 THEN DATE '2024-01-01'
          <first_branch>
          <second_branch>
          ELSE '2024-01-01 12:34:56+02:00'
        END AS result
        FROM VALUES (3) AS t(id)
        """
      Then query result
        | result              |
        | 2024-01-01 02:34:56 |
      And query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

      Examples:
        | case      | first_branch                                                     | second_branch                                                   |
        | NTZ first | WHEN id = 1 THEN TIMESTAMP_NTZ '2024-01-01 00:00:00'             | WHEN id = 2 THEN TIMESTAMP_LTZ '2024-01-01 00:00:00+00:00'      |
        | LTZ first | WHEN id = 2 THEN TIMESTAMP_LTZ '2024-01-01 00:00:00+00:00'        | WHEN id = 1 THEN TIMESTAMP_NTZ '2024-01-01 00:00:00'            |

  @function(nullability)
  Rule: Output schema

    # TODO: Fix pre-existing CASE nullability: ELSE is encoded as WHEN true with no else_expr.
    @sail-bug
    Scenario: a non-null literal input to when yields the schema Spark declares
      When query
        """
        SELECT CASE WHEN 1 > 0 THEN 1 WHEN 2 > 0 THEN 2.0 ELSE 1.2 END AS result
        """
      Then query schema
        """
        root
         |-- result: decimal(11,1) (nullable = false)
        """
