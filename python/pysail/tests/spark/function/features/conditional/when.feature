Feature: when output schema

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
