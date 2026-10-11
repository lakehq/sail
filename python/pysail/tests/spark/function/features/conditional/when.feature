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

  # CaseWhen.nullable (conditionalExpressions.scala:191-200) ORs the nullability of every branch
  # VALUE; it never reasons about whether a branch can be reached. DataFusion does: a THEN guarded
  # by `c IS NOT NULL` or `c > 0` is dropped from its nullability, so the two engines disagree
  # unless the schema is forced.
  @function(nullability)
  Rule: CASE nullability follows the branch values, not the reachability of the branches

    Scenario Outline: a nullable column stays nullable behind a guard: <case>
      When query
        """
        SELECT <expr> AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: <type> (nullable = true)
        """

      Examples:
        | case                      | expr                                                          | type    |
        | IS NOT NULL guard, ELSE   | CASE WHEN c IS NOT NULL THEN c ELSE 0 END                     | integer |
        | comparison guard, ELSE    | CASE WHEN c > 0 THEN c ELSE 0 END                             | integer |
        | IS NOT NULL guard, no ELSE | CASE WHEN c IS NOT NULL THEN c END                           | integer |
        | IS NULL first, ELSE value | CASE WHEN c IS NULL THEN 0 ELSE c END                         | integer |
        | two guards cover all rows | CASE WHEN c IS NOT NULL THEN c WHEN c IS NULL THEN 0 END      | integer |
        | simple CASE               | CASE c WHEN 1 THEN c ELSE 0 END                               | integer |
        | widened branch            | CASE WHEN c > 0 THEN CAST(c AS BIGINT) ELSE 0 END             | long    |
        | literal false condition   | CASE WHEN false THEN c ELSE 1 END                             | integer |
        | literal true condition    | CASE WHEN true THEN c ELSE 1 END                              | integer |

    Scenario Outline: a non-null column stays non-nullable behind the same guard: <case>
      When query
        """
        SELECT <expr> AS result FROM VALUES (1), (2) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

      Examples:
        | case                    | expr                                  |
        | IS NOT NULL guard, ELSE | CASE WHEN c IS NOT NULL THEN c ELSE 0 END |
        | comparison guard, ELSE  | CASE WHEN c > 0 THEN c ELSE 0 END     |

    Scenario: a non-null range id stays non-nullable behind an IS NOT NULL guard
      When query
        """
        SELECT CASE WHEN id IS NOT NULL THEN id ELSE 0 END AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    # Spark takes the FIRST literal-true condition and ignores every later branch
    # (conditionalExpressions.scala:191-200).
    Scenario Outline: a literal true condition that is not last makes the CASE non-nullable: <case>
      When query
        """
        SELECT <expr> AS result <source>
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

      Examples:
        | case                         | expr                                                                          | source          |
        | true then a nullable branch  | CASE WHEN true THEN 1 WHEN id > 0 THEN CAST(NULL AS INT) END                  |  FROM range(1)  |
        | two true conditions          | CASE WHEN true THEN 1 WHEN true THEN CAST(NULL AS INT) END                    |                 |
        | true between two conditions  | CASE WHEN id = 0 THEN 1 WHEN true THEN 2 WHEN id = 2 THEN CAST(NULL AS INT) END |  FROM range(3)  |

  # CaseWhen.checkInputDataTypes (conditionalExpressions.scala:204-213) requires EVERY condition to
  # be BOOLEAN, including the ones after a literal `WHEN true`, which never run. Dropping the
  # unreachable branches must not drop that check.
  Rule: a condition after a literal true is still type checked

    Scenario Outline: a non-boolean condition after WHEN true is rejected: <case>
      When query
        """
        SELECT CASE WHEN true THEN 1 WHEN <condition> THEN 2 END AS result
        """
      Then query error (?i)boolean

      Examples:
        | case              | condition |
        | string            | 'abc'     |
        | array             | array(1)  |
        | decimal           | 1.5       |

    # Pre-existing: `main` accepts an INT condition where Spark raises UNEXPECTED_INPUT_TYPE.
    @sail-bug
    Scenario Outline: an integer or untyped-null condition after WHEN true is rejected: <case>
      When query
        """
        SELECT CASE WHEN true THEN 1 WHEN <condition> THEN 2 END AS result
        """
      Then query error (?i)boolean

      Examples:
        | case        | condition        |
        | int         | 5                |
        | typed null  | CAST(NULL AS INT) |
