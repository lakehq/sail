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

  @function(nullability)
  Rule: A case is nullable where it can fall through to no branch

    Scenario: an ELSE gives the case a value for every row
      When query
        """
        SELECT CASE WHEN id > 1 THEN 'big' ELSE 'small' END AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a case without an ELSE falls through to NULL
      When query
        """
        SELECT CASE WHEN id > 1 THEN 'big' END AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a NULL in the ELSE makes the case nullable
      When query
        """
        SELECT CASE WHEN id > 1 THEN 'big' ELSE NULL END AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable condition does not make the case nullable
      When query
        """
        SELECT CASE WHEN b = 'x' THEN 1 ELSE 2 END AS result
        FROM VALUES ('x'), (NULL) AS t(b)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: a branch guarded by a true literal is the value the case ends with
      When query
        """
        SELECT CASE WHEN id > 1 THEN 'big' WHEN true THEN 'small' END AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable branch before a true literal keeps the case nullable
      When query
        """
        SELECT CASE WHEN id > 1 THEN NULL WHEN true THEN 'small' END AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  @function(nullability)
  Rule: A case reports the type its branches widen to

    @sail-bug
    Scenario: an ELSE wider than the branch widens the type of the case
      When query
        """
        SELECT CASE WHEN id > 1 THEN 1 ELSE 2.5 END AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: decimal(11,1) (nullable = false)
        """

    @sail-bug
    Scenario: a branch wider than the ELSE widens the type of the case
      When query
        """
        SELECT CASE WHEN id > 1 THEN 2.5 ELSE 1 END AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: decimal(11,1) (nullable = false)
        """

  @function(nullability)
  Rule: The branches of a case widen to one type

    # Spark casts every branch value and the ELSE to the type they widen to
    # (`CaseWhenTypeCoercion`), so the case reports that type. Sail reports the type of the first
    # branch instead and leaves the values to widen on their own, so the schema can name a type
    # the rows do not have. These all report the same wrong type on `main`.

    @sail-bug
    Scenario: an integral branch widens with a longer one
      When query
        """
        SELECT CASE WHEN id > 0 THEN 1 ELSE 2L END AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    @sail-bug
    Scenario: an integral branch widens with a floating one
      When query
        """
        SELECT CASE WHEN id > 0 THEN 1 ELSE 2.5D END AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = false)
        """

    @sail-bug
    Scenario: a float branch widens with a double one
      When query
        """
        SELECT CASE WHEN id > 0 THEN CAST(1 AS FLOAT) ELSE 2.5D END AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = false)
        """

    @sail-bug
    Scenario: two decimal branches widen to one that holds both
      When query
        """
        SELECT CASE WHEN id > 0 THEN CAST(1 AS DECIMAL(4,2)) ELSE CAST(2 AS DECIMAL(8,1)) END AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: decimal(9,2) (nullable = true)
        """

    @sail-bug
    Scenario: the branches widen inside an array
      When query
        """
        SELECT CASE WHEN id > 0 THEN ARRAY(1) ELSE ARRAY(2L) END AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: long (containsNull = false)
        """

    @sail-bug
    Scenario: the branches widen inside a struct
      When query
        """
        SELECT CASE WHEN id > 0 THEN named_struct('n', 1) ELSE named_struct('n', 2L) END AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- n: long (nullable = false)
        """

    @sail-bug
    Scenario: the branches widen inside a map
      When query
        """
        SELECT CASE WHEN id > 0 THEN map('k', 1) ELSE map('k', 2L) END AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: string
         |    |-- value: long (valueContainsNull = false)
        """

    @sail-bug
    Scenario: a date branch widens with a timestamp one
      When query
        """
        SELECT CASE WHEN id > 0 THEN DATE '2020-01-01' ELSE TIMESTAMP '2020-02-02 00:00:00' END AS result FROM range(2)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    @sail-bug
    Scenario: a string that is not a number fails the widening to an integral branch
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CASE WHEN id > 0 THEN 1 ELSE 'x' END AS result FROM range(2)
        """
      Then query error (?s).*CAST_INVALID_INPUT.*

  @function(nullability)
  Rule: A branch guarded by a true literal ends the case without removing the others

    # `CaseWhen.nullable` stops at the first branch guarded by a true literal and never reads the
    # `ELSE`, so that branch is the value the case ends with. The branches after it are still part
    # of the case: its type is merged over all of them and each of their conditions is checked
    # (`inputTypesForMerging`, `checkInputDataTypes`), so none of them may be dropped.

    Scenario: a true literal ends the case even where an ELSE was written
      When query
        """
        SELECT CASE WHEN id > 1 THEN 'a' WHEN true THEN 'b' ELSE NULL END AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a true literal in the first branch ends the case as well
      # TODO: a branch after the true literal is unreachable, and Spark reads the case that way
      #   when it decides whether it can be NULL. Sail declares the value the case ends with as
      #   the `ELSE`, which is what DataFusion reads, but DataFusion also reads the branches after
      #   it, and dropping them is what the scenarios below forbid: they are what gives the case
      #   its type and what gets type checked. Saying it needs a nullability of its own rather
      #   than one inferred from the branches. The same query reports the same on `main`.
      When query
        """
        SELECT CASE WHEN true THEN 'x' WHEN id > 1 THEN NULL END AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a branch after the true literal still gives the case its type
      When query
        """
        SELECT CASE WHEN id > 1 THEN NULL WHEN true THEN NULL WHEN id > 0 THEN 5 END AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a branch after the true literal is still checked against the others
      When query
        """
        SELECT CASE WHEN id > 1 THEN 1 WHEN true THEN 2 WHEN id > 0 THEN ARRAY(1) END AS result FROM range(3)
        """
      Then query error (?s).*(DATA_DIFF_TYPES|Failed to coerce).*
