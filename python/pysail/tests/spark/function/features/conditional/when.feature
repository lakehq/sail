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

  Rule: Spark-compatible coercion for numeric branches

    Scenario Outline: CASE widens numeric branches to the Spark common type with ANSI enabled: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          id,
          CASE WHEN id = 0 THEN <first_branch> ELSE <second_branch> END AS result,
          typeof(CASE WHEN id = 0 THEN <first_branch> ELSE <second_branch> END) AS result_type
        FROM VALUES (0), (1) AS t(id)
        """
      Then query result
        | id | result         | result_type   |
        | 0  | <first_value>  | <result_type> |
        | 1  | <second_value> | <result_type> |

      Examples:
        | case                        | first_branch                | second_branch               | first_value   | second_value | result_type   |
        | INT then BIGINT             | 1                           | CAST(3000000000 AS BIGINT)  | 1             | 3000000000   | bigint        |
        | BIGINT then INT             | CAST(3000000000 AS BIGINT)  | 1                           | 3000000000    | 1            | bigint        |
        | SMALLINT then BIGINT        | CAST(1 AS SMALLINT)         | CAST(2 AS BIGINT)           | 1             | 2            | bigint        |
        | INT then DOUBLE             | 1                           | CAST(1.5 AS DOUBLE)         | 1.0           | 1.5          | double        |
        | INT then FLOAT              | 1                           | CAST(1.5 AS FLOAT)          | 1.0           | 1.5          | double        |
        | INT then DECIMAL            | 1                           | CAST(1.75 AS DECIMAL(10,2)) | 1.00          | 1.75         | decimal(12,2) |
        | INT then wider DECIMAL      | 1                           | CAST(1 AS DECIMAL(20,0))    | 1             | 1            | decimal(20,0) |
        | BIGINT then DECIMAL         | CAST(3000000000 AS BIGINT)  | CAST(1.25 AS DECIMAL(5,2))  | 3000000000.00 | 1.25         | decimal(22,2) |
        | FLOAT then DECIMAL          | CAST(1.5 AS FLOAT)          | CAST(1.25 AS DECIMAL(5,2))  | 1.5           | 1.25         | double        |

    @spark-4.0
    Scenario: CASE keeps integral digits when the common DECIMAL precision exceeds the maximum
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          id,
          CASE WHEN id = 0 THEN CAST(1.5 AS DECIMAL(38,10)) ELSE CAST(1 AS DECIMAL(38,0)) END AS result,
          typeof(CASE WHEN id = 0 THEN CAST(1.5 AS DECIMAL(38,10)) ELSE CAST(1 AS DECIMAL(38,0)) END) AS result_type
        FROM VALUES (0), (1) AS t(id)
        """
      Then query result
        | id | result | result_type   |
        | 0  | 2      | decimal(38,0) |
        | 1  | 1      | decimal(38,0) |

    Scenario: CASE widens integral and FLOAT branches to FLOAT with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          id,
          CASE WHEN id = 0 THEN 1 ELSE CAST(1.5 AS FLOAT) END AS result,
          typeof(CASE WHEN id = 0 THEN 1 ELSE CAST(1.5 AS FLOAT) END) AS result_type
        FROM VALUES (0), (1) AS t(id)
        """
      Then query result
        | id | result | result_type |
        | 0  | 1.0    | float       |
        | 1  | 1.5    | float       |

    Scenario: CASE widens numeric branches in order across all branches
      When query
        """
        SELECT
          id,
          CASE
            WHEN id = 0 THEN CAST(1 AS TINYINT)
            WHEN id = 1 THEN NULL
            WHEN id = 2 THEN 2
            ELSE CAST(2.5 AS DECIMAL(3,1))
          END AS result,
          typeof(CASE
            WHEN id = 0 THEN CAST(1 AS TINYINT)
            WHEN id = 1 THEN NULL
            WHEN id = 2 THEN 2
            ELSE CAST(2.5 AS DECIMAL(3,1))
          END) AS result_type,
          CASE WHEN id = 0 THEN 1 WHEN id = 1 THEN CAST(3000000000 AS BIGINT) END AS no_else
        FROM VALUES (0), (1), (2), (3) AS t(id)
        """
      Then query result
        | id | result | result_type   | no_else    |
        | 0  | 1.0    | decimal(11,1) | 1          |
        | 1  | NULL   | decimal(11,1) | 3000000000 |
        | 2  | 2.0    | decimal(11,1) | NULL       |
        | 3  | 2.5    | decimal(11,1) | NULL       |

    Scenario: CASE declares the widened numeric type in the output schema
      When query
        """
        SELECT CASE WHEN c <= 0 THEN 1 ELSE c END AS result
        FROM VALUES (CAST(3000000000 AS BIGINT)), (CAST(0 AS BIGINT)), (CAST(NULL AS BIGINT)) AS t(c)
        """
      Then query result
        | result     |
        | 3000000000 |
        | 1          |
        | NULL       |
      And query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: sequence over a widened CASE bound can be exploded
      When query
        """
        SELECT c, i, typeof(i) AS i_type
        FROM (
          SELECT c, explode(sequence(0, CASE WHEN c <= 0 THEN 1 ELSE c END - 1)) AS i
          FROM VALUES (CAST(3 AS BIGINT)), (CAST(1 AS BIGINT)), (CAST(0 AS BIGINT)) AS t(c)
        )
        """
      Then query result
        | c | i | i_type |
        | 3 | 0 | bigint |
        | 3 | 1 | bigint |
        | 3 | 2 | bigint |
        | 1 | 0 | bigint |
        | 0 | 0 | bigint |

  Rule: Spark-compatible coercion for non-numeric branches

    # TODO: Coerce these non-numeric branches to Spark's wider common type.
    #  Existing DataFusion coercion does not cover Spark's nested ANSI string rules.
    @sail-bug
    Scenario Outline: CASE widens non-numeric branches to the Spark common type: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(CASE WHEN id = 0 THEN <first_branch> ELSE <second_branch> END) AS result_type
        FROM VALUES (0) AS t(id)
        """
      Then query result
        | result_type   |
        | <result_type> |

      Examples:
        | case                         | first_branch                        | second_branch                   | result_type   |
        | INT then STRING              | 1                                   | '2'                             | bigint        |
        | ARRAY INT then ARRAY STRING  | array(1)                            | array('2')                      | array<bigint> |
        | TIMESTAMP_NTZ then TIMESTAMP | TIMESTAMP_NTZ '2024-01-01 00:00:00' | TIMESTAMP '2024-01-01 00:00:00' | timestamp     |

    Scenario: CASE declares the existing common type of nested integral branches
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(CASE WHEN id = 0 THEN array(1) ELSE array(CAST(2 AS BIGINT)) END) AS result_type
        FROM VALUES (0) AS t(id)
        """
      Then query result
        | result_type   |
        | array<bigint> |

    Scenario: CASE over a CASE with INT and STRING branches keeps STRING values with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          id,
          CASE WHEN id = 0 THEN CAST(2 AS BIGINT) ELSE CASE WHEN id = 1 THEN 1 ELSE 'x' END END AS result
        FROM VALUES (0), (1), (2) AS t(id)
        """
      Then query result
        | id | result |
        | 0  | 2      |
        | 1  | 1      |
        | 2  | x      |

    Scenario: CASE preserves STRING values across a projection with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          id,
          CASE WHEN id = 0 THEN CAST(2 AS BIGINT) ELSE v END AS result
        FROM (
          SELECT id, CASE WHEN id = 1 THEN 1 ELSE 'x' END AS v
          FROM VALUES (0), (1), (2) AS t(id)
        ) AS q
        """
      Then query result
        | id | result |
        | 0  | 2      |
        | 1  | 1      |
        | 2  | x      |

  Rule: Legacy DECIMAL truncation

    @sail-bug
    Scenario: CASE keeps DECIMAL fraction digits with the legacy truncation config
      Given config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
      When query
        """
        SELECT
          id,
          CASE WHEN id = 0 THEN CAST(-2.5 AS DECIMAL(38,10)) ELSE CAST(1 AS DECIMAL(38,0)) END AS result,
          typeof(CASE WHEN id = 0 THEN CAST(-2.5 AS DECIMAL(38,10)) ELSE CAST(1 AS DECIMAL(38,0)) END) AS result_type
        FROM VALUES (0), (1) AS t(id)
        """
      Then query result
        | id | result        | result_type    |
        | 0  | -2.5000000000 | decimal(38,10) |
        | 1  | 1.0000000000  | decimal(38,10) |

  Rule: Persistent views

    @sail-bug
    Scenario: CASE in a persistent view keeps the type resolved with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      And statement
        """
        CREATE OR REPLACE VIEW case_float_bigint_legacy_view AS
        SELECT id, CASE WHEN id = 0 THEN CAST(1.5 AS FLOAT) ELSE CAST(3 AS BIGINT) END AS v FROM range(2)
        """
      And final statement
        """
        DROP VIEW IF EXISTS case_float_bigint_legacy_view
        """
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, v, typeof(v) AS v_type FROM case_float_bigint_legacy_view
        """
      Then query result
        | id | v   | v_type |
        | 0  | 1.5 | float  |
        | 1  | 3.0 | float  |

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
