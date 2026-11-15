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

  Rule: Common type of numeric branches

    Scenario Outline: CASE has the common type of numeric branches: <case>
      When query
        """
        SELECT
          id,
          CASE WHEN id = 0 THEN <first_branch> ELSE <second_branch> END AS result,
          typeof(CASE WHEN id = 0 THEN <first_branch> ELSE <second_branch> END) AS result_type
        FROM VALUES (0), (1) AS t(id)
        ORDER BY id
        """
      Then query result ordered
        | id | result         | result_type   |
        | 0  | <first_value>  | <result_type> |
        | 1  | <second_value> | <result_type> |

      Examples:
        | case                   | first_branch               | second_branch               | first_value   | second_value | result_type   |
        | INT then BIGINT        | 1                          | CAST(3000000000 AS BIGINT)  | 1             | 3000000000   | bigint        |
        | BIGINT then INT        | CAST(3000000000 AS BIGINT) | 1                           | 3000000000    | 1            | bigint        |
        | SMALLINT then BIGINT   | CAST(1 AS SMALLINT)        | CAST(2 AS BIGINT)           | 1             | 2            | bigint        |
        | INT then DOUBLE        | 1                          | CAST(1.5 AS DOUBLE)         | 1.0           | 1.5          | double        |
        | INT then DECIMAL       | 1                          | CAST(1.75 AS DECIMAL(10,2)) | 1.00          | 1.75         | decimal(12,2) |
        | INT then DECIMAL(20,0) | 1                          | CAST(1 AS DECIMAL(20,0))    | 1             | 1            | decimal(20,0) |
        | BIGINT then DECIMAL    | CAST(3000000000 AS BIGINT) | CAST(1.25 AS DECIMAL(5,2))  | 3000000000.00 | 1.25         | decimal(22,2) |

    Scenario Outline: CASE has the common type of INT and FLOAT branches with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          id,
          CASE WHEN id = 0 THEN 1 ELSE CAST(1.5 AS FLOAT) END AS result,
          typeof(CASE WHEN id = 0 THEN 1 ELSE CAST(1.5 AS FLOAT) END) AS result_type
        FROM VALUES (0), (1) AS t(id)
        ORDER BY id
        """
      Then query result ordered
        | id | result | result_type   |
        | 0  | 1.0    | <result_type> |
        | 1  | 1.5    | <result_type> |

      Examples:
        | ansi  | result_type |
        | true  | double      |
        | false | float       |

    Scenario: CASE has the common type of all numeric branches
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
          CASE WHEN id = 0 THEN 1 WHEN id = 1 THEN CAST(3000000000 AS BIGINT) END AS no_else,
          typeof(CASE id WHEN 0 THEN 1 ELSE CAST(3000000000 AS BIGINT) END) AS simple_case_type
        FROM VALUES (0), (1), (2), (3) AS t(id)
        ORDER BY id
        """
      Then query result ordered
        | id | result | result_type   | no_else    | simple_case_type |
        | 0  | 1.0    | decimal(11,1) | 1          | bigint           |
        | 1  | NULL   | decimal(11,1) | 3000000000 | bigint           |
        | 2  | 2.0    | decimal(11,1) | NULL       | bigint           |
        | 3  | 2.5    | decimal(11,1) | NULL       | bigint           |

    Scenario: CASE declares the common type of numeric branches in the output schema
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

    Scenario: sequence over a CASE bound with numeric branches of different types can be exploded
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

    Scenario: CASE has the common type of nested numeric branches
      When query
        """
        SELECT
          id,
          CASE WHEN id = 0 THEN array(1) ELSE array(CAST(3000000000 AS BIGINT)) END AS result,
          typeof(CASE WHEN id = 0 THEN array(1) ELSE array(CAST(3000000000 AS BIGINT)) END) AS result_type
        FROM VALUES (0), (1) AS t(id)
        ORDER BY id
        """
      Then query result ordered
        | id | result       | result_type   |
        | 0  | [1]          | array<bigint> |
        | 1  | [3000000000] | array<bigint> |

  Rule: Common type of DATE and TIMESTAMP branches

    Scenario Outline: CASE casts DATE branches to <result_type> in the session time zone: <case>
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          id,
          CAST(CASE WHEN id = 0 THEN <first_branch> ELSE <second_branch> END AS STRING) AS result,
          hour(CASE WHEN id = 0 THEN <first_branch> ELSE <second_branch> END) AS h,
          typeof(CASE WHEN id = 0 THEN <first_branch> ELSE <second_branch> END) AS result_type
        FROM VALUES (0), (1) AS t(id)
        ORDER BY id
        """
      Then query result ordered
        | id | result         | h          | result_type   |
        | 0  | <first_value>  | <first_h>  | <result_type> |
        | 1  | <second_value> | <second_h> | <result_type> |

      Examples:
        | case                    | first_branch                      | second_branch                     | first_value         | first_h | second_value        | second_h | result_type   |
        | DATE then TIMESTAMP     | DATE'2020-01-01'                  | TIMESTAMP'2020-01-01 10:00:00'    | 2020-01-01 00:00:00 | 0       | 2020-01-01 10:00:00 | 10       | timestamp     |
        | TIMESTAMP then DATE     | TIMESTAMP'2020-01-01 10:00:00'    | DATE'2020-01-01'                  | 2020-01-01 10:00:00 | 10      | 2020-01-01 00:00:00 | 0        | timestamp     |
        | DATE then TIMESTAMP_NTZ | DATE'2020-01-01'                  | TIMESTAMP_NTZ'2020-01-01 10:00:00' | 2020-01-01 00:00:00 | 0       | 2020-01-01 10:00:00 | 10       | timestamp_ntz |

    Scenario: CASE casts a DATE branch to the start of its day in the session time zone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          id,
          CASE WHEN id = 0 THEN DATE'2020-01-01' ELSE TIMESTAMP'2020-01-01 10:00:00' END AS result,
          CAST(CASE WHEN id = 0 THEN DATE'2020-01-01' ELSE TIMESTAMP'2020-01-01 10:00:00' END AS LONG) AS seconds
        FROM VALUES (0), (1) AS t(id)
        ORDER BY id
        """
      Then query result ordered
        | id | result              | seconds    |
        | 0  | 2020-01-01 00:00:00 | 1577865600 |
        | 1  | 2020-01-01 10:00:00 | 1577901600 |

    Scenario: CASE casts a DATE branch and a TIMESTAMP column to the session time zone
      Given variable location for temporary directory case_date_timestamp_column
      Given final statement
        """
        DROP TABLE IF EXISTS case_date_timestamp_column
        """
      Given config spark.sql.session.timeZone = UTC
      Given statement template
        """
        CREATE TABLE case_date_timestamp_column (id INT, ts TIMESTAMP) USING parquet LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO case_date_timestamp_column VALUES (0, TIMESTAMP '2020-03-08 10:30:00'), (1, NULL)
        """
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          id,
          hour(CASE WHEN id = 0 THEN ts ELSE DATE '2020-03-08' END) AS h,
          CAST(CASE WHEN id = 0 THEN ts ELSE DATE '2020-03-08' END AS STRING) AS result
        FROM case_date_timestamp_column
        ORDER BY id
        """
      Then query result ordered
        | id | h | result              |
        | 0  | 3 | 2020-03-08 03:30:00 |
        | 1  | 0 | 2020-03-08 00:00:00 |

  Rule: Nested branches with different nullability

    Scenario Outline: CASE returns nested values whose nullability differs across branches: <case>
      When query
        """
        SELECT x, CASE WHEN 0 > x THEN <first_branch> ELSE <second_branch> END AS v
        FROM VALUES (<first_x>), (<second_x>) AS t(x)
        ORDER BY x
        """
      Then query result ordered
        | x          | v              |
        | <first_x>  | <first_value>  |
        | <second_x> | <second_value> |

      Examples:
        | case                                   | first_branch                                    | second_branch                                             | first_x | second_x | first_value | second_value |
        | non-null items first, both branches    | array(named_struct('a', 1))                     | array(CAST(NULL AS STRUCT<a: INT>))                       | -1      | 1        | [{1}]       | [NULL]       |
        | non-null items first, all rows in ELSE | array(named_struct('a', 1))                     | array(CAST(NULL AS STRUCT<a: INT>))                       | 1       | 2        | [NULL]      | [NULL]       |
        | non-null items first, all rows in THEN | array(named_struct('a', 1))                     | array(CAST(NULL AS STRUCT<a: INT>))                       | -2      | -1       | [{1}]       | [{1}]        |
        | nullable items first, both branches    | array(CASE WHEN x < -5 THEN named_struct('a', x) END) | array(named_struct('a', 0))                         | -1      | 1        | [NULL]      | [{0}]        |
        | nullable items first, all rows in ELSE | array(CASE WHEN x < -5 THEN named_struct('a', x) END) | array(named_struct('a', 0))                         | 1       | 2        | [{0}]       | [{0}]        |
        | nullable items first, all rows in THEN | array(CASE WHEN x < -5 THEN named_struct('a', x) END) | array(named_struct('a', 0))                         | -2      | -1       | [NULL]      | [NULL]       |
        | integer items                          | array(CASE WHEN true THEN 1 END)                | array(CAST(NULL AS INT))                                  | -1      | 1        | [1]         | [NULL]       |
        | struct of arrays                       | named_struct('f', array(named_struct('a', 1)))  | named_struct('f', array(CAST(NULL AS STRUCT<a: INT>)))    | -1      | 1        | {[{1}]}     | {[NULL]}     |
        | map of arrays                          | map('k', array(named_struct('a', 1)))           | map('k', array(CAST(NULL AS STRUCT<a: INT>)))             | -1      | 1        | {k -> [{1}]} | {k -> [NULL]} |

    Scenario: CASE returns a column whose nested nullability differs from another branch
      When query
        """
        SELECT x, CASE WHEN x < 0 THEN v ELSE array(CAST(NULL AS STRUCT<a: INT>)) END AS w
        FROM (SELECT x, array(named_struct('a', 1)) AS v FROM VALUES (-1), (1) AS t(x))
        ORDER BY x
        """
      Then query result ordered
        | x  | w      |
        | -1 | [{1}]  |
        | 1  | [NULL] |

    Scenario: explode over CASE branches with different nested nullability
      When query
        """
        SELECT explode(
          CASE
            WHEN x < 0 THEN array(named_struct('a', 1))
            ELSE array(CAST(NULL AS STRUCT<a: INT>), named_struct('a', 2))
          END
        ) AS e
        FROM VALUES (-1), (1) AS t(x)
        """
      Then query result
        | e    |
        | {1}  |
        | NULL |
        | {2}  |

    Scenario Outline: CASE keeps the interval qualifier inside <kind> when other fields are widened
      When query
        """
        SELECT
          id,
          CAST(c<access>.v AS INT) AS years,
          c<access>.n AS n,
          typeof(c<access>.n) AS n_type
        FROM (
          SELECT id, CASE WHEN id = 0 THEN <first_branch> ELSE <second_branch> END AS c
          FROM range(2)
        )
        ORDER BY id
        """
      Then query result ordered
        | id | years | n | n_type |
        | 0  | 1     | 1 | bigint |
        | 1  | NULL  | 2 | bigint |

      Examples:
        | kind                | first_branch                                        | second_branch                                                  | access   |
        | a struct            | struct(INTERVAL '1' YEAR AS v, 1 AS n)              | struct(CAST(NULL AS INTERVAL YEAR) AS v, 2L AS n)              |          |
        | an array of structs | array(struct(INTERVAL '1' YEAR AS v, 1 AS n))       | array(struct(CAST(NULL AS INTERVAL YEAR) AS v, 2L AS n))       | [0]      |
        | a map of structs    | map('k', struct(INTERVAL '1' YEAR AS v, 1 AS n))    | map('k', struct(CAST(NULL AS INTERVAL YEAR) AS v, 2L AS n))    | ['k']    |

    Scenario: CASE keeps a day-time interval qualifier inside a struct when other fields are widened
      When query
        """
        SELECT id, CAST(c.v AS STRING) AS days
        FROM (
          SELECT id,
                 CASE
                   WHEN id = 0 THEN struct(INTERVAL '1' DAY AS v, 1 AS n)
                   ELSE struct(CAST(NULL AS INTERVAL DAY) AS v, 2L AS n)
                 END AS c
          FROM range(2)
        )
        ORDER BY id
        """
      Then query result ordered
        | id | days             |
        | 0  | INTERVAL '1' DAY |
        | 1  | NULL             |

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
