Feature: if output schema

  Rule: Spark-compatible coercion for mixed string and temporal branches

    Scenario: IF coerces date branches to string and keeps downstream parsing valid when ANSI is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          if(use_override, '2026-03-31', period_end) AS result,
          to_date(if(use_override, '2026-03-31', period_end)) AS parsed,
          typeof(if(use_override, '2026-03-31', period_end)) AS result_type
        FROM VALUES
          (1, true, DATE '2026-02-20'),
          (2, false, DATE '2025-12-01'),
          (3, false, CAST(NULL AS DATE))
        AS t(id, use_override, period_end)
        ORDER BY id
        """
      Then query result
        | result     | parsed     | result_type |
        | 2026-03-31 | 2026-03-31 | string      |
        | 2025-12-01 | 2025-12-01 | string      |
        | NULL       | NULL       | string      |

    Scenario: IF coerces string branches to date when ANSI is enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          if(use_override, '2026-03-31', period_end) AS result,
          to_date(if(use_override, '2026-03-31', period_end)) AS parsed,
          typeof(if(use_override, '2026-03-31', period_end)) AS result_type
        FROM VALUES
          (1, true, DATE '2026-02-20'),
          (2, false, DATE '2025-12-01'),
          (3, false, CAST(NULL AS DATE))
        AS t(id, use_override, period_end)
        ORDER BY id
        """
      Then query result
        | result     | parsed     | result_type |
        | 2026-03-31 | 2026-03-31 | date        |
        | 2025-12-01 | 2025-12-01 | date        |
        | NULL       | NULL       | date        |

  Rule: Spark-compatible coercion for numeric branches

    Scenario Outline: IF widens numeric branches to the Spark common type: <case>
      When query
        """
        SELECT
          id,
          if(id = 0, <true_value>, <false_value>) AS result,
          typeof(if(id = 0, <true_value>, <false_value>)) AS result_type
        FROM VALUES (0), (1) AS t(id)
        """
      Then query result
        | id | result         | result_type   |
        | 0  | <first_value>  | <result_type> |
        | 1  | <second_value> | <result_type> |

      Examples:
        | case             | true_value                 | false_value                 | first_value | second_value | result_type   |
        | INT then BIGINT  | 1                          | CAST(3000000000 AS BIGINT)  | 1           | 3000000000   | bigint        |
        | BIGINT then INT  | CAST(3000000000 AS BIGINT) | 1                           | 3000000000  | 1            | bigint        |
        | INT then DOUBLE  | 1                          | CAST(1.5 AS DOUBLE)         | 1.0         | 1.5          | double        |
        | INT then DECIMAL | 1                          | CAST(1.75 AS DECIMAL(10,2)) | 1.00        | 1.75         | decimal(12,2) |

    Scenario: IF declares the widened numeric type in the output schema
      When query
        """
        SELECT if(c <= 0, 1, c) AS result
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

  Rule: Conditional branches over UNION results

    Scenario Outline: Conditionals preserve widened UNION values: <operator>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          id,
          if(id = 0, CAST(2 AS FLOAT), v) AS if_result,
          CASE WHEN id = 0 THEN CAST(2 AS FLOAT) ELSE v END AS case_result,
          nvl2(nullif(id, 1), CAST(2 AS FLOAT), v) AS nvl2_result
        FROM (
          SELECT 0 AS id, 1 AS v
          <operator>
          SELECT 1 AS id, CAST(16777217 AS DOUBLE) AS v
        ) AS q
        ORDER BY id
        """
      Then query result collected
        | id | if_result  | case_result | nvl2_result |
        | 0  | 2.0        | 2.0         | 2.0         |
        | 1  | 16777217.0 | 16777217.0  | 16777217.0  |

      Examples:
        | operator  |
        | UNION ALL |
        | UNION     |

    Scenario Outline: Conditionals preserve UNION strings: <operator>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          id,
          if(id = 0, CAST(2 AS BIGINT), v) AS if_result,
          CASE WHEN id = 0 THEN CAST(2 AS BIGINT) ELSE v END AS case_result
        FROM (
          SELECT 0 AS id, 1 AS v
          <operator>
          SELECT 1 AS id, 'x' AS v
        ) AS q
        ORDER BY id
        """
      Then query result
        | id | if_result | case_result |
        | 0  | 2         | 2           |
        | 1  | x         | x           |

      Examples:
        | operator  |
        | UNION ALL |
        | UNION     |

    @sail-bug
    Scenario: ANSI conditionals reject invalid numeric strings from UNION inputs
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, if(id = 0, CAST(2 AS BIGINT), v) AS result
        FROM (
          SELECT 0 AS id, 1 AS v
          UNION ALL
          SELECT 1 AS id, 'x' AS v
        ) AS q
        ORDER BY id
        """
      Then query error CAST_INVALID_INPUT

    Scenario: ANSI UNION exposes its numeric common type to typeof
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(v) AS result_type
        FROM (
          SELECT CAST(8 AS BIGINT) AS v
          UNION ALL
          SELECT '4' AS v
        ) AS q
        """
      Then query result
        | result_type |
        | bigint      |
        | bigint      |

    @sail-bug
    Scenario: ANSI UNION widens INT with STRING to BIGINT
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(v) AS result_type
        FROM (
          SELECT CAST(8 AS INT) AS v
          UNION ALL
          SELECT '4' AS v
        ) AS q
        """
      Then query result
        | result_type |
        | bigint      |
        | bigint      |

    @sail-bug
    Scenario Outline: UNION exposes the Spark numeric common type to typeof: <case>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(v) AS result_type
        FROM (
          SELECT <first> AS v
          UNION ALL
          SELECT CAST(2.5 AS FLOAT) AS v
        ) AS q
        """
      Then query result
        | result_type |
        | double      |
        | double      |

      Examples:
        | case              | ansi  | first                      |
        | DECIMAL and FLOAT | false | CAST(0.5 AS DECIMAL(11,1)) |
        | DECIMAL and FLOAT | true  | CAST(0.5 AS DECIMAL(11,1)) |
        | BIGINT and FLOAT  | true  | CAST(1 AS BIGINT)          |

  Rule: Nested numeric and STRING branches

    Scenario: IF preserves nested STRING values with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          id,
          if(id = 0, CAST(2 AS BIGINT), if(id = 1, 1, 'x')) AS result
        FROM VALUES (0), (1), (2) AS t(id)
        """
      Then query result
        | id | result |
        | 0  | 2      |
        | 1  | 1      |
        | 2  | x      |

    Scenario: IF preserves large numeric strings in nested branches with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          id,
          CAST(if(id = 0, CAST(1.25 AS DECIMAL(5,2)), CASE WHEN id = 1 THEN 1 ELSE '9223372036854775807' END) AS DECIMAL(22,2)) AS direct_result,
          CAST(if(id = 0, CAST(1.25 AS DECIMAL(5,2)), v) AS DECIMAL(22,2)) AS projected_result
        FROM (
          SELECT id, CASE WHEN id = 1 THEN 1 ELSE '9223372036854775807' END AS v
          FROM VALUES (0), (1), (2) AS t(id)
        ) AS q
        """
      Then query result
        | id | direct_result         | projected_result      |
        | 0  | 1.25                  | 1.25                  |
        | 1  | 1.00                  | 1.00                  |
        | 2  | 9223372036854775807.00 | 9223372036854775807.00 |

  Rule: Numeric branches extracted from nested CASE values

    Scenario Outline: IF preserves values extracted from nested CASE branches: <case>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          id,
          if(id = 0, CAST(2 AS BIGINT), (CASE WHEN id = 1 THEN <first_branch> ELSE <second_branch> END)<access>) AS direct_result,
          if(id = 0, CAST(2 AS BIGINT), v<access>) AS projected_result,
          typeof(if(id = 0, CAST(2 AS BIGINT), (CASE WHEN id = 1 THEN <first_branch> ELSE <second_branch> END)<access>)) AS direct_type,
          typeof(if(id = 0, CAST(2 AS BIGINT), v<access>)) AS projected_type
        FROM (
          SELECT id, CASE WHEN id = 1 THEN <first_branch> ELSE <second_branch> END AS v
          FROM VALUES (0), (1), (2) AS t(id)
        ) AS q
        """
      Then query result
        | id | direct_result | projected_result | direct_type | projected_type |
        | 0  | 2.0           | 2.0              | double      | double         |
        | 1  | 1.0           | 1.0              | double      | double         |
        | 2  | 1.5           | 1.5              | double      | double         |

      Examples:
        | case   | ansi  | first_branch         | second_branch                         | access |
        | ARRAY  | true  | array(1)             | array(CAST(1.5 AS DOUBLE))             | [0]    |
        | ARRAY  | false | array(1)             | array(CAST(1.5 AS DOUBLE))             | [0]    |
        | STRUCT | true  | named_struct('a', 1) | named_struct('a', CAST(1.5 AS DOUBLE)) | .a     |
        | STRUCT | false | named_struct('a', 1) | named_struct('a', CAST(1.5 AS DOUBLE)) | .a     |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to if yields the schema Spark declares
      When query
        """
        SELECT if(1 < 2, 'a', 'b') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable column input to if stays nullable
      When query
        """
        SELECT if(c, 'a', 'b') AS result FROM VALUES (1 < 2), (CAST(NULL AS BOOLEAN)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

  Rule: Numeric conditionals over parsed decimal values

    Scenario Outline: Numeric conditionals preserve parsed decimal values with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          id,
          if_result,
          case_result,
          nvl2_result,
          typeof(if_result) AS if_type,
          typeof(case_result) AS case_type,
          typeof(nvl2_result) AS nvl2_type
        FROM (
          SELECT
            id,
            if(id = 0, CAST(2 AS BIGINT), if(id = 1, 1, to_number('1.25', '9.99'))) AS if_result,
            CASE WHEN id = 0 THEN CAST(2 AS DECIMAL(5,1))
              ELSE CASE WHEN id = 1 THEN CAST(1 AS DECIMAL(3,1))
                ELSE try_to_number('1.25', '9.99') END
              END AS case_result,
            nvl2(nullif(id, 2), CAST(1.5 AS FLOAT), to_number('1.25', '9.99')) AS nvl2_result
          FROM VALUES (0), (1), (2) AS t(id)
        ) AS q
        ORDER BY id
        """
      Then query result ordered
        | id | if_result | case_result | nvl2_result | if_type       | case_type    | nvl2_type |
        | 0  | 2.00      | 2.00        | 1.5         | decimal(22,2) | decimal(6,2) | double    |
        | 1  | 1.00      | 1.00        | 1.5         | decimal(22,2) | decimal(6,2) | double    |
        | 2  | 1.25      | 1.25        | 1.25        | decimal(22,2) | decimal(6,2) | double    |

      Examples:
        | ansi  |
        | false |
        | true  |
