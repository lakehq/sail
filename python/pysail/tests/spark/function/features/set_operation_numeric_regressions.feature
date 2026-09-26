Feature: Numeric UNION types used by conditional expressions

  Scenario Outline: UNION conditional consumers preserve fractional precision: <operator>, <first>, ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, typeof(v) AS value_type,
        CAST(IF(id = 0, CAST(2 AS FLOAT), v) AS DOUBLE) AS if_value,
        CAST(CASE WHEN id = 0 THEN CAST(2 AS FLOAT) ELSE v END AS DOUBLE) AS case_value,
        CAST(NVL2(NULLIF(id, 1), CAST(2 AS FLOAT), v) AS DOUBLE) AS nvl2_value
      FROM (
        SELECT 0 AS id, CAST(1.25 AS <first>) AS v
        <operator>
        SELECT 1 AS id, CAST(16777217.25 AS <second>) AS v
      ) AS q
      ORDER BY id
      """
    Then query result collected ordered
      | id | value_type | if_value    | case_value  | nvl2_value  |
      | 0  | double     | 2.0         | 2.0         | 2.0         |
      | 1  | double     | 16777217.25 | 16777217.25 | 16777217.25 |

    Examples:
      | operator  | first         | second        | ansi  |
      | UNION ALL | FLOAT         | DECIMAL(10,2) | true  |
      | UNION ALL | FLOAT         | DECIMAL(10,2) | false |
      | UNION     | FLOAT         | DECIMAL(10,2) | true  |
      | UNION     | FLOAT         | DECIMAL(10,2) | false |
      | UNION ALL | DECIMAL(10,2) | DOUBLE        | true  |
      | UNION ALL | DECIMAL(10,2) | DOUBLE        | false |
      | UNION     | DECIMAL(10,2) | DOUBLE        | true  |
      | UNION     | DECIMAL(10,2) | DOUBLE        | false |

  Scenario Outline: Nullable nested conditional branches preserve UNION precision: <expression>, ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, result[0] AS value, typeof(result) AS value_type
      FROM (
        SELECT id, <expression> AS result
        FROM (
          SELECT 0 AS id, array(CAST(1.25 AS FLOAT)) AS v
          UNION ALL
          SELECT 1 AS id, array(CAST(16777217.25 AS DECIMAL(10,2))) AS v
        ) AS q
      ) AS results
      ORDER BY id
      """
    Then query result collected ordered
      | id | value       | value_type    |
      | 0  | NULL        | array<double> |
      | 1  | 16777217.25 | array<double> |
    And query schema
      """
      root
       |-- id: integer (nullable = false)
       |-- value: double (nullable = true)
       |-- value_type: string (nullable = false)
      """

    Examples:
      | expression                                                      | ansi  |
      | IF(id = 0, array(CAST(NULL AS FLOAT)), v)                        | true  |
      | IF(id = 0, array(CAST(NULL AS FLOAT)), v)                        | false |
      | CASE WHEN id = 0 THEN array(CAST(NULL AS FLOAT)) ELSE v END      | true  |
      | CASE WHEN id = 0 THEN array(CAST(NULL AS FLOAT)) ELSE v END      | false |
      | NVL2(NULLIF(id, 1), array(CAST(NULL AS FLOAT)), v)                | true  |
      | NVL2(NULLIF(id, 1), array(CAST(NULL AS FLOAT)), v)                | false |

  Scenario Outline: UNION promotes nested numeric leaves while retaining interval and timestamp consumers: ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    And config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT id, CAST(s.y AS INT) AS years,
        NVL2(NULLIF(id, 1), CAST(2 AS FLOAT), s.n) AS struct_value,
        NVL2(NULLIF(id, 1), CAST(2 AS FLOAT), m['n'][0]) AS map_value,
        CAST(from_utc_timestamp(s.ts, 'America/Los_Angeles') AS STRING) AS shifted
      FROM (
        SELECT 0 AS id,
          struct(INTERVAL '1' YEAR AS y, CAST(1.25 AS FLOAT) AS n,
            TIMESTAMP '2024-06-15 12:00:00' AS ts) AS s,
          map('n', array(CAST(1.25 AS FLOAT))) AS m
        UNION ALL
        SELECT 1 AS id,
          struct(INTERVAL '2' YEAR AS y, CAST(16777217.25 AS DECIMAL(10,2)) AS n,
            '2024-06-16 12:00:00' AS ts) AS s,
          map('n', array(CAST(16777217.25 AS DECIMAL(10,2)))) AS m
      ) AS q
      ORDER BY id
      """
    Then query result collected ordered
      | id | years | struct_value | map_value   | shifted             |
      | 0  | 1     | 2.0          | 2.0         | 2024-06-15 05:00:00 |
      | 1  | 2     | 16777217.25  | 16777217.25 | 2024-06-16 05:00:00 |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario Outline: Reopened views preserve UNION precision: <kind>, creation ANSI <creation_ansi>
    Given config spark.sql.ansi.enabled = <creation_ansi>
    And statement
      """
      CREATE OR REPLACE <kind> VIEW numeric_union_precision_view AS
      SELECT 0 AS id, CAST(1.25 AS FLOAT) AS v
      UNION ALL
      SELECT 1 AS id, CAST(16777217.25 AS DECIMAL(10,2)) AS v
      """
    And final statement
      """
      DROP VIEW IF EXISTS numeric_union_precision_view
      """
    And config spark.sql.ansi.enabled = <read_ansi>
    When query
      """
      SELECT id, NVL2(NULLIF(id, 1), CAST(2 AS FLOAT), v) AS value, typeof(v) AS value_type
      FROM numeric_union_precision_view
      ORDER BY id
      """
    Then query result collected ordered
      | id | value       | value_type |
      | 0  | 2.0         | double     |
      | 1  | 16777217.25 | double     |

    Examples:
      | kind | creation_ansi | read_ansi |
      | TEMP | true          | false     |
      | TEMP | false         | true      |
      |      | true          | false     |
      |      | false         | true      |

  Scenario Outline: UNION promotes integral and FLOAT inputs according to ANSI mode: <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, CAST(v AS BIGINT) AS value, typeof(v) AS value_type
      FROM (
        SELECT 0 AS id, 16777217L AS v
        UNION ALL
        SELECT 1 AS id, CAST(2 AS FLOAT) AS v
      ) AS q
      ORDER BY id
      """
    Then query result collected ordered
      | id | value   | value_type |
      | 0  | <value> | <type>     |
      | 1  | 2       | <type>     |

    Examples:
      | ansi  | value    | type   |
      | true  | 16777217 | double |
      | false | 16777216 | float  |

  Scenario: A DOUBLE UNION input retains values outside the DECIMAL range
    When query
      """
      SELECT id, v > 1E99 AS large, typeof(v) AS value_type
      FROM (
        SELECT 0 AS id, CAST(1E100 AS DOUBLE) AS v
        UNION ALL
        SELECT 1 AS id, CAST(1.25 AS DECIMAL(10,2)) AS v
      ) AS q
      ORDER BY id
      """
    Then query result collected ordered
      | id | large | value_type |
      | 0  | true  | double     |
      | 1  | false | double     |

  Scenario Outline: Numeric UNION promotion preserves empty and nullable arrays: ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, coalesce(size(v), -1) AS length,
        IF(size(v) > 0, v[0], CAST(NULL AS DOUBLE)) AS value,
        typeof(v) AS value_type
      FROM (
        SELECT 0 AS id, array() AS v
        UNION ALL
        SELECT 1 AS id, array(CAST(NULL AS FLOAT)) AS v
        UNION ALL
        SELECT 2 AS id, array(CAST(16777217.25 AS DECIMAL(10,2))) AS v
        UNION ALL
        SELECT 3 AS id, CAST(NULL AS ARRAY<DECIMAL(10,2)>) AS v
      ) AS q
      ORDER BY id
      """
    Then query result collected ordered
      | id | length | value       | value_type    |
      | 0  | 0      | NULL        | array<double> |
      | 1  | 1      | NULL        | array<double> |
      | 2  | 1      | 16777217.25 | array<double> |
      | 3  | -1     | NULL        | array<double> |
    And query schema
      """
      root
       |-- id: integer (nullable = false)
       |-- length: integer (nullable = false)
       |-- value: double (nullable = true)
       |-- value_type: string (nullable = false)
      """

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario Outline: UNION DISTINCT uses the promoted numeric type when removing duplicates: ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT count(*) AS count, CAST(min(v) AS BIGINT) AS minimum,
        CAST(max(v) AS BIGINT) AS maximum, typeof(min(v)) AS value_type
      FROM (
        SELECT 16777217L AS v
        UNION
        SELECT CAST(16777216 AS FLOAT) AS v
      ) AS q
      """
    Then query result collected
      | count   | minimum  | maximum   | value_type |
      | <count> | 16777216 | <maximum> | <type>     |

    Examples:
      | ansi  | count | maximum  | type   |
      | true  | 2     | 16777217 | double |
      | false | 1     | 16777216 | float  |
