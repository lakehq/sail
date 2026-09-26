Feature: Conditional values in stored views retain their precision

  Scenario Outline: An ANSI <kind> view retains its UNION <function> values when read with ANSI <read_ansi>
    Given config spark.sql.ansi.enabled = true
    And final statement
      """
      DROP VIEW IF EXISTS <view_name>
      """
    And statement
      """
      CREATE OR REPLACE <kind> VIEW conditional_view_precision AS
      SELECT id, <expression> AS x
      FROM (
        SELECT 0 AS id, '2.5' AS v
        UNION ALL
        SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v
      ) AS q
      """
    And config spark.sql.ansi.enabled = <read_ansi>
    When query
      """
      SELECT id, CAST(IF(id = 9, 0L, x) AS DOUBLE) AS value
      FROM <view_name> ORDER BY id
      """
    Then query result ordered
      | id | value |
      | 0  | 0.0   |
      | 1  | 2.5   |

    Examples:
      | kind        | view_name                             | function | expression                        | read_ansi |
      | TEMP        | conditional_view_precision            | IF       | IF(id = 0, 0, v)                  | false     |
      | GLOBAL TEMP | global_temp.conditional_view_precision | IF       | IF(id = 0, 0, v)                  | false     |
      | TEMP        | conditional_view_precision            | CASE     | CASE WHEN id = 0 THEN 0 ELSE v END | false     |
      | GLOBAL TEMP | global_temp.conditional_view_precision | CASE     | CASE WHEN id = 0 THEN 0 ELSE v END | false     |
      | TEMP        | conditional_view_precision            | NVL2     | NVL2(NULLIF(id, 0), v, 0)          | false     |
      | GLOBAL TEMP | global_temp.conditional_view_precision | NVL2     | NVL2(NULLIF(id, 0), v, 0)          | false     |
      | TEMP        | conditional_view_precision            | NVL2     | NVL2(NULLIF(id, 1), 0, v)          | false     |
      | GLOBAL TEMP | global_temp.conditional_view_precision | NVL2     | NVL2(NULLIF(id, 1), 0, v)          | false     |
      | TEMP        | conditional_view_precision            | NVL2     | NVL2(NULLIF(id, 1), 0, v)          | true      |
      | GLOBAL TEMP | global_temp.conditional_view_precision | NVL2     | NVL2(NULLIF(id, 1), 0, v)          | true      |

  Scenario Outline: An ANSI <kind> view retains its materialized <function> values when read with ANSI <read_ansi>
    Given config spark.sql.ansi.enabled = true
    And variable location for temporary directory conditional_view_source
    And final statement
      """
      DROP TABLE IF EXISTS conditional_view_source
      """
    And final statement
      """
      DROP VIEW IF EXISTS <view_name>
      """
    And statement template
      """
      CREATE TABLE conditional_view_source USING PARQUET LOCATION {{ location.sql }} AS
      SELECT 0 AS id, '2.5' AS v
      UNION ALL
      SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v
      """
    And statement
      """
      CREATE OR REPLACE <kind> VIEW conditional_view_precision AS
      SELECT id, <expression> AS x FROM conditional_view_source
      """
    And config spark.sql.ansi.enabled = <read_ansi>
    When query
      """
      SELECT id, CAST(IF(id = 9, 0L, x) AS DOUBLE) AS value
      FROM <view_name> ORDER BY id
      """
    Then query result ordered
      | id | value |
      | 0  | 0.0   |
      | 1  | 2.5   |

    Examples:
      | kind        | view_name                             | function | expression                        | read_ansi |
      | TEMP        | conditional_view_precision            | IF       | IF(id = 0, 0, v)                  | false     |
      | GLOBAL TEMP | global_temp.conditional_view_precision | IF       | IF(id = 0, 0, v)                  | false     |
      | TEMP        | conditional_view_precision            | CASE     | CASE WHEN id = 0 THEN 0 ELSE v END | false     |
      | GLOBAL TEMP | global_temp.conditional_view_precision | CASE     | CASE WHEN id = 0 THEN 0 ELSE v END | false     |
      | TEMP        | conditional_view_precision            | NVL2     | NVL2(NULLIF(id, 0), v, 0)          | false     |
      | GLOBAL TEMP | global_temp.conditional_view_precision | NVL2     | NVL2(NULLIF(id, 0), v, 0)          | false     |
      | TEMP        | conditional_view_precision            | NVL2     | NVL2(NULLIF(id, 1), 0, v)          | false     |
      | GLOBAL TEMP | global_temp.conditional_view_precision | NVL2     | NVL2(NULLIF(id, 1), 0, v)          | false     |
      | TEMP        | conditional_view_precision            | NVL2     | NVL2(NULLIF(id, 1), 0, v)          | true      |
      | GLOBAL TEMP | global_temp.conditional_view_precision | NVL2     | NVL2(NULLIF(id, 1), 0, v)          | true      |

  Scenario Outline: An ANSI <kind> numeric-first UNION view retains fractional precision when read without ANSI
    Given config spark.sql.ansi.enabled = true
    And final statement
      """
      DROP VIEW IF EXISTS <view_name>
      """
    And statement
      """
      CREATE OR REPLACE <kind> VIEW conditional_view_precision AS
      SELECT 0 AS id, CAST(2.5 AS FLOAT) AS v
      UNION ALL
      SELECT 1 AS id, '16777217.25' AS v
      """
    And config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, CAST(IF(id = 9, 0L, v) AS DOUBLE) AS value
      FROM <view_name> ORDER BY id
      """
    Then query result collected ordered
      | id | value       |
      | 0  | 2.5         |
      | 1  | 16777217.25 |

    Examples:
      | kind        | view_name                             |
      | TEMP        | conditional_view_precision            |
      | GLOBAL TEMP | global_temp.conditional_view_precision |

  Scenario Outline: A non-ANSI <kind> STRING view keeps unrelated numeric conditionals precisely typed
    Given config spark.sql.ansi.enabled = false
    And final statement
      """
      DROP VIEW IF EXISTS <view_name>
      """
    And statement
      """
      CREATE OR REPLACE <kind> VIEW conditional_view_precision AS
      SELECT 0 AS id, '2.5' AS v, <first_extra> AS extra
      UNION ALL
      SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v, <second_extra> AS extra
      """
    When query
      """
      SELECT id, typeof(IF(id = 0, 1, 2L)) AS result_type
      FROM <view_name> ORDER BY id
      """
    Then query result ordered
      | id | result_type |
      | 0  | bigint      |
      | 1  | bigint      |

    Examples:
      | kind        | view_name                             | first_extra       | second_extra                    |
      | TEMP        | conditional_view_precision            | 1                 | 2                               |
      | GLOBAL TEMP | global_temp.conditional_view_precision | 1                 | 2                               |
      | TEMP        | conditional_view_precision            | DATE '2020-01-01' | TIMESTAMP '2020-01-02 00:00:00' |
      | GLOBAL TEMP | global_temp.conditional_view_precision | DATE '2020-01-01' | TIMESTAMP '2020-01-02 00:00:00' |

  Scenario Outline: A stored <wrapper> conditional in a <kind> view keeps unrelated numeric expressions typed
    Given config spark.sql.ansi.enabled = true
    And final statement
      """
      DROP VIEW IF EXISTS <view_name>
      """
    And statement
      """
      CREATE OR REPLACE <kind> VIEW conditional_view_precision AS
      SELECT id, <expression> AS x
      FROM (
        SELECT 0 AS id, '2.5' AS v
        UNION ALL
        SELECT 1 AS id, CAST(2.5 AS DECIMAL(2,1)) AS v
      ) AS q
      """
    And config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, typeof(IF(id = 0, 1, 2L)) AS result_type
      FROM <view_name> ORDER BY id
      """
    Then query result ordered
      | id | result_type |
      | 0  | bigint      |
      | 1  | bigint      |

    Examples:
      | kind        | view_name                             | wrapper | expression                           |
      | TEMP        | conditional_view_precision            | unused  | IF(id = 0, 0, v)                     |
      | GLOBAL TEMP | global_temp.conditional_view_precision | unused  | IF(id = 0, 0, v)                     |
      | TEMP        | conditional_view_precision            | STRING  | CAST(IF(id = 0, 0, v) AS STRING)     |
      | GLOBAL TEMP | global_temp.conditional_view_precision | STRING  | CAST(IF(id = 0, 0, v) AS STRING)     |
      | TEMP        | conditional_view_precision            | DOUBLE  | CAST(IF(id = 0, 0, v) AS DOUBLE)     |
      | GLOBAL TEMP | global_temp.conditional_view_precision | DOUBLE  | CAST(IF(id = 0, 0, v) AS DOUBLE)     |
      | TEMP        | conditional_view_precision            | CONCAT  | CONCAT(IF(id = 0, 0, v), '')         |
      | GLOBAL TEMP | global_temp.conditional_view_precision | CONCAT  | CONCAT(IF(id = 0, 0, v), '')         |

  Scenario Outline: A <kind> view keeps unrelated interval metadata beside <expression>
    Given config spark.sql.ansi.enabled = true
    And final statement
      """
      DROP VIEW IF EXISTS <view_name>
      """
    And statement
      """
      CREATE OR REPLACE <kind> VIEW conditional_view_precision AS
      SELECT id, <expression> AS x, i
      FROM (
        SELECT 0 AS id, CAST(2.5 AS FLOAT) AS v, INTERVAL '2' MONTH AS i
        UNION ALL
        SELECT 1 AS id, '7.5' AS v, INTERVAL '1' YEAR AS i
      ) AS q
      """
    And config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, CAST(i AS INT) AS months, typeof(IF(id = 0, 1, 2L)) AS result_type
      FROM <view_name> ORDER BY id
      """
    Then query result ordered
      | id | months | result_type |
      | 0  | 2      | bigint      |
      | 1  | 12     | bigint      |

    Examples:
      | kind        | view_name                             | expression      |
      | TEMP        | conditional_view_precision            | v               |
      | GLOBAL TEMP | global_temp.conditional_view_precision | v               |
      | TEMP        | conditional_view_precision            | IF(id = 0, 0, v) |
      | GLOBAL TEMP | global_temp.conditional_view_precision | IF(id = 0, 0, v) |

  Scenario Outline: Stored conditional repair preserves sibling fields inside a <container>
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And final statement
      """
      DROP VIEW IF EXISTS conditional_view_nested
      """
    And statement
      """
      CREATE OR REPLACE TEMP VIEW conditional_view_nested AS
      SELECT id, <value> AS s
      FROM (
        SELECT 0 AS id, CAST(2.5 AS FLOAT) AS v,
               DATE '2020-01-01' AS y, INTERVAL '2' MONTH AS i
        UNION ALL
        SELECT 1 AS id, '7.5' AS v,
               TIMESTAMP '2020-01-02 03:04:05' AS y, INTERVAL '1' YEAR AS i
      ) q
      """
    And config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, CAST(IF(id = 9, 0L, <field>.x) AS DOUBLE) AS value,
             CAST(to_utc_timestamp(<field>.y, 'UTC') AS STRING) AS utc_value,
             CAST(from_utc_timestamp(<field>.y, 'UTC') AS STRING) AS local_value,
             CAST(<field>.i AS INT) AS months
      FROM conditional_view_nested ORDER BY id
      """
    Then query result ordered
      | id | value | utc_value           | local_value         | months |
      | 0  | 0.0   | 2020-01-01 00:00:00 | 2020-01-01 00:00:00 | 2      |
      | 1  | 7.5   | 2020-01-02 03:04:05 | 2020-01-02 03:04:05 | 12     |

    Examples:
      | container | value                                                              | field  |
      | struct    | named_struct('x', IF(id = 0, 0, v), 'y', y, 'i', i)                  | s      |
      | array     | array(named_struct('x', IF(id = 0, 0, v), 'y', y, 'i', i))           | s[0]   |
      | map       | map('k', named_struct('x', IF(id = 0, 0, v), 'y', y, 'i', i))        | s['k'] |

  Scenario Outline: An ANSI <kind> view preserves numeric sibling division when read without ANSI
    Given config spark.sql.ansi.enabled = true
    And final statement
      """
      DROP VIEW IF EXISTS <view_name>
      """
    And statement
      """
      CREATE OR REPLACE <kind> VIEW conditional_view_numeric_siblings AS
      SELECT id, IF(id = 0, 0, v) AS x,
        IF(id = 0, CAST(1.25 AS FLOAT), CAST(16777217.25 AS DECIMAL(10,2))) AS n,
        named_struct('x', IF(id = 0, 0, v),
          'n', IF(id = 0, CAST(1.25 AS FLOAT), CAST(16777217.25 AS DECIMAL(10,2)))) AS s
      FROM VALUES (0, '2'), (1, '2') AS t(id, v)
      """
    And config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, n / 2 AS scalar_value, typeof(n / 2) AS scalar_type,
        s.n / 2 AS nested_value, typeof(s.n / 2) AS nested_type
      FROM <view_name> ORDER BY id
      """
    Then query result collected ordered
      | id | scalar_value | scalar_type | nested_value | nested_type |
      | 0  | 0.625        | double      | 0.625        | double      |
      | 1  | 8388608.625  | double      | 8388608.625  | double      |

    Examples:
      | kind        | view_name                                    |
      | TEMP        | conditional_view_numeric_siblings            |
      | GLOBAL TEMP | global_temp.conditional_view_numeric_siblings |

  Scenario Outline: An ANSI <kind> view preserves late numeric values in <function> over a <container>
    Given config spark.sql.ansi.enabled = true
    And final statement
      """
      DROP VIEW IF EXISTS <view_name>
      """
    And statement
      """
      CREATE OR REPLACE <kind> VIEW conditional_view_late_numeric AS
      SELECT id, IF(id = 0, 0, v) AS x, <stored_expression> AS y
      FROM VALUES (0, '7'), (1, '8') AS t(id, v)
      """
    And config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, CAST(<expression> AS DOUBLE) AS value
      FROM <view_name> ORDER BY id
      """
    Then query result ordered
      | id | value |
      | 0  | 0.0   |
      | 1  | 2.5   |

    Examples:
      | kind        | view_name                                | function | container | stored_expression                         | expression                                   |
      | TEMP        | conditional_view_late_numeric            | IF       | scalar    | IF(id = 0, 0, 2.5D)                        | IF(id = 9, 0L, y)                            |
      | GLOBAL TEMP | global_temp.conditional_view_late_numeric | IF       | scalar    | IF(id = 0, 0, 2.5D)                        | IF(id = 9, 0L, y)                            |
      | TEMP        | conditional_view_late_numeric            | CASE     | scalar    | IF(id = 0, 0, 2.5D)                        | CASE WHEN id = 9 THEN 0L ELSE y END           |
      | GLOBAL TEMP | global_temp.conditional_view_late_numeric | CASE     | scalar    | IF(id = 0, 0, 2.5D)                        | CASE WHEN id = 9 THEN 0L ELSE y END           |
      | TEMP        | conditional_view_late_numeric            | NVL2     | scalar    | IF(id = 0, 0, 2.5D)                        | NVL2(NULLIF(id, 1), 0L, y)                    |
      | GLOBAL TEMP | global_temp.conditional_view_late_numeric | NVL2     | scalar    | IF(id = 0, 0, 2.5D)                        | NVL2(NULLIF(id, 1), 0L, y)                    |
      | TEMP        | conditional_view_late_numeric            | IF       | struct    | named_struct('n', IF(id = 0, 0, 2.5D))     | IF(id = 9, 0L, y.n)                          |
      | GLOBAL TEMP | global_temp.conditional_view_late_numeric | IF       | struct    | named_struct('n', IF(id = 0, 0, 2.5D))     | IF(id = 9, 0L, y.n)                          |
      | TEMP        | conditional_view_late_numeric            | CASE     | struct    | named_struct('n', IF(id = 0, 0, 2.5D))     | CASE WHEN id = 9 THEN 0L ELSE y.n END         |
      | GLOBAL TEMP | global_temp.conditional_view_late_numeric | CASE     | struct    | named_struct('n', IF(id = 0, 0, 2.5D))     | CASE WHEN id = 9 THEN 0L ELSE y.n END         |
      | TEMP        | conditional_view_late_numeric            | NVL2     | struct    | named_struct('n', IF(id = 0, 0, 2.5D))     | NVL2(NULLIF(id, 1), 0L, y.n)                  |
      | GLOBAL TEMP | global_temp.conditional_view_late_numeric | NVL2     | struct    | named_struct('n', IF(id = 0, 0, 2.5D))     | NVL2(NULLIF(id, 1), 0L, y.n)                  |

  Scenario Outline: An ANSI <kind> view preserves NVL2 values when both branches initially appear integral
    Given config spark.sql.ansi.enabled = true
    And final statement
      """
      DROP VIEW IF EXISTS <view_name>
      """
    And statement
      """
      CREATE OR REPLACE <kind> VIEW conditional_view_same_type AS
      SELECT id, IF(id = 0, 0, v) AS x, IF(id = 0, 0, 2.5D) AS y
      FROM VALUES (0, '7'), (1, '8') AS t(id, v)
      """
    And config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, CAST(<expression> AS DOUBLE) AS value
      FROM <view_name> ORDER BY id
      """
    Then query result ordered
      | id | value |
      | 0  | 0.0   |
      | 1  | 2.5   |

    Examples:
      | kind        | view_name                             | expression                |
      | TEMP        | conditional_view_same_type            | NVL2(NULLIF(id, 0), y, 0) |
      | GLOBAL TEMP | global_temp.conditional_view_same_type | NVL2(NULLIF(id, 0), y, 0) |
      | TEMP        | conditional_view_same_type            | NVL2(NULLIF(id, 1), 0, y) |
      | GLOBAL TEMP | global_temp.conditional_view_same_type | NVL2(NULLIF(id, 1), 0, y) |

  Scenario Outline: An ANSI <kind> view preserves sibling precision in <function> over a <container>
    Given config spark.sql.ansi.enabled = true
    And final statement
      """
      DROP VIEW IF EXISTS <view_name>
      """
    And statement
      """
      CREATE OR REPLACE <kind> VIEW conditional_view_sibling_precision AS
      SELECT id, IF(id = 0, 0, v) AS x, <stored_expression> AS y
      FROM VALUES (0, '7'), (1, '8') AS t(id, v)
      """
    And config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, CAST(<expression> AS DOUBLE) AS value
      FROM <view_name> ORDER BY id
      """
    Then query result collected ordered
      | id | value      |
      | 0  | 16777217.0 |
      | 1  | 1.25       |

    Examples:
      | kind        | view_name                                     | function | container | stored_expression                                                               | expression                                    |
      | TEMP        | conditional_view_sibling_precision            | IF       | scalar    | IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DOUBLE))                              | IF(id = 0, 16777217L, y)                      |
      | GLOBAL TEMP | global_temp.conditional_view_sibling_precision | IF       | scalar    | IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DOUBLE))                              | IF(id = 0, 16777217L, y)                      |
      | TEMP        | conditional_view_sibling_precision            | CASE     | scalar    | IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DOUBLE))                              | CASE WHEN id = 0 THEN 16777217L ELSE y END     |
      | GLOBAL TEMP | global_temp.conditional_view_sibling_precision | CASE     | scalar    | IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DOUBLE))                              | CASE WHEN id = 0 THEN 16777217L ELSE y END     |
      | TEMP        | conditional_view_sibling_precision            | NVL2     | scalar    | IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DOUBLE))                              | NVL2(NULLIF(id, 1), 16777217L, y)              |
      | GLOBAL TEMP | global_temp.conditional_view_sibling_precision | NVL2     | scalar    | IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DOUBLE))                              | NVL2(NULLIF(id, 1), 16777217L, y)              |
      | TEMP        | conditional_view_sibling_precision            | IF       | struct    | named_struct('n', IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DOUBLE)))           | IF(id = 0, 16777217L, y.n)                    |
      | GLOBAL TEMP | global_temp.conditional_view_sibling_precision | IF       | struct    | named_struct('n', IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DOUBLE)))           | IF(id = 0, 16777217L, y.n)                    |
      | TEMP        | conditional_view_sibling_precision            | CASE     | struct    | named_struct('n', IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DOUBLE)))           | CASE WHEN id = 0 THEN 16777217L ELSE y.n END   |
      | GLOBAL TEMP | global_temp.conditional_view_sibling_precision | CASE     | struct    | named_struct('n', IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DOUBLE)))           | CASE WHEN id = 0 THEN 16777217L ELSE y.n END   |
      | TEMP        | conditional_view_sibling_precision            | NVL2     | struct    | named_struct('n', IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DOUBLE)))           | NVL2(NULLIF(id, 1), 16777217L, y.n)            |
      | GLOBAL TEMP | global_temp.conditional_view_sibling_precision | NVL2     | struct    | named_struct('n', IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DOUBLE)))           | NVL2(NULLIF(id, 1), 16777217L, y.n)            |

  Scenario Outline: An ANSI <kind> view keeps its exposed integral conditional type without ANSI
    Given config spark.sql.ansi.enabled = true
    And final statement
      """
      DROP VIEW IF EXISTS <view_name>
      """
    And statement
      """
      CREATE OR REPLACE <kind> VIEW conditional_view_integral_type AS
      SELECT id, IF(id = 0, 0L, v) AS x
      FROM VALUES (0, '2'), (1, '2') AS t(id, v)
      """
    And config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, typeof(x) AS stored_type,
        typeof(IF(id = 9, 0L, x)) AS conditional_type
      FROM <view_name> ORDER BY id
      """
    Then query result ordered
      | id | stored_type | conditional_type |
      | 0  | bigint      | bigint           |
      | 1  | bigint      | bigint           |

    Examples:
      | kind        | view_name                                 |
      | TEMP        | conditional_view_integral_type            |
      | GLOBAL TEMP | global_temp.conditional_view_integral_type |

  Scenario Outline: An ANSI view retains DOUBLE results with a late <source> in <function> and <order> order
    Given config spark.sql.ansi.enabled = true
    And final statement
      """
      DROP VIEW IF EXISTS conditional_view_late_decimal
      """
    And statement
      """
      CREATE OR REPLACE TEMP VIEW conditional_view_late_decimal AS
      SELECT id, IF(id = 0, 0, v) AS x, <stored_expression> AS y
      FROM VALUES (0, '7'), (1, '8') AS t(id, v)
      """
    And config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, <expression> AS value
      FROM conditional_view_late_decimal ORDER BY id
      """
    Then query result collected ordered
      | id | value       |
      | 0  | 16777217.25 |
      | 1  | 1.25        |

    Examples:
      | source        | function | order    | stored_expression                                        | expression                              |
      | FLOAT-DECIMAL | IF       | constant | IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DECIMAL(5,2))) | IF(id = 0, 16777217.25D, y)             |
      | FLOAT-DECIMAL | IF       | column   | IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DECIMAL(5,2))) | IF(id = 1, y, 16777217.25D)             |
      | FLOAT-DECIMAL | NVL2     | constant | IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DECIMAL(5,2))) | NVL2(NULLIF(id, 1), 16777217.25D, y)     |
      | FLOAT-DECIMAL | NVL2     | column   | IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DECIMAL(5,2))) | NVL2(NULLIF(id, 0), y, 16777217.25D)     |
      | BIGINT-DECIMAL | IF       | constant | IF(id = 0, 0L, CAST(1.25 AS DECIMAL(20,2)))               | IF(id = 0, 16777217.25D, y)             |
      | BIGINT-DECIMAL | IF       | column   | IF(id = 0, 0L, CAST(1.25 AS DECIMAL(20,2)))               | IF(id = 1, y, 16777217.25D)             |
      | BIGINT-DECIMAL | NVL2     | constant | IF(id = 0, 0L, CAST(1.25 AS DECIMAL(20,2)))               | NVL2(NULLIF(id, 1), 16777217.25D, y)     |
      | BIGINT-DECIMAL | NVL2     | column   | IF(id = 0, 0L, CAST(1.25 AS DECIMAL(20,2)))               | NVL2(NULLIF(id, 0), y, 16777217.25D)     |

  Scenario: An ANSI view retains a large FLOAT sibling beside a late DECIMAL in NVL2
    Given config spark.sql.ansi.enabled = true
    And final statement
      """
      DROP VIEW IF EXISTS conditional_view_late_decimal
      """
    And statement
      """
      CREATE OR REPLACE TEMP VIEW conditional_view_late_decimal AS
      SELECT id, IF(id = 0, 0, v) AS x,
        IF(id = 0, CAST(2.5 AS FLOAT), CAST(1.25 AS DECIMAL(5,2))) AS y
      FROM VALUES (0, '7'), (1, '8') AS t(id, v)
      """
    And config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, CAST(NVL2(NULLIF(id, 0), y, CAST(16777217 AS FLOAT)) AS DOUBLE) AS value
      FROM conditional_view_late_decimal ORDER BY id
      """
    Then query result collected ordered
      | id | value      |
      | 0  | 16777216.0 |
      | 1  | 1.25       |

  Scenario Outline: An ANSI <kind> view retains time of day in NVL2 with the <branch> branch selected
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = UTC
    And final statement
      """
      DROP VIEW IF EXISTS <view_name>
      """
    And statement
      """
      CREATE OR REPLACE <kind> VIEW conditional_view_temporal AS
      SELECT 0 AS id, DATE '2020-01-01' AS y
      UNION ALL
      SELECT 1 AS id, TIMESTAMP '2020-01-02 03:04:05' AS y
      """
    And config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, CAST(<expression> AS STRING) AS value,
        unix_micros(<expression>) AS epoch_micros
      FROM <view_name> ORDER BY id
      """
    Then query result ordered
      | id | value               | epoch_micros     |
      | 0  | 2019-01-01 00:00:00 | 1546300800000000 |
      | 1  | 2020-01-02 03:04:05 | 1577934245000000 |

    Examples:
      | kind        | view_name                            | branch | expression                                                                                        |
      | TEMP        | conditional_view_temporal            | null   | NVL2(CASE WHEN id = 0 THEN TIMESTAMP '2020-01-01' END, DATE '2019-01-01', y)                        |
      | GLOBAL TEMP | global_temp.conditional_view_temporal | null   | NVL2(CASE WHEN id = 0 THEN TIMESTAMP '2020-01-01' END, DATE '2019-01-01', y)                        |
      | TEMP        | conditional_view_temporal            | value  | NVL2(CASE WHEN id = 1 THEN TIMESTAMP '2020-01-01' END, y, DATE '2019-01-01')                        |
      | GLOBAL TEMP | global_temp.conditional_view_temporal | value  | NVL2(CASE WHEN id = 1 THEN TIMESTAMP '2020-01-01' END, y, DATE '2019-01-01')                        |

  @sail-bug
  Scenario Outline: A stored NVL2 preserves its creation timezone from <creation_zone> to <reader_zone>
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = <creation_zone>
    And final statement
      """
      DROP VIEW IF EXISTS conditional_view_timezone
      """
    And statement
      """
      CREATE OR REPLACE TEMP VIEW conditional_view_timezone AS
      SELECT id,
        NVL2(CASE WHEN id = 0 THEN TIMESTAMP '2020-01-01' END,
             DATE '2019-01-01', y) AS x
      FROM (
        SELECT 0 AS id, DATE '2020-01-01' AS y
        UNION ALL
        SELECT 1 AS id, TIMESTAMP '2020-01-02 03:04:05' AS y
      ) AS q
      """
    And config spark.sql.ansi.enabled = false
    And config spark.sql.session.timeZone = <reader_zone>
    When query
      """
      SELECT id, CAST(x AS STRING) AS value, unix_micros(x) AS epoch_micros
      FROM conditional_view_timezone ORDER BY id
      """
    Then query result ordered
      | id | value          | epoch_micros   |
      | 0  | <first_value>  | <first_micros> |
      | 1  | <second_value> | <second_micros> |

    # Shared temporal casts in a stored plan can still use the reader's timezone.
    Examples:
      | creation_zone       | reader_zone         | first_value         | first_micros     | second_value        | second_micros    |
      | UTC                 | America/Los_Angeles | 2018-12-31 16:00:00 | 1546300800000000 | 2020-01-01 19:04:05 | 1577934245000000 |
      | America/Los_Angeles | UTC                 | 2019-01-01 08:00:00 | 1546329600000000 | 2020-01-02 11:04:05 | 1577963045000000 |
