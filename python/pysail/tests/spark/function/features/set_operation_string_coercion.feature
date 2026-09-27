Feature: UNION common types for STRING with temporal and DataFusion-incompatible inputs

  Scenario Outline: UNION of a STRING-first column and a <temporal> value has type <type> with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT DISTINCT typeof(c) AS result_type
      FROM (SELECT <text> AS c UNION ALL SELECT <value>)
      """
    Then query result collected
      | result_type |
      | <type>      |

    Examples:
      | ansi  | temporal  | text                  | value                           | type      |
      | false | DATE      | '2020-01-02'          | DATE'2020-01-01'                | string    |
      | true  | DATE      | '2020-01-02'          | DATE'2020-01-01'                | date      |
      | false | TIMESTAMP | '2020-01-02 00:00:00' | TIMESTAMP'2020-01-01 01:02:03'  | string    |
      | true  | TIMESTAMP | '2020-01-02 00:00:00' | TIMESTAMP'2020-01-01 01:02:03'  | timestamp |

  Scenario Outline: Non-ANSI UNION of a STRING-first column and a <temporal> value keeps a STRING schema
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT * FROM (SELECT <text> AS c UNION ALL SELECT <value>)
      """
    Then query schema
      """
      root
       |-- c: string (nullable = false)
      """

    Examples:
      | temporal  | text                  | value                          |
      | DATE      | '2020-01-02'          | DATE'2020-01-01'               |
      | TIMESTAMP | '2020-01-02 00:00:00' | TIMESTAMP'2020-01-01 01:02:03' |

  Scenario: Non-ANSI UNION of a STRING-first column and a DATE value concatenates as STRING
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT concat(c, '#') AS result
      FROM (SELECT '2020-01-02' AS c UNION ALL SELECT DATE'2020-01-01')
      """
    Then query result collected
      | result      |
      | 2020-01-01# |
      | 2020-01-02# |

  Scenario Outline: An unreferenced CTE over a <pair> UNION resolves with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      WITH t AS (SELECT <first> AS c UNION ALL SELECT <second>) SELECT 1 AS x
      """
    Then query result collected
      | x |
      | 1 |

    Examples:
      | ansi  | pair            | first            | second           |
      | true  | STRING/BOOLEAN  | 'true'           | false            |
      | true  | BOOLEAN/STRING  | false            | 'true'           |
      | false | INTERVAL/STRING | INTERVAL '1' DAY | 'x'              |
      | false | STRING/INTERVAL | 'x'              | INTERVAL '1' DAY |

  Scenario: A temporary view over an ANSI STRING/BOOLEAN UNION can be created without being queried
    Given config spark.sql.ansi.enabled = true
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW union_string_boolean_view AS
      SELECT 'true' AS c UNION ALL SELECT false
      """
    Given final statement
      """
      DROP VIEW IF EXISTS union_string_boolean_view
      """
    When query
      """
      SELECT 1 AS x
      """
    Then query result collected
      | x |
      | 1 |

  Scenario Outline: UNION of incompatible <pair> columns fails with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT <first> AS c UNION ALL SELECT <second>
      """
    Then query error (?i)(INCOMPATIBLE_COLUMN_TYPE|Incompatible inputs for Union)

    Examples:
      | ansi  | pair          | first | second   |
      | true  | INT/ARRAY     | 1     | array(1) |
      | false | INT/ARRAY     | 1     | array(1) |
      | true  | BOOLEAN/INT   | true  | 1        |
      | false | BOOLEAN/INT   | true  | 1        |
      | false | STRING/BOOLEAN | 'true' | false  |

  Scenario Outline: Non-ANSI INSERT of a STRING-first <temporal> UNION into a STRING column writes readable STRING values
    Given config spark.sql.ansi.enabled = false
    Given variable location for temporary directory union_string_temporal_target
    Given final statement
      """
      DROP TABLE IF EXISTS union_string_temporal_target
      """
    Given statement template
      """
      CREATE TABLE union_string_temporal_target (c STRING)
      USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO union_string_temporal_target SELECT <text> AS c UNION ALL SELECT <value>
      """
    When query
      """
      SELECT c FROM union_string_temporal_target
      """
    Then query result collected
      | c        |
      | <first>  |
      | <second> |

    Examples:
      | temporal  | text                  | value                          | first               | second              |
      | DATE      | '2020-01-02'          | DATE'2020-01-01'               | 2020-01-02          | 2020-01-01          |
      | TIMESTAMP | '2020-01-02 00:00:00' | TIMESTAMP'2020-01-01 01:02:03' | 2020-01-02 00:00:00 | 2020-01-01 01:02:03 |

  Scenario: Non-ANSI CTAS from a STRING-first DATE UNION creates a STRING column
    Given config spark.sql.ansi.enabled = false
    Given variable location for temporary directory union_string_date_ctas
    Given final statement
      """
      DROP TABLE IF EXISTS union_string_date_ctas
      """
    Given statement template
      """
      CREATE TABLE union_string_date_ctas USING parquet LOCATION {{ location.sql }} AS
      SELECT '2020-01-02' AS c UNION ALL SELECT DATE'2020-01-01'
      """
    When query
      """
      SELECT typeof(c) AS result_type, c FROM union_string_date_ctas
      """
    Then query result collected
      | result_type | c          |
      | string      | 2020-01-02 |
      | string      | 2020-01-01 |

  Scenario: Non-ANSI UNION of a STRING-first column and a TIMESTAMP value formats TIMESTAMP values as STRING
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT c, length(c) AS n
      FROM (SELECT '2020-01-02 00:00:00' AS c UNION ALL SELECT TIMESTAMP'2020-01-01 01:02:03')
      """
    Then query result collected
      | c                   | n  |
      | 2020-01-02 00:00:00 | 19 |
      | 2020-01-01 01:02:03 | 19 |
