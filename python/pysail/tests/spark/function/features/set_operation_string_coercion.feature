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
      | true  | STRING/BOOLEAN arrays | array('true') | array(false) |
      | true  | BOOLEAN/STRING arrays | array(false) | array('true') |
      | true  | STRING/BOOLEAN structs | named_struct('x', 'true') | named_struct('x', false) |
      | true  | BOOLEAN/STRING structs | named_struct('x', false) | named_struct('x', 'true') |
      | true  | STRING/BOOLEAN map values | map('k', 'true') | map('k', false) |
      | true  | BOOLEAN/STRING map values | map('k', false) | map('k', 'true') |
      | true  | STRING/BOOLEAN nested structs | array(named_struct('x', 'true', 'n', 1)) | array(named_struct('x', false, 'n', 2L)) |
      | true  | STRING/BOOLEAN nested map keys | map(array('true'), 1) | map(array(false), 2) |
      | true  | STRING/BOOLEAN maps with widened keys | map(1, 'true') | map(2L, false) |
      | true  | STRING/BOOLEAN maps with timestamp keys | map(DATE '2020-01-01', 'true') | map(TIMESTAMP_LTZ '2020-01-02', false) |
      | false | STRING/INTERVAL arrays | array('x') | array(INTERVAL '1' DAY) |
      | false | INTERVAL/STRING arrays | array(INTERVAL '1' YEAR) | array('x') |
      | false | STRING/INTERVAL structs | named_struct('x', 'x') | named_struct('x', INTERVAL '1' YEAR) |
      | false | INTERVAL/STRING structs | named_struct('x', INTERVAL '1' DAY) | named_struct('x', 'x') |
      | false | STRING/INTERVAL map values | map('k', 'x') | map('k', INTERVAL '1' DAY) |
      | false | INTERVAL/STRING map values | map('k', INTERVAL '1' YEAR) | map('k', 'x') |
      | false | STRING/INTERVAL nested structs | array(named_struct('x', 'x')) | array(named_struct('x', INTERVAL '1' YEAR)) |
      | false | STRING/INTERVAL map keys | map('x', 1) | map(INTERVAL '1' DAY, 2) |

  Scenario Outline: A temporary view over an ANSI STRING/BOOLEAN UNION of <container> can be created without being queried
    Given config spark.sql.ansi.enabled = true
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW union_string_boolean_view AS
      SELECT <first> AS c UNION ALL SELECT <second>
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

    Examples:
      | container | first | second |
      | scalars | 'true' | false |
      | arrays | array('true') | array(false) |
      | structs | named_struct('x', 'true') | named_struct('x', false) |
      | map values | map('k', 'true') | map('k', false) |

  Scenario Outline: An unused UNION still rejects incompatible <pair> columns with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      WITH t AS (SELECT <first> AS c UNION ALL SELECT <second>) SELECT 1 AS x
      """
    Then query error (?i)(INCOMPATIBLE_COLUMN_TYPE|Incompatible inputs for Union)

    Examples:
      | ansi | pair | first | second |
      | true | INT/ARRAY | 1 | array(1) |
      | true | BOOLEAN/INT | true | 1 |
      | false | STRING/BOOLEAN arrays | array('true') | array(false) |
      | true | different struct names | named_struct('x', 'true') | named_struct('y', false) |
      | true | nullable BOOLEAN map keys | map('true', 1) | map(false, 2) |
      | true | nullable numeric map keys beside BOOLEAN values | map('1', 'true') | map(2, false) |
      | true | DATE/numeric map keys | map(1, 'true') | map(DATE '2020-01-01', false) |
      | true | DATE/TIMESTAMP_NTZ map keys | map(DATE '2020-01-01', 'true') | map(TIMESTAMP_NTZ '2020-01-02', false) |
      | true | different interval families in map keys | map(INTERVAL '1' DAY, 'true') | map(INTERVAL '1' YEAR, false) |

  Scenario Outline: Unused nested UNION accepts safe decimal map-key casts
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = false
    When query
      """
      WITH t AS (
        SELECT map(<first>, 'true') AS c
        UNION ALL SELECT map(CAST(.1 AS DECIMAL(38,38)), false)
      ) SELECT 1 AS x
      """
    Then query result collected
      | x |
      | 1 |

    Examples:
      | first |
      | CAST(1 AS DECIMAL(38,0)) |
      | 1L |

  Scenario Outline: Unused nested UNION rejects decimal map-key casts that can overflow
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
    When query
      """
      WITH t AS (
        SELECT map(<first>, 'true') AS c
        UNION ALL SELECT map(CAST(.1 AS DECIMAL(38,38)), false)
      ) SELECT 1 AS x
      """
    Then query error (?i)(INCOMPATIBLE_COLUMN_TYPE|Incompatible inputs for Union)

    Examples:
      | first |
      | CAST(1 AS DECIMAL(38,0)) |
      | 1L |

  # TODO: Coerce these nested STRING pairs when the UNION is actually used.
  # DataFusion rejects their common types during analysis, before column pruning.
  @sail-bug
  Scenario Outline: A referenced UNION can prune compatible nested <pair> values
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT count(*) AS n FROM (SELECT <first> AS c UNION ALL SELECT <second>)
      """
    Then query result collected
      | n |
      | 2 |

    Examples:
      | ansi | pair | first | second |
      | true | STRING/BOOLEAN arrays | array('true') | array(false) |
      | true | STRING/BOOLEAN structs | named_struct('x', 'true') | named_struct('x', false) |
      | true | STRING/BOOLEAN map values | map('k', 'true') | map('k', false) |
      | false | STRING/INTERVAL arrays | array('x') | array(INTERVAL '1' DAY) |
      | false | STRING/INTERVAL structs | named_struct('x', 'x') | named_struct('x', INTERVAL '1' YEAR) |
      | false | STRING/INTERVAL map values | map('k', 'x') | map('k', INTERVAL '1' DAY) |

  Scenario Outline: Unused nested UNION fallback respects case-sensitive struct names with <case_sensitive>
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.caseSensitive = <case_sensitive>
    When query
      """
      WITH t AS (
        SELECT named_struct('x', 'true') AS c
        UNION ALL SELECT named_struct('<field>', false)
      ) SELECT 1 AS x
      """
    Then query result collected
      | x |
      | 1 |

    Examples:
      | case_sensitive | field |
      | false | X |
      | true | x |

  Scenario: Unused nested UNION rejects different field case in case-sensitive mode
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.caseSensitive = true
    When query
      """
      WITH t AS (
        SELECT named_struct('x', 'true') AS c
        UNION ALL SELECT named_struct('X', false)
      ) SELECT 1 AS x
      """
    Then query error (?i)(INCOMPATIBLE_COLUMN_TYPE|Incompatible inputs for Union)

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
