Feature: Stored timestamps retain their values when the session timezone changes

  Scenario Outline: Repeated fields remain distinct inside a <shape>
    Given config spark.sql.session.timeZone = UTC
    And final statement
      """
      DROP VIEW IF EXISTS timestamp_repeated_fields
      """
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW timestamp_repeated_fields AS
      SELECT <expression> AS stored FROM (
        SELECT named_struct('t', timestamp_seconds(0), 't', timestamp_seconds(3600)) AS s
      )
      """
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(stored AS STRING) AS result FROM timestamp_repeated_fields
      """
    Then query result
      | result   |
      | <result> |

    Examples:
      | shape         | expression               | result                                     |
      | struct        | s                        | {1969-12-31 16:00:00, 1969-12-31 17:00:00}   |
      | list          | array(s)                 | [{1969-12-31 16:00:00, 1969-12-31 17:00:00}] |
      | nested struct | named_struct('outer', s)  | {{1969-12-31 16:00:00, 1969-12-31 17:00:00}} |

  Scenario Outline: Grouped struct wildcard <arguments> preserves <timestamp_type> after a timezone change
    Given config spark.sql.session.timeZone = UTC
    And final statement
      """
      DROP VIEW IF EXISTS timestamp_grouped_star
      """
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW timestamp_grouped_star AS
      SELECT CAST(timestamp_seconds(seconds) AS <timestamp_type>) AS ts
      FROM VALUES (0L), (0L), (CAST(NULL AS BIGINT)) AS t(seconds)
      """
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(actual.ts AS STRING) AS value,
             CAST(explicit.ts AS STRING) AS explicit_value,
             typeof(actual) AS value_type, actual IS NULL AS is_null, copies
      FROM (
        SELECT struct(<arguments>) AS actual, struct(ts) AS explicit, count(*) AS copies
        FROM timestamp_grouped_star v
        GROUP BY <grouping>
      )
      ORDER BY value NULLS FIRST
      """
    Then query result ordered
      | value   | explicit_value | value_type    | is_null | copies |
      | NULL    | NULL           | <struct_type> | false   | 1      |
      | <value> | <value>        | <struct_type> | false   | 2      |

    Examples:
      | timestamp_type | arguments | grouping   | struct_type              | value               |
      | TIMESTAMP_LTZ  | *         | ts         | struct<ts:timestamp>     | 1969-12-31 16:00:00 |
      | TIMESTAMP_NTZ  | *         | ts         | struct<ts:timestamp_ntz> | 1970-01-01 00:00:00 |
      | TIMESTAMP_LTZ  | v.*       | ts         | struct<ts:timestamp>     | 1969-12-31 16:00:00 |
      | TIMESTAMP_NTZ  | v.*       | ts         | struct<ts:timestamp_ntz> | 1970-01-01 00:00:00 |
      | TIMESTAMP_LTZ  | *         | struct(ts) | struct<ts:timestamp>     | 1969-12-31 16:00:00 |
      | TIMESTAMP_NTZ  | *         | struct(ts) | struct<ts:timestamp_ntz> | 1970-01-01 00:00:00 |

  Scenario Outline: Grouped nested struct wildcard preserves <timestamp_type> after a timezone change
    Given config spark.sql.session.timeZone = UTC
    And final statement
      """
      DROP VIEW IF EXISTS timestamp_grouped_nested_star
      """
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW timestamp_grouped_nested_star AS
      SELECT named_struct(
        'ts', CAST(timestamp_seconds(seconds) AS <timestamp_type>), 'n', 7
      ) AS s
      FROM VALUES (0L), (0L), (CAST(NULL AS BIGINT)) AS t(seconds)
      """
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(actual.ts AS STRING) AS value,
             CAST(explicit.ts AS STRING) AS explicit_value,
             typeof(actual) AS value_type, actual.n AS n, copies
      FROM (
        SELECT struct(s.*) AS actual, struct(s.ts, s.n) AS explicit, count(*) AS copies
        FROM timestamp_grouped_nested_star
        GROUP BY s.ts, s.n
      )
      ORDER BY value NULLS FIRST
      """
    Then query result ordered
      | value   | explicit_value | value_type    | n | copies |
      | NULL    | NULL           | <struct_type> | 7 | 1      |
      | <value> | <value>        | <struct_type> | 7 | 2      |

    Examples:
      | timestamp_type | struct_type                    | value               |
      | TIMESTAMP_LTZ  | struct<ts:timestamp,n:int>     | 1969-12-31 16:00:00 |
      | TIMESTAMP_NTZ  | struct<ts:timestamp_ntz,n:int> | 1970-01-01 00:00:00 |

  Scenario Outline: Grouped struct wildcard still rejects an ungrouped <timestamp_type> column
    Given config spark.sql.session.timeZone = UTC
    And final statement
      """
      DROP VIEW IF EXISTS timestamp_ungrouped_star
      """
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW timestamp_ungrouped_star AS
      SELECT CAST(timestamp_seconds(id) AS <timestamp_type>) AS ts, id % 2 AS k
      FROM range(3)
      """
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT struct(*) FROM timestamp_ungrouped_star GROUP BY k
      """
    Then query error (?i)(MISSING_AGGREGATION|No field named)

    Examples:
      | timestamp_type |
      | TIMESTAMP_LTZ  |
      | TIMESTAMP_NTZ  |
