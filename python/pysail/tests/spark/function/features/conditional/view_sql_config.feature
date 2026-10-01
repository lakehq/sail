Feature: Persistent views capture SQL configuration

  Scenario Outline: View timestamp literals use the captured time zone unless current configurations are requested
    Given config spark.sql.session.timeZone = <creation_zone>
    And final statement
      """
      DROP VIEW IF EXISTS view_sql_config_timezone
      """
    And statement
      """
      CREATE VIEW view_sql_config_timezone AS
      SELECT unix_micros(TIMESTAMP '2020-01-01 00:00:00') AS epoch_micros
      """
    And config spark.sql.session.timeZone = <reader_zone>
    And config spark.sql.legacy.useCurrentConfigsForView = <use_current>
    When query
      """
      SELECT epoch_micros FROM view_sql_config_timezone
      """
    Then query result
      | epoch_micros |
      | <expected>  |

    Examples:
      | creation_zone       | reader_zone         | use_current | expected         |
      | UTC                 | America/Los_Angeles | false       | 1577836800000000 |
      | America/Los_Angeles | UTC                 | false       | 1577865600000000 |
      | UTC                 | America/Los_Angeles | true        | 1577865600000000 |
      | America/Los_Angeles | UTC                 | true        | 1577836800000000 |

  Scenario: View SQL configuration replaces supplied properties only under its configuration prefix
    Given config spark.sql.caseSensitive = false
    And final statement
      """
      DROP VIEW IF EXISTS view_sql_config_case
      """
    And statement
      """
      CREATE VIEW view_sql_config_case
      TBLPROPERTIES (
        'view.sqlConfig.spark.sql.caseSensitive' = 'true',
        'view.spark.sql.caseSensitive' = 'invalid',
        'review.owner' = 'team'
      ) AS SELECT a AS value FROM VALUES (7) AS t(A)
      """
    And config spark.sql.caseSensitive = true
    When query
      """
      SELECT value FROM view_sql_config_case
      """
    Then query result
      | value |
      | 7     |

  Scenario Outline: Views capture ordinary SQL settings as well as ANSI mode
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.legacy.sizeOfNull = <creation_value>
    And final statement
      """
      DROP VIEW IF EXISTS view_sql_config_null_size
      """
    And statement
      """
      CREATE VIEW view_sql_config_null_size AS
      SELECT size(CAST(NULL AS ARRAY<INT>)) AS value
      """
    And config spark.sql.legacy.sizeOfNull = <reader_value>
    And config spark.sql.ansi.enabled = true
    When query
      """
      SELECT value FROM view_sql_config_null_size
      """
    Then query result
      | value      |
      | <expected> |

    Examples:
      | creation_value | reader_value | expected |
      | true           | false        | -1       |
      | false          | true         | NULL     |

  Scenario: Nested views keep their own captured SQL settings
    Given config spark.sql.ansi.enabled = false
    And config spark.sql.legacy.sizeOfNull = true
    And final statement
      """
      DROP VIEW IF EXISTS view_sql_config_inner
      """
    And final statement
      """
      DROP VIEW IF EXISTS view_sql_config_outer
      """
    And statement
      """
      CREATE VIEW view_sql_config_inner AS
      SELECT size(CAST(NULL AS ARRAY<INT>)) AS inner_value
      """
    And config spark.sql.legacy.sizeOfNull = false
    And statement
      """
      CREATE VIEW view_sql_config_outer AS
      SELECT inner_value, size(CAST(NULL AS ARRAY<INT>)) AS outer_value
      FROM view_sql_config_inner
      """
    And config spark.sql.legacy.sizeOfNull = true
    When query
      """
      SELECT inner_value, outer_value FROM view_sql_config_outer
      """
    Then query result
      | inner_value | outer_value |
      | -1          | NULL        |
