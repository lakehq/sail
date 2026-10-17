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
