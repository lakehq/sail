Feature: Data source temporary views

  Background:
    Given variable location for temporary directory temp_view_using
    Given final statement
      """
      DROP TABLE IF EXISTS temp_view_using_data
      """
    Given statement template
      """
      CREATE TABLE temp_view_using_data (id INT, name STRING) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO temp_view_using_data VALUES (1, 'Alice'), (2, 'Bob')
      """

  Scenario: Read from a temporary view backed by a parquet path
    Given final statement
      """
      DROP VIEW IF EXISTS parquet_temp_view
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW parquet_temp_view USING parquet OPTIONS (path {{ location.sql }})
      """
    When query
      """
      SELECT * FROM parquet_temp_view ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | Alice |
      | 2  | Bob   |

  Scenario: Replace a temporary view backed by a parquet path
    Given final statement
      """
      DROP VIEW IF EXISTS parquet_temp_view_replace
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW parquet_temp_view_replace USING parquet OPTIONS (path {{ location.sql }})
      """
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW parquet_temp_view_replace USING parquet OPTIONS (path {{ location.sql }})
      """
    When query
      """
      SELECT count(*) AS cnt FROM parquet_temp_view_replace
      """
    Then query result
      | cnt |
      | 2   |

  Scenario: Read from a global temporary view backed by a parquet path
    Given final statement
      """
      DROP VIEW IF EXISTS global_temp.parquet_global_temp_view
      """
    Given statement
      """
      DROP VIEW IF EXISTS global_temp.parquet_global_temp_view
      """
    Given statement template
      """
      CREATE GLOBAL TEMPORARY VIEW parquet_global_temp_view USING parquet OPTIONS (path {{ location.sql }})
      """
    When query
      """
      SELECT id FROM global_temp.parquet_global_temp_view ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |

  Scenario: Options other than the path are forwarded to the data source
    Given variable csv_location for temporary directory temp_view_using_csv
    Given final statement
      """
      DROP TABLE IF EXISTS temp_view_using_csv_data
      """
    Given final statement
      """
      DROP VIEW IF EXISTS csv_temp_view
      """
    Given statement template
      """
      CREATE TABLE temp_view_using_csv_data (id INT, name STRING)
      USING csv OPTIONS (header 'true') LOCATION {{ csv_location.sql }}
      """
    Given statement
      """
      INSERT INTO temp_view_using_csv_data VALUES (1, 'Alice'), (2, 'Bob')
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW csv_temp_view USING csv OPTIONS (path {{ csv_location.sql }}, header 'true')
      """
    When query
      """
      SELECT id, name FROM csv_temp_view ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | Alice |
      | 2  | Bob   |

  @sail-only
  Scenario: USING is rejected for persistent views
    Given statement template with error only supported for temporary views
      """
      CREATE VIEW persistent_view_using USING parquet OPTIONS (path {{ location.sql }})
      """

  @sail-only
  Scenario: USING cannot be combined with an AS query
    Given statement template with error cannot be defined with an AS query
      """
      CREATE TEMPORARY VIEW temp_view_using_bad USING parquet OPTIONS (path {{ location.sql }}) AS SELECT 1
      """

  @sail-only
  Scenario: COMMENT and TBLPROPERTIES are rejected for data source temporary views
    Given statement template with error cannot be used with CREATE TEMPORARY VIEW
      """
      CREATE TEMPORARY VIEW temp_view_using_props
      USING parquet OPTIONS (path {{ location.sql }})
      TBLPROPERTIES (meow = 'mix')
      """
