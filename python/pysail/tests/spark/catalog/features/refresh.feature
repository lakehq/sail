Feature: Refresh catalog tables

  Background:
    Given statement
    """
    DROP TABLE IF EXISTS refresh_tbl
    """

  Scenario: REFRESH TABLE for a regular table is a no-op and the data is still visible
    Given statement
    """
    CREATE TABLE refresh_tbl (id INT, name STRING) USING parquet
    """
    Given statement
    """
    INSERT INTO refresh_tbl VALUES (1, 'a'), (2, 'b')
    """
    Given statement
    """
    REFRESH TABLE refresh_tbl
    """
    When query
    """
    SELECT id, name FROM refresh_tbl ORDER BY id
    """
    Then query result ordered
      | id | name |
      | 1  | a    |
      | 2  | b    |

    Given final statement
    """
    DROP TABLE refresh_tbl
    """

  Scenario: REFRESH TABLE accepts a fully qualified table name
    Given statement
    """
    CREATE TABLE refresh_tbl (id INT) USING parquet
    """
    Given statement
    """
    REFRESH TABLE sail.default.refresh_tbl
    """
    Given final statement
    """
    DROP TABLE refresh_tbl
    """

  Scenario: REFRESH TABLE on a view succeeds
    Given statement
    """
    CREATE OR REPLACE TEMPORARY VIEW refresh_tmp_view AS SELECT 1 AS id
    """
    Given statement
    """
    REFRESH TABLE refresh_tmp_view
    """
    Given final statement
    """
    DROP VIEW refresh_tmp_view
    """

  Scenario: REFRESH TABLE on a missing table raises an error
    Given statement with error .*
    """
    REFRESH TABLE non_existent_refresh_tbl
    """
