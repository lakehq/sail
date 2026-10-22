Feature: TRUNCATE TABLE

  @sail-only
  Scenario: TRUNCATE TABLE removes all rows and table remains queryable
    Given variable location for temporary directory truncate_basic
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_basic_t
      """
    Given statement template
      """
      CREATE TABLE truncate_basic_t
      USING PARQUET
      LOCATION {{ location.uri }}
      AS SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie') AS t(id, name)
      """
    When query
      """
      SELECT COUNT(*) AS cnt FROM truncate_basic_t
      """
    Then query result
      | cnt |
      | 3   |
    Given statement
      """
      TRUNCATE TABLE truncate_basic_t
      """
    When query
      """
      SELECT COUNT(*) AS cnt FROM truncate_basic_t
      """
    Then query result
      | cnt |
      | 0   |

  @sail-only
  Scenario: TRUNCATE TABLE allows inserting new rows after truncation
    Given variable location for temporary directory truncate_reinsert
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_reinsert_t
      """
    Given statement template
      """
      CREATE TABLE truncate_reinsert_t
      USING PARQUET
      LOCATION {{ location.uri }}
      AS SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob') AS t(id, name)
      """
    Given statement
      """
      TRUNCATE TABLE truncate_reinsert_t
      """
    Given statement
      """
      INSERT INTO truncate_reinsert_t VALUES (10, 'New')
      """
    When query
      """
      SELECT id, name FROM truncate_reinsert_t
      """
    Then query result
      | id | name |
      | 10 | New  |

  @sail-only
  Scenario: TRUNCATE TABLE works with plain absolute path location (no file:// prefix)
    Given variable location for temporary directory truncate_plain_path
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_plain_path_t
      """
    Given statement template
      """
      CREATE TABLE truncate_plain_path_t
      USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (1), (2), (3) AS t(id)
      """
    Given statement
      """
      TRUNCATE TABLE truncate_plain_path_t
      """
    When query
      """
      SELECT COUNT(*) AS cnt FROM truncate_plain_path_t
      """
    Then query result
      | cnt |
      | 0   |

  @sail-only
  Scenario: TRUNCATE TABLE on a managed table (no explicit LOCATION) removes all rows
    Given variable warehouse for temporary directory truncate_managed_warehouse
    Given config spark.sql.warehouse.dir = {{ warehouse.string }}
    Given final statement
      """
      DROP DATABASE IF EXISTS truncate_managed_db CASCADE
      """
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS truncate_managed_db
      """
    Given statement
      """
      CREATE TABLE truncate_managed_db.managed_t (id INT, name STRING) USING PARQUET
      """
    Given statement
      """
      INSERT INTO truncate_managed_db.managed_t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')
      """
    When query
      """
      SELECT COUNT(*) AS cnt FROM truncate_managed_db.managed_t
      """
    Then query result
      | cnt |
      | 3   |
    Given statement
      """
      TRUNCATE TABLE truncate_managed_db.managed_t
      """
    When query
      """
      SELECT COUNT(*) AS cnt FROM truncate_managed_db.managed_t
      """
    Then query result
      | cnt |
      | 0   |

  @sail-only
  Scenario: TRUNCATE TABLE ignores non-Iceberg metadata subdirectory
    Given variable location for temporary directory truncate_meta_subdir
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_meta_subdir_t
      """
    Given statement template
      """
      CREATE TABLE truncate_meta_subdir_t
      USING PARQUET
      LOCATION {{ location.uri }}
      AS SELECT * FROM VALUES (1), (2), (3) AS t(id)
      """
    Given subdirectory metadata in location exists
    Given statement
      """
      TRUNCATE TABLE truncate_meta_subdir_t
      """
    When query
      """
      SELECT COUNT(*) AS cnt FROM truncate_meta_subdir_t
      """
    Then query result
      | cnt |
      | 0   |

  @sail-only
  Scenario: TRUNCATE TABLE on a view raises an error
    Given variable location for temporary directory truncate_view_err
    Given final statement
      """
      DROP VIEW IF EXISTS truncate_view_err_v
      """
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_view_err_t
      """
    Given statement template
      """
      CREATE TABLE truncate_view_err_t
      USING PARQUET
      LOCATION {{ location.uri }}
      AS SELECT * FROM VALUES (1) AS t(id)
      """
    Given statement
      """
      CREATE VIEW truncate_view_err_v AS SELECT * FROM truncate_view_err_t
      """
    Given statement with error .*
      """
      TRUNCATE TABLE truncate_view_err_v
      """
