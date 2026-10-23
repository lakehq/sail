Feature: TRUNCATE TABLE

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
  Scenario: TRUNCATE TABLE removes the data of a managed table whose warehouse path has a space
    Given variable warehouse for temporary directory truncate warehouse with space
    Given config spark.sql.warehouse.dir = {{ warehouse.file_uri }}
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_space_managed_t
      """
    Given statement
      """
      CREATE TABLE truncate_space_managed_t (id INT) USING PARQUET
      """
    Given statement
      """
      INSERT INTO truncate_space_managed_t VALUES (1), (2)
      """
    Given variable table_dir for location of table truncate_space_managed_t
    Then data files in table_dir count is at least 1
    Given statement
      """
      TRUNCATE TABLE truncate_space_managed_t
      """
    Then data files in table_dir count is 0
    When query
      """
      SELECT COUNT(*) AS cnt FROM truncate_space_managed_t
      """
    Then query result
      | cnt |
      | 0   |

  @sail-only
  Scenario: TRUNCATE TABLE twice leaves a table that can be written again
    Given variable warehouse for temporary directory truncate_twice_warehouse
    Given config spark.sql.warehouse.dir = {{ warehouse.string }}
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_twice_t
      """
    Given statement
      """
      CREATE TABLE truncate_twice_t USING PARQUET AS SELECT 1 AS id, 'a' AS name
      """
    Given statement
      """
      TRUNCATE TABLE truncate_twice_t
      """
    Given statement
      """
      TRUNCATE TABLE truncate_twice_t
      """
    Given statement
      """
      INSERT INTO truncate_twice_t VALUES (10, 'x')
      """
    When query
      """
      SELECT id, name FROM truncate_twice_t
      """
    Then query result
      | id | name |
      | 10 | x    |

  @sail-only
  Scenario: TRUNCATE TABLE ignores non-Iceberg metadata subdirectory
    Given variable warehouse for temporary directory truncate_meta_subdir_warehouse
    Given config spark.sql.warehouse.dir = {{ warehouse.string }}
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_meta_subdir_t
      """
    Given statement
      """
      CREATE TABLE truncate_meta_subdir_t USING PARQUET AS SELECT * FROM VALUES (1), (2), (3) AS t(id)
      """
    Given variable dir for location of table truncate_meta_subdir_t
    Given subdirectory metadata in dir exists
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
  Scenario: TRUNCATE TABLE replaces a symlinked table directory with a plain directory and leaves the target untouched
    Given variable warehouse for temporary directory truncate_symlink_warehouse
    Given variable real for temporary directory truncate_symlink_real
    Given config spark.sql.warehouse.dir = {{ warehouse.string }}
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_symlink_t
      """
    Given statement
      """
      CREATE TABLE truncate_symlink_t (id INT) USING PARQUET
      """
    Given statement
      """
      INSERT INTO truncate_symlink_t VALUES (1)
      """
    Given variable dir for location of table truncate_symlink_t
    Given dir is removed
    Given symlink dir to real exists
    Given file keep.txt in real is created
    Given statement
      """
      TRUNCATE TABLE truncate_symlink_t
      """
    Then dir is an empty directory
    And file keep.txt in real exists

  @sail-only
  Scenario: TRUNCATE TABLE keeps the permissions of the table directory
    Given variable warehouse for temporary directory truncate_mode_warehouse
    Given config spark.sql.warehouse.dir = {{ warehouse.string }}
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_mode_t
      """
    Given statement
      """
      CREATE TABLE truncate_mode_t USING PARQUET AS SELECT * FROM VALUES (1) AS t(id)
      """
    Given variable dir for location of table truncate_mode_t
    Given mode of dir is set to 705
    Given statement
      """
      TRUNCATE TABLE truncate_mode_t
      """
    Then data files in dir count is 0
    And mode of dir is 705

  @sail-only
  Scenario: TRUNCATE TABLE recreates a managed table directory that is gone
    Given variable warehouse for temporary directory truncate_gone_warehouse
    Given config spark.sql.warehouse.dir = {{ warehouse.string }}
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_gone_t
      """
    Given statement
      """
      CREATE TABLE truncate_gone_t (id INT) USING PARQUET
      """
    Given statement
      """
      INSERT INTO truncate_gone_t VALUES (1), (2)
      """
    Given variable dir for location of table truncate_gone_t
    Given dir is removed
    Given statement
      """
      TRUNCATE TABLE truncate_gone_t
      """
    Then dir is an empty directory
    When query
      """
      SELECT COUNT(*) AS cnt FROM truncate_gone_t
      """
    Then query result
      | cnt |
      | 0   |

  @sail-only
  Scenario: TRUNCATE TABLE on a partitioned table only empties the partition directories
    Given variable warehouse for temporary directory truncate_part_warehouse
    Given config spark.sql.warehouse.dir = {{ warehouse.string }}
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_part_t
      """
    Given statement
      """
      CREATE TABLE truncate_part_t (a INT, p INT) USING PARQUET PARTITIONED BY (p)
      """
    Given statement
      """
      INSERT INTO truncate_part_t VALUES (1, 1), (2, 2)
      """
    Given variable dir for location of table truncate_part_t
    Given file _stray.txt in dir is created
    Given subdirectory stray_dir in dir exists
    Given statement
      """
      TRUNCATE TABLE truncate_part_t
      """
    When query
      """
      SELECT COUNT(*) AS cnt FROM truncate_part_t
      """
    Then query result
      | cnt |
      | 0   |
    And file _stray.txt in dir exists
    And subdirectories in dir count is 3

  @spark-4.2
  Scenario Outline: TRUNCATE TABLE refuses an external table and keeps its files
    Given variable location for temporary directory truncate_external
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_external_t
      """
    Given statement template
      """
      <create>
      """
    Then data files in location count is at least 1
    Given statement with error .*Operation not allowed: TRUNCATE TABLE on external tables.*
      """
      TRUNCATE TABLE truncate_external_t
      """
    Then data files in location count is at least 1
    When query
      """
      SELECT COUNT(*) AS cnt FROM truncate_external_t
      """
    Then query result
      | cnt |
      | 2   |

    Examples:
      | create                                                                                                                  |
      | CREATE TABLE truncate_external_t USING PARQUET LOCATION {{ location.sql }} AS SELECT * FROM VALUES (1), (2) AS t(id)    |
      | CREATE TABLE truncate_external_t USING PARQUET OPTIONS (path {{ location.sql }}) AS SELECT * FROM VALUES (1), (2) AS t(id) |

  @spark-4.2
  Scenario Outline: TRUNCATE TABLE refuses every kind of view
    Given statement
      """
      <create_view>
      """
    Given final statement
      """
      <drop_view>
      """
    Given statement with error .*expects a table but .* is a view.*
      """
      TRUNCATE TABLE <target>
      """

    Examples:
      | create_view                                                | drop_view                                   | target                  |
      | CREATE TEMPORARY VIEW truncate_tv AS SELECT 1 AS id        | DROP VIEW IF EXISTS truncate_tv             | truncate_tv             |
      | CREATE GLOBAL TEMPORARY VIEW truncate_gv AS SELECT 1 AS id | DROP VIEW IF EXISTS global_temp.truncate_gv | global_temp.truncate_gv |
      | CREATE VIEW truncate_pv AS SELECT 1 AS id                  | DROP VIEW IF EXISTS truncate_pv             | truncate_pv             |

  @sail-only
  Scenario: TRUNCATE TABLE refuses a temporary view that shadows a table and keeps the table data
    Given variable warehouse for temporary directory truncate_shadow_warehouse
    Given config spark.sql.warehouse.dir = {{ warehouse.string }}
    Given final statement
      """
      DROP VIEW IF EXISTS truncate_shadow_t
      """
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_shadow_t
      """
    Given statement
      """
      CREATE TABLE truncate_shadow_t USING PARQUET AS SELECT * FROM VALUES (1), (2) AS t(id)
      """
    Given statement
      """
      CREATE TEMPORARY VIEW truncate_shadow_t AS SELECT 7 AS id
      """
    Given statement with error .*expects a table but .* is a view.*
      """
      TRUNCATE TABLE truncate_shadow_t
      """
    Given statement
      """
      DROP VIEW truncate_shadow_t
      """
    When query
      """
      SELECT COUNT(*) AS cnt FROM truncate_shadow_t
      """
    Then query result
      | cnt |
      | 2   |

  @sail-only
  Scenario Outline: TRUNCATE TABLE refuses a managed table of a lakehouse format and keeps its rows
    Given variable warehouse for temporary directory truncate_lakehouse_warehouse
    Given config spark.sql.warehouse.dir = {{ warehouse.string }}
    Given final statement
      """
      DROP TABLE IF EXISTS truncate_lakehouse_t
      """
    Given statement
      """
      CREATE TABLE truncate_lakehouse_t (id INT) USING <format>
      """
    Given statement
      """
      INSERT INTO truncate_lakehouse_t VALUES (1), (2)
      """
    Given statement with error .*transaction logs.*
      """
      TRUNCATE TABLE truncate_lakehouse_t
      """
    When query
      """
      SELECT COUNT(*) AS cnt FROM truncate_lakehouse_t
      """
    Then query result
      | cnt |
      | 2   |

    Examples:
      | format  |
      | DELTA   |
      | ICEBERG |
