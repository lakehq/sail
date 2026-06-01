Feature: CREATE TABLE default location

  @sail-only
  Scenario: CREATE TABLE uses database location as the default table location
    Given variable db_location for temporary directory create_table_db_loc
    Given final statement
      """
      DROP DATABASE IF EXISTS create_table_db_loc CASCADE
      """
    Given statement template
      """
      CREATE DATABASE IF NOT EXISTS create_table_db_loc
      LOCATION {{ db_location.sql }}
      """
    Given statement
      """
      CREATE TABLE create_table_db_loc.my_table (id INT, name STRING)
      USING DELTA
      """
    Given statement
      """
      INSERT INTO create_table_db_loc.my_table VALUES (1, 'Alice'), (2, 'Bob')
      """
    When query
      """
      SELECT * FROM create_table_db_loc.my_table ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | Alice |
      | 2  | Bob   |
    Then file tree in db_location matches
      """
      📂 my_table
        📄 part-<id>.<codec>.parquet
      """

  @sail-only
  Scenario: CREATE TABLE AS SELECT uses database location as the default table location
    Given variable db_location for temporary directory create_table_ctas_db_loc
    Given final statement
      """
      DROP DATABASE IF EXISTS create_table_ctas_db_loc CASCADE
      """
    Given statement template
      """
      CREATE DATABASE IF NOT EXISTS create_table_ctas_db_loc
      LOCATION {{ db_location.sql }}
      """
    Given statement
      """
      CREATE TABLE create_table_ctas_db_loc.ctas_table
      USING DELTA
      AS SELECT * FROM VALUES
        (1, 'Alice', 10.5),
        (2, 'Bob', 20.75),
        (3, 'Charlie', 30.25)
      AS t(id, name, value)
      """
    When query
      """
      SELECT * FROM create_table_ctas_db_loc.ctas_table ORDER BY id
      """
    Then query result ordered
      | id | name    | value |
      | 1  | Alice   | 10.50 |
      | 2  | Bob     | 20.75 |
      | 3  | Charlie | 30.25 |
    Then file tree in db_location matches
      """
      📂 ctas_table
        📄 part-<id>.<codec>.parquet
      """

  @sail-only
  Scenario: CREATE TABLE preserves special characters in table name like Spark
    Given variable db_location for temporary directory create_table_special_char_db
    Given final statement
      """
      DROP DATABASE IF EXISTS create_table_special_char_db CASCADE
      """
    Given statement template
      """
      CREATE DATABASE IF NOT EXISTS create_table_special_char_db
      LOCATION {{ db_location.sql }}
      """
    Given statement
      """
      CREATE TABLE `create_table_special_char_db`.`my@table` (id INT)
      USING DELTA
      """
    Given statement
      """
      INSERT INTO `create_table_special_char_db`.`my@table` VALUES (1), (2)
      """
    When query
      """
      SELECT * FROM `create_table_special_char_db`.`my@table` ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |
    Then file tree in db_location matches
      """
      📂 my@table
        📄 part-<id>.<codec>.parquet
      """

  @sail-only
  Scenario: CREATE TABLE uses warehouse directory when database has no location
    Given variable warehouse for temporary directory create_table_fallback_warehouse
    Given config spark.sql.warehouse.dir = {{ warehouse.string }}
    Given final statement
      """
      DROP DATABASE IF EXISTS create_table_fallback_db CASCADE
      """
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS create_table_fallback_db
      """
    Given statement
      """
      CREATE TABLE create_table_fallback_db.fallback_t (id INT, name STRING)
      USING DELTA
      """
    Given statement
      """
      INSERT INTO create_table_fallback_db.fallback_t VALUES (1, 'Alice')
      """
    When query
      """
      SELECT * FROM create_table_fallback_db.fallback_t ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | Alice |
    Then file tree in warehouse matches
      """
      📂 create_table_fallback_db.db
        📂 fallback_t
          📄 part-<id>.<codec>.parquet
      """

  @sail-only
  Scenario: CREATE TABLE in default database uses the session warehouse directory
    Given variable warehouse for temporary directory create_table_default_db_warehouse
    Given config spark.sql.warehouse.dir = {{ warehouse.string }}
    Given final statement
      """
      DROP TABLE IF EXISTS default_location_t
      """
    Given statement
      """
      CREATE TABLE default_location_t (id INT, name STRING)
      USING DELTA
      """
    Given statement
      """
      INSERT INTO default_location_t VALUES (1, 'Alice')
      """
    When query
      """
      SELECT * FROM default_location_t ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | Alice |
    Then file tree in warehouse matches
      """
      📂 default_location_t
        📄 part-<id>.<codec>.parquet
      """
