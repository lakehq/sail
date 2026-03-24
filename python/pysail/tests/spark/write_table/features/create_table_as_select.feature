Feature: CREATE TABLE AS SELECT
  Scenario: CREATE TABLE AS SELECT with LOCATION clause
    Given variable location for temporary directory ctas_location_table
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_location_table
      """
    Given statement template
      """
      CREATE TABLE ctas_location_table
      USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES
        (1, 'Alice', 10.5),
        (2, 'Bob', 20.75),
        (3, 'Charlie', 30.25)
      AS t(id, name, value)
      """
    When query
      """
      SELECT * FROM ctas_location_table ORDER BY id
      """
    Then query result ordered
      | id | name    | value |
      | 1  | Alice   | 10.50 |
      | 2  | Bob     | 20.75 |
      | 3  | Charlie | 30.25 |
    Then data files in location count is at least 1


  Scenario: CREATE TABLE AS SELECT with various data types and expressions
    Given variable location for temporary directory ctas_table
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_table
      """
    Given statement template
      """
      CREATE TABLE ctas_table
      USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT
          id,
          name,
          age,
          salary,
          is_active,
          birth_date,
          'extra_string' as extra_str,
          42 as extra_int,
          CAST(3.14 AS DOUBLE) as extra_double,
          true as extra_bool
      FROM VALUES
        (1, 'Alice', 25, 50000.50, true, '1998-05-15'),
        (2, 'Bob', 30, 60000.75, false, '1993-08-20'),
        (3, 'Charlie', 35, 70000.25, true, '1988-12-10')
      AS t(id, name, age, salary, is_active, birth_date)
      """
    When query
      """
      SELECT * FROM ctas_table ORDER BY id
      """
    Then query result ordered
      | id | name    | age | salary   | is_active | birth_date | extra_str     | extra_int | extra_double | extra_bool |
      | 1  | Alice   | 25  | 50000.50 | true      | 1998-05-15 | extra_string  | 42        | 3.14         | true       |
      | 2  | Bob     | 30  | 60000.75 | false     | 1993-08-20 | extra_string  | 42        | 3.14         | true       |
      | 3  | Charlie | 35  | 70000.25 | true      | 1988-12-10 | extra_string  | 42        | 3.14         | true       |


  Scenario: CREATE TABLE AS SELECT using CSV format with OPTIONS
    Given variable location for temporary directory ctas_csv_table
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_csv_table
      """
    Given statement template
      """
      CREATE TABLE ctas_csv_table
      USING CSV
      LOCATION {{ location.sql }}
      OPTIONS (header 'false', delimiter '|')
      AS SELECT * FROM VALUES
        (1, 'Alice', 10.5),
        (2, 'Bob', 20.75),
        (3, 'Charlie', 30.25)
      AS t(id, name, value)
      """
    When query
      """
      SELECT * FROM ctas_csv_table ORDER BY id
      """
    Then query result ordered
      | id | name    | value |
      | 1  | Alice   | 10.50 |
      | 2  | Bob     | 20.75 |
      | 3  | Charlie | 30.25 |
    Then CSV files in location first line is 1|Alice|10.50


  Scenario: CREATE TABLE AS SELECT with PARTITIONED BY clause
    Given variable location for temporary directory ctas_partitioned_table
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_partitioned_table
      """
    Given statement template
      """
      CREATE TABLE ctas_partitioned_table
      USING PARQUET
      LOCATION {{ location.sql }}
      PARTITIONED BY (category)
      AS SELECT * FROM VALUES
        (1, 'A', 10.5),
        (2, 'A', 20.75),
        (3, 'B', 30.25),
        (4, 'B', 40.5)
      AS t(id, category, value)
      """
    When query
      """
      SELECT * FROM ctas_partitioned_table ORDER BY id
      """
    Then query result ordered
      | id | value  | category |
      | 1  | 10.50  | A        |
      | 2  | 20.75  | A        |
      | 3  | 30.25  | B        |
      | 4  | 40.50  | B        |
    Then subdirectories in location count is 2


  Scenario: CREATE TABLE AS SELECT IF NOT EXISTS when table does not exist
    Given variable location for temporary directory ctas_if_not_exists_1
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_if_not_exists_1
      """
    Given statement template
      """
      CREATE TABLE IF NOT EXISTS ctas_if_not_exists_1
      USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES
        (1, 'Alice'),
        (2, 'Bob')
      AS t(id, name)
      """
    When query
      """
      SELECT * FROM ctas_if_not_exists_1 ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | Alice |
      | 2  | Bob   |


  Scenario: CREATE TABLE AS SELECT IF NOT EXISTS preserves data when table already exists
    Given variable location for temporary directory ctas_if_not_exists_preserve
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_if_not_exists_preserve
      """
    Given statement template
      """
      CREATE TABLE ctas_if_not_exists_preserve
      USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES
        (1, 'Alice'),
        (2, 'Bob')
      AS t(id, name)
      """
    When query
      """
      SELECT * FROM ctas_if_not_exists_preserve ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | Alice |
      | 2  | Bob   |
    Given statement template
      """
      CREATE TABLE IF NOT EXISTS ctas_if_not_exists_preserve
      USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES
        (3, 'Charlie'),
        (4, 'David')
      AS t(id, name)
      """
    When query
      """
      SELECT * FROM ctas_if_not_exists_preserve ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | Alice |
      | 2  | Bob   |
