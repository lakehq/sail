Feature: INSERT INTO and INSERT OVERWRITE semantics

  Scenario: INSERT INTO appends rows
    Given variable location for temporary directory insert_into_append
    Given final statement
      """
      DROP TABLE IF EXISTS insert_into_append_table
      """
    Given statement template
      """
      CREATE TABLE insert_into_append_table
      USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (1), (2) AS t(id)
      """
    Given statement
      """
      INSERT INTO insert_into_append_table
      SELECT * FROM VALUES (3), (4) AS t(id)
      """
    When query
      """
      SELECT id FROM insert_into_append_table ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |
      | 3  |
      | 4  |

  Scenario: INSERT OVERWRITE replaces existing rows
    Given variable location for temporary directory insert_overwrite_replace
    Given final statement
      """
      DROP TABLE IF EXISTS insert_overwrite_replace_table
      """
    Given statement template
      """
      CREATE TABLE insert_overwrite_replace_table
      USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES (1), (2) AS t(id)
      """
    Given statement
      """
      INSERT OVERWRITE TABLE insert_overwrite_replace_table
      SELECT * FROM VALUES (7), (8) AS t(id)
      """
    When query
      """
      SELECT id FROM insert_overwrite_replace_table ORDER BY id
      """
    Then query result ordered
      | id |
      | 7  |
      | 8  |
