Feature: HMS catalog CREATE OR REPLACE TABLE

  REPLACE on the Hive Metastore is a drop-then-create under an exclusive table
  lock (HMS has no atomic swap). These scenarios exercise the observable
  behaviour through Spark SQL against Sail's HMS provider: schema and
  partitioning changes, correct reads of the replaced definition, the
  create-vs-replace-on-missing distinction, and the critical regression that an
  invalid replacement must leave the original table intact.

  Background:
    Given the HMS test database as db

  Scenario: CREATE OR REPLACE TABLE changes the schema
    Given statement template
      """
      CREATE TABLE {{ db }}.t_schema (id INT) USING PARQUET
      """
    Given statement template
      """
      CREATE OR REPLACE TABLE {{ db }}.t_schema (id INT, value DOUBLE) USING PARQUET
      """
    When query template
      """
      DESCRIBE TABLE {{ db }}.t_schema
      """
    Then query result has row where "col_name" is "id"
    Then query result has row where "col_name" is "value"
    Then query result row where "col_name" is "value" has "data_type" equal to "double"

  Scenario: CREATE OR REPLACE TABLE drops a column from the declared schema
    Given statement template
      """
      CREATE TABLE {{ db }}.t_drop_col (id INT, legacy STRING) USING PARQUET
      """
    Given statement template
      """
      CREATE OR REPLACE TABLE {{ db }}.t_drop_col (id INT) USING PARQUET
      """
    # REPLACE is a metadata drop+create; the catalog now reports only `id`. (The projected
    # header of an empty read is exactly the new schema, so a dropped column cannot leak in.)
    When query template
      """
      SELECT * FROM {{ db }}.t_drop_col LIMIT 0
      """
    Then query result
      | id |
    When query template
      """
      DESCRIBE TABLE {{ db }}.t_drop_col
      """
    Then query result has row where "col_name" is "id"

  Scenario: CREATE OR REPLACE TABLE changes the partitioning
    Given statement template
      """
      CREATE TABLE {{ db }}.t_part (id INT, region STRING, country STRING)
      USING PARQUET
      PARTITIONED BY (region)
      """
    Given statement template
      """
      CREATE OR REPLACE TABLE {{ db }}.t_part (id INT, region STRING, country STRING)
      USING PARQUET
      PARTITIONED BY (country)
      """
    When query template
      """
      DESCRIBE TABLE EXTENDED {{ db }}.t_part
      """
    Then query result has row where "col_name" is "# Partition Information"
    Then query result has row where "col_name" is "country"

  Scenario: Replaced table reads back under the new schema, not the old one
    Given statement template
      """
      CREATE TABLE {{ db }}.t_read (id INT, legacy STRING) USING PARQUET
      """
    Given statement template
      """
      CREATE OR REPLACE TABLE {{ db }}.t_read (id INT, label STRING) USING PARQUET
      """
    Given statement template
      """
      INSERT INTO {{ db }}.t_read VALUES (10, 'fresh')
      """
    # The replaced definition exposes exactly the new columns (id, label); the old `legacy`
    # column is gone from the schema, and a row written after REPLACE reads back correctly.
    When query template
      """
      SELECT * FROM {{ db }}.t_read WHERE id = 10 ORDER BY id
      """
    Then query result ordered
      | id | label |
      | 10 | fresh |

  Scenario: CREATE OR REPLACE on a missing table creates it
    Given statement template
      """
      CREATE OR REPLACE TABLE {{ db }}.t_cor_new (id INT, name STRING) USING PARQUET
      """
    When query template
      """
      SHOW TABLES IN {{ db }} LIKE 't_cor_new'
      """
    Then query result has row where "tableName" is "t_cor_new"
    When query template
      """
      DESCRIBE TABLE {{ db }}.t_cor_new
      """
    Then query result has row where "col_name" is "id"
    Then query result has row where "col_name" is "name"

  Scenario: bare REPLACE TABLE on a missing table errors
    Given statement template with error (TABLE_OR_VIEW_NOT_FOUND|not found|does not exist|cannot be found|NoSuchTable)
      """
      REPLACE TABLE {{ db }}.t_missing_replace (id INT) USING PARQUET
      """

  Scenario: invalid replacement preserves the original table
    Given statement template
      """
      CREATE TABLE {{ db }}.t_survive (id INT COMMENT 'original id', keep STRING) USING PARQUET
      """
    Given statement template
      """
      INSERT INTO {{ db }}.t_survive VALUES (1, 'alive')
      """
    Given statement template with error (duplicate column|COLUMN_ALREADY_EXISTS|already exists|duplicate)
      """
      CREATE OR REPLACE TABLE {{ db }}.t_survive (id INT, id INT) USING PARQUET
      """
    When query template
      """
      DESCRIBE TABLE {{ db }}.t_survive
      """
    Then query result has row where "col_name" is "id"
    Then query result has row where "col_name" is "keep"
    When query template
      """
      SELECT id, keep FROM {{ db }}.t_survive ORDER BY id
      """
    Then query result ordered
      | id | keep  |
      | 1  | alive |
