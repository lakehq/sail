Feature: CREATE TABLE AS SELECT error handling

  Scenario: CREATE TABLE AS SELECT fails when table already exists
    Given variable location for temporary directory ctas_exists_table
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_exists_table
      """
    Given statement template
      """
      CREATE TABLE ctas_exists_table
      USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES
        (1, 'Alice'),
        (2, 'Bob')
      AS t(id, name)
      """
    Given statement template with error table already exists
      """
      CREATE TABLE ctas_exists_table
      USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES
        (1, 'Alice'),
        (2, 'Bob')
      AS t(id, name)
      """
    When query
      """
      SELECT * FROM ctas_exists_table ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | Alice |
      | 2  | Bob   |

  Scenario: CREATE TABLE AS SELECT fails with COMMENT clause
    Given variable location for temporary directory ctas_error_comment
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_error_comment
      """
    Given statement template with error COMMENT in CREATE TABLE AS SELECT statement
      """
      CREATE TABLE ctas_error_comment
      USING PARQUET
      LOCATION {{ location.sql }}
      COMMENT 'This is a comment'
      AS SELECT * FROM VALUES
        (1, 'Alice'),
        (2, 'Bob')
      AS t(id, name)
      """

  Scenario: CREATE TABLE AS SELECT fails with TBLPROPERTIES clause
    Given variable location for temporary directory ctas_error_properties
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_error_properties
      """
    Given statement template with error PROPERTIES in CREATE TABLE AS SELECT statement
      """
      CREATE TABLE ctas_error_properties
      USING PARQUET
      LOCATION {{ location.sql }}
      TBLPROPERTIES ('p1'='v1', 'p2'='v2')
      AS SELECT * FROM VALUES
        (1, 'Alice'),
        (2, 'Bob')
      AS t(id, name)
      """

  Scenario: CREATE TABLE AS SELECT fails with SORT BY clause
    Given variable location for temporary directory ctas_error_sort_by
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_error_sort_by
      """
    Given statement template with error SORT_BY in CREATE TABLE AS SELECT statement
      """
      CREATE TABLE ctas_error_sort_by
      USING PARQUET
      LOCATION {{ location.sql }}
      CLUSTERED BY (id) SORTED BY (id) INTO 4 BUCKETS
      AS SELECT * FROM VALUES
        (1, 'Alice'),
        (2, 'Bob')
      AS t(id, name)
      """

  Scenario: CREATE TABLE AS SELECT fails with BUCKET BY clause
    Given variable location for temporary directory ctas_error_bucket_by
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_error_bucket_by
      """
    Given statement template with error BUCKET_BY in CREATE TABLE AS SELECT statement
      """
      CREATE TABLE ctas_error_bucket_by
      USING PARQUET
      LOCATION {{ location.sql }}
      CLUSTERED BY (id) INTO 4 BUCKETS
      AS SELECT * FROM VALUES
        (1, 'Alice'),
        (2, 'Bob')
      AS t(id, name)
      """

  Scenario: CREATE TABLE AS SELECT fails with CLUSTER BY clause
    Given variable location for temporary directory ctas_error_cluster_by
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_error_cluster_by
      """
    Given statement template with error CLUSTER BY in CREATE TABLE AS SELECT statement
      """
      CREATE TABLE ctas_error_cluster_by
      USING PARQUET
      LOCATION {{ location.sql }}
      CLUSTER BY (id)
      AS SELECT * FROM VALUES
        (1, 'Alice'),
        (2, 'Bob')
      AS t(id, name)
      """

  Scenario: CREATE TABLE AS SELECT fails with ROW FORMAT clause
    Given variable location for temporary directory ctas_error_row_format
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_error_row_format
      """
    Given statement template with error ROW FORMAT in CREATE TABLE AS SELECT statement
      """
      CREATE TABLE ctas_error_row_format
      USING PARQUET
      LOCATION {{ location.sql }}
      ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
      AS SELECT * FROM VALUES
        (1, 'Alice'),
        (2, 'Bob')
      AS t(id, name)
      """

  Scenario: CREATE TABLE AS SELECT fails with REPLACE clause
    Given variable location for temporary directory ctas_error_replace
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_error_replace
      """
    Given statement template with error REPLACE in CREATE TABLE AS SELECT statement
      """
      CREATE OR REPLACE TABLE ctas_error_replace
      USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES
        (1, 'Alice'),
        (2, 'Bob')
      AS t(id, name)
      """


  Scenario: CREATE TABLE AS SELECT fails when schema is specified
    Given variable location for temporary directory ctas_error_schema
    Given final statement
      """
      DROP TABLE IF EXISTS ctas_error_schema
      """
    Given statement template with error Schema may not be specified in a Create Table As Select
    """
      CREATE TABLE ctas_error_schema (
          id INT,
          name STRING
      ) USING PARQUET
      LOCATION {{ location.sql }}
      AS SELECT * FROM VALUES
        (1, 'Alice'),
        (2, 'Bob')
      AS t(id, name)
      """
