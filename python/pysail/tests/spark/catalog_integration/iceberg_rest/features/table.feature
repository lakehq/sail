Feature: Iceberg REST catalog table operations

  Background:
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS iceberg_table_test
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS iceberg_table_test CASCADE
      """

  Scenario: Create a table with columns and comment
    Given statement
      """
      CREATE TABLE iceberg_table_test.t1 (
        foo STRING,
        bar INT NOT NULL COMMENT 'meow',
        baz BOOLEAN
      )
      USING iceberg
      COMMENT 'peow'
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test LIKE 't1'
      """
    Then query result
      | database           | tableName | isTemporary |
      | iceberg_table_test | t1        | false       |

  Scenario: Create duplicate table fails
    Given statement
      """
      CREATE TABLE iceberg_table_test.dup_t (id INT) USING iceberg
      """
    Given statement with error .*
      """
      CREATE TABLE iceberg_table_test.dup_t (id INT) USING iceberg
      """

  Scenario: Create table with IF NOT EXISTS does not raise error
    Given statement
      """
      CREATE TABLE iceberg_table_test.ine_t (id INT) USING iceberg
      """
    Given statement
      """
      CREATE TABLE IF NOT EXISTS iceberg_table_test.ine_t (id INT) USING iceberg
      """

  Scenario: Create a partitioned table
    Given statement
      """
      CREATE TABLE iceberg_table_test.partitioned_t (
        foo STRING,
        bar INT NOT NULL COMMENT 'meow',
        baz BOOLEAN
      )
      USING iceberg
      COMMENT 'test table'
      PARTITIONED BY (baz)
      LOCATION 's3://icebergdata/custom/path/meow'
      TBLPROPERTIES (owner = 'mr. meow', team = 'data-eng')
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test LIKE 'partitioned_t'
      """
    Then query result
      | database           | tableName    | isTemporary |
      | iceberg_table_test | partitioned_t | false       |

  Scenario: Describe existing table shows columns
    Given statement
      """
      CREATE TABLE iceberg_table_test.get_t (
        foo STRING,
        bar INT NOT NULL COMMENT 'meow',
        baz BOOLEAN
      )
      USING iceberg
      COMMENT 'test table'
      PARTITIONED BY (baz)
      TBLPROPERTIES (owner = 'mr. meow', team = 'data-eng')
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test LIKE 'get_t'
      """
    Then query result
      | database           | tableName | isTemporary |
      | iceberg_table_test | get_t     | false       |
    When query
      """
      DESCRIBE TABLE iceberg_table_test.get_t
      """
    Then query result has row where "col_name" is "foo"
    Then query result has row where "col_name" is "bar"
    Then query result has row where "col_name" is "baz"
    Then query result row where "col_name" is "foo" has "data_type" equal to "string"
    Then query result row where "col_name" is "bar" has "data_type" equal to "int"
    Then query result row where "col_name" is "baz" has "data_type" equal to "boolean"
    Then query result row where "col_name" is "bar" has "comment" equal to "meow"

  Scenario: Create a table with identity partition transform
    Given statement
      """
      CREATE TABLE iceberg_table_test.identity_part_t (
        id INT,
        category STRING
      )
      USING iceberg
      PARTITIONED BY (category)
      """
    When query
      """
      DESCRIBE TABLE iceberg_table_test.identity_part_t
      """
    Then query result has row where "col_name" is "id"
    Then query result has row where "col_name" is "category"

  Scenario: Create a table with PRIMARY KEY constraint
    Given statement
      """
      CREATE TABLE iceberg_table_test.pk_t (
        id INT NOT NULL,
        name STRING
      )
      USING iceberg
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test LIKE 'pk_t'
      """
    Then query result
      | database           | tableName | isTemporary |
      | iceberg_table_test | pk_t      | false       |

  Scenario: Drop table with PURGE removes it
    Given statement
      """
      CREATE TABLE iceberg_table_test.purge_t (id INT) USING iceberg
      """
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_table_test.purge_t
      """
    Given statement
      """
      DROP TABLE iceberg_table_test.purge_t PURGE
      """
    When query
      """
      DESCRIBE TABLE iceberg_table_test.purge_t
      """
    Then query error .*

  Scenario: Describe non-existent table raises error
    When query
      """
      DESCRIBE TABLE iceberg_table_test.nonexistent_t
      """
    Then query error .*

  Scenario: List tables in a namespace
    Given statement
      """
      CREATE TABLE iceberg_table_test.list_t1 (id INT) USING iceberg
      """
    Given statement
      """
      CREATE TABLE iceberg_table_test.list_t2 (id INT) USING iceberg
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test
      """
    Then query result
      | database           | tableName | isTemporary |
      | iceberg_table_test | list_t1   | false       |
      | iceberg_table_test | list_t2   | false       |

  Scenario: Drop existing table removes it
    Given statement
      """
      CREATE TABLE iceberg_table_test.drop_t (id INT) USING iceberg
      """
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_table_test.drop_t
      """
    Given statement
      """
      DROP TABLE iceberg_table_test.drop_t
      """
    When query
      """
      DESCRIBE TABLE iceberg_table_test.drop_t
      """
    Then query error .*

  Scenario: Drop non-existent table with IF EXISTS does not raise error
    Given statement
      """
      DROP TABLE IF EXISTS iceberg_table_test.nonexistent_drop_t
      """

  Scenario: Drop non-existent table fails
    Given statement with error .*
      """
      DROP TABLE iceberg_table_test.nonexistent_drop_t
      """

  Scenario: Create a table with year partition transform
    Given statement
      """
      CREATE TABLE iceberg_table_test.year_part_t (
        id INT,
        event_date DATE
      )
      USING iceberg
      PARTITIONED BY (years(event_date))
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test LIKE 'year_part_t'
      """
    Then query result
      | database           | tableName   | isTemporary |
      | iceberg_table_test | year_part_t | false       |

  Scenario: Create a table with bucket partition transform
    Given statement
      """
      CREATE TABLE iceberg_table_test.bucket_part_t (
        user_id INT,
        name STRING
      )
      USING iceberg
      PARTITIONED BY (bucket(4, user_id))
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test LIKE 'bucket_part_t'
      """
    Then query result
      | database           | tableName     | isTemporary |
      | iceberg_table_test | bucket_part_t | false       |

  Scenario: Create a table with truncate partition transform
    Given statement
      """
      CREATE TABLE iceberg_table_test.trunc_part_t (
        code STRING,
        value INT
      )
      USING iceberg
      PARTITIONED BY (truncate(3, code))
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test LIKE 'trunc_part_t'
      """
    Then query result
      | database           | tableName    | isTemporary |
      | iceberg_table_test | trunc_part_t | false       |

  Scenario: DESCRIBE TABLE EXTENDED reports table comment, provider and location
    Given statement
      """
      CREATE TABLE iceberg_table_test.desc_ext_t (
        id INT,
        category STRING
      )
      USING iceberg
      COMMENT 'extended describe table'
      PARTITIONED BY (category)
      """
    When query
      """
      DESCRIBE TABLE EXTENDED iceberg_table_test.desc_ext_t
      """
    Then query result row where "col_name" is "Comment" has "data_type" equal to "extended describe table"
    Then query result row where "col_name" is "Provider" has "data_type" equal to "iceberg"
    Then query result has row where "col_name" is "Location"

  Scenario: DESCRIBE TABLE EXTENDED returns TBLPROPERTIES set on create
    Given statement
      """
      CREATE TABLE iceberg_table_test.props_t (id INT)
      USING iceberg
      TBLPROPERTIES (owner = 'mr. meow', team = 'data-eng')
      """
    When query
      """
      DESCRIBE TABLE EXTENDED iceberg_table_test.props_t
      """
    Then query result row where "col_name" is "Table Properties" has "data_type" containing "owner=mr. meow"
    Then query result row where "col_name" is "Table Properties" has "data_type" containing "team=data-eng"

  Scenario: Drop non-existent table with IF EXISTS and PURGE does not raise error
    Given statement
      """
      DROP TABLE IF EXISTS iceberg_table_test.nonexistent_purge_t PURGE
      """
