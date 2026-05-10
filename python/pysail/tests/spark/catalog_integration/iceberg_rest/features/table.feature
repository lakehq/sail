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

  Scenario: Create a table with a top-level STRUCT column
    Given statement
      """
      CREATE TABLE iceberg_table_test.struct_t (
        id INT,
        addr STRUCT<street: STRING, city: STRING, zip: INT>
      )
      USING iceberg
      """
    When query
      """
      DESCRIBE TABLE iceberg_table_test.struct_t
      """
    Then query result has row where "col_name" is "id"
    Then query result has row where "col_name" is "addr"
    Then query result row where "col_name" is "addr" has "data_type" containing "struct"
    Then query result row where "col_name" is "addr" has "data_type" containing "street"
    Then query result row where "col_name" is "addr" has "data_type" containing "city"
    Then query result row where "col_name" is "addr" has "data_type" containing "zip"

  Scenario: Create a table with an ARRAY of primitives column
    Given statement
      """
      CREATE TABLE iceberg_table_test.array_t (
        id INT,
        tags ARRAY<STRING>
      )
      USING iceberg
      """
    When query
      """
      DESCRIBE TABLE iceberg_table_test.array_t
      """
    Then query result has row where "col_name" is "tags"
    Then query result row where "col_name" is "tags" has "data_type" containing "array"
    Then query result row where "col_name" is "tags" has "data_type" containing "string"

  Scenario: Create a table with a MAP column
    Given statement
      """
      CREATE TABLE iceberg_table_test.map_t (
        id INT,
        attrs MAP<STRING, INT>
      )
      USING iceberg
      """
    When query
      """
      DESCRIBE TABLE iceberg_table_test.map_t
      """
    Then query result has row where "col_name" is "attrs"
    Then query result row where "col_name" is "attrs" has "data_type" containing "map"
    Then query result row where "col_name" is "attrs" has "data_type" containing "string"
    Then query result row where "col_name" is "attrs" has "data_type" containing "int"

  Scenario: Create a table with an ARRAY of STRUCT column
    Given statement
      """
      CREATE TABLE iceberg_table_test.array_struct_t (
        id INT,
        items ARRAY<STRUCT<sku: STRING, qty: INT>>
      )
      USING iceberg
      """
    When query
      """
      DESCRIBE TABLE iceberg_table_test.array_struct_t
      """
    Then query result has row where "col_name" is "items"
    Then query result row where "col_name" is "items" has "data_type" containing "array"
    Then query result row where "col_name" is "items" has "data_type" containing "struct"
    Then query result row where "col_name" is "items" has "data_type" containing "sku"
    Then query result row where "col_name" is "items" has "data_type" containing "qty"

  Scenario: Create a table with deeply nested STRUCT, ARRAY and MAP columns
    Given statement
      """
      CREATE TABLE iceberg_table_test.deep_nested_t (
        id INT,
        profile STRUCT<
          name: STRING,
          contacts: ARRAY<STRUCT<kind: STRING, value: STRING>>,
          scores: MAP<STRING, ARRAY<INT>>
        >
      )
      USING iceberg
      """
    When query
      """
      DESCRIBE TABLE iceberg_table_test.deep_nested_t
      """
    Then query result has row where "col_name" is "id"
    Then query result has row where "col_name" is "profile"
    Then query result row where "col_name" is "profile" has "data_type" containing "struct"
    Then query result row where "col_name" is "profile" has "data_type" containing "name"
    Then query result row where "col_name" is "profile" has "data_type" containing "contacts"
    Then query result row where "col_name" is "profile" has "data_type" containing "array"
    Then query result row where "col_name" is "profile" has "data_type" containing "kind"
    Then query result row where "col_name" is "profile" has "data_type" containing "value"
    Then query result row where "col_name" is "profile" has "data_type" containing "scores"
    Then query result row where "col_name" is "profile" has "data_type" containing "map"

  Scenario: Create a partitioned table with a nested STRUCT column
    Given statement
      """
      CREATE TABLE iceberg_table_test.part_nested_t (
        id INT,
        category STRING,
        meta STRUCT<created_by: STRING, created_at: STRING>
      )
      USING iceberg
      PARTITIONED BY (category)
      """
    When query
      """
      DESCRIBE TABLE iceberg_table_test.part_nested_t
      """
    Then query result has row where "col_name" is "id"
    Then query result has row where "col_name" is "category"
    Then query result has row where "col_name" is "meta"
    Then query result row where "col_name" is "meta" has "data_type" containing "struct"
    Then query result row where "col_name" is "meta" has "data_type" containing "created_by"
    Then query result row where "col_name" is "meta" has "data_type" containing "created_at"

  Scenario: Create a table with a NOT NULL column alongside a nested STRUCT column
    Given statement
      """
      CREATE TABLE iceberg_table_test.pk_nested_t (
        id INT NOT NULL,
        addr STRUCT<street: STRING, city: STRING>
      )
      USING iceberg
      """
    When query
      """
      SHOW TABLES IN iceberg_table_test LIKE 'pk_nested_t'
      """
    Then query result
      | database           | tableName    | isTemporary |
      | iceberg_table_test | pk_nested_t  | false       |
    When query
      """
      DESCRIBE TABLE iceberg_table_test.pk_nested_t
      """
    Then query result has row where "col_name" is "id"
    Then query result has row where "col_name" is "addr"

  Scenario: Create a table with multiple STRUCT columns having same nested field names
    Given statement
      """
      CREATE TABLE iceberg_table_test.multi_struct_t (
        id INT,
        a STRUCT<x: INT, y: INT>,
        b STRUCT<x: INT, y: INT>
      )
      USING iceberg
      """
    When query
      """
      DESCRIBE TABLE iceberg_table_test.multi_struct_t
      """
    Then query result has row where "col_name" is "a"
    Then query result has row where "col_name" is "b"
    Then query result row where "col_name" is "a" has "data_type" containing "struct"
    Then query result row where "col_name" is "b" has "data_type" containing "struct"
