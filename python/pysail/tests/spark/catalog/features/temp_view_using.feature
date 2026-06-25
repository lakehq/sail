Feature: Data source temporary views

  Background:
    Given variable location for temporary directory temp_view_using
    Given final statement
      """
      DROP TABLE IF EXISTS temp_view_using_data
      """
    Given statement template
      """
      CREATE TABLE temp_view_using_data (id INT, name STRING) USING parquet LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO temp_view_using_data VALUES (1, 'Alice'), (2, 'Bob')
      """

  Scenario: Read from a temporary view backed by a parquet path
    Given final statement
      """
      DROP VIEW IF EXISTS parquet_temp_view
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW parquet_temp_view USING parquet OPTIONS (path {{ location.sql }})
      """
    When query
      """
      SELECT * FROM parquet_temp_view ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | Alice |
      | 2  | Bob   |

  Scenario: Replace a temporary view backed by a parquet path
    Given final statement
      """
      DROP VIEW IF EXISTS parquet_temp_view_replace
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW parquet_temp_view_replace USING parquet OPTIONS (path {{ location.sql }})
      """
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW parquet_temp_view_replace USING parquet OPTIONS (path {{ location.sql }})
      """
    When query
      """
      SELECT count(*) AS cnt FROM parquet_temp_view_replace
      """
    Then query result
      | cnt |
      | 2   |

  Scenario: Read from a global temporary view backed by a parquet path
    Given final statement
      """
      DROP VIEW IF EXISTS global_temp.parquet_global_temp_view
      """
    Given statement
      """
      DROP VIEW IF EXISTS global_temp.parquet_global_temp_view
      """
    Given statement template
      """
      CREATE GLOBAL TEMPORARY VIEW parquet_global_temp_view USING parquet OPTIONS (path {{ location.sql }})
      """
    When query
      """
      SELECT id FROM global_temp.parquet_global_temp_view ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |

  Scenario: Options other than the path are forwarded to the data source
    Given variable csv_location for temporary directory temp_view_using_csv
    Given final statement
      """
      DROP TABLE IF EXISTS temp_view_using_csv_data
      """
    Given final statement
      """
      DROP VIEW IF EXISTS csv_temp_view
      """
    Given statement template
      """
      CREATE TABLE temp_view_using_csv_data (id INT, name STRING)
      USING csv OPTIONS (header 'true') LOCATION {{ csv_location.sql }}
      """
    Given statement
      """
      INSERT INTO temp_view_using_csv_data VALUES (1, 'Alice'), (2, 'Bob')
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW csv_temp_view USING csv OPTIONS (path {{ csv_location.sql }}, header 'true')
      """
    When query
      """
      SELECT id, name FROM csv_temp_view ORDER BY id
      """
    Then query result ordered
      | id | name  |
      | 1  | Alice |
      | 2  | Bob   |

  # The error regexes match either engine: these invalid grammar shapes may be
  # rejected by the parser or, in older Sail builds, by the analyzer.

  Scenario: USING is rejected for persistent views
    Given statement template with error (only supported for temporary views|PARSE_SYNTAX_ERROR|found USING)
      """
      CREATE VIEW persistent_view_using USING parquet OPTIONS (path {{ location.sql }})
      """

  Scenario: USING cannot be combined with an AS query
    Given statement template with error (cannot be defined with an AS query|PARSE_SYNTAX_ERROR|found AS)
      """
      CREATE TEMPORARY VIEW temp_view_using_bad USING parquet OPTIONS (path {{ location.sql }}) AS SELECT 1
      """

  Scenario: COMMENT and TBLPROPERTIES are rejected for data source temporary views
    Given statement template with error (cannot be used with CREATE TEMPORARY VIEW|PARSE_SYNTAX_ERROR|found TBLPROPERTIES)
      """
      CREATE TEMPORARY VIEW temp_view_using_props
      USING parquet OPTIONS (path {{ location.sql }})
      TBLPROPERTIES (meow = 'mix')
      """

  Scenario: Converted doctest creates and replaces a parquet-backed temporary view
    Given variable txt_products for temporary directory temp_view_using_txt_products
    Given statement
      """
      DROP VIEW IF EXISTS v_using
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_using
      """
    Given statement
      """
      DROP TABLE IF EXISTS temp_view_using_txt_products
      """
    Given final statement
      """
      DROP TABLE IF EXISTS temp_view_using_txt_products
      """
    Given statement template
      """
      CREATE TABLE temp_view_using_txt_products (id INT, name STRING)
      USING parquet LOCATION {{ txt_products.sql }}
      """
    Given statement
      """
      INSERT INTO temp_view_using_txt_products VALUES (1, 'a'), (2, 'b')
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW v_using USING parquet OPTIONS (path {{ txt_products.sql }})
      """
    When query
      """
      SELECT id, name FROM v_using ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | a    |
      | 2  | b    |
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_using USING parquet OPTIONS (path {{ txt_products.sql }})
      """
    When query
      """
      SELECT count(*) AS cnt FROM v_using
      """
    Then query result
      | cnt |
      | 2   |

  Scenario: Converted doctest creates a parquet-backed global temporary view
    Given variable txt_global_products for temporary directory temp_view_using_txt_global_products
    Given statement
      """
      DROP VIEW IF EXISTS global_temp.v_using_global
      """
    Given final statement
      """
      DROP VIEW IF EXISTS global_temp.v_using_global
      """
    Given statement
      """
      DROP TABLE IF EXISTS temp_view_using_txt_global_products
      """
    Given final statement
      """
      DROP TABLE IF EXISTS temp_view_using_txt_global_products
      """
    Given statement template
      """
      CREATE TABLE temp_view_using_txt_global_products (id INT, name STRING)
      USING parquet LOCATION {{ txt_global_products.sql }}
      """
    Given statement
      """
      INSERT INTO temp_view_using_txt_global_products VALUES (1, 'a'), (2, 'b')
      """
    Given statement template
      """
      CREATE GLOBAL TEMPORARY VIEW v_using_global USING parquet OPTIONS (path {{ txt_global_products.sql }})
      """
    When query
      """
      SELECT id FROM global_temp.v_using_global ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |

  Scenario: Converted doctest forwards CSV options to the data source
    Given variable txt_csv for temporary directory temp_view_using_txt_csv
    Given statement
      """
      DROP VIEW IF EXISTS v_using_csv
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_using_csv
      """
    Given statement
      """
      DROP TABLE IF EXISTS temp_view_using_txt_csv
      """
    Given final statement
      """
      DROP TABLE IF EXISTS temp_view_using_txt_csv
      """
    Given statement template
      """
      CREATE TABLE temp_view_using_txt_csv (id INT, name STRING)
      USING csv OPTIONS (header 'true') LOCATION {{ txt_csv.sql }}
      """
    Given statement
      """
      INSERT INTO temp_view_using_txt_csv VALUES (1, 'a'), (2, 'b')
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW v_using_csv USING csv OPTIONS (path {{ txt_csv.sql }}, header 'true')
      """
    When query
      """
      SELECT id, name FROM v_using_csv ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | a    |
      | 2  | b    |

  Scenario: Converted doctest rejects invalid USING view forms
    Given statement template with error (only supported for temporary views|PARSE_SYNTAX_ERROR|found USING)
      """
      CREATE VIEW v_persistent USING parquet OPTIONS (path {{ location.sql }})
      """
    Given statement template with error (cannot be defined with an AS query|PARSE_SYNTAX_ERROR|found AS)
      """
      CREATE TEMPORARY VIEW v_bad USING parquet OPTIONS (path {{ location.sql }}) AS SELECT 1
      """

  Scenario: Insert works for a data source temporary view
    Given variable insert_path for temporary directory temp_view_using_insert
    Given statement
      """
      DROP VIEW IF EXISTS works
      """
    Given final statement
      """
      DROP VIEW IF EXISTS works
      """
    Given statement
      """
      DROP TABLE IF EXISTS temp_view_using_insert_data
      """
    Given final statement
      """
      DROP TABLE IF EXISTS temp_view_using_insert_data
      """
    Given statement template
      """
      CREATE TABLE temp_view_using_insert_data (id INT, name STRING)
      USING parquet LOCATION {{ insert_path.sql }}
      """
    Given statement
      """
      INSERT INTO temp_view_using_insert_data VALUES (1, 'a'), (2, 'b'), (4, 'a')
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW works USING parquet OPTIONS (path {{ insert_path.sql }})
      """
    When query
      """
      SELECT * FROM works
      """
    Then query result
      | id | name |
      | 1  | a    |
      | 2  | b    |
      | 4  | a    |
    Given statement
      """
      INSERT INTO works VALUES (5, 'c')
      """
    When query
      """
      SELECT * FROM works
      """
    Then query result
      | id | name |
      | 1  | a    |
      | 2  | b    |
      | 4  | a    |
      | 5  | c    |

  Scenario: Insert fails for a query temporary view over a data source path
    Given variable insert_fail_path for temporary directory temp_view_using_insert_fail
    Given statement
      """
      DROP VIEW IF EXISTS fails
      """
    Given final statement
      """
      DROP VIEW IF EXISTS fails
      """
    Given statement
      """
      DROP TABLE IF EXISTS temp_view_using_insert_fail_data
      """
    Given final statement
      """
      DROP TABLE IF EXISTS temp_view_using_insert_fail_data
      """
    Given statement template
      """
      CREATE TABLE temp_view_using_insert_fail_data (id INT, name STRING)
      USING parquet LOCATION {{ insert_fail_path.sql }}
      """
    Given statement
      """
      INSERT INTO temp_view_using_insert_fail_data VALUES (1, 'a'), (2, 'b'), (4, 'a'), (5, 'c')
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW fails AS
      SELECT * FROM parquet.`{{ insert_fail_path.string }}`
      """
    When query
      """
      SELECT * FROM fails
      """
    Then query result
      | id | name |
      | 1  | a    |
      | 2  | b    |
      | 4  | a    |
      | 5  | c    |
    Given statement with error (EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE|table does not exist.*fails)
      """
      INSERT INTO fails VALUES (7, 'e')
      """

  Scenario: SQL-only temporary data source views and related grammar pins from test_view
    Given variable view_parquet_data for temporary directory test_view_parquet_data
    Given variable view_csv_data for temporary directory test_view_csv_data
    Given variable view_json_data for temporary directory test_view_json_data
    Given variable view_missing_path for temporary directory test_view_does_not_exist
    Given variable view_t_ctas for temporary directory test_view_t_ctas
    Given variable view_t_ctas2 for temporary directory test_view_t_ctas2
    Given variable view_part_data for temporary directory test_view_part_data
    Given final statement
      """
      DROP VIEW IF EXISTS v_parquet
      """
    Given final statement
      """
      DROP VIEW IF EXISTS global_temp.v_global
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_csv
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_json
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_upper
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_eq
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_dup_case
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_path_loc
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_loc_path
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_typed
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_typed_bad
      """
    Given final statement
      """
      DROP VIEW IF EXISTS using
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_tmp_comment
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_global_tmp_comment
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_as_cols
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_as_cols_named
      """
    Given final statement
      """
      DROP TABLE IF EXISTS t_ctas
      """
    Given final statement
      """
      DROP TABLE IF EXISTS t_ctas2
      """
    Given final statement
      """
      DROP DATABASE IF EXISTS foo CASCADE
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_parquet
      """
    Given statement
      """
      DROP VIEW IF EXISTS global_temp.v_global
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_csv
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_json
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_upper
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_eq
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_dup_case
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_path_loc
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_loc_path
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_typed
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_typed_bad
      """
    Given statement
      """
      DROP VIEW IF EXISTS using
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_tmp_comment
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_global_tmp_comment
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_as_cols
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_as_cols_named
      """
    Given statement
      """
      DROP TABLE IF EXISTS t_ctas
      """
    Given statement
      """
      DROP TABLE IF EXISTS t_ctas2
      """
    Given statement
      """
      DROP DATABASE IF EXISTS foo CASCADE
      """
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ view_parquet_data.sql }}
      USING parquet
      SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)
      """
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ view_csv_data.sql }}
      USING csv OPTIONS (header 'true')
      SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)
      """
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ view_json_data.sql }}
      USING json
      SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW v_parquet
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT * FROM v_parquet ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | a    |
      | 2  | b    |
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_parquet
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT count(*) AS cnt FROM v_parquet
      """
    Then query result
      | cnt |
      | 2   |
    Given statement template
      """
      CREATE TEMPORARY VIEW v_tmp_comment (id INT COMMENT 'Unique identification number', name STRING COMMENT 'Some name')
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW v_global_tmp_comment (id INT COMMENT 'Unique identification number', name STRING COMMENT 'Some name')
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      """
    Given statement
      """
      CREATE TEMPORARY VIEW v_as_cols (id COMMENT 'Unique identification number', name COMMENT 'Some name')
      AS SELECT 1, 'a'
      """
    When query
      """
      SELECT * FROM v_as_cols
      """
    Then query result
      | id | name |
      | 1  | a    |
    Given statement
      """
      CREATE TEMPORARY VIEW v_as_cols_named (id COMMENT 'Unique identification number', name COMMENT 'Some name')
      AS SELECT 1 AS id, 'a' AS name
      """
    When query
      """
      SELECT * FROM v_as_cols_named
      """
    Then query result
      | id | name |
      | 1  | a    |
    Given statement template
      """
      CREATE GLOBAL TEMPORARY VIEW v_global
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT id FROM global_temp.v_global ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |
    Given statement template
      """
      CREATE OR REPLACE GLOBAL TEMPORARY VIEW v_global
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW v_csv
      USING csv
      OPTIONS (path {{ view_csv_data.sql }}, header 'true')
      """
    When query
      """
      SELECT id, name FROM v_csv ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | a    |
      | 2  | b    |
    Given statement template
      """
      CREATE TEMPORARY VIEW v_json
      USING json
      OPTIONS (path {{ view_json_data.sql }})
      """
    When query
      """
      SELECT id, name FROM v_json ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | a    |
      | 2  | b    |
    Given statement template
      """
      CREATE TEMPORARY VIEW v_upper
      USING parquet
      OPTIONS (PATH {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT count(*) AS cnt FROM v_upper
      """
    Then query result
      | cnt |
      | 2   |
    Given statement template
      """
      CREATE TEMPORARY VIEW v_eq
      USING parquet
      OPTIONS (path = {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT count(*) AS cnt FROM v_eq
      """
    Then query result
      | cnt |
      | 2   |
    Given statement template
      """
      CREATE TEMPORARY VIEW v_path_loc
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }}, location {{ view_missing_path.sql }})
      """
    When query
      """
      SELECT count(*) AS cnt FROM v_path_loc
      """
    Then query result
      | cnt |
      | 2   |
    Given statement template
      """
      CREATE TEMPORARY VIEW v_loc_path
      USING parquet
      OPTIONS (location {{ view_missing_path.sql }}, path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT count(*) AS cnt FROM v_loc_path
      """
    Then query result
      | cnt |
      | 2   |
    Given statement template
      """
      CREATE TEMPORARY VIEW v_typed (id INT, name STRING)
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT * FROM v_typed ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | a    |
      | 2  | b    |
    Given statement template
      """
      CREATE TEMPORARY VIEW using
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT count(*) AS cnt FROM using
      """
    Then query result
      | cnt |
      | 2   |
    # The `AS` keyword is optional for `CREATE TABLE ... AS SELECT`, unlike views.
    Given statement template
      """
      CREATE TABLE t_ctas USING parquet LOCATION {{ view_t_ctas.sql }}
      AS SELECT 1 AS a
      """
    Given statement template
      """
      CREATE TABLE t_ctas2 USING parquet LOCATION {{ view_t_ctas2.sql }}
      SELECT 1 AS a
      """
    Given statement
      """
      DROP TABLE IF EXISTS t_ctas
      """
    Given statement
      """
      DROP TABLE IF EXISTS t_ctas2
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW v_dup_case
      USING parquet
      OPTIONS (path {{ view_missing_path.sql }}, PATH {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT count(*) AS cnt FROM v_dup_case
      """
    Then query result
      | cnt |
      | 2   |
    Given statement template
      """
      CREATE TEMPORARY VIEW v_typed_bad (id BIGINT)
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT * FROM v_typed_bad ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS foo
      """
    Given statement
      """
      CREATE TABLE foo.1m(a INT)
      """
    When query
      """
      DESCRIBE TABLE foo.1m
      """
    Then query result ordered
      | col_name | data_type | comment |
      | a        | int       | NULL    |
    Given statement
      """
      INSERT INTO foo.1m VALUES (1), (2)
      """
    When query
      """
      SELECT a FROM foo.1m ORDER BY a
      """
    Then query result ordered
      | a |
      | 1 |
      | 2 |
    Given statement
      """
      DROP TABLE IF EXISTS foo.1m
      """
    Given statement
      """
      CREATE TABLE foo.2m(a INT) USING parquet
      """
    When query
      """
      DESCRIBE TABLE foo.2m
      """
    Then query result ordered
      | col_name | data_type | comment |
      | a        | int       | NULL    |
    Given statement
      """
      INSERT INTO foo.2m VALUES (1), (2)
      """
    When query
      """
      SELECT a FROM foo.2m ORDER BY a
      """
    Then query result ordered
      | a |
      | 1 |
      | 2 |
    Given statement
      """
      DROP TABLE IF EXISTS foo.2m
      """
    Given statement
      """
      DROP DATABASE IF EXISTS foo CASCADE
      """
    When query
      """
      SELECT 1y
      """
    Then query result
      | 1 |
      | 1 |
    When query
      """
      SELECT 1m
      """
    Then query error .*
    Given statement template with error .*
      """
      CREATE VIEW v_persistent
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      """
    Given statement template with error .*
      """
      CREATE TEMPORARY VIEW IF NOT EXISTS v_ine
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      """
    Given statement template with error .*
      """
      CREATE TEMPORARY VIEW v_as
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      AS SELECT 1
      """
    Given statement template with error .*
      """
      CREATE TEMPORARY VIEW v_sel
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      SELECT 1
      """
    Given statement template with error .*
      """
      CREATE TEMPORARY VIEW v_props
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      TBLPROPERTIES ('k'='v')
      """
    Given statement template with error .*
      """
      CREATE TEMPORARY VIEW v_comment
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      COMMENT 'c'
      """
    Given statement template with error .*
      """
      CREATE TEMPORARY VIEW v_cols (a, b)
      USING parquet
      OPTIONS (path {{ view_parquet_data.sql }})
      """
    # AS is mandatory for the query form of CREATE VIEW.
    Given statement with error .*
      """
      CREATE VIEW v_noas SELECT 1
      """
    # AS is mandatory for the query form of CREATE VIEW.
    Given statement with error .*
      """
      CREATE VIEW v_paren (SELECT 1)
      """
    # The data source path must be specified: Spark errors at CREATE
    Given statement with error .*
      """
      CREATE TEMPORARY VIEW v_nopath
      USING parquet
      """
    Given statement with error .*
      """
      CREATE TEMPORARY VIEW v_emptypath
      USING parquet
      OPTIONS (path '')
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_parquet
      """
    Given statement
      """
      DROP VIEW IF EXISTS global_temp.v_global
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_csv
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_json
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_upper
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_eq
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_dup_case
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_path_loc
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_loc_path
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_typed
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_typed_bad
      """
    Given statement
      """
      DROP VIEW IF EXISTS using
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_tmp_comment
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_global_tmp_comment
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_as_cols
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_as_cols_named
      """
    Given final statement
      """
      DROP TABLE IF EXISTS part_src
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_desc
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_desc_typed
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_nn
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_reorder
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_subset
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_case_cols
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_extra
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_pq_upper
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_csv_bool
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_csv_infer
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_opt_dotted
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_glob
      """
    Given final statement
      """
      DROP VIEW IF EXISTS `v-dash`
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_orc
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_swap
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_part
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_refresh
      """
    Given final statement
      """
      DROP VIEW IF EXISTS v_show_create
      """
    Given final statement
      """
      DROP VIEW IF EXISTS global_temp.v_gtv
      """
    Given statement
      """
      DROP TABLE IF EXISTS part_src
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_desc
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_desc_typed
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_nn
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_reorder
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_subset
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_case_cols
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_extra
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_pq_upper
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_csv_bool
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_csv_infer
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_opt_dotted
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_glob
      """
    Given statement
      """
      DROP VIEW IF EXISTS `v-dash`
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_orc
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_swap
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_part
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_refresh
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_show_create
      """
    Given statement
      """
      DROP VIEW IF EXISTS global_temp.v_gtv
      """
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_desc USING parquet OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      DESCRIBE TABLE v_desc
      """
    Then query result ordered
      | col_name | data_type | comment |
      | id       | int       | NULL    |
      | name     | string    | NULL    |
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_desc_typed (id INT COMMENT 'c1', name STRING COMMENT 'c2') USING parquet OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      DESCRIBE TABLE v_desc_typed
      """
    Then query result ordered
      | col_name | data_type | comment |
      | id       | int       | c1      |
      | name     | string    | c2      |
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_nn (id INT NOT NULL, name STRING) USING parquet OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT * FROM v_nn ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | a    |
      | 2  | b    |
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_reorder (name STRING, id INT) USING parquet OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT * FROM v_reorder ORDER BY id
      """
    Then query result ordered
      | name | id |
      | a    | 1  |
      | b    | 2  |
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_subset (id INT) USING parquet OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT * FROM v_subset ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_case_cols (ID INT, NAME STRING) USING parquet OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT * FROM v_case_cols ORDER BY ID
      """
    Then query result ordered
      | ID | NAME |
      | 1  | a    |
      | 2  | b    |
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_extra (id INT, name STRING, missing DOUBLE) USING parquet OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT * FROM v_extra ORDER BY id
      """
    Then query result ordered
      | id | name | missing |
      | 1  | a    | NULL    |
      | 2  | b    | NULL    |
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_pq_upper USING PARQUET OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT count(*) AS cnt FROM v_pq_upper
      """
    Then query result
      | cnt |
      | 2   |
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_csv_bool USING csv OPTIONS (path {{ view_csv_data.sql }}, header true)
      """
    When query
      """
      SELECT id, name FROM v_csv_bool ORDER BY id
      """
    Then query result ordered
      | id | name |
      | 1  | a    |
      | 2  | b    |
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_csv_infer USING csv OPTIONS (path {{ view_csv_data.sql }}, header 'true', inferSchema 'true')
      """
    When query
      """
      DESCRIBE TABLE v_csv_infer
      """
    Then query result ordered
      | col_name | data_type | comment |
      | id       | bigint    | NULL    |
      | name     | string    | NULL    |
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_opt_dotted USING csv OPTIONS (path {{ view_csv_data.sql }}, custom.option 'x', header 'true')
      """
    When query
      """
      SELECT count(*) AS cnt FROM v_opt_dotted
      """
    Then query result
      | cnt |
      | 2   |
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_glob USING parquet OPTIONS (path '{{ view_parquet_data.file_uri }}/*.parquet')
      """
    When query
      """
      SELECT count(*) AS cnt FROM v_glob
      """
    Then query result
      | cnt |
      | 2   |
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW `v-dash` USING parquet OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT count(*) AS cnt FROM `v-dash`
      """
    Then query result
      | cnt |
      | 2   |
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW v_swap AS SELECT 42 AS answer
      """
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_swap USING parquet OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SELECT count(*) AS cnt FROM v_swap
      """
    Then query result
      | cnt |
      | 2   |
    Given statement
      """
      CREATE OR REPLACE TEMPORARY VIEW v_swap AS SELECT 42 AS answer
      """
    When query
      """
      SELECT * FROM v_swap
      """
    Then query result
      | answer |
      | 42     |
    Given statement template
      """
      CREATE TABLE part_src (id INT, p STRING) USING parquet PARTITIONED BY (p) LOCATION {{ view_part_data.sql }}
      """
    Given statement
      """
      INSERT INTO part_src VALUES (1, 'x'), (2, 'y')
      """
    Given statement template
      """
      CREATE OR REPLACE TEMPORARY VIEW v_part USING parquet OPTIONS (path {{ view_part_data.sql }})
      """
    When query
      """
      DESCRIBE TABLE v_part
      """
    Then query result ordered
      | col_name | data_type | comment |
      | id       | int       | NULL    |
      | p        | string    | NULL    |
    When query
      """
      SELECT id, p FROM v_part ORDER BY id
      """
    Then query result ordered
      | id | p |
      | 1  | x |
      | 2  | y |
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ view_parquet_data.sql }} USING parquet SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, name)
      """
    Given statement template
      """
      INSERT OVERWRITE DIRECTORY {{ view_parquet_data.sql }} USING parquet SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)
      """
    Given statement template
      """
      CREATE TEMPORARY VIEW v_show_create USING parquet OPTIONS (path {{ view_parquet_data.sql }})
      """
    When query
      """
      SHOW VIEWS LIKE 'v_show_create'
      """
    Then query result
      | tableName     | catalog | namespace | description | tableType | isTemporary |
      | v_show_create | NULL    | []        | NULL        | TEMPORARY | true        |
    Given statement
      """
      INSERT INTO v_show_create VALUES (3, 'c')
      """
    Given statement template
      """
      CREATE OR REPLACE GLOBAL TEMPORARY VIEW v_gtv USING parquet OPTIONS (path {{ view_parquet_data.sql }})
      """
    Given statement
      """
      DROP TABLE IF EXISTS part_src
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_desc
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_desc_typed
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_nn
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_reorder
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_subset
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_case_cols
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_extra
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_pq_upper
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_csv_bool
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_csv_infer
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_opt_dotted
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_glob
      """
    Given statement
      """
      DROP VIEW IF EXISTS `v-dash`
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_orc
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_swap
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_part
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_refresh
      """
    Given statement
      """
      DROP VIEW IF EXISTS v_show_create
      """
    Given statement
      """
      DROP VIEW IF EXISTS global_temp.v_gtv
      """
