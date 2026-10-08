Feature: Iceberg Basic IO

  Rule: Create table and basic operations
    Background:
      Given variable location for temporary directory iceberg_io
      Given final statement
        """
        DROP TABLE IF EXISTS test_table
        """

    Scenario: Create a new table and append data
      Given statement template
        """
        CREATE TABLE test_table (id INT, data STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO test_table VALUES (1, 'a'), (2, 'b')
        """
      Then iceberg metadata contains current snapshot
      Then iceberg snapshot operation is append
      When query
        """
        SELECT * FROM test_table ORDER BY id
        """
      Then query result ordered
        | id | data |
        | 1  | a    |
        | 2  | b    |

    Scenario: CTAS catalog table records metadata location
      Given final statement
        """
        DROP TABLE IF EXISTS iceberg_catalog_ctas_table
        """
      Given statement template
        """
        CREATE TABLE iceberg_catalog_ctas_table
        USING iceberg
        LOCATION {{ location.uri }}
        AS SELECT 1 AS id
        """
      When query
        """
        DESCRIBE EXTENDED iceberg_catalog_ctas_table
        """
      Then query result row where "col_name" is "Table Properties" has "data_type" containing "metadata_location="
      When query
        """
        SELECT * FROM iceberg_catalog_ctas_table
        """
      Then query result
        | id |
        | 1  |

    Scenario: Multiple inserts create multiple snapshots
      Given statement template
        """
        CREATE TABLE test_table (id INT, value STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO test_table VALUES (1, 'first')
        """
      Then iceberg snapshot count is 1
      Given statement
        """
        INSERT INTO test_table VALUES (2, 'second')
        """
      Then iceberg snapshot count is 2
      When query
        """
        SELECT * FROM test_table ORDER BY id
        """
      Then query result ordered
        | id | value  |
        | 1  | first  |
        | 2  | second |

    Scenario: Create or replace preserves historical lineage and clears main
      Given statement template
        """
        CREATE TABLE test_table (id INT, value STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO test_table VALUES (1, 'before-replace')
        """
      Given iceberg tag before_replace in location points to snapshot index 0
      Given statement template
        """
        CREATE OR REPLACE TABLE test_table (id INT, value STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Then iceberg metadata matches snapshot
      Then iceberg metadata contains
        | path                | value |
        | current-snapshot-id | -1    |
      Then iceberg snapshot count is 1
      When query
        """
        SELECT * FROM test_table
        """
      Then query result
        | id | value |
      When query
        """
        SELECT * FROM test_table VERSION AS OF 'before_replace'
        """
      Then query result
        | id | value          |
        | 1  | before-replace |
      Given statement
        """
        INSERT INTO test_table VALUES (2, 'after-replace')
        """
      Then iceberg snapshot count is 2
      Then iceberg metadata contains
        | path                 | value |
        | last-sequence-number | 2     |
      When query
        """
        SELECT * FROM test_table VERSION AS OF 'main'
        """
      Then query result
        | id | value         |
        | 2  | after-replace |
      When query
        """
        SELECT * FROM test_table VERSION AS OF 'before_replace'
        """
      Then query result
        | id | value          |
        | 1  | before-replace |

    Scenario: Append after latest metadata uses UUID-prefixed naming
      Given statement template
        """
        CREATE TABLE test_table (id INT, value STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO test_table VALUES (1, 'first')
        """
      Given iceberg latest metadata file uses UUID-prefixed naming
      Given statement
        """
        INSERT INTO test_table VALUES (2, 'second')
        """
      Then iceberg latest metadata file is v3.metadata.json
      Then iceberg version hint is 3
      When query
        """
        SELECT * FROM test_table ORDER BY id
        """
      Then query result ordered
        | id | value  |
        | 1  | first  |
        | 2  | second |

    Scenario: Append after latest metadata uses UUID-prefixed gzip naming
      Given statement template
        """
        CREATE TABLE test_table (id INT, value STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO test_table VALUES (1, 'first')
        """
      Given iceberg latest metadata file uses UUID-prefixed gzip naming
      Given statement
        """
        INSERT INTO test_table VALUES (2, 'second')
        """
      Then iceberg latest metadata file is v3.metadata.json
      Then iceberg version hint is 3
      When query
        """
        SELECT * FROM test_table ORDER BY id
        """
      Then query result ordered
        | id | value  |
        | 1  | first  |
        | 2  | second |

  Rule: Verify file layout for unpartitioned tables
    Background:
      Given variable location for temporary directory iceberg_io_layout
      Given final statement
        """
        DROP TABLE IF EXISTS unpart_table
        """

    Scenario: Unpartitioned table writes to data directory
      Given statement template
        """
        CREATE TABLE unpart_table (id INT, name STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO unpart_table VALUES (1, 'alice'), (2, 'bob')
        """
      Then file tree in location matches
        """
        📂 data
          📄 *.parquet
        📂 metadata
          📄 *.metadata.json
          📄 *.metadata.json
          📄 snap-*.avro
        """

  Rule: Overwrite data creates new snapshot
    Background:
      Given variable location for temporary directory iceberg_io_overwrite
      Given final statement
        """
        DROP TABLE IF EXISTS overwrite_table
        """

    Scenario: INSERT OVERWRITE replaces data
      Given statement template
        """
        CREATE TABLE overwrite_table (id INT, status STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO overwrite_table VALUES (1, 'old'), (2, 'old')
        """
      Then iceberg snapshot operation is append
      Given statement
        """
        INSERT OVERWRITE TABLE overwrite_table VALUES (3, 'new'), (4, 'new')
        """
      Then iceberg snapshot operation is overwrite
      Then iceberg snapshot count is 2
      When query
        """
        SELECT * FROM overwrite_table ORDER BY id
        """
      Then query result ordered
        | id | status |
        | 3  | new    |
        | 4  | new    |

  Scenario Outline: Replacement preserves schema and partition IDs and allocates new partition IDs
    Given variable location for temporary directory iceberg_replace_ids
    Given final statement
      """
      DROP TABLE IF EXISTS iceberg_replace_ids
      """
    Given statement template
      """
      CREATE TABLE iceberg_replace_ids (id INT, p INT) USING iceberg PARTITIONED BY (p)
      LOCATION {{ location.uri }} TBLPROPERTIES ('format-version' = '<version>')
      """
    Given statement
      """
      INSERT INTO iceberg_replace_ids VALUES (1, 7)
      """
    Given variable snapshot_ids for iceberg snapshot ids in location
    Given statement template
      """
      CREATE OR REPLACE TABLE iceberg_replace_ids (id INT, p INT) USING iceberg PARTITIONED BY (p)
      LOCATION {{ location.uri }} TBLPROPERTIES ('format-version' = '<version>')
      """
    Then iceberg metadata contains
      | path                     | value |
      | current-schema-id        | 0     |
      | last-column-id           | 2     |
      | default-spec-id          | 0     |
      | last-partition-id        | 1000  |
    When query
      """
      SELECT count(*) AS count FROM iceberg_replace_ids
      """
    Then query result ordered
      | count |
      | 0     |
    Given statement template
      """
      CREATE OR REPLACE TABLE iceberg_replace_ids (id INT, p INT, q BIGINT) USING iceberg PARTITIONED BY (q)
      LOCATION {{ location.uri }} TBLPROPERTIES ('format-version' = '<version>')
      """
    Then iceberg metadata contains
      | path                                      | value |
      | current-schema-id                         | 1     |
      | last-column-id                            | 3     |
      | schemas[1].fields[0].id                    | 1     |
      | schemas[1].fields[1].id                    | 2     |
      | schemas[1].fields[2].id                    | 3     |
      | default-spec-id                           | 1     |
      | last-partition-id                         | 1001  |
      | partition-specs[0].fields[0]['source-id']   | 2     |
      | partition-specs[0].fields[0]['field-id']    | 1000  |
      | partition-specs[1].fields[<partition_index>]['source-id']   | 3     |
      | partition-specs[1].fields[<partition_index>]['field-id']    | 1001  |
    Given statement
      """
      INSERT INTO iceberg_replace_ids VALUES (2, 8, 9)
      """
    When query
      """
      SELECT * FROM iceberg_replace_ids
      """
    Then query result ordered
      | id | p | q |
      | 2  | 8 | 9 |
    When query template
      """
      SELECT id, p FROM iceberg_replace_ids VERSION AS OF {{ snapshot_ids[0] }}
      """
    Then query result ordered
      | id | p |
      | 1  | 7 |

    Examples:
      | version | partition_index |
      | 1       | 1               |
      | 2       | 0               |
      | 3       | 0               |
