Feature: Read queries validate physical table schemas

  Scenario: SELECT * fails when table provider schema has duplicate field names
    Given variable location for temporary directory bad_table_duplicate_schema
    Given final statement
      """
      DROP TABLE IF EXISTS bad_table
      """
    Given statement template
      """
      CREATE TABLE bad_table (
        a BIGINT,
        a BIGINT
      )
      USING PARQUET
      LOCATION {{ location.sql }}
      """
    When query
      """
      SELECT * FROM bad_table
      """
    Then query error duplicate field names in table schema are not supported without physical renaming
