Feature: Delta Lake Column Mapping (DDL TBLPROPERTIES)

  Rule: Column mapping name mode creates proper protocol and metadata
    Background:
      Given variable location for temporary directory cm_name
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cm_name_snapshot
        """

    Scenario: Create table with column mapping name mode (DDL) and first write materializes mapping
      Given statement template
        """
        CREATE TABLE delta_cm_name_snapshot (
          id INT,
          name STRING,
          value DOUBLE
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.columnMapping.mode' = 'name'
        )
        """
      Given statement
        """
        INSERT INTO delta_cm_name_snapshot VALUES (1, 'test', 1.0)
        """
      Then delta log first commit protocol and metadata matches snapshot

  Rule: Column mapping id mode creates proper protocol and metadata
    Background:
      Given variable location for temporary directory cm_id
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cm_id_snapshot
        """

    Scenario: Create table with column mapping id mode (DDL) and first write materializes mapping
      Given statement template
        """
        CREATE TABLE delta_cm_id_snapshot (
          id INT,
          name STRING,
          value DOUBLE
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.columnMapping.mode' = 'id'
        )
        """
      Given statement
        """
        INSERT INTO delta_cm_id_snapshot VALUES (1, 'test', 1.0)
        """
      Then delta log first commit protocol and metadata matches snapshot

  Rule: Column mapping with nested struct creates proper schema annotations
    Background:
      Given variable location for temporary directory cm_nested
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cm_nested_snapshot
        """

    Scenario: Create table with nested struct in column mapping name mode (DDL)
      Given statement template
        """
        CREATE TABLE delta_cm_nested_snapshot (
          id INT,
          user STRUCT<name: STRING, age: INT>,
          tags ARRAY<STRING>
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.columnMapping.mode' = 'name'
        )
        """
      Given statement
        """
        INSERT INTO delta_cm_nested_snapshot VALUES (1, named_struct('name', 'alice', 'age', 30), array('a', 'b'))
        """
      Then delta log first commit protocol and metadata matches snapshot

  Rule: Column mapping with partitioned table
    Background:
      Given variable location for temporary directory cm_partitioned
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cm_partitioned_snapshot
        """

    Scenario: Create partitioned table with column mapping name mode (DDL)
      Given statement template
        """
        CREATE TABLE delta_cm_partitioned_snapshot (
          id INT,
          data STRING,
          region STRING
        )
        USING DELTA
        PARTITIONED BY (region)
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.columnMapping.mode' = 'name'
        )
        """
      Given statement
        """
        INSERT INTO delta_cm_partitioned_snapshot VALUES (1, 'test', 'us')
        """
      Then delta log first commit protocol and metadata matches snapshot

  Rule: Column IDs remain unique through consecutive collection types
    Background:
      Given variable location for temporary directory cm_consecutive_arrays
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cm_consecutive_arrays
        """

    Scenario: mergeSchema allocates a new ID after array of array of struct
      Given statement template
        """
        CREATE TABLE delta_cm_consecutive_arrays (
          matrix ARRAY<ARRAY<STRUCT<value: BIGINT>>>
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.columnMapping.mode' = 'name'
        )
        """
      Given append query to delta table in location with mergeSchema
        """
        SELECT
          array(array(named_struct('value', CAST(2 AS BIGINT)))) AS matrix,
          'new' AS label
        """
      Then delta log latest effective protocol and metadata matches snapshot

  Rule: Scalar mapping metadata is not propagated to a non-mapped table
    Background:
      Given variable source_location for temporary directory cm_scalar_source
      Given variable location for temporary directory cm_scalar_target
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cm_scalar_source
        """
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cm_scalar_target
        """

    Scenario: Writing a scalar name-mapped table strips physical read metadata
      Given statement template
        """
        CREATE TABLE delta_cm_scalar_source (
          id BIGINT,
          label STRING
        )
        USING DELTA
        LOCATION {{ source_location.sql }}
        TBLPROPERTIES (
          'delta.columnMapping.mode' = 'name'
        )
        """
      Given statement
        """
        INSERT INTO delta_cm_scalar_source VALUES (1, 'a')
        """
      Given statement template
        """
        CREATE TABLE delta_cm_scalar_target
        USING DELTA
        LOCATION {{ location.sql }}
        AS SELECT * FROM delta_cm_scalar_source
        """
      Then delta log latest effective protocol and metadata matches snapshot

    Scenario: Writing a scalar ID-mapped table strips physical read metadata
      Given statement template
        """
        CREATE TABLE delta_cm_scalar_source (
          id BIGINT,
          label STRING
        )
        USING DELTA
        LOCATION {{ source_location.sql }}
        TBLPROPERTIES (
          'delta.columnMapping.mode' = 'id'
        )
        """
      Given statement
        """
        INSERT INTO delta_cm_scalar_source VALUES (1, 'a')
        """
      Given statement template
        """
        CREATE TABLE delta_cm_scalar_target
        USING DELTA
        LOCATION {{ location.sql }}
        AS SELECT * FROM delta_cm_scalar_source
        """
      Then delta log latest effective protocol and metadata matches snapshot

  Rule: Column mapping preserves special characters in column names
    Background:
      Given variable location for temporary directory cm_special_chars
      Given final statement
        """
        DROP TABLE IF EXISTS delta_cm_special_chars
        """

    Scenario: Create and query a table whose column names contain supported special characters
      Given statement template
        """
        CREATE TABLE delta_cm_special_chars (
          `first.name` STRING,
          `name with space` INT,
          `a,b` STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        TBLPROPERTIES (
          'delta.columnMapping.mode' = 'name'
        )
        """
      Given statement
        """
        INSERT INTO delta_cm_special_chars VALUES ('alice', 1, 'x=y'), ('bob', 2, 'p=q')
        """
      When query
        """
        SELECT `first.name`, `name with space`, `a,b`
        FROM delta_cm_special_chars
        ORDER BY `name with space`
        """
      Then query result ordered
        | first.name | name with space | a,b |
        | alice      | 1               | x=y |
        | bob        | 2               | p=q |
