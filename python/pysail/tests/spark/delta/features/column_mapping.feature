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
