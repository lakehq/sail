Feature: Delta Lake write operations

  Rule: Append / Overwrite / Overwrite-if plans are explainable

    Scenario: EXPLAIN CODEGEN for Delta append (INSERT INTO)
      Given variable location for temporary directory x
      Given final statement
        """
        DROP TABLE IF EXISTS delta_write_append
        """
      Given statement template
        """
        CREATE TABLE delta_write_append (
          id INT,
          category STRING,
          value BIGINT
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      When query
        """
        EXPLAIN CODEGEN
        INSERT INTO delta_write_append
        SELECT * FROM VALUES
          (1, 'A', CAST(10 AS BIGINT)),
          (2, 'B', CAST(20 AS BIGINT))
        AS t(id, category, value)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN CODEGEN for Delta overwrite (INSERT OVERWRITE)
      Given variable location for temporary directory x
      Given final statement
        """
        DROP TABLE IF EXISTS delta_write_overwrite
        """
      Given statement template
        """
        CREATE TABLE delta_write_overwrite (
          id INT,
          category STRING,
          value BIGINT
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_write_overwrite
        SELECT * FROM VALUES
          (1, 'A', CAST(10 AS BIGINT)),
          (2, 'B', CAST(20 AS BIGINT))
        AS t(id, category, value)
        """
      When query
        """
        EXPLAIN CODEGEN
        INSERT OVERWRITE TABLE delta_write_overwrite
        SELECT * FROM VALUES
          (3, 'A', CAST(30 AS BIGINT)),
          (4, 'C', CAST(40 AS BIGINT))
        AS t(id, category, value)
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN CODEGEN for Delta overwrite-if (REPLACE WHERE)
      Given variable location for temporary directory x
      Given final statement
        """
        DROP TABLE IF EXISTS delta_write_overwrite_if
        """
      Given statement template
        """
        CREATE TABLE delta_write_overwrite_if (
          id INT,
          category STRING,
          value BIGINT
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_write_overwrite_if
        SELECT * FROM VALUES
          (1, 'A', CAST(10 AS BIGINT)),
          (2, 'A', CAST(60 AS BIGINT)),
          (3, 'B', CAST(20 AS BIGINT)),
          (4, 'B', CAST(999 AS BIGINT))
        AS t(id, category, value)
        """
      When query
        """
        EXPLAIN CODEGEN
        INSERT INTO TABLE delta_write_overwrite_if
        REPLACE WHERE category = 'A' AND value < CAST(50 AS BIGINT)
        SELECT * FROM VALUES
          (5, 'A', CAST(100 AS BIGINT)),
          (6, 'A', CAST(200 AS BIGINT)),
          (7, 'A', CAST(10 AS BIGINT)),
          (8, 'B', CAST(999 AS BIGINT))
        AS t(id, category, value)
        """
      Then query plan matches snapshot


