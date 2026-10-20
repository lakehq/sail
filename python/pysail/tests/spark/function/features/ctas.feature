@ctas
Feature: CREATE TABLE AS SELECT (CTAS) correctness

  Rule: Column nullability (DataFusion #22087)

    # Spark always makes CTAS columns nullable regardless of the source query.
    # A literal SELECT 1 produces non-nullable columns in the query plan, but
    # the resulting table should still have nullable = true on all columns.
    Scenario: CTAS from literal SELECT produces nullable columns
      Given variable loc for temporary directory ctas_nullable
      Given statement template
        """
        CREATE TABLE ctas_nullable_test
        USING PARQUET
        LOCATION {{ loc.sql }}
        AS SELECT 1 AS a, 'hello' AS b
        """
      When query
        """
        SELECT * FROM ctas_nullable_test
        """
      Then query schema
        """
        root
         |-- a: integer (nullable = true)
         |-- b: string (nullable = true)
        """
      Given final statement
        """
        DROP TABLE IF EXISTS ctas_nullable_test
        """

    Scenario: CTAS from non-nullable source columns produces nullable columns
      Given variable loc for temporary directory ctas_nonnull_source
      Given statement template
        """
        CREATE TABLE ctas_source_nullable
        USING PARQUET
        LOCATION {{ loc.sql }}
        AS SELECT * FROM (SELECT 1 AS id) t
        """
      When query
        """
        SELECT * FROM ctas_source_nullable
        """
      Then query schema
        """
        root
         |-- id: integer (nullable = true)
        """
      Given final statement
        """
        DROP TABLE IF EXISTS ctas_source_nullable
        """

    Scenario: CTAS from nullable expression produces nullable columns
      Given variable loc for temporary directory ctas_nullable_expr
      Given statement template
        """
        CREATE TABLE ctas_nullable_expr
        USING PARQUET
        LOCATION {{ loc.sql }}
        AS SELECT CASE WHEN true THEN 1 ELSE NULL END AS a
        """
      When query
        """
        SELECT * FROM ctas_nullable_expr
        """
      Then query schema
        """
        root
         |-- a: integer (nullable = true)
        """
      Given final statement
        """
        DROP TABLE IF EXISTS ctas_nullable_expr
        """

    Scenario: CTAS with struct columns produces nullable top-level columns
      Given variable loc for temporary directory ctas_struct
      Given statement template
        """
        CREATE TABLE ctas_struct_test
        USING PARQUET
        LOCATION {{ loc.sql }}
        AS SELECT named_struct('x', 1) AS s
        """
      When query
        """
        SELECT * FROM ctas_struct_test
        """
      Then query schema
        """
        root
         |-- s: struct (nullable = true)
         |    |-- x: integer (nullable = true)
        """
      Given final statement
        """
        DROP TABLE IF EXISTS ctas_struct_test
        """

    Scenario: CTAS with empty result still produces nullable columns
      Given variable loc for temporary directory ctas_empty
      Given statement template
        """
        CREATE TABLE ctas_empty_test
        USING PARQUET
        LOCATION {{ loc.sql }}
        AS SELECT 1 AS a WHERE false
        """
      When query
        """
        SELECT * FROM ctas_empty_test
        """
      Then query schema
        """
        root
         |-- a: integer (nullable = true)
        """
      Given final statement
        """
        DROP TABLE IF EXISTS ctas_empty_test
        """

  Rule: Duplicate column names (DataFusion #22167)

    # CTAS with duplicate column names (e.g. from SELECT * on a self-join)
    # should raise an error at creation time rather than silently produce a
    # broken table that fails on every subsequent SELECT.
    Scenario: CTAS with duplicate column names raises an error
      Given variable loc for temporary directory ctas_dup
      Given statement template with error .*
        """
        CREATE TABLE ctas_dup_test
        USING PARQUET
        LOCATION {{ loc.sql }}
        AS SELECT t1.id, t2.id
        FROM (SELECT 1 AS id) t1
        JOIN (SELECT 1 AS id) t2 ON t1.id = t2.id
        """
