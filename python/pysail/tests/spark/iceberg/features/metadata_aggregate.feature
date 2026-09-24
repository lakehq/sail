Feature: Iceberg metadata aggregate results

  Rule: Aggregate results remain correct across metadata optimization boundaries
    Background:
      Given variable location for temporary directory iceberg_metadata_aggregates
      Given final statement
        """
        DROP TABLE IF EXISTS metadata_agg
        """
      Given statement template
        """
        CREATE TABLE metadata_agg (
          id INT, a INT, b INT, z INT, text STRING, payload STRUCT<value: INT>
        ) USING iceberg LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO metadata_agg VALUES
          (2, 20, NULL, NULL, '2', named_struct('value', 2)),
          (3, NULL, 30, NULL, 'invalid', named_struct('value', CAST(NULL AS INT)))
        """
      Given statement
        """
        INSERT INTO metadata_agg VALUES (10, 100, 100, NULL, '10', named_struct('value', 10))
        """

    Scenario: Counts distinguish rows nonnull values and allnull values
      When query
        """
        SELECT COUNT(*) AS x, COUNT(a) AS y, COUNT(z) AS z FROM metadata_agg
        """
      Then query result
        | x | y | z |
        | 3 | 2 | 0 |
      When query
        """
        EXPLAIN SELECT COUNT(*) AS x, COUNT(a) AS y, COUNT(z) AS z FROM metadata_agg
        """
      Then query plan matches snapshot

    Scenario: Numeric and allnull extrema preserve their values
      When query
        """
        SELECT MIN(id) AS x, MAX(id) AS y, MIN(z) AS z FROM metadata_agg
        """
      Then query result
        | x | y | z |
        | 2 | 10 | NULL |
      When query
        """
        EXPLAIN SELECT MIN(id) AS x, MAX(id) AS y, MIN(z) AS z FROM metadata_agg
        """
      Then query plan matches snapshot

    Scenario: Widening casts preserve exact aggregate statistics
      When query
        """
        SELECT MIN(CAST(id AS BIGINT)) AS x, MAX(CAST(id AS BIGINT)) AS y, COUNT(CAST(a AS BIGINT)) AS z FROM metadata_agg
        """
      Then query result
        | x | y | z |
        | 2 | 10 | 2 |
      When query
        """
        EXPLAIN SELECT MIN(CAST(id AS BIGINT)) AS x, MAX(CAST(id AS BIGINT)) AS y, COUNT(CAST(a AS BIGINT)) AS z FROM metadata_agg
        """
      Then query plan matches snapshot

    Scenario: Literals and distinct literals use snapshot cardinality
      When query
        """
        SELECT COUNT(42) AS x, COUNT(DISTINCT 1) AS y, COUNT(NULL) AS z FROM metadata_agg
        """
      Then query result
        | x | y | z |
        | 3 | 1 | 0 |
      When query
        """
        EXPLAIN SELECT COUNT(42) AS x, COUNT(DISTINCT 1) AS y, COUNT(NULL) AS z FROM metadata_agg
        """
      Then query plan matches snapshot

    Scenario: Metadata aggregates and row aggregates return consistent results
      When query
        """
        SELECT COUNT(*) AS x, MIN(id) AS y, SUM(a) AS z FROM metadata_agg
        """
      Then query result
        | x | y | z |
        | 3 | 2 | 120 |
      When query
        """
        EXPLAIN SELECT COUNT(*) AS x, MIN(id) AS y, SUM(a) AS z FROM metadata_agg
        """
      Then query plan matches snapshot

    Scenario: Sum and distinct counts preserve their results
      When query
        """
        SELECT SUM(id) AS x, COUNT(DISTINCT id) AS y, COUNT(DISTINCT a) AS z FROM metadata_agg
        """
      Then query result
        | x | y | z |
        | 15 | 3 | 2 |
      When query
        """
        EXPLAIN SELECT SUM(id) AS x, COUNT(DISTINCT id) AS y, COUNT(DISTINCT a) AS z FROM metadata_agg
        """
      Then query plan matches snapshot

    Scenario: Multiple nullable arguments require joint row evaluation
      When query
        """
        SELECT COUNT(id, a) AS x, COUNT(a, b) AS y, COUNT(id, z) AS z FROM metadata_agg
        """
      Then query result
        | x | y | z |
        | 2 | 1 | 0 |
      When query
        """
        EXPLAIN SELECT COUNT(id, a) AS x, COUNT(a, b) AS y, COUNT(id, z) AS z FROM metadata_agg
        """
      Then query plan matches snapshot

    Scenario: Aggregate filters retain their own row selection
      When query
        """
        SELECT COUNT(*) FILTER (WHERE id > 2) AS x, MIN(id) FILTER (WHERE id > 2) AS y, COUNT(*) AS z FROM metadata_agg
        """
      Then query result
        | x | y | z |
        | 2 | 3 | 3 |
      When query
        """
        EXPLAIN SELECT COUNT(*) FILTER (WHERE id > 2) AS x, MIN(id) FILTER (WHERE id > 2) AS y, COUNT(*) AS z FROM metadata_agg
        """
      Then query plan matches snapshot

    Scenario: Nonmonotonic casts preserve lexical extrema
      When query
        """
        SELECT MIN(CAST(id AS STRING)) AS x, MAX(CAST(id AS STRING)) AS y, COUNT(*) AS z FROM metadata_agg
        """
      Then query result
        | x | y | z |
        | 10 | 3 | 3 |
      When query
        """
        EXPLAIN SELECT MIN(CAST(id AS STRING)) AS x, MAX(CAST(id AS STRING)) AS y, COUNT(*) AS z FROM metadata_agg
        """
      Then query plan matches snapshot

    Scenario: Fallible casts do not inherit source null counts
      When query
        """
        SELECT COUNT(TRY_CAST(text AS INT)) AS x, MIN(TRY_CAST(text AS INT)) AS y, COUNT(*) AS z FROM metadata_agg
        """
      Then query result
        | x | y | z |
        | 2 | 2 | 3 |
      When query
        """
        EXPLAIN SELECT COUNT(TRY_CAST(text AS INT)) AS x, MIN(TRY_CAST(text AS INT)) AS y, COUNT(*) AS z FROM metadata_agg
        """
      Then query plan matches snapshot

    Scenario: Allmatch file predicates permit metadata aggregation
      When query
        """
        SELECT COUNT(*) AS x, MIN(id) AS y, MAX(id) AS z FROM metadata_agg WHERE id >= 2
        """
      Then query result
        | x | y | z |
        | 3 | 2 | 10 |
      When query
        """
        EXPLAIN SELECT COUNT(*) AS x, MIN(id) AS y, MAX(id) AS z FROM metadata_agg WHERE id >= 2
        """
      Then query plan matches snapshot

    Scenario: Residual file predicates prevent metadata aggregation
      When query
        """
        SELECT COUNT(*) AS x, MIN(id) AS y, MAX(id) AS z FROM metadata_agg WHERE id > 2
        """
      Then query result
        | x | y | z |
        | 2 | 3 | 10 |
      When query
        """
        EXPLAIN SELECT COUNT(*) AS x, MIN(id) AS y, MAX(id) AS z FROM metadata_agg WHERE id > 2
        """
      Then query plan matches snapshot

    Scenario: Unsupported predicates retain logical row filtering
      When query
        """
        SELECT COUNT(*) AS x, MIN(id) AS y, MAX(id) AS z FROM metadata_agg WHERE id % 2 = 0
        """
      Then query result
        | x | y | z |
        | 2 | 2 | 10 |
      When query
        """
        EXPLAIN SELECT COUNT(*) AS x, MIN(id) AS y, MAX(id) AS z FROM metadata_agg WHERE id % 2 = 0
        """
      Then query plan matches snapshot

    Scenario: An empty file selection produces one aggregate row
      When query
        """
        SELECT COUNT(*) AS x, MIN(id) AS y, MAX(id) AS z FROM metadata_agg WHERE id < 0
        """
      Then query result
        | x | y | z |
        | 0 | NULL | NULL |
      When query
        """
        EXPLAIN SELECT COUNT(*) AS x, MIN(id) AS y, MAX(id) AS z FROM metadata_agg WHERE id < 0
        """
      Then query plan matches snapshot

    Scenario: Nested field aggregates preserve null semantics
      When query
        """
        SELECT COUNT(*) AS x, MIN(payload.value) AS y, MAX(payload.value) AS z FROM metadata_agg
        """
      Then query result
        | x | y | z |
        | 3 | 2 | 10 |
      When query
        """
        EXPLAIN SELECT COUNT(*) AS x, MIN(payload.value) AS y, MAX(payload.value) AS z FROM metadata_agg
        """
      Then query plan matches snapshot

    Scenario: Aggregate expressions resolve aliases in subquery projections
      When query
        """
        SELECT COUNT(*) AS x, MIN(renamed) AS y, SUM(amount) AS z FROM (SELECT CAST(id AS BIGINT) AS renamed, a AS amount FROM metadata_agg) q
        """
      Then query result
        | x | y | z |
        | 3 | 2 | 120 |
      When query
        """
        EXPLAIN SELECT COUNT(*) AS x, MIN(renamed) AS y, SUM(amount) AS z FROM (SELECT CAST(id AS BIGINT) AS renamed, a AS amount FROM metadata_agg) q
        """
      Then query plan matches snapshot

    Scenario: Input limits preserve selected row cardinality
      When query
        """
        SELECT COUNT(*) AS x, MIN(id) AS y, MAX(id) AS z FROM (SELECT id FROM metadata_agg ORDER BY id LIMIT 2) q
        """
      Then query result
        | x | y | z |
        | 2 | 2 | 3 |
      When query
        """
        EXPLAIN SELECT COUNT(*) AS x, MIN(id) AS y, MAX(id) AS z FROM (SELECT id FROM metadata_agg ORDER BY id LIMIT 2) q
        """
      Then query plan matches snapshot

    Scenario: Scalar aggregate subqueries return independent results
      When query
        """
        SELECT (SELECT COUNT(*) FROM metadata_agg) AS x, (SELECT MIN(id) FROM metadata_agg) AS y, (SELECT SUM(a) FROM metadata_agg) AS z
        """
      Then query result
        | x | y | z |
        | 3 | 2 | 120 |
      When query
        """
        EXPLAIN SELECT (SELECT COUNT(*) FROM metadata_agg) AS x, (SELECT MIN(id) FROM metadata_agg) AS y, (SELECT SUM(a) FROM metadata_agg) AS z
        """
      Then query plan matches snapshot

    Scenario: Grouped aggregates retain group cardinality
      When query
        """
        SELECT id % 2 AS k, COUNT(*) AS n, SUM(a) AS total
        FROM metadata_agg GROUP BY id % 2 ORDER BY k
        """
      Then query result ordered
        | k | n | total |
        | 0 | 2 | 120   |
        | 1 | 1 | NULL  |
      When query
        """
        EXPLAIN SELECT id % 2 AS k, COUNT(*) AS n, SUM(a) AS total
        FROM metadata_agg GROUP BY id % 2 ORDER BY k
        """
      Then query plan matches snapshot

  Rule: Tables without a snapshot still produce typed aggregate results
    Background:
      Given variable location for temporary directory iceberg_empty_aggregates
      Given final statement
        """
        DROP TABLE IF EXISTS metadata_agg_empty
        """
      Given statement template
        """
        CREATE TABLE metadata_agg_empty (id INT) USING iceberg LOCATION {{ location.uri }}
        """

    Scenario: Empty snapshots preserve counts and nullable extrema
      When query
        """
        SELECT COUNT(*) AS rows, COUNT(DISTINCT 1) AS distinct_literal,
          MIN(id) AS minimum, MAX(id) AS maximum, SUM(id) AS total
        FROM metadata_agg_empty
        """
      Then query result
        | rows | distinct_literal | minimum | maximum | total |
        | 0    | 0                | NULL    | NULL    | NULL  |
      Then query schema
        """
        root
         |-- rows: long (nullable = false)
         |-- distinct_literal: long (nullable = false)
         |-- minimum: integer (nullable = true)
         |-- maximum: integer (nullable = true)
         |-- total: long (nullable = true)
        """
      When query
        """
        EXPLAIN SELECT COUNT(*) AS rows, COUNT(DISTINCT 1) AS distinct_literal,
          MIN(id) AS minimum, MAX(id) AS maximum, SUM(id) AS total
        FROM metadata_agg_empty
        """
      Then query plan matches snapshot

  Rule: Explicit lazy reads preserve aggregate results
    Background:
      Given variable location for temporary directory iceberg_lazy_aggregates
      Given final statement
        """
        DROP TABLE IF EXISTS metadata_agg_lazy
        """
      Given statement template
        """
        CREATE TABLE metadata_agg_lazy (id INT, a INT) USING iceberg LOCATION {{ location.uri }}
        OPTIONS (metadataAsDataRead 'true')
        """
      Given statement
        """
        INSERT INTO metadata_agg_lazy VALUES (2, 20), (3, NULL), (10, 100)
        """

    Scenario Outline: Lazy reads return correct aggregate values
      When query
        """
        SELECT <expression> AS value FROM metadata_agg_lazy
        """
      Then query result
        | value      |
        | <expected> |
      When query
        """
        EXPLAIN SELECT <expression> AS value FROM metadata_agg_lazy
        """
      Then query plan matches snapshot

      Examples:
        | expression | expected |
        | COUNT(*)   | 3        |
        | MIN(id)    | 2        |
        | SUM(a)     | 120      |
