Feature: Iceberg Query Optimization

  Rule: Verify EXPLAIN output for partition pruning
    Background:
      Given variable location for temporary directory iceberg_explain_prune
      Given final statement
        """
        DROP TABLE IF EXISTS prune_table
        """

    Scenario: EXPLAIN shows partition pruning for identity partitioning
      Given statement template
        """
        CREATE TABLE prune_table (id INT, category INT, value STRING)
        USING iceberg
        PARTITIONED BY (category)
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO prune_table VALUES
          (1, 1, 'cat1'), (2, 1, 'cat1'),
          (3, 2, 'cat2'), (4, 2, 'cat2'),
          (5, 3, 'cat3'), (6, 3, 'cat3')
        """
      When query
        """
        EXPLAIN SELECT * FROM prune_table WHERE category = 1
        """
      Then query plan matches snapshot

    Scenario: EXPLAIN for unpartitioned table scan
      Given statement template
        """
        CREATE TABLE scan_table (id INT, name STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO scan_table VALUES (1, 'alice'), (2, 'bob')
        """
      When query
        """
        EXPLAIN SELECT * FROM scan_table WHERE id > 0
        """
      Then query plan matches snapshot

  Rule: Verify projection pushdown
    Background:
      Given variable location for temporary directory iceberg_explain_projection
      Given final statement
        """
        DROP TABLE IF EXISTS proj_table
        """

    Scenario: EXPLAIN shows column projection
      Given statement template
        """
        CREATE TABLE proj_table (id INT, name STRING, age INT, dept STRING)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO proj_table VALUES 
          (1, 'alice', 25, 'eng'),
          (2, 'bob', 30, 'sales')
        """
      When query
        """
        EXPLAIN SELECT name, age FROM proj_table WHERE id < 10
        """
      Then query plan matches snapshot

  Rule: Verify aggregation pushdown
    Background:
      Given variable location for temporary directory iceberg_explain_agg
      Given final statement
        """
        DROP TABLE IF EXISTS agg_table
        """

    Scenario: EXPLAIN for COUNT aggregation
      Given statement template
        """
        CREATE TABLE agg_table (id INT, category INT, value INT)
        USING iceberg
        PARTITIONED BY (category)
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO agg_table VALUES
          (1, 1, 100), (2, 1, 200),
          (3, 2, 300), (4, 2, 400)
        """
      When query
        """
        EXPLAIN SELECT category, COUNT(*) as cnt 
        FROM agg_table 
        GROUP BY category
        """
      Then query plan matches snapshot

  Rule: Verify filter pushdown for complex predicates
    Background:
      Given variable location for temporary directory iceberg_explain_filter
      Given final statement
        """
        DROP TABLE IF EXISTS filter_table
        """

    Scenario: EXPLAIN for AND/OR predicates
      Given statement template
        """
        CREATE TABLE filter_table (id INT, status STRING, value INT)
        USING iceberg
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO filter_table VALUES
          (1, 'active', 100),
          (2, 'inactive', 200),
          (3, 'active', 300)
        """
      When query
        """
        EXPLAIN SELECT * FROM filter_table 
        WHERE (status = 'active' AND value > 150) OR id = 2
        """
      Then query plan matches snapshot

  Rule: Verify sort optimization
    Background:
      Given variable location for temporary directory iceberg_explain_sort
      Given final statement
        """
        DROP TABLE IF EXISTS sort_table
        """

    Scenario: EXPLAIN for ORDER BY on partitioned table
      Given statement template
        """
        CREATE TABLE sort_table (id INT, partition_col INT, name STRING)
        USING iceberg
        PARTITIONED BY (partition_col)
        LOCATION {{ location.uri }}
        """
      Given statement
        """
        INSERT INTO sort_table VALUES
          (1, 1, 'a'), (2, 1, 'b'),
          (3, 2, 'c'), (4, 2, 'd')
        """
      When query
        """
        EXPLAIN SELECT * FROM sort_table ORDER BY partition_col, id
        """
      Then query plan matches snapshot

  Rule: Verify join optimization
    Background:
      Given variable left_location for temporary directory iceberg_explain_join_left
      Given variable right_location for temporary directory iceberg_explain_join_right
      Given final statement
        """
        DROP TABLE IF EXISTS join_left
        """
      Given final statement
        """
        DROP TABLE IF EXISTS join_right
        """

    Scenario: EXPLAIN for JOIN between two Iceberg tables
      Given statement template
        """
        CREATE TABLE join_left (id INT, name STRING)
        USING iceberg
        LOCATION {{ left_location.uri }}
        """
      Given statement template
        """
        CREATE TABLE join_right (id INT, value INT)
        USING iceberg
        LOCATION {{ right_location.uri }}
        """
      Given statement
        """
        INSERT INTO join_left VALUES (1, 'alice'), (2, 'bob')
        """
      Given statement
        """
        INSERT INTO join_right VALUES (1, 100), (2, 200)
        """
      When query
        """
        EXPLAIN 
        SELECT l.name, r.value 
        FROM join_left l 
        JOIN join_right r ON l.id = r.id
        WHERE l.id > 0
        """
      Then query plan matches snapshot
