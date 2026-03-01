Feature: Bucketed Parquet Writing

  Rule: Basic bucketed write and read
    Scenario: single-column bucketing write and read back
      Given variable location for temporary directory test_bucket
      Given final statement
        """
        DROP TABLE IF EXISTS test_bucket
        """
      Given statement template
        """
        CREATE TABLE test_bucket USING parquet
        LOCATION {{ location.sql }}
        CLUSTERED BY (id) INTO 4 BUCKETS
        AS SELECT * FROM VALUES (1,'a'), (2,'b'), (3,'c'), (4,'d') AS t(id, name)
        """
      When query
        """
        SELECT count(*) as cnt FROM test_bucket
        """
      Then query result
        | cnt |
        | 4   |

    Scenario: single-column bucketing preserves all data
      Given variable location for temporary directory test_bucket_sum
      Given final statement
        """
        DROP TABLE IF EXISTS test_bucket_sum
        """
      Given statement template
        """
        CREATE TABLE test_bucket_sum USING parquet
        LOCATION {{ location.sql }}
        CLUSTERED BY (id) INTO 8 BUCKETS
        AS SELECT * FROM VALUES (1,10), (2,20), (3,30), (4,40), (5,50) AS t(id, amount)
        """
      When query
        """
        SELECT sum(amount) as total FROM test_bucket_sum
        """
      Then query result
        | total |
        | 150   |

  Rule: Multi-column bucketing
    Scenario: multi-column bucket preserves all data
      Given variable location for temporary directory test_multi_bucket
      Given final statement
        """
        DROP TABLE IF EXISTS test_multi_bucket
        """
      Given statement template
        """
        CREATE TABLE test_multi_bucket USING parquet
        LOCATION {{ location.sql }}
        CLUSTERED BY (category, region) INTO 8 BUCKETS
        AS SELECT * FROM VALUES
          ('A','US',100), ('B','EU',200), ('A','EU',150), ('B','US',50)
        AS t(category, region, amount)
        """
      When query
        """
        SELECT sum(amount) as total FROM test_multi_bucket
        """
      Then query result
        | total |
        | 500   |

  Rule: Edge cases
    Scenario: bucketing with more buckets than rows
      Given variable location for temporary directory test_bucket_sparse
      Given final statement
        """
        DROP TABLE IF EXISTS test_bucket_sparse
        """
      Given statement template
        """
        CREATE TABLE test_bucket_sparse USING parquet
        LOCATION {{ location.sql }}
        CLUSTERED BY (id) INTO 16 BUCKETS
        AS SELECT * FROM VALUES (1,'x'), (2,'y') AS t(id, name)
        """
      When query
        """
        SELECT count(*) as cnt FROM test_bucket_sparse
        """
      Then query result
        | cnt |
        | 2   |

    Scenario: bucketing with single bucket
      Given variable location for temporary directory test_bucket_one
      Given final statement
        """
        DROP TABLE IF EXISTS test_bucket_one
        """
      Given statement template
        """
        CREATE TABLE test_bucket_one USING parquet
        LOCATION {{ location.sql }}
        CLUSTERED BY (id) INTO 1 BUCKETS
        AS SELECT * FROM VALUES (1,'a'), (2,'b'), (3,'c') AS t(id, name)
        """
      When query
        """
        SELECT count(*) as cnt FROM test_bucket_one
        """
      Then query result
        | cnt |
        | 3   |

  Rule: Bucketed join
    @sail-only
    Scenario: join two bucketed tables on bucket column
      Given variable left_loc for temporary directory left_bucket
      Given variable right_loc for temporary directory right_bucket
      Given final statement
        """
        DROP TABLE IF EXISTS left_bucket
        """
      Given final statement
        """
        DROP TABLE IF EXISTS right_bucket
        """
      Given statement template
        """
        CREATE TABLE left_bucket USING parquet
        LOCATION {{ left_loc.sql }}
        CLUSTERED BY (id) INTO 4 BUCKETS
        AS SELECT * FROM VALUES (1,'Alice'), (2,'Bob'), (3,'Carol'), (4,'Dave') AS t(id, name)
        """
      Given statement template
        """
        CREATE TABLE right_bucket USING parquet
        LOCATION {{ right_loc.sql }}
        CLUSTERED BY (id) INTO 4 BUCKETS
        AS SELECT * FROM VALUES (1,100), (2,200), (3,300), (4,400) AS t(id, amount)
        """
      When query
        """
        SELECT l.name, r.amount
        FROM left_bucket l JOIN right_bucket r ON l.id = r.id
        ORDER BY l.name
        """
      Then query result ordered
        | name  | amount |
        | Alice | 100    |
        | Bob   | 200    |
        | Carol | 300    |
        | Dave  | 400    |

    @sail-only
    Scenario: join bucketed tables with aggregation
      Given variable left_loc for temporary directory left_bucket_agg
      Given variable right_loc for temporary directory right_bucket_agg
      Given final statement
        """
        DROP TABLE IF EXISTS left_bucket_agg
        """
      Given final statement
        """
        DROP TABLE IF EXISTS right_bucket_agg
        """
      Given statement template
        """
        CREATE TABLE left_bucket_agg USING parquet
        LOCATION {{ left_loc.sql }}
        CLUSTERED BY (key) INTO 4 BUCKETS
        AS SELECT * FROM VALUES (1,10), (2,20), (1,30), (2,40) AS t(key, val)
        """
      Given statement template
        """
        CREATE TABLE right_bucket_agg USING parquet
        LOCATION {{ right_loc.sql }}
        CLUSTERED BY (key) INTO 4 BUCKETS
        AS SELECT * FROM VALUES (1,100), (2,200) AS t(key, multiplier)
        """
      When query
        """
        SELECT l.key, sum(l.val * r.multiplier) as total
        FROM left_bucket_agg l JOIN right_bucket_agg r ON l.key = r.key
        GROUP BY l.key
        ORDER BY l.key
        """
      Then query result ordered
        | key | total |
        | 1   | 4000  |
        | 2   | 12000 |

  Rule: Colocated aggregation
    Scenario: GROUP BY on bucket columns avoids repartition
      Given variable location for temporary directory test_bucket_colocated
      Given final statement
        """
        DROP TABLE IF EXISTS test_bucket_colocated
        """
      Given statement template
        """
        CREATE TABLE test_bucket_colocated USING parquet
        LOCATION {{ location.sql }}
        CLUSTERED BY (key) INTO 4 BUCKETS
        AS SELECT * FROM VALUES (1,10), (2,20), (1,30), (2,40), (3,50) AS t(key, val)
        """
      When query
        """
        SELECT key, sum(val) as total FROM test_bucket_colocated GROUP BY key ORDER BY key
        """
      Then query result ordered
        | key | total |
        | 1   | 40    |
        | 2   | 60    |
        | 3   | 50    |

  Rule: Bucket pruning
    @sail-only
    Scenario: WHERE on bucket column returns correct single row
      Given variable location for temporary directory test_bucket_prune
      Given final statement
        """
        DROP TABLE IF EXISTS test_bucket_prune
        """
      Given statement template
        """
        CREATE TABLE test_bucket_prune USING parquet
        LOCATION {{ location.sql }}
        CLUSTERED BY (id) INTO 4 BUCKETS
        AS SELECT * FROM VALUES (1,'Alice'), (2,'Bob'), (3,'Carol'), (4,'Dave') AS t(id, name)
        """
      When query
        """
        SELECT name FROM test_bucket_prune WHERE id = 3
        """
      Then query result
        | name  |
        | Carol |

    @sail-only
    Scenario: WHERE IN on bucket column returns correct subset
      Given variable location for temporary directory test_bucket_prune_in
      Given final statement
        """
        DROP TABLE IF EXISTS test_bucket_prune_in
        """
      Given statement template
        """
        CREATE TABLE test_bucket_prune_in USING parquet
        LOCATION {{ location.sql }}
        CLUSTERED BY (id) INTO 8 BUCKETS
        AS SELECT * FROM VALUES (1,'a'), (2,'b'), (3,'c'), (4,'d'), (5,'e') AS t(id, name)
        """
      When query
        """
        SELECT id, name FROM test_bucket_prune_in WHERE id IN (1, 3, 5) ORDER BY id
        """
      Then query result ordered
        | id | name |
        | 1  | a    |
        | 3  | c    |
        | 5  | e    |

    @sail-only
    Scenario: WHERE on multiple bucket columns prunes correctly
      Given variable location for temporary directory test_multi_prune
      Given final statement
        """
        DROP TABLE IF EXISTS test_multi_prune
        """
      Given statement template
        """
        CREATE TABLE test_multi_prune USING parquet
        LOCATION {{ location.sql }}
        CLUSTERED BY (category, region) INTO 8 BUCKETS
        AS SELECT * FROM VALUES
          ('A','US',100), ('B','EU',200), ('A','EU',150), ('B','US',50)
        AS t(category, region, amount)
        """
      When query
        """
        SELECT amount FROM test_multi_prune WHERE category = 'A' AND region = 'EU'
        """
      Then query result
        | amount |
        | 150    |

    @sail-only
    Scenario: WHERE IN on multiple bucket columns prunes correctly
      Given variable location for temporary directory test_multi_prune_in
      Given final statement
        """
        DROP TABLE IF EXISTS test_multi_prune_in
        """
      Given statement template
        """
        CREATE TABLE test_multi_prune_in USING parquet
        LOCATION {{ location.sql }}
        CLUSTERED BY (category, region) INTO 8 BUCKETS
        AS SELECT * FROM VALUES
          ('A','US',100), ('B','EU',200), ('A','EU',150), ('B','US',50)
        AS t(category, region, amount)
        """
      When query
        """
        SELECT category, region, amount FROM test_multi_prune_in
        WHERE category IN ('A', 'B') AND region = 'US'
        ORDER BY category
        """
      Then query result ordered
        | category | region | amount |
        | A        | US     | 100    |
        | B        | US     | 50     |
