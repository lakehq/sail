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
