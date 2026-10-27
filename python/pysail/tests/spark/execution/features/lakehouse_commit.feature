Feature: Lakehouse commits in distributed execution

  Rule: Delta Lake commit execution
    Background:
      Given variable location for temporary directory distributed_delta_commit
      Given final statement
        """
        DROP TABLE IF EXISTS distributed_delta_commit
        """
      Given statement template
        """
        CREATE TABLE distributed_delta_commit (id BIGINT)
        USING delta
        LOCATION {{ location.sql }}
        """

    Scenario: Delta commit runs on the driver after parallel file writing
      When query
        """
        EXPLAIN CODEGEN
        INSERT INTO distributed_delta_commit
        SELECT id FROM range(0, 400, 1, 4)
        """
      Then query plan matches snapshot

    Scenario: Delta file writing remains parallel on workers
      When query
        """
        INSERT INTO distributed_delta_commit
        SELECT id FROM range(0, 400, 1, 4)
        """
      Then query result collected
        | count |
        | 400   |
      Then data files in location count is 4
      Then delta log latest commit info contains
        | path                           | value |
        | operationMetrics.numFiles      | 4     |
        | operationMetrics.numOutputRows | 400   |
      When query
        """
        SELECT COUNT(*) AS count FROM distributed_delta_commit
        """
      Then query result
        | count |
        | 400   |

  Rule: Iceberg commit execution
    Background:
      Given variable location for temporary directory distributed_iceberg_commit
      Given final statement
        """
        DROP TABLE IF EXISTS distributed_iceberg_commit
        """
      Given statement template
        """
        CREATE TABLE distributed_iceberg_commit (id BIGINT)
        USING iceberg
        LOCATION {{ location.uri }}
        """

    Scenario: Iceberg commit runs on the driver after parallel file writing
      When query
        """
        EXPLAIN CODEGEN
        INSERT INTO distributed_iceberg_commit
        SELECT id FROM range(0, 400, 1, 4)
        """
      Then query plan matches snapshot

    Scenario: Iceberg file writing remains parallel on workers
      When query
        """
        INSERT INTO distributed_iceberg_commit
        SELECT id FROM range(0, 400, 1, 4)
        """
      Then query result collected
        | count |
        | 400   |
      Then data files in location count is 4
      When query
        """
        SELECT COUNT(*) AS count FROM distributed_iceberg_commit
        """
      Then query result
        | count |
        | 400   |

    Scenario Outline: Iceberg COW rewrites run on workers and commit on the driver
      Given statement
        """
        INSERT INTO distributed_iceberg_commit SELECT id FROM range(0, 400, 1, 4)
        """
      When query
        """
        EXPLAIN CODEGEN <statement>
        """
      Then query plan matches snapshot
      Given statement
        """
        <statement>
        """
      When query
        """
        SELECT COUNT(*) AS count,
               SUM(CASE WHEN id >= 1000 THEN 1 ELSE 0 END) AS changed
        FROM distributed_iceberg_commit
        """
      Then query result
        | count   | changed   |
        | <count> | <changed> |

      Examples:
        | statement                                                                                                                                             | count | changed |
        | DELETE FROM distributed_iceberg_commit WHERE id < 10                                                                                                  | 390   | 0       |
        | UPDATE distributed_iceberg_commit SET id = id + 1000 WHERE id < 10                                                                                    | 400   | 10      |
        | MERGE INTO distributed_iceberg_commit t USING (SELECT id FROM range(10)) s ON t.id = s.id AND t.id < 10 WHEN MATCHED THEN UPDATE SET id = t.id + 1000 | 400   | 10      |

    Scenario: Iceberg metadata DELETE commits without scanning data files
      Given statement
        """
        INSERT INTO distributed_iceberg_commit SELECT id FROM range(0, 400, 1, 4)
        """
      When query
        """
        EXPLAIN CODEGEN DELETE FROM distributed_iceberg_commit WHERE id >= 0
        """
      Then query plan matches snapshot
      Given statement
        """
        DELETE FROM distributed_iceberg_commit WHERE id >= 0
        """
      When query
        """
        SELECT COUNT(*) AS count FROM distributed_iceberg_commit
        """
      Then query result
        | count |
        | 0     |
