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
        EXPLAIN
        INSERT INTO distributed_delta_commit
        SELECT id FROM range(0, 400, 1, 4)
        """
      Then query plan matches snapshot

    Scenario: Delta file writing remains parallel on workers
      Given statement
        """
        INSERT INTO distributed_delta_commit
        SELECT id FROM range(0, 400, 1, 4)
        """
      Then data files in location count is 4
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
        EXPLAIN
        INSERT INTO distributed_iceberg_commit
        SELECT id FROM range(0, 400, 1, 4)
        """
      Then query plan matches snapshot

    Scenario: Iceberg file writing remains parallel on workers
      Given statement
        """
        INSERT INTO distributed_iceberg_commit
        SELECT id FROM range(0, 400, 1, 4)
        """
      Then data files in location count is 4
      When query
        """
        SELECT COUNT(*) AS count FROM distributed_iceberg_commit
        """
      Then query result
        | count |
        | 400   |
