Feature: min and max over a partition column are answered from directory listings

  The scan that would normally answer `SELECT max(dt) FROM t` opens every file of
  the table. When the aggregate only reads a partition column, the directory names
  already hold the answer, so the plan is replaced by a `PartitionBounds` node and
  no `DataSourceExec` is left behind.

  Requires `execution.partition_bounds_from_listing`, which is off by default.

  Background:
    Given variable location for temporary directory partition_bounds_flat
    Given final statement
      """
      DROP TABLE IF EXISTS partition_bounds_flat
      """
    Given statement template
      """
      CREATE TABLE partition_bounds_flat (id INT, dt STRING)
      USING parquet PARTITIONED BY (dt) LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO partition_bounds_flat VALUES
        (1, '2025-10-01'),
        (2, '2025-10-02'),
        (3, '2025-10-03'),
        (4, '2025-10-04')
      """

  Rule: the aggregate is resolved without reading any file

    Scenario: max over the partition column
      When query
        """
        EXPLAIN SELECT max(dt) FROM partition_bounds_flat
        """
      Then query plan matches snapshot

    # The physical plan alone cannot tell this rule apart from the folding DataFusion
    # already does from file statistics, since both end in a literal over a placeholder
    # row. The logical plan is where the rewrite is visible.
    Scenario: the aggregate is replaced by a PartitionBounds node
      When query
        """
        EXPLAIN EXTENDED SELECT max(dt) FROM partition_bounds_flat
        """
      Then query plan matches snapshot

    Scenario: the node is absent when the aggregate is not resolvable from directories
      When query
        """
        EXPLAIN EXTENDED SELECT max(id) FROM partition_bounds_flat
        """
      Then query plan matches snapshot

    Scenario: max over the partition column returns the latest partition
      When query
        """
        SELECT max(dt) AS result FROM partition_bounds_flat
        """
      Then query result
        | result     |
        | 2025-10-04 |

    Scenario: min over the partition column returns the earliest partition
      When query
        """
        SELECT min(dt) AS result FROM partition_bounds_flat
        """
      Then query result
        | result     |
        | 2025-10-01 |

    Scenario: min and max in the same aggregate
      When query
        """
        EXPLAIN SELECT min(dt), max(dt) FROM partition_bounds_flat
        """
      Then query plan matches snapshot

    Scenario: DISTINCT does not change the answer of min or max
      When query
        """
        SELECT max(DISTINCT dt) AS result FROM partition_bounds_flat
        """
      Then query result
        | result     |
        | 2025-10-04 |

  Rule: queries the rule deliberately leaves alone still scan the files

    # The scalar subquery is not a literal when the outer scan is planned, so the
    # outer listing cannot be narrowed. Only the inner half becomes free.
    Scenario: a partition filter driven by a scalar subquery still scans every file
      When query
        """
        EXPLAIN SELECT count(*) FROM partition_bounds_flat
        WHERE dt = (SELECT max(dt) FROM partition_bounds_flat)
        """
      Then query plan matches snapshot

    # `rank()` has to see every row, so no listing shortcut exists.
    Scenario: a window function over the partition column still scans every file
      When query
        """
        EXPLAIN SELECT dt FROM (
          SELECT dt, rank() OVER (ORDER BY dt DESC) AS rk FROM partition_bounds_flat
        ) WHERE rk = 1
        """
      Then query plan matches snapshot

    # `id` lives inside the files, not in the directory names.
    Scenario: max over a data column still scans every file
      When query
        """
        EXPLAIN SELECT max(id) FROM partition_bounds_flat
        """
      Then query plan matches snapshot

    Scenario: a range filter on the partition column is not a pinned prefix
      When query
        """
        EXPLAIN SELECT max(dt) FROM partition_bounds_flat WHERE dt > '2025-10-01'
        """
      Then query plan matches snapshot
