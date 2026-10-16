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

  Rule: a filter driven by a partition bound narrows the listing to one directory

    This is how the latest partition is usually asked for. The subquery is resolved
    while planning and replaced by the literal it produced, so the scan lists only the
    directory of that partition instead of every one of them.

    Scenario: the subquery is inlined and only its partition is listed
      When query
        """
        EXPLAIN SELECT count(*) FROM partition_bounds_flat
        WHERE dt = (SELECT max(dt) FROM partition_bounds_flat)
        """
      Then query plan matches snapshot

    # The logical plan below still shows the subquery. The inlining happens while
    # planning physically, after the plan `EXPLAIN` prints, so the physical plan is
    # the only place the effect is visible. Reading the two together is the point:
    # a filter on a subquery above, a single file below.
    Scenario: the logical plan keeps the subquery while the physical plan reads one file
      When query
        """
        EXPLAIN EXTENDED SELECT count(*) FROM partition_bounds_flat
        WHERE dt = (SELECT max(dt) FROM partition_bounds_flat)
        """
      Then query plan matches snapshot

    Scenario: inlining the subquery keeps the answer
      When query
        """
        SELECT count(*) AS result FROM partition_bounds_flat
        WHERE dt = (SELECT max(dt) FROM partition_bounds_flat)
        """
      Then query result
        | result |
        | 1      |

    Scenario: the rows of the latest partition are returned, not just their count
      When query
        """
        SELECT id, dt FROM partition_bounds_flat
        WHERE dt = (SELECT max(dt) FROM partition_bounds_flat)
        """
      Then query result
        | id | dt         |
        | 4  | 2025-10-04 |

  Rule: an aggregate the rule only partly recognises is left whole

    The rewrite replaces the entire aggregate, so it may only fire when every one of
    its expressions is a bound over the same partition column. These are the shapes
    where that holds for some expressions but not all, and each must be answered by
    reading the files rather than by a node with the wrong number of outputs.

    Scenario: a bound alongside an aggregate that is not one
      When query
        """
        SELECT max(dt) AS bound, count(*) AS rows FROM partition_bounds_flat
        """
      Then query result
        | bound      | rows |
        | 2025-10-04 | 4    |

    Scenario: a bound alongside a sum
      When query
        """
        SELECT max(dt) AS bound, sum(id) AS total FROM partition_bounds_flat
        """
      Then query result
        | bound      | total |
        | 2025-10-04 | 10    |

    Scenario: the same bound asked for twice
      When query
        """
        SELECT max(dt) AS a, max(dt) AS b FROM partition_bounds_flat
        """
      Then query result
        | a          | b          |
        | 2025-10-04 | 2025-10-04 |

    Scenario: a bound over a partition column and one over a data column
      When query
        """
        SELECT max(dt) AS bound, max(id) AS top FROM partition_bounds_flat
        """
      Then query result
        | bound      | top |
        | 2025-10-04 | 4   |

  Rule: shapes the inlining does not speed up still answer correctly

    These are documented as gaining nothing, not as being wrong. Each still has to
    return what a full scan would.

    Scenario: an extra condition on a data column alongside the subquery
      When query
        """
        SELECT id FROM partition_bounds_flat
        WHERE dt = (SELECT max(dt) FROM partition_bounds_flat) AND id > 1
        """
      Then query result
        | id |
        | 4  |

    Scenario: an extra condition that excludes the latest partition
      When query
        """
        SELECT count(*) AS result FROM partition_bounds_flat
        WHERE dt = (SELECT max(dt) FROM partition_bounds_flat) AND id > 100
        """
      Then query result
        | result |
        | 0      |

    Scenario: the subquery filter behind a join
      Given statement
        """
        CREATE OR REPLACE TEMP VIEW wanted AS SELECT * FROM VALUES (4), (99) AS t(id)
        """
      When query
        """
        SELECT a.id, a.dt FROM partition_bounds_flat a JOIN wanted w ON a.id = w.id
        WHERE a.dt = (SELECT max(dt) FROM partition_bounds_flat)
        """
      Then query result
        | id | dt         |
        | 4  | 2025-10-04 |

    Scenario: the subquery compared with a column other than the partition column
      When query
        """
        SELECT count(*) AS result FROM partition_bounds_flat
        WHERE CAST(id AS STRING) = (SELECT max(dt) FROM partition_bounds_flat)
        """
      Then query result
        | result |
        | 0      |

    # Two bounds in one predicate: both are inlined, but an `OR` is not something the
    # scan can turn into a prefix, so it reads what it read before.
    Scenario: two subqueries joined by OR
      When query
        """
        SELECT count(*) AS result FROM partition_bounds_flat
        WHERE dt = (SELECT max(dt) FROM partition_bounds_flat)
           OR dt = (SELECT min(dt) FROM partition_bounds_flat)
        """
      Then query result
        | result |
        | 2      |

    Scenario: a subquery under an inequality
      When query
        """
        SELECT count(*) AS result FROM partition_bounds_flat
        WHERE dt <> (SELECT max(dt) FROM partition_bounds_flat)
        """
      Then query result
        | result |
        | 3      |

    Scenario: a subquery under a comparison
      When query
        """
        SELECT count(*) AS result FROM partition_bounds_flat
        WHERE dt >= (SELECT max(dt) FROM partition_bounds_flat)
        """
      Then query result
        | result |
        | 1      |

  Rule: queries the rule deliberately leaves alone still scan the files

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
