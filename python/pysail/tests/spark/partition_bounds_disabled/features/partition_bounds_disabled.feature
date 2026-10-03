Feature: partition bounds are read from the files when the setting is off

  The mirror of the `partition_bounds` package. `execution.partition_bounds_from_listing`
  is left at its default, so none of the shortcuts apply: every query below must give
  the same answer by reading the files, and no `PartitionBounds` node may appear.

  This is what guards the setting being off by default. A change that made the rule
  fire regardless would show up here as a plan without a scan.

  Background:
    Given variable location for temporary directory partition_bounds_off
    Given final statement
      """
      DROP TABLE IF EXISTS partition_bounds_off
      """
    Given statement template
      """
      CREATE TABLE partition_bounds_off (id INT, dt STRING)
      USING parquet PARTITIONED BY (dt) LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO partition_bounds_off VALUES
        (1, '2025-10-01'),
        (2, '2025-10-02'),
        (3, '2025-10-03'),
        (4, '2025-10-04')
      """

  Rule: the answers are the same as with the setting on

    Scenario Outline: bounds over the partition column
      When query
        """
        SELECT <aggregate> AS result FROM partition_bounds_off
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | aggregate         | result     |
        | max(dt)           | 2025-10-04 |
        | min(dt)           | 2025-10-01 |
        | max(DISTINCT dt)  | 2025-10-04 |

    Scenario: a filter driven by a scalar subquery
      When query
        """
        SELECT id, dt FROM partition_bounds_off
        WHERE dt = (SELECT max(dt) FROM partition_bounds_off)
        """
      Then query result
        | id | dt         |
        | 4  | 2025-10-04 |

  Rule: no shortcut appears in the plan

    Scenario: max over the partition column reads the files
      When query
        """
        EXPLAIN EXTENDED SELECT max(dt) FROM partition_bounds_off
        """
      Then query plan matches snapshot

    Scenario: a scalar subquery filter is left for execution to resolve
      When query
        """
        EXPLAIN EXTENDED SELECT count(*) FROM partition_bounds_off
        WHERE dt = (SELECT max(dt) FROM partition_bounds_off)
        """
      Then query plan matches snapshot