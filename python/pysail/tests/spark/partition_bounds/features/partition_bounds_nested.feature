Feature: min and max reach a nested partition level when its ancestors are pinned

  A table partitioned by year, month and day only has one directory to list for a
  given level once every level above it is pinned by an equality filter. Asking for
  the three levels in turn therefore yields the latest partition in three listings,
  no matter how many partitions the table holds.

  Note that `SELECT max(year), max(month), max(day)` is a different question: it
  reports the largest value of each column separately, which need not be a partition
  that exists. The rule leaves it alone.

  Requires `execution.partition_bounds_from_listing`, which is off by default.

  Background:
    Given variable location for temporary directory partition_bounds_nested
    Given final statement
      """
      DROP TABLE IF EXISTS partition_bounds_nested
      """
    Given statement template
      """
      CREATE TABLE partition_bounds_nested (id INT, year STRING, month STRING, day STRING)
      USING parquet PARTITIONED BY (year, month, day) LOCATION {{ location.sql }}
      """
    # The latest partition is 2025/10/04. Note that month 12 and day 31 both exist
    # under 2024, so the largest value of each column on its own is 2025/12/31,
    # which is not a partition of this table.
    Given statement
      """
      INSERT INTO partition_bounds_nested VALUES
        (1, '2024', '12', '31'),
        (2, '2025', '03', '07'),
        (3, '2025', '10', '01'),
        (4, '2025', '10', '04')
      """

  Rule: each level is reached once the levels above it are pinned

    Scenario: the leading level needs no filter
      When query
        """
        EXPLAIN SELECT max(year) FROM partition_bounds_nested
        """
      Then query plan matches snapshot

    Scenario Outline: walking down one level at a time
      When query
        """
        SELECT <aggregate> AS result FROM partition_bounds_nested <filter>
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | aggregate  | filter                                | result |
        | max(year)  |                                       | 2025   |
        | max(month) | WHERE year = '2025'                   | 10     |
        | max(day)   | WHERE year = '2025' AND month = '10'  | 04     |
        | min(month) | WHERE year = '2025'                   | 03     |
        | min(day)   | WHERE year = '2025' AND month = '10'  | 01     |

    # The candidate directories at this level hold further directories rather than
    # files, so the check for a partition that actually holds data has to descend.
    Scenario: a middle level with only its parent pinned
      When query
        """
        EXPLAIN SELECT max(month) FROM partition_bounds_nested WHERE year = '2025'
        """
      Then query plan matches snapshot

    Scenario: the deepest level with both ancestors pinned
      When query
        """
        EXPLAIN SELECT max(day) FROM partition_bounds_nested
        WHERE year = '2025' AND month = '10'
        """
      Then query plan matches snapshot

  Rule: a level whose ancestors are not pinned still scans the files

    # Every month of every year would have to be listed to answer this.
    Scenario: a deeper level without any filter still scans every file
      When query
        """
        EXPLAIN SELECT max(day) FROM partition_bounds_nested
        """
      Then query plan matches snapshot

    # `month` is unpinned, so `day` spans more than one directory.
    Scenario: a partially pinned prefix still scans every file
      When query
        """
        EXPLAIN SELECT max(day) FROM partition_bounds_nested WHERE year = '2025'
        """
      Then query plan matches snapshot

    Scenario: the largest value of each level separately is not a partition
      When query
        """
        SELECT max(year) AS y, max(month) AS m, max(day) AS d FROM partition_bounds_nested
        """
      Then query result
        | y    | m  | d  |
        | 2025 | 12 | 31 |
