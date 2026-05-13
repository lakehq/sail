@sail-only
Feature: Explicit repartition end-to-end behavior

  Rule: Round-robin repartition via DataFrame API

    Scenario: Repartition assigns rows to output partitions in round-robin order
      Given dataframe source is query
        """
        SELECT id FROM range(0, 6, 1, 1)
        """
      And dataframe repartitioned is repartition of source to 2 partitions
      And dataframe repartitioned is registered as temp view repartitioned_rows
      When query
        """
        WITH repartitioned AS (
          SELECT id, spark_partition_id() AS pid
          FROM repartitioned_rows
        )
        SELECT id, DENSE_RANK() OVER (ORDER BY pid) - 1 AS pid
        FROM repartitioned
        ORDER BY id
        """
      Then query result ordered
        | id | pid |
        | 0  | 0   |
        | 1  | 1   |
        | 2  | 0   |
        | 3  | 1   |
        | 4  | 0   |
        | 5  | 1   |

    Scenario: Repartition spreads identical rows across all requested partitions
      Given dataframe source is query
        """
        SELECT 'same' AS value FROM range(0, 64, 1, 1)
        """
      And dataframe repartitioned is repartition of source to 4 partitions
      Then dataframe repartitioned partition count is 4
      And dataframe repartitioned distinct partition ids are 0, 1, 2, 3
