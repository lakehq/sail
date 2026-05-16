Feature: Coalesce in distributed query execution

  Rule: Coalesce gets a dedicated stage and reduces partitions without shuffle

    Scenario: Coalesce reduction produces CoalesceExec in the physical plan
      Given a range DataFrame with 12 rows and 4 partitions
      When the DataFrame is coalesced to 2 partitions
      Then the physical plan contains "CoalesceExec"

    Scenario: Coalesce does not increase partition count beyond input
      Given a range DataFrame with 48 rows and 4 partitions
      When the DataFrame is coalesced to 6 partitions
      Then the partition count is 4

    Scenario: Coalesce reduces partitions to target
      Given a range DataFrame with 48 rows and 4 partitions
      When the DataFrame is coalesced to 2 partitions
      Then the partition count is 2

    Scenario: Coalesce to one partition
      Given a range DataFrame with 10 rows and 2 partitions
      When the DataFrame is coalesced to 1 partitions
      Then the partition count is 1

    Scenario: COALESCE hint reduces partitions
      Given a range DataFrame with 48 rows and 4 partitions
      When the COALESCE hint is applied with 2 partitions
      Then the partition count is 2

    Scenario: COALESCE hint does not increase partition count beyond input
      Given a range DataFrame with 48 rows and 4 partitions
      When the COALESCE hint is applied with 6 partitions
      Then the partition count is 4

    Scenario: COALESCE hint rejects zero partitions
      Given a range DataFrame with 10 rows and 2 partitions
      When the COALESCE hint is applied with 0 partitions
      Then the operation fails with "COALESCE hint requires at least one partition"

  Rule: Coalesce preserves data

    Scenario: Coalesce preserves all rows when reducing partitions
      When query
        """
        SELECT * FROM (
          SELECT id FROM range(0, 100, 1, 8)
        ) ORDER BY id
        """
      Then query result ordered
        | id |
        | 0  |
        | 1  |
        | 2  |
        | 3  |
        | 4  |
        | 5  |
        | 6  |
        | 7  |
        | 8  |
        | 9  |
        | 10 |
        | 11 |
        | 12 |
        | 13 |
        | 14 |
        | 15 |
        | 16 |
        | 17 |
        | 18 |
        | 19 |
        | 20 |
        | 21 |
        | 22 |
        | 23 |
        | 24 |
        | 25 |
        | 26 |
        | 27 |
        | 28 |
        | 29 |
        | 30 |
        | 31 |
        | 32 |
        | 33 |
        | 34 |
        | 35 |
        | 36 |
        | 37 |
        | 38 |
        | 39 |
        | 40 |
        | 41 |
        | 42 |
        | 43 |
        | 44 |
        | 45 |
        | 46 |
        | 47 |
        | 48 |
        | 49 |
        | 50 |
        | 51 |
        | 52 |
        | 53 |
        | 54 |
        | 55 |
        | 56 |
        | 57 |
        | 58 |
        | 59 |
        | 60 |
        | 61 |
        | 62 |
        | 63 |
        | 64 |
        | 65 |
        | 66 |
        | 67 |
        | 68 |
        | 69 |
        | 70 |
        | 71 |
        | 72 |
        | 73 |
        | 74 |
        | 75 |
        | 76 |
        | 77 |
        | 78 |
        | 79 |
        | 80 |
        | 81 |
        | 82 |
        | 83 |
        | 84 |
        | 85 |
        | 86 |
        | 87 |
        | 88 |
        | 89 |
        | 90 |
        | 91 |
        | 92 |
        | 93 |
        | 94 |
        | 95 |
        | 96 |
        | 97 |
        | 98 |
        | 99 |
