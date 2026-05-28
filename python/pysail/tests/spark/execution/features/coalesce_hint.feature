Feature: COALESCE hint on DataFrame input
  Background:
    Given statement
      """
      CREATE OR REPLACE TEMP VIEW coalesce_hint_input AS
      SELECT id, id % 3 AS grp FROM range(0, 12, 1, 4)
      """

  Scenario: COALESCE hint lowers the DataFrame partition count and preserves rows
    When dataframe for table "coalesce_hint_input" with COALESCE hint 2
    Then dataframe has 2 partitions
    And dataframe result
      | id | grp |
      | 0  | 0   |
      | 1  | 1   |
      | 2  | 2   |
      | 3  | 0   |
      | 4  | 1   |
      | 5  | 2   |
      | 6  | 0   |
      | 7  | 1   |
      | 8  | 2   |
      | 9  | 0   |
      | 10 | 1   |
      | 11 | 2   |

  Scenario: COALESCE hint does not increase the DataFrame partition count
    When dataframe for table "coalesce_hint_input" with COALESCE hint 6
    Then dataframe has 4 partitions

  Scenario: COALESCE hint rejects zero partitions
    When dataframe for table "coalesce_hint_input" with COALESCE hint 0
    Then dataframe error COALESCE hint requires at least one partition

  Scenario: COALESCE hint rejects negative partitions
    When dataframe for table "coalesce_hint_input" with COALESCE hint -1
    Then dataframe error COALESCE hint requires at least one partition