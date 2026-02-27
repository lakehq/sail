Feature: Table function queries

  Rule: Wildcard aggregation for table function

    Scenario: count star on range table function
      When query
      """
      SELECT count(*) FROM range(2)
      """
      Then query result
      | count(1) |
      | 2        |

    Scenario: count column on range table function
      When query
      """
      SELECT count(id) FROM range(2)
      """
      Then query result
      | count(id) |
      | 2         |

    Scenario: select star on range table function
      When query
      """
      SELECT * FROM range(2)
      """
      Then query result
      | id |
      | 0  |
      | 1  |

    Scenario: select duplicate columns on range table function
      When query
      """
      SELECT id, id FROM range(2)
      """
      Then query result
      | id | id |
      | 0  | 0  |
      | 1  | 1  |
