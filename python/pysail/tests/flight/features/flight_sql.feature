Feature: Arrow Flight SQL server connectivity and query execution
  As a data engineer
  I want to execute SQL queries via Arrow Flight SQL protocol
  So that I can work with Sail using Flight SQL clients

  Background:
    Given a running Flight SQL server

  Scenario: Connect to Flight SQL server
    When I connect to the Flight SQL server
    Then the connection should be successful

  Scenario: Execute simple query with VALUES clause
    When I execute the SQL query "SELECT * FROM (VALUES (1), (2)) AS t(one)"
    Then I should get 2 rows
    And row 0 should contain (1,)
    And row 1 should contain (2,)

  Scenario: Execute query with multiple columns
    When I execute the SQL query "SELECT 1 AS a, 2 AS b, 3 AS c"
    Then I should get 1 row
    And the row should have 3 columns
    And row 0 should contain (1, 2, 3)

  Scenario: Handle query errors for nonexistent table
    When I try to execute the SQL query "SELECT * FROM nonexistent_table_12345"
    Then I should get an error containing "nonexistent_table"

  Scenario: Handle unimplemented function errors
    When I try to execute the SQL query "SELECT json_tuple('{\"a\":1,\"b\":2}', 'a', 'b')"
    Then I should get an error containing "not implemented"

  Scenario Outline: Execute queries with different column counts
    When I execute the SQL query "<query>"
    Then I should get at least 1 row
    And the row should have <columns> columns

    Examples:
      | query                         | columns |
      | SELECT 1 AS a                 | 1       |
      | SELECT 1 AS a, 2 AS b         | 2       |
      | SELECT 1 AS a, 2 AS b, 3 AS c | 3       |

  Scenario Outline: Execute numeric operations
    When I execute the SQL query "<query>"
    Then I should get 1 row
    And row 0 column 0 should equal <result>

    Examples:
      | query                 | result |
      | SELECT 1 + 2 AS result | 3     |
      | SELECT 10 - 3 AS result | 7    |
      | SELECT 4 * 5 AS result  | 20   |
      | SELECT 20 / 4 AS result | 5    |

  Scenario: Reuse cursor for multiple queries
    When I create a cursor
    And I execute "SELECT 1 AS first" with the cursor
    Then the cursor result should be 1
    When I execute "SELECT 2 AS second" with the same cursor
    Then the cursor result should be 2
    When I execute "SELECT 3 AS third" with the same cursor
    Then the cursor result should be 3

  Scenario: Use multiple cursors on same connection
    When I create cursor "cur1"
    And I create cursor "cur2"
    And I execute "SELECT 1 AS result" with cursor "cur1"
    And I execute "SELECT 2 AS result" with cursor "cur2"
    Then cursor "cur1" result should be 1
    And cursor "cur2" result should be 2
