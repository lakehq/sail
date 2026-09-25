@function(lambda)
Feature: map_zip_with inherits deferred nested map cast behavior

  Background:
    Given config spark.sql.ansi.enabled = true
    Given config spark.sql.session.timeZone = America/Los_Angeles

  @sail-bug
  Scenario: Legacy timestamp keys use Spark string formatting
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT map_values(map_zip_with(map(TIMESTAMP'2020-01-01 00:00:00', 1),
                                     map('2020-01-01 00:00:00', 2),
                                     (k, x, y) -> coalesce(x, 0) + coalesce(y, 0))) AS result
      """
    Then query result
      | result |
      | [3]    |

  @sail-bug
  Scenario Outline: Timestamp map key casts resolve a daylight-saving <case>
    When query
      """
      SELECT map_values(map_zip_with(map(TIMESTAMP_NTZ'<local>', 1),
                                     map(TIMESTAMP'<instant>', 2),
                                     (k, x, y) -> x + y)) AS result
      """
    Then query result
      | result |
      | [3]    |

    Examples:
      | case    | local               | instant                   |
      | overlap | 2021-11-07 01:30:00 | 2021-11-07 01:30:00-07:00 |
      | gap     | 2021-03-14 02:30:00 | 2021-03-14 03:30:00-07:00 |
