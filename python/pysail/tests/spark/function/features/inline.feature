@inline
Feature: inline generator output column names

  Scenario: inline uses struct field name for single-field structs
    When query
      """
      SELECT inline(array(struct(1), struct(2)))
      """
    Then query result ordered
      | col1 |
      | 1    |
      | 2    |

  Scenario: inline_outer uses struct field name for single-field structs
    When query
      """
      SELECT inline_outer(array(struct(1), struct(2)))
      """
    Then query result ordered
      | col1 |
      | 1    |
      | 2    |

