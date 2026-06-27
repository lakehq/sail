Feature: Catalog function queries

  Scenario: SHOW SYSTEM FUNCTIONS filters built-in functions
    When query
      """
      SHOW SYSTEM FUNCTIONS LIKE 'to_date'
      """
    Then query result
      | function |
      | to_date  |

  Scenario: SHOW USER FUNCTIONS excludes built-in functions
    When query
      """
      SHOW USER FUNCTIONS LIKE 'to_date'
      """
    Then query result
      | function |

  Scenario: SHOW FUNCTIONS supports namespace and legacy pattern forms
    When query
      """
      SHOW FUNCTIONS IN default LIKE 'to_date'
      """
    Then query result
      | function |
      | to_date  |

    When query
      """
      SHOW FUNCTIONS LIKE default.to_date
      """
    Then query result
      | function |
      | to_date  |
