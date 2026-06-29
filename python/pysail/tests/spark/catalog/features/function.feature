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

    When query
      """
      SHOW FUNCTIONS no_such_db.to_date
      """
    Then query result
      | function |
      | to_date  |

  Scenario: SHOW FUNCTIONS rejects namespace with unquoted legacy pattern
    When query
      """
      SHOW FUNCTIONS IN default LIKE to_date
      """
    Then query error (?i)(expected|parse|syntax|extra input)

  Scenario: DESCRIBE FUNCTION returns usage for a built-in function
    When query
      """
      DESCRIBE FUNCTION abs
      """
    Then query result ordered
      | function_desc                                                             |
      | Function: abs                                                             |
      | Class: org.apache.spark.sql.catalyst.expressions.Abs                       |
      | Usage: abs(expr) - Returns the absolute value of the numeric or interval value. |

  Scenario: DESCRIBE FUNCTION supports string literal names
    When query
      """
      DESC FUNCTION 'concat'
      """
    Then query result ordered
      | function_desc                                                                    |
      | Function: concat                                                                 |
      | Class: org.apache.spark.sql.catalyst.expressions.Concat                           |
      | Usage: concat(col1, col2, ..., colN) - Returns the concatenation of col1, col2, ..., colN. |

  Scenario: DESCRIBE FUNCTION supports operator names
    When query
      """
      DESCRIBE FUNCTION +
      """
    Then query result ordered
      | function_desc                              |
      | Function: +                                |
      | Class: org.apache.spark.sql.catalyst.expressions.Add |
      | Usage: expr1 + expr2 - Returns expr1+expr2. |

  Scenario: DESCRIBE FUNCTION does not resolve qualified built-in names
    When query
      """
      DESCRIBE FUNCTION default.to_date
      """
    Then query error (?i)(not found|function)
