Feature: Generator function arguments
  Scenario Outline: Explode accepts a named collection argument
    When query
      """
      SELECT <function>(collection => array(10, 20)) AS value
      """
    Then query result
      | value |
      | 10    |
      | 20    |
    When query
      """
      SELECT col AS value FROM <function>(collection => array(10, 20))
      """
    Then query result
      | value |
      | 10    |
      | 20    |

    Examples:
      | function      |
      | explode       |
      | explode_outer |

  Scenario Outline: Positional explode accepts a named collection argument
    When query
      """
      SELECT <function>(collection => array(10, 20)) AS (pos, value)
      """
    Then query result
      | pos | value |
      | 0   | 10    |
      | 1   | 20    |

    Examples:
      | function         |
      | posexplode       |
      | posexplode_outer |

  Scenario Outline: Inline accepts a named input argument
    When query
      """
      SELECT <function>(input => array(struct(10 AS value), struct(20 AS value)))
      """
    Then query result
      | value |
      | 10    |
      | 20    |

    Examples:
      | function     |
      | inline       |
      | inline_outer |

  Scenario: Stack fills rows and pads the last row with nulls
    When query
      """
      SELECT stack(2, 1, 2, 3)
      """
    Then query result
      | col0 | col1 |
      | 1    | 2    |
      | 3    | NULL |
    When query
      """
      SELECT * FROM stack(2, 1, 'a', 2) AS t(number, letter)
      """
    Then query result
      | number | letter |
      | 1      | a      |
      | 2      | NULL   |

  Scenario: Stack expands each input row and preserves other columns
    When query
      """
      SELECT id, stack(2, id, id + 1, id + 2) AS (x, y)
      FROM VALUES (10), (20) AS t(id)
      """
    Then query result
      | id | x  | y    |
      | 10 | 10 | 11   |
      | 10 | 12 | NULL |
      | 20 | 20 | 21   |
      | 20 | 22 | NULL |

  Scenario Outline: Stack requires a positive integer literal row count
    When query
      """
      SELECT stack(<rows>, 1, 2)
      """
    Then query error (?i)stack

    Examples:
      | rows |
      | 0    |
      | -1   |
      | NULL |
      | 1.5  |
