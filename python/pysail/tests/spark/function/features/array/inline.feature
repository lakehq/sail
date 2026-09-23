Feature: inline generator output names

  @sail-bug
  Scenario Outline: <function> preserves the default name of a single struct field
    When query
      """
      SELECT <function>(array(named_struct('a', 1)))
      """
    Then query result
      | a |
      | 1 |

    Examples:
      | function     |
      | inline       |
      | inline_outer |

  Scenario Outline: <function> preserves default struct field names
    When query
      """
      SELECT <function>(array(named_struct('a', 1, 'b', 2)))
      """
    Then query result
      | a | b |
      | 1 | 2 |

    Examples:
      | function     |
      | inline       |
      | inline_outer |

  Scenario Outline: <function> preserves explicit output aliases
    When query
      """
      SELECT <function>(array(named_struct('a', 1, 'b', 2))) AS (x, y)
      """
    Then query result
      | x | y |
      | 1 | 2 |

    Examples:
      | function     |
      | inline       |
      | inline_outer |

  Scenario Outline: inline_outer preserves default names for <input>
    When query
      """
      SELECT inline_outer(items)
      FROM VALUES (CAST(<input> AS ARRAY<STRUCT<number:INT,label:STRING>>)) AS input(items)
      """
    Then query result
      | number | label |
      | NULL   | NULL  |

    Examples:
      | input   |
      | NULL    |
      | array() |
