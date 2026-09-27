Feature: Sort reference resolution
  Scenario Outline: Sort expressions combine visible aliases with aggregate arguments
    When query
      """
      SELECT -a AS a, sum(b) AS total
      FROM VALUES (1, 10), (2, 30), (2, 5) AS t(a, b)
      GROUP BY a
      <sort> a + sum(b) DESC
      """
    Then query result ordered
      | a  | total |
      | -2 | 35    |
      | -1 | 10    |

    Examples:
      | sort     |
      | ORDER BY |
      | SORT BY  |

  Scenario: Sort ordinals refer to visible output alongside a recovered key
    When query
      """
      SELECT a FROM VALUES (1, 30), (2, 10) AS t(a, b) ORDER BY 1 DESC, b
      """
    Then query result ordered
      | a |
      | 2 |
      | 1 |

  Scenario: Recovering a sort key does not make an invalid ordinal valid
    When query
      """
      SELECT a FROM VALUES (1, 30), (2, 10) AS t(a, b) ORDER BY b, 2
      """
    Then query error (?i)(position|pos_out_of_range)

  Scenario Outline: Missing literal function names precede hidden sort columns
    When query
      """
      SELECT id, payload
      FROM VALUES (1, 'z', 'a'), (1, 'a', 'z') AS t(id, user, payload)
      <sort> id, user, payload
      """
    Then query result ordered
      | id | payload |
      | 1  | a       |
      | 1  | z       |

    Examples:
      | sort     |
      | ORDER BY |
      | SORT BY  |

  Scenario Outline: Failed sort output resolution discards bindings only within that sort key
    When query
      """
      SELECT -k * 100 AS k, named_struct('y', 9) AS s
      FROM VALUES (1, named_struct('x', 1)), (2, named_struct('x', 2)) AS t(k, s)
      ORDER BY <keys>
      """
    Then query result ordered
      | k        | s   |
      | <first>  | {9} |
      | <second> | {9} |

    Examples:
      | keys        | first | second |
      | k + s.x     | -100  | -200   |
      | k, s.x      | -200  | -100   |
      | s.x DESC, 1 | -200  | -100   |

  Scenario: Failed sort output resolution does not cross distinct
    When query
      """
      SELECT DISTINCT k, named_struct('y', 9) AS s
      FROM VALUES (1, named_struct('x', 1)), (2, named_struct('x', 2)) AS t(k, s)
      ORDER BY s.x
      """
    Then query error (?i)(UNRESOLVED_COLUMN|cannot resolve attribute)
