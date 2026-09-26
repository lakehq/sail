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
