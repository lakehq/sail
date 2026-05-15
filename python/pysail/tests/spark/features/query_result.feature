Feature: Query result display
  These tests ensure the BDD steps are implemented correctly.

  Scenario: Query result with Unicode characters
    When query
      """
      SELECT v FROM VALUES ('hello'), ('你好'), ('👋') AS t(v)
      """
    Then query result
      | v     |
      | hello |
      | 你好  |
      | 👋    |

  Scenario: Query result with escape characters
  There are bugs parsing escape characters in data tables,
  so we have to resort to other representations for the query result.

    When query
        """
        SELECT len(v), ascii(v) FROM VALUES ('\n'), ('\t'), ('\\') AS t(v)
        """
    Then query result
      | len(v) | ascii(v) |
      | 1      | 10       |
      | 1      | 9        |
      | 1      | 92       |

    When query
        """
        SELECT encode(v, 'utf-8') AS x FROM VALUES ('hi\nhello') AS t(v)
        """
    Then query result
      | x                         |
      | [68 69 0A 68 65 6C 6C 6F] |
