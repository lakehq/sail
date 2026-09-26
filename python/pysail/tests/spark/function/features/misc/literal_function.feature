Feature: Literal function references

  Scenario Outline: an unresolved attribute resolves to its literal function
    When query
      """
      SELECT `<name>` = <function>() AS matches
      """
    Then query result
      | matches |
      | true    |

    Examples:
      | name              | function          |
      | current_date      | current_date      |
      | CURRENT_DATE      | current_date      |
      | current_timestamp | current_timestamp |
      | current_user      | current_user      |
      | user              | current_user      |
      | session_user      | current_user      |

  @spark-4.1
  Scenario: an unresolved current_time attribute resolves to the literal function
    When query
      """
      SELECT `current_time` = current_time() AS matches
      """
    Then query result
      | matches |
      | true    |

  @spark-4.1
  Scenario: current_time includes its default precision in the output name
    When query
      """
      SELECT `current_time(6)` IS NOT NULL AS named
      FROM (SELECT `current_time`)
      """
    Then query result
      | named |
      | true  |

  Scenario Outline: user literal functions use the canonical output name
    When query
      """
      SELECT `<name>`
      """
    Then query schema
      """
      root
       |-- current_user(): string (nullable = false)
      """

    Examples:
      | name         |
      | current_user |
      | user         |
      | session_user |

  @sail-bug
  Scenario: the Hive grouping ID literal resolves within grouping sets
    When query
      """
      SELECT a, grouping__id AS gid
      FROM VALUES (1), (2) t(a)
      GROUP BY ROLLUP(a)
      ORDER BY gid, a
      """
    Then query result ordered
      | a    | gid |
      | 1    | 0   |
      | 2    | 0   |
      | NULL | 1   |
