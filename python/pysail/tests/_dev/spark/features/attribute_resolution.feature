Feature: Attribute identity in column resolution

  Scenario: Repeated CTE references allow duplicate occurrences of the same attribute
    When query
      """
      WITH t AS (SELECT id, id FROM range(2))
      SELECT b.id FROM t a CROSS JOIN t b
      """
    Then query result
      | id |
      | 0  |
      | 0  |
      | 1  |
      | 1  |

  Scenario: Derived tables allow duplicate occurrences of the same attribute
    When query
      """
      SELECT id FROM (SELECT id, id FROM range(2))
      """
    Then query result
      | id |
      | 0  |
      | 1  |

  Scenario: Duplicate occurrences of a union attribute resolve to the first column
    When query
      """
      SELECT id FROM (SELECT id, id FROM range(2) UNION ALL SELECT id + 10, id + 20 FROM range(2))
      WHERE id > 5
      """
    Then query result
      | id |
      | 10 |
      | 11 |

  @spark-4.2
  @sail-bug
  Scenario: Extracting a field or item from a NULL column yields NULL
    When query
      """
      SELECT n.x, n[0] FROM (SELECT NULL AS n)
      """
    Then query result
      | x    | n[0] |
      | NULL | NULL |

  Scenario: A dotted name extracts a map value by key
    When query
      """
      SELECT m.a, m.missing FROM VALUES (map('a', 'x')) AS t(m)
      """
    Then query result
      | a | missing |
      | x | NULL    |

  Scenario: A dotted map key matches exactly when names are case-insensitive
    Given config spark.sql.caseSensitive = false
    When query
      """
      SELECT m.A FROM VALUES (map('a', 'x', 'A', 'y')) AS t(m)
      """
    Then query result
      | A |
      | y |

  Scenario: A qualified dotted name extracts a map value
    When query
      """
      SELECT t.m.a FROM VALUES (map('a', 'x')) AS t(m)
      """
    Then query result
      | a |
      | x |

  Scenario: A dotted map value can be used in a filter
    When query
      """
      SELECT id FROM VALUES (1, map('a', 'x')), (2, map('a', 'y')) AS t(id, m)
      WHERE m.a = 'x'
      """
    Then query result
      | id |
      | 1  |

  Scenario Outline: A dotted name continues into a struct map value
    When query
      """
      SELECT m.<key>.s FROM VALUES (map('a', named_struct('s', 1))) AS t(m)
      """
    Then query result
      | s       |
      | <value> |

    Examples:
      | key     | value |
      | a       | 1     |
      | missing | NULL  |

  Scenario: A missing field of a struct map value is an error
    When query
      """
      SELECT m.a.x FROM VALUES (map('a', named_struct('s', 1))) AS t(m)
      """
    Then query error (?i)(cannot resolve|unresolved|not found|no such struct field)

  Scenario: A dotted name extracts a map value inside a struct
    When query
      """
      SELECT s.m.a FROM VALUES (named_struct('m', map('a', 1))) AS t(s)
      """
    Then query result
      | a |
      | 1 |

  Scenario: A lateral alias exposes map values by dotted names
    When query
      """
      SELECT map('a', 1) AS m, m.a + 1 AS n
      """
    Then query result
      | m        | n |
      | {a -> 1} | 2 |

  Scenario: An outer reference extracts a map value by a dotted name
    When query
      """
      SELECT id FROM VALUES (1, map('a', 'x')), (2, map('a', 'y')) AS t(id, m)
      WHERE EXISTS (SELECT 1 FROM range(1) WHERE t.m.a = 'x')
      """
    Then query result
      | id |
      | 1  |

  Scenario: A dotted name selects a map value by a non-string key
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT m.`1`, m.`2` FROM VALUES (map(1, 'a')) AS t(m)
      """
    Then query result
      | 1 | 2    |
      | a | NULL |

  Scenario Outline: A dotted name is cast to the map key type
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT m.`<name>` AS v FROM VALUES (map(<key>, 'a')) AS t(m)
      """
    Then query result
      | v       |
      | <value> |

    Examples:
      | ansi  | key                    | name       | value |
      | false | 1                      | 1          | a     |
      | false | 1                      | x          | NULL  |
      | false | 1                      | 1.9        | a     |
      | true  | 1                      | 1          | a     |
      | false | CAST(1 AS BIGINT)      | 1          | a     |
      | false | CAST(1 AS BIGINT)      | x          | NULL  |
      | false | CAST(1 AS BIGINT)      | 1.9        | a     |
      | false | CAST(-1 AS BIGINT)     | -1.9       | a     |
      | false | CAST(1 AS BIGINT)      | 1.x        | NULL  |
      | true  | CAST(1 AS BIGINT)      | 1          | a     |
      | false | CAST(1 AS SMALLINT)    | 1.9        | a     |
      | false | CAST(1 AS SMALLINT)    | 65537      | NULL  |
      | false | CAST(1 AS TINYINT)     | 1.9        | a     |
      | false | CAST(1 AS TINYINT)     | 257        | NULL  |
      | false | CAST(1 AS DECIMAL(3))  | 1          | a     |
      | false | 1.5D                   | 1.5        | a     |
      | false | DATE '2024-01-01'      | 2024-01-01 | a     |
      | false | DATE '2024-01-01'      | x          | NULL  |
      | true  | DATE '2024-01-01'      | 2024-01-01 | a     |

  Scenario: A padded dotted name is trimmed before it is cast to an integer map key
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT m.` 1` AS v, n.`1 ` AS w
      FROM VALUES (map(1, 'a'), map(CAST(1 AS BIGINT), 'b')) AS t(m, n)
      """
    Then query result
      | v | w |
      | a | b |

  Scenario Outline: A dotted name that is not a valid map key fails in ANSI mode
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT m.`<name>` FROM VALUES (map(<key>, 'a')) AS t(m)
      """
    Then query error (?i)cast

    Examples:
      | key               | name |
      | 1                 | x    |
      | 1                 | 1.9  |
      | CAST(1 AS BIGINT) | x    |
      | CAST(1 AS BIGINT) | 1.9  |
      | DATE '2024-01-01' | x    |
