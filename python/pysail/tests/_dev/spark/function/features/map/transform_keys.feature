@function(lambda)
Feature: transform_keys with lambda

  Background:
    Given config spark.sql.ansi.enabled = true
    Given config spark.sql.session.timeZone = UTC

  Rule: The lambda rewrites every key and keeps every value

    Scenario: Shift the keys of a quantity map
      When query
        """
        SELECT transform_keys(map(1, 10, 2, 20), (k, v) -> k + 1) AS result
        """
      Then query result
        | result             |
        | {2 -> 10, 3 -> 20} |

    Scenario Outline: Lambda argument binding: <case>
      When query
        """
        SELECT transform_keys(map(1, 10, 2, 20), (k, v) -> <expression>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | expression | result               |
        | key only                    | k * 100    | {100 -> 10, 200 -> 20} |
        | value only                  | v          | {10 -> 10, 20 -> 20} |
        | both parameters             | k + v      | {11 -> 10, 22 -> 20} |
        | key referenced after value  | v - k      | {9 -> 10, 18 -> 20}  |

    Scenario: A null value is kept while its key is rewritten
      When query
        """
        SELECT transform_keys(map(1, CAST(NULL AS INT)), (k, v) -> k + 1) AS result
        """
      Then query result
        | result      |
        | {2 -> NULL} |

  Rule: The result key type comes from the lambda, not from the input

    Scenario: Rewrite integer keys as strings
      When query
        """
        SELECT transform_keys(map(1, 10, 2, 20), (k, v) -> CAST(k AS STRING)) AS result
        """
      Then query result
        | result             |
        | {1 -> 10, 2 -> 20} |

    Scenario: Rewrite string keys as integers
      When query
        """
        SELECT transform_keys(map('1', 10), (k, v) -> CAST(k AS INT)) AS result
        """
      Then query result
        | result    |
        | {1 -> 10} |

  Rule: A rewritten key may not be null

    Scenario: A null key raises an error
      When query
        """
        SELECT transform_keys(map(1, 10), (k, v) -> CAST(NULL AS INT)) AS result
        """
      Then query error .*\[NULL_MAP_KEY\].*

  Rule: Keys that collide after the rewrite follow spark.sql.mapKeyDedupPolicy

    Scenario: Duplicate keys raise an error under the default EXCEPTION policy
      When query
        """
        SELECT transform_keys(map(1, 10, 3, 30), (k, v) -> k % 2) AS result
        """
      Then query error .*\[DUPLICATED_MAP_KEY\].*

    Scenario: The last value wins under the LAST_WIN policy
      Given config spark.sql.mapKeyDedupPolicy = LAST_WIN
      When query
        """
        SELECT transform_keys(map(1, 10, 3, 30), (k, v) -> k % 2) AS result
        """
      Then query result
        | result    |
        | {1 -> 30} |

  Rule: Null and empty maps are preserved without evaluating the lambda

    Scenario: Null and empty maps remain distinct across rows
      When query
        """
        SELECT id, transform_keys(m, (k, v) -> concat(k, '!')) AS result
        FROM VALUES
          (1, map('a', 1, 'b', 2)),
          (2, CAST(NULL AS MAP<STRING, INT>)),
          (3, CAST(map() AS MAP<STRING, INT>)),
          (4, map('c', 3, 'd', CAST(NULL AS INT)))
        AS t(id, m)
        """
      Then query result
        | id | result                 |
        | 1  | {a! -> 1, b! -> 2}     |
        | 2  | NULL                   |
        | 3  | {}                     |
        | 4  | {c! -> 3, d! -> NULL}  |

    Scenario: The lambda never runs for entries behind a null map
      When query
        """
        SELECT id, transform_keys(m, (k, v) -> 1 / v) AS result
        FROM VALUES
          (1, map('a', 2)),
          (2, CAST(NULL AS MAP<STRING, INT>))
        AS t(id, m)
        """
      Then query result
        | id | result     |
        | 1  | {0.5 -> 2} |
        | 2  | NULL       |

  Rule: Lambdas resolve outer columns and nested scopes

    Scenario: Capture a different offset for each input row
      When query
        """
        SELECT id, transform_keys(m, (k, v) -> k + offset) AS result
        FROM VALUES
          (1, map(1, 10), 100),
          (2, CAST(NULL AS MAP<INT, INT>), 0),
          (3, CAST(map() AS MAP<INT, INT>), 0)
        AS t(id, m, offset)
        """
      Then query result
        | id | result      |
        | 1  | {101 -> 10} |
        | 2  | NULL        |
        | 3  | {}          |

    Scenario: Nested transform_keys over a map of maps
      When query
        """
        SELECT transform_keys(map('outer', map(1, 'x')),
                              (k, v) -> concat(k, '!')) AS result
        """
      Then query result
        | result               |
        | {outer! -> {1 -> x}} |

  Rule: An ordinary expression is bound as a lambda that ignores its parameters

    Scenario: A constant replaces the only key
      When query
        """
        SELECT transform_keys(map(1, 2), 5) AS result
        """
      Then query result
        | result   |
        | {5 -> 2} |

    Scenario: A constant key collides when the map has several entries
      When query
        """
        SELECT transform_keys(map(1, 2, 3, 4), 5) AS result
        """
      Then query error .*\[DUPLICATED_MAP_KEY\].*

    Scenario Outline: An untyped null map with an ordinary expression is null: <case>
      When query
        """
        SELECT transform_keys(NULL, <expression>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case     | expression |
        | constant | 1          |
        | null     | NULL       |

  Rule: Invalid map and lambda arguments are rejected

    Scenario Outline: Invalid transform_keys argument: <case>
      When query
        """
        SELECT transform_keys(<arguments>) AS result
        """
      Then query error .*

      Examples:
        | case                     | arguments                 |
        | untyped null with lambda | NULL, (k, v) -> k         |
        | non-map input            | array(1), (k, v) -> k     |
        | one lambda parameter     | map(1, 2), k -> k         |
        | three lambda parameters  | map(1, 2), (k, v, i) -> k |
        | missing function         | map(1, 2)                 |
