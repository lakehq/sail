@function(lambda)
Feature: transform_values with lambda

  Background:
    Given config spark.sql.ansi.enabled = true
    Given config spark.sql.session.timeZone = UTC

  Rule: The lambda rewrites every value and keeps every key

    Scenario: Scale the values of a quantity map
      When query
        """
        SELECT transform_values(map('a', 1, 'b', 2), (k, v) -> v * 10) AS result
        """
      Then query result
        | result             |
        | {a -> 10, b -> 20} |

    Scenario Outline: Lambda argument binding: <case>
      When query
        """
        SELECT transform_values(map(1, 10, 2, 20), (k, v) -> <expression>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                        | expression | result               |
        | key only                    | k          | {1 -> 1, 2 -> 2}     |
        | value only                  | v          | {1 -> 10, 2 -> 20}   |
        | both parameters             | k + v      | {1 -> 11, 2 -> 22}   |
        | value referenced before key | v - k      | {1 -> 9, 2 -> 18}    |
        | neither parameter           | 0          | {1 -> 0, 2 -> 0}     |

    Scenario: A null value stays reachable by the lambda
      When query
        """
        SELECT transform_values(map(1, CAST(NULL AS INT)), (k, v) -> v + 1) AS result
        """
      Then query result
        | result    |
        | {1 -> NULL} |

    Scenario: Keys keep their order and their type
      When query
        """
        SELECT transform_values(map('b', 1, 'a', 2), (k, v) -> concat(k, CAST(v AS STRING))) AS result
        """
      Then query result
        | result             |
        | {b -> b1, a -> a2} |

  Rule: The result value type comes from the lambda, not from the input

    Scenario: Rewrite integer values as strings
      When query
        """
        SELECT transform_values(map('a', 1, 'b', 2), (k, v) -> CAST(v AS STRING)) AS result
        """
      Then query result
        | result           |
        | {a -> 1, b -> 2} |

    Scenario: Rewrite values as arrays
      When query
        """
        SELECT transform_values(map('a', 2), (k, v) -> array(v, v)) AS result
        """
      Then query result
        | result        |
        | {a -> [2, 2]} |

  Rule: Null and empty maps are preserved without evaluating the lambda

    Scenario: Null and empty maps remain distinct across rows
      When query
        """
        SELECT id, transform_values(m, (k, v) -> v * 10) AS result
        FROM VALUES
          (1, map('a', 1, 'b', 2)),
          (2, CAST(NULL AS MAP<STRING, INT>)),
          (3, CAST(map() AS MAP<STRING, INT>)),
          (4, map('c', 3, 'd', CAST(NULL AS INT)))
        AS t(id, m)
        """
      Then query result
        | id | result                 |
        | 1  | {a -> 10, b -> 20}     |
        | 2  | NULL                   |
        | 3  | {}                     |
        | 4  | {c -> 30, d -> NULL}   |

    Scenario: The lambda never runs for entries behind a null map
      When query
        """
        SELECT id, transform_values(m, (k, v) -> 1 / v) AS result
        FROM VALUES
          (1, map('a', 2)),
          (2, CAST(NULL AS MAP<STRING, INT>))
        AS t(id, m)
        """
      Then query result
        | id | result     |
        | 1  | {a -> 0.5} |
        | 2  | NULL       |

  Rule: Lambdas resolve outer columns and nested scopes

    Scenario: Capture a different addend for each input row
      When query
        """
        SELECT id, transform_values(m, (k, v) -> v + addend) AS result
        FROM VALUES
          (1, map('a', 1), 10),
          (2, CAST(NULL AS MAP<STRING, INT>), 0),
          (3, CAST(map() AS MAP<STRING, INT>), 0),
          (4, map('b', 2), CAST(NULL AS INT))
        AS t(id, m, addend)
        """
      Then query result
        | id | result       |
        | 1  | {a -> 11}    |
        | 2  | NULL         |
        | 3  | {}           |
        | 4  | {b -> NULL}  |

    Scenario: Nested transform_values over a map of maps
      When query
        """
        SELECT transform_values(map('outer', map('inner', 1)),
                                (k, v) -> transform_values(v, (k2, v2) -> v2 + 1)) AS result
        """
      Then query result
        | result                    |
        | {outer -> {inner -> 2}}   |
