Feature: Date and interval arithmetic

  Rule: Adding a sub-day interval to a date

    Scenario Outline: Addition produces a timestamp: <case>
      When query
        """
        SELECT <expression> AS result, typeof(<expression>) AS type
        """
      Then query result
        | result   | type          |
        | <result> | timestamp_ntz |

      Examples:
        | case                    | expression                                                   | result              |
        | minutes                 | DATE '2026-07-30' + INTERVAL 5 MINUTES                        | 2026-07-30 00:05:00 |
        | hours                   | DATE '2026-07-30' + INTERVAL 2 HOURS                          | 2026-07-30 02:00:00 |
        | seconds                 | DATE '2026-07-30' + INTERVAL 30 SECONDS                       | 2026-07-30 00:00:30 |
        | reversed operands       | INTERVAL 5 MINUTES + DATE '2026-07-30'                        | 2026-07-30 00:05:00 |
        | nullable date           | CAST(NULL AS DATE) + INTERVAL 5 MINUTES                       | NULL                |
        | nullable interval       | DATE '2026-07-30' + CAST(NULL AS INTERVAL MINUTE TO SECOND)   | NULL                |

    Scenario: A column-derived interval preserves distinct sub-day timestamps
      When query
        """
        SELECT n, result, typeof(result) AS type
        FROM (
          SELECT n, DATE '2026-07-30' + (n * 5) * INTERVAL 1 MINUTE AS result
          FROM VALUES (1), (2) AS t(n)
        ) AS q
        ORDER BY n
        """
      Then query result
        | n | result              | type          |
        | 1 | 2026-07-30 00:05:00 | timestamp_ntz |
        | 2 | 2026-07-30 00:10:00 | timestamp_ntz |

  Rule: Adding a number to a date

    Scenario Outline: Addition continues to produce a date: <case>
      When query
        """
        SELECT <expression> AS result, typeof(<expression>) AS type
        """
      Then query result
        | result     | type |
        | 2026-07-31 | date |

      Examples:
        | case              | expression                    |
        | date plus number  | DATE '2026-07-30' + 1         |
        | number plus date  | 1 + DATE '2026-07-30'         |
