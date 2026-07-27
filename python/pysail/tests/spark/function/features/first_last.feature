@first_last
Feature: first / last / any_value inherit ordering from an adjacent ORDER BY

  # The row STORAGE order in every VALUES list below is deliberately scrambled so
  # that it does NOT match k-ascending nor k-descending order. Therefore a passing
  # result can ONLY come from the inner ORDER BY driving the aggregate, never from
  # coincidental scan order. All expected values are verified against Spark JVM 4.x
  # (rule: first = value of the min-key row, last = value of the max-key row).
  #
  #   storage: v = 10, 20, 30, 40   with   k = 'b', 'a', 'd', 'c'
  #   k ASC:  a(20) b(10) c(40) d(30)  -> first=20  last=30
  #   k DESC: d(30) c(40) b(10) a(20)  -> first=30  last=20

  Rule: An ORDER BY directly below the aggregate makes first/last deterministic

    Scenario: first respects ascending inner ORDER BY
      When query
        """
        SELECT first(v) AS result
        FROM (SELECT * FROM VALUES (10, 'b'), (20, 'a'), (30, 'd'), (40, 'c') AS t(v, k) ORDER BY k)
        """
      Then query result
        | result |
        | 20     |

    Scenario: last respects ascending inner ORDER BY
      When query
        """
        SELECT last(v) AS result
        FROM (SELECT * FROM VALUES (10, 'b'), (20, 'a'), (30, 'd'), (40, 'c') AS t(v, k) ORDER BY k)
        """
      Then query result
        | result |
        | 30     |

    Scenario: first respects descending inner ORDER BY
      When query
        """
        SELECT first(v) AS result
        FROM (SELECT * FROM VALUES (10, 'b'), (20, 'a'), (30, 'd'), (40, 'c') AS t(v, k) ORDER BY k DESC)
        """
      Then query result
        | result |
        | 30     |

    Scenario: last respects descending inner ORDER BY
      When query
        """
        SELECT last(v) AS result
        FROM (SELECT * FROM VALUES (10, 'b'), (20, 'a'), (30, 'd'), (40, 'c') AS t(v, k) ORDER BY k DESC)
        """
      Then query result
        | result |
        | 20     |

    Scenario: any_value behaves like first and respects the inner ORDER BY
      When query
        """
        SELECT any_value(v) AS result
        FROM (SELECT * FROM VALUES (10, 'b'), (20, 'a'), (30, 'd'), (40, 'c') AS t(v, k) ORDER BY k)
        """
      Then query result
        | result |
        | 20     |

  Rule: An aliased derived table behaves identically to an unaliased one

    Scenario: first respects the inner ORDER BY through an aliased derived table
      When query
        """
        SELECT first(v) AS result
        FROM (SELECT * FROM VALUES (10, 'b'), (20, 'a'), (30, 'd'), (40, 'c') AS t(v, k) ORDER BY k) AS sub
        """
      Then query result
        | result |
        | 20     |

    Scenario: last respects the inner ORDER BY through an aliased derived table
      When query
        """
        SELECT last(v) AS result
        FROM (SELECT * FROM VALUES (10, 'b'), (20, 'a'), (30, 'd'), (40, 'c') AS t(v, k) ORDER BY k) AS sub
        """
      Then query result
        | result |
        | 30     |

    Scenario: first respects the inner ORDER BY through an aliased derived table with a column list
      When query
        """
        SELECT first(v) AS result
        FROM (SELECT * FROM VALUES (10, 'b'), (20, 'a'), (30, 'd'), (40, 'c') AS t(v, k) ORDER BY k) AS sub(v, k)
        """
      Then query result
        | result |
        | 20     |

  Rule: The full multi-key ordering drives the result, not just the leading key

    # storage v = 30, 10, 20  with (k1,k2) = (2,'a'), (1,'b'), (1,'a')
    # sorted by k1 ASC, k2 ASC: (1,'a')->20, (1,'b')->10, (2,'a')->30  -> first=20 last=30
    Scenario: first honors the secondary sort key
      When query
        """
        SELECT first(v) AS result
        FROM (
          SELECT * FROM VALUES (30, 2, 'a'), (10, 1, 'b'), (20, 1, 'a') AS t(v, k1, k2)
          ORDER BY k1 ASC, k2 ASC
        )
        """
      Then query result
        | result |
        | 20     |

    Scenario: last honors the secondary sort key
      When query
        """
        SELECT last(v) AS result
        FROM (
          SELECT * FROM VALUES (30, 2, 'a'), (10, 1, 'b'), (20, 1, 'a') AS t(v, k1, k2)
          ORDER BY k1 ASC, k2 ASC
        )
        """
      Then query result
        | result |
        | 30     |

  Rule: Ordering is applied per group

    # storage v = 10, 20, 30, 40  with k = 'b','a','d','c'  g = 0,0,1,1
    # g=0 sorted by k: a(20) b(10) -> first=20 last=10
    # g=1 sorted by k: c(40) d(30) -> first=40 last=30
    Scenario: grouped first respects the inner ORDER BY within each group
      When query
        """
        SELECT g, first(v) AS result
        FROM (
          SELECT * FROM VALUES (10, 'b', 0), (20, 'a', 0), (30, 'd', 1), (40, 'c', 1) AS t(v, k, g)
          ORDER BY k
        )
        GROUP BY g
        ORDER BY g
        """
      Then query result
        | g | result |
        | 0 | 20     |
        | 1 | 40     |

    Scenario: grouped last respects the inner ORDER BY within each group
      When query
        """
        SELECT g, last(v) AS result
        FROM (
          SELECT * FROM VALUES (10, 'b', 0), (20, 'a', 0), (30, 'd', 1), (40, 'c', 1) AS t(v, k, g)
          ORDER BY k
        )
        GROUP BY g
        ORDER BY g
        """
      Then query result
        | g | result |
        | 0 | 10     |
        | 1 | 30     |

  Rule: Null treatment is honored alongside the inherited ordering

    # storage v = 30, NULL, 10  with k = 'c','a','b'
    # k ASC: a(NULL) b(10) c(30) -> respect-nulls first=NULL, ignore-nulls first=10
    Scenario: first keeps a leading NULL when nulls are respected
      When query
        """
        SELECT first(v) AS result
        FROM (SELECT * FROM VALUES (30, 'c'), (CAST(NULL AS INT), 'a'), (10, 'b') AS t(v, k) ORDER BY k)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: first skips a leading NULL when nulls are ignored
      When query
        """
        SELECT first(v, true) AS result
        FROM (SELECT * FROM VALUES (30, 'c'), (CAST(NULL AS INT), 'a'), (10, 'b') AS t(v, k) ORDER BY k)
        """
      Then query result
        | result |
        | 10     |
