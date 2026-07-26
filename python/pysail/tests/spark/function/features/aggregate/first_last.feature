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

    Scenario Outline: <fn> respects the <order> inner ORDER BY
      When query
        """
        SELECT <fn>(v) AS result
        FROM (SELECT * FROM VALUES (10, 'b'), (20, 'a'), (30, 'd'), (40, 'c') AS t(v, k) ORDER BY <ord>)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | fn        | order      | ord    | result |
        | first     | ascending  | k      | 20     |
        | last      | ascending  | k      | 30     |
        | first     | descending | k DESC | 30     |
        | last      | descending | k DESC | 20     |
        | any_value | ascending  | k      | 20     |

  Rule: An aliased derived table behaves identically to an unaliased one

    Scenario Outline: <fn> respects the inner ORDER BY through <alias_desc>
      When query
        """
        SELECT <fn>(v) AS result
        FROM (SELECT * FROM VALUES (10, 'b'), (20, 'a'), (30, 'd'), (40, 'c') AS t(v, k) ORDER BY k) AS <alias>
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | fn    | alias_desc                            | alias     | result |
        | first | an aliased derived table              | sub       | 20     |
        | last  | an aliased derived table              | sub       | 30     |
        | first | an aliased derived table with columns | sub(v, k) | 20     |

  Rule: The full multi-key ordering drives the result, not just the leading key

    # storage v = 30, 10, 20  with (k1,k2) = (2,'a'), (1,'b'), (1,'a')
    # sorted by k1 ASC, k2 ASC: (1,'a')->20, (1,'b')->10, (2,'a')->30  -> first=20 last=30
    Scenario Outline: <fn> honors the secondary sort key
      When query
        """
        SELECT <fn>(v) AS result
        FROM (
          SELECT * FROM VALUES (30, 2, 'a'), (10, 1, 'b'), (20, 1, 'a') AS t(v, k1, k2)
          ORDER BY k1 ASC, k2 ASC
        )
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | fn    | result |
        | first | 20     |
        | last  | 30     |

  Rule: Ordering is applied per group

    # storage v = 10, 20, 30, 40  with k = 'b','a','d','c'  g = 0,0,1,1
    # g=0 sorted by k: a(20) b(10) -> first=20 last=10
    # g=1 sorted by k: c(40) d(30) -> first=40 last=30
    Scenario Outline: grouped <fn> respects the inner ORDER BY within each group
      When query
        """
        SELECT g, <fn>(v) AS result
        FROM (
          SELECT * FROM VALUES (10, 'b', 0), (20, 'a', 0), (30, 'd', 1), (40, 'c', 1) AS t(v, k, g)
          ORDER BY k
        )
        GROUP BY g
        ORDER BY g
        """
      Then query result
        | g | result |
        | 0 | <g0>   |
        | 1 | <g1>   |

      Examples:
        | fn    | g0 | g1 |
        | first | 20 | 40 |
        | last  | 10 | 30 |

  Rule: Null treatment is honored alongside the inherited ordering

    # storage v = 30, NULL, 10  with k = 'c','a','b'
    # k ASC: a(NULL) b(10) c(30) -> respect-nulls first=NULL, ignore-nulls first=10
    Scenario Outline: first <case> a leading NULL when nulls are <treatment>
      When query
        """
        SELECT first(<args>) AS result
        FROM (SELECT * FROM VALUES (30, 'c'), (CAST(NULL AS INT), 'a'), (10, 'b') AS t(v, k) ORDER BY k)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case  | treatment | args    | result |
        | keeps | respected | v       | NULL   |
        | skips | ignored   | v, true | 10     |
