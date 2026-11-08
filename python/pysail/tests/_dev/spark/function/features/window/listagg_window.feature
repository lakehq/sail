Feature: listagg with WITHIN GROUP over a window

  # Spark allows `listagg(...) WITHIN GROUP (ORDER BY ...) OVER (...)` only when the window has
  # no ORDER BY and covers the whole partition. The values of the partition are concatenated
  # in the WITHIN GROUP order.
  Rule: The values of the whole partition are ordered by the WITHIN GROUP ordering

    Scenario Outline: <function> WITHIN GROUP (ORDER BY <ordering>) over a partition
      When query
        """
        SELECT g, v, <function>(v, '<delimiter>') WITHIN GROUP (ORDER BY <ordering>) OVER (PARTITION BY g) AS l
        FROM VALUES (1, 'b'), (1, 'a'), (1, 'c'), (2, 'z'), (2, 'y') AS t(g, v)
        ORDER BY g, v
        """
      Then query result ordered
        | g | v | l        |
        | 1 | a | <first>  |
        | 1 | b | <first>  |
        | 1 | c | <first>  |
        | 2 | y | <second> |
        | 2 | z | <second> |

      Examples:
        | function   | delimiter | ordering | first | second |
        | listagg    | ,         | v        | a,b,c | y,z    |
        | listagg    | ,         | v DESC   | c,b,a | z,y    |
        | string_agg | -         | v        | a-b-c | y-z    |

    Scenario: listagg WITHIN GROUP over a frame that covers the whole partition
      When query
        """
        SELECT g, v, listagg(v, ',') WITHIN GROUP (ORDER BY v) OVER (
          PARTITION BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS l
        FROM VALUES (1, 'b'), (1, 'a'), (2, 'c') AS t(g, v)
        ORDER BY g, v
        """
      Then query result ordered
        | g | v | l   |
        | 1 | a | a,b |
        | 1 | b | a,b |
        | 2 | c | c   |

    Scenario: listagg WITHIN GROUP over a named window
      When query
        """
        SELECT g, v, listagg(v, ',') WITHIN GROUP (ORDER BY v) OVER w AS l
        FROM VALUES (1, 'b'), (1, 'a'), (2, 'c') AS t(g, v)
        WINDOW w AS (PARTITION BY g)
        ORDER BY g, v
        """
      Then query result ordered
        | g | v | l   |
        | 1 | a | a,b |
        | 1 | b | a,b |
        | 2 | c | c   |

    Scenario: listagg WITHIN GROUP over a window ignores NULL values
      When query
        """
        SELECT g, v, listagg(v, '|') WITHIN GROUP (ORDER BY v DESC NULLS FIRST) OVER (PARTITION BY g) AS l
        FROM VALUES (1, 'b'), (1, NULL), (1, 'a'), (2, NULL) AS t(g, v)
        ORDER BY g, v NULLS FIRST
        """
      Then query result ordered
        | g | v    | l    |
        | 1 | NULL | b\|a |
        | 1 | a    | b\|a |
        | 1 | b    | b\|a |
        | 2 | NULL | NULL |

    Scenario: listagg WITHIN GROUP over a window with several ordering keys
      When query
        """
        SELECT g, k, v, listagg(v, ',') WITHIN GROUP (ORDER BY k DESC, v) OVER (PARTITION BY g) AS l
        FROM VALUES (1, 1, 'b'), (1, 2, 'a'), (1, 1, 'a'), (2, 1, 'z') AS t(g, k, v)
        ORDER BY g, k, v
        """
      Then query result ordered
        | g | k | v | l     |
        | 1 | 1 | a | a,a,b |
        | 1 | 1 | b | a,a,b |
        | 1 | 2 | a | a,a,b |
        | 2 | 1 | z | z     |

    Scenario: listagg WITHIN GROUP over the whole input ordered by an expression
      When query
        """
        SELECT v, listagg(v, ',') WITHIN GROUP (ORDER BY length(v), v) OVER () AS l
        FROM VALUES ('bb'), ('a'), ('c') AS t(v)
        ORDER BY v
        """
      Then query result ordered
        | v  | l      |
        | a  | a,c,bb |
        | bb | a,c,bb |
        | c  | a,c,bb |

    Scenario: listagg WITHIN GROUP over a window with a NULL delimiter
      When query
        """
        SELECT g, v, listagg(v, NULL) WITHIN GROUP (ORDER BY v) OVER (PARTITION BY g) AS l
        FROM VALUES (1, 'b'), (1, 'a') AS t(g, v)
        ORDER BY g, v
        """
      Then query result ordered
        | g | v | l  |
        | 1 | a | ab |
        | 1 | b | ab |

    Scenario: listagg WITHIN GROUP over a window of binary values
      When query
        """
        SELECT g, v, string_agg(CAST(v AS BINARY)) WITHIN GROUP (ORDER BY v) OVER (PARTITION BY g) AS l
        FROM VALUES (1, 'b'), (1, 'a') AS t(g, v)
        ORDER BY g, v
        """
      Then query result ordered
        | g | v | l       |
        | 1 | a | [61 62] |
        | 1 | b | [61 62] |

    Scenario: listagg WITHIN GROUP next to a window with ORDER BY
      When query
        """
        SELECT
          g,
          v,
          listagg(v, ',') WITHIN GROUP (ORDER BY v) OVER (PARTITION BY g) AS l,
          count(*) OVER (PARTITION BY g ORDER BY v) AS c
        FROM VALUES (1, 'b'), (1, 'a'), (1, 'c') AS t(g, v)
        ORDER BY g, v
        """
      Then query result ordered
        | g | v | l     | c |
        | 1 | a | a,b,c | 1 |
        | 1 | b | a,b,c | 2 |
        | 1 | c | a,b,c | 3 |

  Rule: The window cannot have an ORDER BY or a smaller frame

    Scenario Outline: listagg WITHIN GROUP over a window with <case> is an error
      When query
        """
        SELECT g, v, listagg(v, ',') WITHIN GROUP (ORDER BY v) OVER (<window>) AS l
        FROM VALUES (1, 'b'), (1, 'a') AS t(g, v)
        """
      Then query error INVALID_WINDOW_SPEC_FOR_AGGREGATION_FUNC

      Examples:
        | case            | window                                                                   |
        | an ORDER BY     | PARTITION BY g ORDER BY v                                                |
        | a growing frame | PARTITION BY g ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW          |
        | a RANGE frame   | PARTITION BY g RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING |

    Scenario: DISTINCT listagg WITHIN GROUP over a window is an error
      When query
        """
        SELECT g, v, listagg(DISTINCT v, ',') WITHIN GROUP (ORDER BY v) OVER (PARTITION BY g) AS l
        FROM VALUES (1, 'b'), (1, 'a'), (1, 'a') AS t(g, v)
        """
      Then query error DISTINCT_WINDOW_FUNCTION_UNSUPPORTED
