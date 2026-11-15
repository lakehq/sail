Feature: input_order
  Aggregate/window functions observe input row order established by an
  upstream ORDER BY (migrated SQL-expressible subset of test_input_order.txt).

  Rule: Order-dependent results (migrated from test_input_order.txt doctests)

    Scenario: input_order doctest #1 — first(age) per name
      When query
        """
        SELECT name, first(age) AS f FROM (SELECT * FROM VALUES ('Alice', 2), ('Bob', 5), ('Alice', CAST(NULL AS INT)) AS t(name, age) ORDER BY age) GROUP BY name ORDER BY name
        """
      Then query result ordered
        | name  | f    |
        | Alice | NULL |
        | Bob   | 5    |

    Scenario: input_order doctest #2 — last(age) per name
      When query
        """
        SELECT name, last(age) AS l FROM (SELECT * FROM VALUES ('Alice', 2), ('Bob', 5), ('Alice', CAST(NULL AS INT)) AS t(name, age) ORDER BY age) GROUP BY name ORDER BY name
        """
      Then query result ordered
        | name  | l |
        | Alice | 2 |
        | Bob   | 5 |

    Scenario: input_order doctest #3 — first(v) ordered by a+b
      When query
        """
        SELECT first(v) AS f FROM (SELECT * FROM VALUES (30,2,2),(10,5,-20),(20,1,1) AS t(v,a,b) ORDER BY a+b)
        """
      Then query result
        | f  |
        | 10 |

    Scenario: input_order doctest #4 — first(v) sorted CTE
      When query
        """
        WITH sorted AS (SELECT * FROM VALUES (10,'b'),(20,'a'),(30,'d'),(40,'c') AS t(v,k) ORDER BY k) SELECT first(v) AS r FROM sorted
        """
      Then query result
        | r  |
        | 20 |

    Scenario: input_order doctest #5 — collect_list ordered
      When query
        """
        SELECT collect_list(v) AS r FROM (SELECT * FROM VALUES (30,'c'),(10,'a'),(20,'b') AS t(v,k) ORDER BY k)
        """
      Then query result
        | r            |
        | [10, 20, 30] |

    Scenario: input_order doctest #6 — first over partition
      When query
        """
        SELECT name, age, first(age) OVER (PARTITION BY name) AS f FROM (SELECT * FROM VALUES ('Alice', 2), ('Bob', 5), ('Alice', CAST(NULL AS INT)) AS t(name, age) ORDER BY age) ORDER BY name, age NULLS FIRST
        """
      Then query result ordered
        | name  | age  | f    |
        | Alice | NULL | NULL |
        | Alice | 2    | NULL |
        | Bob   | 5    | 5    |

    Scenario: input_order doctest #7 — last over partition
      When query
        """
        SELECT name, age, last(age) OVER (PARTITION BY name) AS l FROM (SELECT * FROM VALUES ('Alice', 2), ('Bob', 5), ('Alice', CAST(NULL AS INT)) AS t(name, age) ORDER BY age) ORDER BY name, age NULLS FIRST
        """
      Then query result ordered
        | name  | age  | l |
        | Alice | NULL | 2 |
        | Alice | 2    | 2 |
        | Bob   | 5    | 5 |

    Scenario: input_order doctest #8 — first(v) single group same key
      When query
        """
        SELECT first(v) AS f FROM (SELECT * FROM VALUES (1,'a'),(2,'a'),(3,'a') AS t(v,k) ORDER BY k)
        """
      Then query result
        | f |
        | 1 |

    Scenario: input_order doctest #9 — union then first/last
      When query
        """
        SELECT first(v) AS f, last(v) AS l FROM (SELECT * FROM (VALUES (2,'b'),(1,'a'),(4,'d'),(3,'c')) AS t(v,k) ORDER BY k)
        """
      Then query result
        | f | l |
        | 1 | 4 |

    Scenario: input_order doctest #10 — first over + last over
      When query
        """
        SELECT name, age, first(age) OVER (PARTITION BY name) AS f, last(age) OVER (PARTITION BY name) AS l FROM (SELECT * FROM VALUES ('Alice', 2), ('Bob', 5), ('Alice', CAST(NULL AS INT)) AS t(name, age) ORDER BY age) ORDER BY name, age NULLS FIRST
        """
      Then query result ordered
        | name  | age  | f    | l |
        | Alice | NULL | NULL | 2 |
        | Alice | 2    | NULL | 2 |
        | Bob   | 5    | 5    | 5 |

    Scenario: input_order doctest #11 — last over + last over sum
      When query
        """
        SELECT name, age, (last(age) OVER (PARTITION BY name) + last(age) OVER (PARTITION BY name)) AS s FROM (SELECT * FROM VALUES ('Alice', 2), ('Bob', 5), ('Alice', CAST(NULL AS INT)) AS t(name, age) ORDER BY age) ORDER BY name, age NULLS FIRST
        """
      Then query result ordered
        | name  | age  | s  |
        | Alice | NULL | 4  |
        | Alice | 2    | 4  |
        | Bob   | 5    | 10 |

    Scenario: input_order doctest #12 — sum over + last over
      When query
        """
        SELECT name, age, sum(age) OVER (PARTITION BY name) AS sm, last(age) OVER (PARTITION BY name) AS l FROM (SELECT * FROM VALUES ('Alice', 2), ('Bob', 5), ('Alice', CAST(NULL AS INT)) AS t(name, age) ORDER BY age) ORDER BY name, age NULLS FIRST
        """
      Then query result ordered
        | name  | age  | sm | l |
        | Alice | NULL | 2  | 2 |
        | Alice | 2    | 2  | 2 |
        | Bob   | 5    | 5  | 5 |

    Scenario: input_order doctest #13 — listagg ordered
      When query
        """
        SELECT listagg(k) AS s FROM (SELECT * FROM VALUES (30,'c'),(10,'a'),(20,'b') AS t(v,k) ORDER BY v)
        """
      Then query result
        | s   |
        | abc |

    Scenario: input_order doctest #14 — HAVING first
      When query
        """
        SELECT g, first(v) AS f FROM (SELECT * FROM VALUES (30,1),(10,1),(20,2) AS t(v,g) ORDER BY v) GROUP BY g HAVING first(v) > 15
        """
      Then query result
        | g | f  |
        | 2 | 20 |

    Scenario: input_order doctest #15 — CUBE first
      When query
        """
        SELECT g, first(v) AS f FROM (SELECT * FROM VALUES (30,1),(10,1),(20,2) AS t(v,g) ORDER BY v) GROUP BY CUBE(g) ORDER BY g NULLS FIRST
        """
      Then query result ordered
        | g    | f  |
        | NULL | 10 |
        | 1    | 10 |
        | 2    | 20 |

    Scenario: input_order doctest #16 — nth_value over ordered
      When query
        """
        SELECT name, age, nth_value(age, 2) OVER (PARTITION BY name ORDER BY age) AS n FROM (SELECT * FROM VALUES ('Alice', 2), ('Bob', 5), ('Alice', CAST(NULL AS INT)) AS t(name, age) ORDER BY age) ORDER BY name, age NULLS FIRST
        """
      Then query result ordered
        | name  | age  | n    |
        | Alice | NULL | NULL |
        | Alice | 2    | 2    |
        | Bob   | 5    | NULL |

    Scenario: input_order doctest #17 — aliased subquery first
      When query
        """
        SELECT first(v) AS f FROM (SELECT v FROM (SELECT * FROM VALUES (30,'c'),(10,'a'),(20,'b') AS t(v,k) ORDER BY k))
        """
      Then query result
        | f  |
        | 10 |

    Scenario: input_order doctest #18 — select v then first
      When query
        """
        SELECT first(v) AS f FROM (SELECT v FROM (SELECT * FROM VALUES (30,'c'),(10,'a'),(20,'b') AS t(v,k) ORDER BY k))
        """
      Then query result
        | f  |
        | 10 |

    Scenario: input_order doctest #19 — sum/first/last over
      When query
        """
        SELECT name, age, sum(age) OVER (PARTITION BY name) AS sm, first(age) OVER (PARTITION BY name) AS f, last(age) OVER (PARTITION BY name) AS l FROM (SELECT * FROM VALUES ('Alice', 2), ('Bob', 5), ('Alice', CAST(NULL AS INT)) AS t(name, age) ORDER BY age) ORDER BY name, age NULLS FIRST
        """
      Then query result ordered
        | name  | age  | sm | f    | l |
        | Alice | NULL | 2  | NULL | 2 |
        | Alice | 2    | 2  | NULL | 2 |
        | Bob   | 5    | 5  | 5    | 5 |

    Scenario Outline: input_order doctest <case> — first/last DISTINCT <variant>
      When query
        """
        SELECT first(DISTINCT v) AS f, last(DISTINCT v) AS l FROM (SELECT * FROM VALUES <values> AS t(v,k) ORDER BY k)
        """
      Then query result
        | f   | l   |
        | <f> | <l> |

      Examples:
        | case | variant  | values                              | f  | l  |
        | #20  |          | (30,'c'),(10,'a'),(20,'b')          | 10 | 30 |
        | #21  | with dup | (30,'c'),(20,'b'),(10,'a'),(20,'d') | 10 | 20 |

    Scenario: input_order doctest #22 — count/first/last
      When query
        """
        SELECT count(*) AS n, first(v) AS f, last(v) AS l FROM (SELECT * FROM VALUES (30,'c'),(10,'a'),(20,'b') AS t(v,k) ORDER BY k)
        """
      Then query result
        | n | f  | l  |
        | 3 | 10 | 30 |

    Scenario: input_order doctest #23 — explode then first
      When query
        """
        SELECT first(v) AS f FROM (SELECT v, e FROM (SELECT * FROM VALUES (30,'c'),(10,'a'),(20,'b') AS t(v,k) ORDER BY k) LATERAL VIEW explode(array(1)) AS e)
        """
      Then query result
        | f  |
        | 10 |

    Scenario: input_order doctest #24 — avg ordered
      When query
        """
        SELECT avg(v) AS a FROM (SELECT * FROM VALUES (CAST(1e16 AS DOUBLE),'b'),(CAST(1.0 AS DOUBLE),'c'),(CAST(-1e16 AS DOUBLE),'a') AS t(v,k) ORDER BY k)
        """
      Then query result
        | a                  |
        | 0.3333333333333333 |

    Scenario: input_order doctest #25 — sum ordered
      When query
        """
        SELECT sum(v) AS s FROM (SELECT * FROM VALUES (CAST(1e16 AS DOUBLE),'b'),(CAST(1.0 AS DOUBLE),'c'),(CAST(-1e16 AS DOUBLE),'a') AS t(v,k) ORDER BY k)
        """
      Then query result
        | s   |
        | 1.0 |

    Scenario: input_order doctest #26 — join then first
      When query
        """
        SELECT first(v) AS f FROM (SELECT l.v FROM VALUES (2,'b'),(1,'a') AS l(v,k) JOIN VALUES ('a'),('b') AS r(k) ON l.k = r.k ORDER BY l.k DESC)
        """
      Then query result
        | f |
        | 2 |
