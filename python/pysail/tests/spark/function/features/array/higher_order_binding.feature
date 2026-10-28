@function(lambda)
Feature: a plain expression in a lambda slot binds the column it names

  Rule: Columns are bound through their qualifier and alias

    Scenario: a qualified column is captured by a plain expression
      When query
        """
        SELECT t.id, transform(t.arr, t.k) AS r FROM VALUES (1, array(1, 2, 3), 10, 100), (2, array(4, 5), 20, 200), (3, CAST(NULL AS ARRAY<INT>), 30, 300) AS t(id, arr, k, x) ORDER BY t.id
        """
      Then query result ordered
        | id | r |
        | 1 | [10, 10, 10] |
        | 2 | [20, 20] |
        | 3 | NULL |

    Scenario: a table alias is captured by a plain expression
      When query
        """
        SELECT x.id, transform(x.arr, x.k) AS r FROM (SELECT * FROM VALUES (1, array(1, 2, 3), 10, 100), (2, array(4, 5), 20, 200), (3, CAST(NULL AS ARRAY<INT>), 30, 300) AS t(id, arr, k, x)) AS x ORDER BY x.id
        """
      Then query result ordered
        | id | r |
        | 1 | [10, 10, 10] |
        | 2 | [20, 20] |
        | 3 | NULL |

    Scenario: a qualified column in a WHERE predicate
      When query
        """
        SELECT id FROM VALUES (1, array(1, 2, 3), 10, 100), (2, array(4, 5), 20, 200), (3, CAST(NULL AS ARRAY<INT>), 30, 300) AS t(id, arr, k, x) WHERE exists(t.arr, t.k > 15) ORDER BY id
        """
      Then query result ordered
        | id |
        | 2 |

  Rule: A plain expression and a lambda parameter do not mix up with a column of the same name

    Scenario: a bare name is the column, not a parameter
      When query
        """
        SELECT transform(arr, x) AS r FROM VALUES (1, array(1, 2, 3), 10, 100), (2, array(4, 5), 20, 200), (3, CAST(NULL AS ARRAY<INT>), 30, 300) AS t(id, arr, k, x) WHERE id = 1
        """
      Then query result ordered
        | r |
        | [100, 100, 100] |

    Scenario: a lambda parameter shadows a column of the same name
      When query
        """
        SELECT transform(arr, x -> x * 2) AS r FROM VALUES (1, array(1, 2, 3), 10, 100), (2, array(4, 5), 20, 200), (3, CAST(NULL AS ARRAY<INT>), 30, 300) AS t(id, arr, k, x) WHERE id = 1
        """
      Then query result ordered
        | r |
        | [2, 4, 6] |

    Scenario: a plain expression inside a lambda sees the outer parameter
      When query
        """
        SELECT transform(arr, x -> exists(arr, x > 1)) AS r FROM VALUES (1, array(1, 2, 3), 10, 100), (2, array(4, 5), 20, 200), (3, CAST(NULL AS ARRAY<INT>), 30, 300) AS t(id, arr, k, x) WHERE id = 1
        """
      Then query result ordered
        | r |
        | [false, true, true] |

  Rule: The same column name on both sides of a join

    Scenario: a plain expression takes the column of the side it names
      When query
        """
        SELECT l.id, transform(l.arr, r.k) AS a, transform(r.arr, l.k) AS b
        FROM VALUES (1, array(1, 2), 10), (2, array(3), 20) AS l(id, arr, k)
        JOIN VALUES (1, array(7, 8), 1000), (2, array(9), 2000) AS r(id, arr, k) ON l.id = r.id ORDER BY l.id
        """
      Then query result ordered
        | id | a | b |
        | 1 | [1000, 1000] | [10, 10] |
        | 2 | [2000] | [20] |

    Scenario: a plain predicate over the right array reads the left column
      When query
        """
        SELECT l.id, exists(r.arr, l.k > 5) AS e
        FROM VALUES (1, array(1, 2), 10), (2, array(3), 20) AS l(id, arr, k)
        JOIN VALUES (1, array(7, 8), 1000), (2, array(9), 2000) AS r(id, arr, k) ON l.id = r.id ORDER BY l.id
        """
      Then query result ordered
        | id | e |
        | 1 | true |
        | 2 | true |

    Scenario: a plain expression uses both sides at once
      When query
        """
        SELECT l.id, transform(l.arr, l.k + r.k) AS a
        FROM VALUES (1, array(1, 2), 10), (2, array(3), 20) AS l(id, arr, k)
        JOIN VALUES (1, array(7, 8), 1000), (2, array(9), 2000) AS r(id, arr, k) ON l.id = r.id ORDER BY l.id
        """
      Then query result ordered
        | id | a |
        | 1 | [1010, 1010] |
        | 2 | [2020] |

    Scenario: an aggregate zero comes from the other side
      When query
        """
        SELECT l.id, aggregate(l.arr, r.k, (acc, v) -> acc + v) AS a
        FROM VALUES (1, array(1, 2), 10), (2, array(3), 20) AS l(id, arr, k)
        JOIN VALUES (1, array(7, 8), 1000), (2, array(9), 2000) AS r(id, arr, k) ON l.id = r.id ORDER BY l.id
        """
      Then query result ordered
        | id | a |
        | 1 | 1003 |
        | 2 | 2003 |

    Scenario: a lambda body captures a column of the other side
      When query
        """
        SELECT l.id, filter(l.arr, v -> v < r.k) AS f
        FROM VALUES (1, array(1, 2), 10), (2, array(3), 20) AS l(id, arr, k)
        JOIN VALUES (1, array(7, 8), 1000), (2, array(9), 2000) AS r(id, arr, k) ON l.id = r.id ORDER BY l.id
        """
      Then query result ordered
        | id | f |
        | 1 | [1, 2] |
        | 2 | [3] |

  Rule: A higher-order call in every part of a query

    Scenario: in GROUP BY and ORDER BY
      When query
        """
        SELECT exists(arr, true) AS e, count(*) AS c FROM VALUES (1, array(1, 2, 3), 10, 100), (2, array(4, 5), 20, 200), (3, CAST(NULL AS ARRAY<INT>), 30, 300) AS t(id, arr, k, x) GROUP BY exists(arr, true) ORDER BY e
        """
      Then query result ordered
        | e | c |
        | NULL | 1 |
        | true | 2 |

    Scenario: in HAVING
      When query
        """
        SELECT id FROM VALUES (1, array(1, 2, 3), 10, 100), (2, array(4, 5), 20, 200), (3, CAST(NULL AS ARRAY<INT>), 30, 300) AS t(id, arr, k, x) GROUP BY id, arr HAVING exists(arr, v -> v > 4) ORDER BY id
        """
      Then query result ordered
        | id |
        | 2 |

    Scenario: in a LATERAL VIEW
      When query
        """
        SELECT id, v FROM VALUES (1, array(1, 2, 3), 10, 100), (2, array(4, 5), 20, 200), (3, CAST(NULL AS ARRAY<INT>), 30, 300) AS t(id, arr, k, x) LATERAL VIEW explode(transform(arr, 2)) e AS v WHERE id = 2 ORDER BY v
        """
      Then query result ordered
        | id | v |
        | 2 | 2 |
        | 2 | 2 |

    Scenario: next to a window function
      When query
        """
        SELECT id, size(transform(arr, 1)) AS n, sum(id) OVER (ORDER BY id) AS s FROM VALUES (1, array(1, 2, 3), 10, 100), (2, array(4, 5), 20, 200), (3, CAST(NULL AS ARRAY<INT>), 30, 300) AS t(id, arr, k, x) ORDER BY id
        """
      Then query result ordered
        | id | n | s |
        | 1 | 3 | 1 |
        | 2 | 2 | 3 |
        | 3 | NULL | 6 |
