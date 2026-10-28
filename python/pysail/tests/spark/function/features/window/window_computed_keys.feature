Feature: Window partition and order keys computed from expressions
  # Keys such as `length(name) > 3` compare a function result. All expected values were
  # captured on Spark JVM 4.x.

  Rule: Computed keys order and partition rows like their values

    Scenario: row_number ordered by a comparison on a function result
      When query
        """
        SELECT id, row_number() OVER (PARTITION BY k ORDER BY length(name) > 3, id) AS rn
        FROM VALUES (1, 'a', 'bob'), (1, 'b', 'alexander'), (1, 'c', 'al'),
                    (2, 'd', CAST(NULL AS STRING)) AS t(k, id, name)
        ORDER BY id
        """
      Then query result ordered
        | id | rn |
        | a  | 1  |
        | b  | 3  |
        | c  | 2  |
        | d  | 1  |

    Scenario: row_number ordered by CASE WHEN on a function result
      When query
        """
        SELECT id,
               row_number() OVER (
                 PARTITION BY k ORDER BY CASE WHEN length(name) > 3 THEN 0 ELSE 1 END, id
               ) AS rn
        FROM VALUES (1, 'a', 'bob'), (1, 'b', 'alexander'), (1, 'c', 'al'),
                    (2, 'd', CAST(NULL AS STRING)) AS t(k, id, name)
        ORDER BY id
        """
      Then query result ordered
        | id | rn |
        | a  | 2  |
        | b  | 1  |
        | c  | 3  |
        | d  | 1  |

    Scenario: row_number partitioned by a comparison on a function result
      When query
        """
        SELECT id, row_number() OVER (PARTITION BY length(name) > 3 ORDER BY id) AS rn
        FROM VALUES (1, 'a', 'bob'), (1, 'b', 'alexander'), (1, 'c', 'al'),
                    (2, 'd', CAST(NULL AS STRING)) AS t(k, id, name)
        ORDER BY id
        """
      Then query result ordered
        | id | rn |
        | a  | 1  |
        | b  | 1  |
        | c  | 2  |
        | d  | 1  |

    Scenario: rank ordered by a descending computed key
      When query
        """
        SELECT id, rank() OVER (ORDER BY abs(length(name) - 4) > 1 DESC, id) AS r
        FROM VALUES (1, 'a', 'bob'), (1, 'b', 'alexander'), (1, 'c', 'al'),
                    (2, 'd', CAST(NULL AS STRING)) AS t(k, id, name)
        ORDER BY id
        """
      Then query result ordered
        | id | r |
        | a  | 3 |
        | b  | 1 |
        | c  | 2 |
        | d  | 4 |

  Rule: An outer ORDER BY on grouping() still sees the aggregate below computed keys

    Scenario: row_number over a computed aggregate key with ROLLUP and ORDER BY grouping
      When query
        """
        SELECT name, row_number() OVER (ORDER BY sum(age) + 1) AS rn
        FROM VALUES (2, 'Alice'), (5, 'Bob') AS t(age, name)
        GROUP BY ROLLUP(name)
        ORDER BY grouping(name), name
        """
      Then query result ordered
        | name  | rn |
        | Alice | 1  |
        | Bob   | 2  |
        | NULL  | 3  |

    Scenario: rank partitioned by grouping and ordered by a computed aggregate key
      When query
        """
        SELECT name, rank() OVER (PARTITION BY grouping(name) ORDER BY sum(age) * -1) AS r
        FROM VALUES (2, 'Alice'), (5, 'Bob'), (4, 'Bob') AS t(age, name)
        GROUP BY ROLLUP(name)
        ORDER BY grouping(name), name
        """
      Then query result ordered
        | name  | r |
        | Alice | 2 |
        | Bob   | 1 |
        | NULL  | 1 |
