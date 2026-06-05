@ntile
Feature: ntile window function comprehensive tests

  Rule: Ntile distributes extra rows to first buckets (Spark behavior)

    Scenario: ntile(4) over 10 rows - extra rows go to first buckets
      When query
        """
        SELECT id, ntile(4) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10) AS t(id)
        """
      Then query result
        | id | bucket |
        | 1  | 1      |
        | 2  | 1      |
        | 3  | 1      |
        | 4  | 2      |
        | 5  | 2      |
        | 6  | 2      |
        | 7  | 3      |
        | 8  | 3      |
        | 9  | 4      |
        | 10 | 4      |

    Scenario: ntile(3) over 10 rows - first bucket gets extra row
      When query
        """
        SELECT id, ntile(3) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10) AS t(id)
        """
      Then query result
        | id | bucket |
        | 1  | 1      |
        | 2  | 1      |
        | 3  | 1      |
        | 4  | 1      |
        | 5  | 2      |
        | 6  | 2      |
        | 7  | 2      |
        | 8  | 3      |
        | 9  | 3      |
        | 10 | 3      |

    Scenario: ntile(1) over multiple rows - all in one bucket
      When query
        """
        SELECT id, ntile(1) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3), (4), (5) AS t(id)
        """
      Then query result
        | id | bucket |
        | 1  | 1      |
        | 2  | 1      |
        | 3  | 1      |
        | 4  | 1      |
        | 5  | 1      |

    Scenario: ntile with more buckets than rows
      When query
        """
        SELECT id, ntile(10) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query result
        | id | bucket |
        | 1  | 1      |
        | 2  | 2      |
        | 3  | 3      |

    Scenario: ntile with equal buckets and rows
      When query
        """
        SELECT id, ntile(5) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3), (4), (5) AS t(id)
        """
      Then query result
        | id | bucket |
        | 1  | 1      |
        | 2  | 2      |
        | 3  | 3      |
        | 4  | 4      |
        | 5  | 5      |

  Rule: Ntile with partitions

    Scenario: ntile with PARTITION BY
      When query
        """
        SELECT group_id, id, ntile(2) OVER (PARTITION BY group_id ORDER BY id) AS bucket
        FROM VALUES ('A', 1), ('A', 2), ('A', 3), ('B', 1), ('B', 2) AS t(group_id, id)
        ORDER BY group_id, id
        """
      Then query result
        | group_id | id | bucket |
        | A        | 1  | 1      |
        | A        | 2  | 1      |
        | A        | 3  | 2      |
        | B        | 1  | 1      |
        | B        | 2  | 2      |

  Rule: Edge cases

    Scenario: ntile single row
      When query
        """
        SELECT id, ntile(3) OVER (ORDER BY id) AS bucket
        FROM VALUES (1) AS t(id)
        """
      Then query result
        | id | bucket |
        | 1  | 1      |

    Scenario: ntile(2) even rows
      When query
        """
        SELECT id, ntile(2) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3), (4) AS t(id)
        """
      Then query result
        | id | bucket |
        | 1  | 1      |
        | 2  | 1      |
        | 3  | 2      |
        | 4  | 2      |

    Scenario: ntile(2) odd rows
      When query
        """
        SELECT id, ntile(2) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query result
        | id | bucket |
        | 1  | 1      |
        | 2  | 1      |
        | 3  | 2      |

    Scenario: ntile(100) with 5 rows
      When query
        """
        SELECT id, ntile(100) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3), (4), (5) AS t(id)
        """
      Then query result
        | id | bucket |
        | 1  | 1      |
        | 2  | 2      |
        | 3  | 3      |
        | 4  | 4      |
        | 5  | 5      |

    Scenario: ntile with DESC order
      When query
        """
        SELECT id, ntile(3) OVER (ORDER BY id DESC) AS bucket
        FROM VALUES (1), (2), (3), (4), (5) AS t(id)
        """
      Then query result
        | id | bucket |
        | 5  | 1      |
        | 4  | 1      |
        | 3  | 2      |
        | 2  | 2      |
        | 1  | 3      |

    Scenario: ntile with duplicate values
      When query
        """
        SELECT id, ntile(3) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (1), (2), (2), (3) AS t(id)
        """
      Then query result
        | id | bucket |
        | 1  | 1      |
        | 1  | 1      |
        | 2  | 2      |
        | 2  | 2      |
        | 3  | 3      |

    Scenario: ntile with expression argument
      When query
        """
        SELECT id, ntile(2+1) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query result
        | id | bucket |
        | 1  | 1      |
        | 2  | 2      |
        | 3  | 3      |

    Scenario: ntile empty result
      When query
        """
        SELECT id, ntile(3) OVER (ORDER BY id) AS bucket
        FROM VALUES (1) AS t(id) WHERE id > 10
        """
      Then query result
        | id | bucket |

  Rule: NULL handling

    Scenario: ntile with NULL in ORDER BY
      When query
        """
        SELECT id, ntile(2) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (NULL), (3), (2) AS t(id)
        """
      Then query result
        | id   | bucket |
        | NULL | 1      |
        | 1    | 1      |
        | 2    | 2      |
        | 3    | 2      |

    Scenario: ntile with NULL in PARTITION BY
      When query
        """
        SELECT grp, id, ntile(2) OVER (PARTITION BY grp ORDER BY id) AS bucket
        FROM VALUES ('A', 1), ('A', 2), (NULL, 3), (NULL, 4) AS t(grp, id)
        ORDER BY grp NULLS LAST, id
        """
      Then query result
        | grp  | id | bucket |
        | A    | 1  | 1      |
        | A    | 2  | 2      |
        | NULL | 3  | 1      |
        | NULL | 4  | 2      |

  Rule: Multiple windows

    Scenario: ntile multiple window functions
      When query
        """
        SELECT id, ntile(2) OVER (ORDER BY id) AS b2, ntile(3) OVER (ORDER BY id) AS b3
        FROM VALUES (1), (2), (3), (4), (5), (6) AS t(id)
        """
      Then query result
        | id | b2 | b3 |
        | 1  | 1  | 1  |
        | 2  | 1  | 1  |
        | 3  | 1  | 2  |
        | 4  | 2  | 2  |
        | 5  | 2  | 3  |
        | 6  | 2  | 3  |

  Rule: Argument type restrictions

    @sail-bug
    # Sail accepts TINYINT/SMALLINT via auto-coercion — Spark rejects all non-INT types
    Scenario: ntile SMALLINT arg errors (only INT allowed)
      When query
        """
        SELECT id, ntile(CAST(3 AS SMALLINT)) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query error .*

    @sail-bug
    # Sail accepts TINYINT via auto-coercion — Spark rejects all non-INT types
    Scenario: ntile TINYINT arg errors (only INT allowed)
      When query
        """
        SELECT id, ntile(CAST(3 AS TINYINT)) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query error .*

    @sail-bug
    # Sail accepts BIGINT — Spark only accepts INT
    Scenario: ntile BIGINT arg errors (only INT allowed)
      When query
        """
        SELECT id, ntile(CAST(3 AS BIGINT)) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query error .*

    Scenario: ntile NULL arg errors
      When query
        """
        SELECT id, ntile(NULL) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2) AS t(id)
        """
      Then query error .*

    Scenario: ntile FLOAT arg errors
      When query
        """
        SELECT id, ntile(3.5) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query error .*

    Scenario: ntile STRING arg errors
      When query
        """
        SELECT id, ntile('3') OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query error .*

    Scenario: ntile column reference errors (must be foldable)
      When query
        """
        SELECT id, ntile(id) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query error .*

    Scenario: ntile MAX_INT
      When query
        """
        SELECT id, ntile(2147483647) OVER (ORDER BY id) AS bucket
        FROM VALUES (1), (2), (3) AS t(id)
        """
      Then query result
        | id | bucket |
        | 1  | 1      |
        | 2  | 2      |
        | 3  | 3      |

  Rule: ORDER BY multiple columns

    Scenario: ntile with multi-column ORDER BY
      When query
        """
        SELECT a, b, ntile(2) OVER (ORDER BY a, b) AS bucket
        FROM VALUES (1, 'x'), (1, 'y'), (2, 'x'), (2, 'y') AS t(a, b)
        """
      Then query result
        | a | b | bucket |
        | 1 | x | 1      |
        | 1 | y | 1      |
        | 2 | x | 2      |
        | 2 | y | 2      |

  Rule: Large and uneven partitions

    Scenario: ntile(4) over 20 rows
      When query
        """
        SELECT id, ntile(4) OVER (ORDER BY id) AS bucket
        FROM VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20) AS t(id)
        """
      Then query result
        | id | bucket |
        | 1  | 1      |
        | 2  | 1      |
        | 3  | 1      |
        | 4  | 1      |
        | 5  | 1      |
        | 6  | 2      |
        | 7  | 2      |
        | 8  | 2      |
        | 9  | 2      |
        | 10 | 2      |
        | 11 | 3      |
        | 12 | 3      |
        | 13 | 3      |
        | 14 | 3      |
        | 15 | 3      |
        | 16 | 4      |
        | 17 | 4      |
        | 18 | 4      |
        | 19 | 4      |
        | 20 | 4      |

    Scenario: ntile with uneven partition sizes
      When query
        """
        SELECT grp, id, ntile(2) OVER (PARTITION BY grp ORDER BY id) AS bucket
        FROM VALUES ('A',1),('A',2),('A',3),('B',1),('B',2),('B',3),('B',4),('B',5) AS t(grp, id)
        ORDER BY grp, id
        """
      Then query result
        | grp | id | bucket |
        | A   | 1  | 1      |
        | A   | 2  | 1      |
        | A   | 3  | 2      |
        | B   | 1  | 1      |
        | B   | 2  | 1      |
        | B   | 3  | 1      |
        | B   | 4  | 2      |
        | B   | 5  | 2      |

  Rule: Error conditions

    Scenario: ntile(0) errors
      When query
        """
        SELECT id, ntile(0) OVER (ORDER BY id) AS bucket
        FROM VALUES (1) AS t(id)
        """
      Then query error .*

    Scenario: ntile negative errors
      When query
        """
        SELECT id, ntile(-1) OVER (ORDER BY id) AS bucket
        FROM VALUES (1) AS t(id)
        """
      Then query error .*
