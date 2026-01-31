Feature: ntile window function with Spark-compatible distribution

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
