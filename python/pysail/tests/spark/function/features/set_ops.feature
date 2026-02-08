Feature: Set operations (INTERSECT, EXCEPT)

  Rule: INTERSECT DISTINCT

    Scenario: intersect distinct two tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (3), (4), (5), (6), (7)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 3  |
        | 4  |
        | 5  |

    Scenario: intersect distinct removes duplicates
      When query
        """
        SELECT * FROM (VALUES (1), (1), (2), (2), (3)) AS a(id)
        INTERSECT DISTINCT
        SELECT * FROM (VALUES (1), (2), (2), (4)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: intersect distinct three tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (2), (3), (4), (5), (6)) AS b(id)
        INTERSECT
        SELECT * FROM (VALUES (3), (4), (5), (6), (7)) AS c(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 3  |
        | 4  |
        | 5  |

  Rule: INTERSECT ALL

    Scenario: intersect all preserves duplicates
      When query
        """
        SELECT * FROM (VALUES (1), (1), (2), (2), (3)) AS a(id)
        INTERSECT ALL
        SELECT * FROM (VALUES (1), (2), (2), (4)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 2  |

    Scenario: intersect all three tables
      When query
        """
        SELECT * FROM (VALUES (1), (1), (1), (2), (2), (3)) AS a(id)
        INTERSECT ALL
        SELECT * FROM (VALUES (1), (1), (2), (2), (2)) AS b(id)
        INTERSECT ALL
        SELECT * FROM (VALUES (1), (2), (2)) AS c(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 2  |

  Rule: EXCEPT DISTINCT

    Scenario: except distinct two tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        EXCEPT
        SELECT * FROM (VALUES (3), (4), (5), (6), (7)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: except distinct removes duplicates
      When query
        """
        SELECT * FROM (VALUES (1), (1), (2), (2), (3)) AS a(id)
        EXCEPT DISTINCT
        SELECT * FROM (VALUES (2), (4)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 3  |

    Scenario: except distinct three tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        EXCEPT
        SELECT * FROM (VALUES (4), (5), (6)) AS b(id)
        EXCEPT
        SELECT * FROM (VALUES (1), (7)) AS c(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 2  |
        | 3  |

  Rule: EXCEPT ALL

    Scenario: except all preserves duplicates
      When query
        """
        SELECT * FROM (VALUES (1), (1), (2), (2), (3)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (2)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |

    Scenario: except all three tables
      When query
        """
        SELECT * FROM (VALUES (1), (1), (1), (2), (2), (3), (3)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (2), (3)) AS b(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (3)) AS c(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: except all subtracts matching count
      When query
        """
        SELECT * FROM (VALUES (1), (1), (1)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (1)) AS b(id)
        """
      Then query result
        | id |
        | 1  |

  Rule: Wide table set operations

    Scenario: intersect distinct with multiple columns
      When query
        """
        SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS a(id, name)
        INTERSECT
        SELECT * FROM (VALUES (2, 'b'), (3, 'c'), (4, 'd')) AS b(id, name)
        ORDER BY id
        """
      Then query result ordered
        | id | name |
        | 2  | b    |
        | 3  | c    |

    Scenario: except all with multiple columns
      When query
        """
        SELECT * FROM (VALUES (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')) AS a(id, name)
        EXCEPT ALL
        SELECT * FROM (VALUES (1, 'a'), (3, 'c')) AS b(id, name)
        ORDER BY id
        """
      Then query result ordered
        | id | name |
        | 1  | a    |
        | 2  | b    |

  Rule: Null handling

    Scenario: intersect with nulls
      When query
        """
        SELECT * FROM (VALUES (1), (NULL), (3)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (NULL), (3), (4)) AS b(id)
        ORDER BY id ASC NULLS LAST
        """
      Then query result ordered
        | id   |
        | 3    |
        | NULL |

    Scenario: except all with nulls
      When query
        """
        SELECT * FROM (VALUES (1), (NULL), (NULL), (3)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (NULL), (3)) AS b(id)
        ORDER BY id ASC NULLS LAST
        """
      Then query result ordered
        | id   |
        | 1    |
        | NULL |

  Rule: Empty results

    Scenario: intersect with no common rows
      When query
        """
        SELECT * FROM (VALUES (1), (2)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (3), (4)) AS b(id)
        """
      Then query result
        | id |

    Scenario: except all removing everything
      When query
        """
        SELECT * FROM (VALUES (1), (2)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (2), (3)) AS b(id)
        """
      Then query result
        | id |
