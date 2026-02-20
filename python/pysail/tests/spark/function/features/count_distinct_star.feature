Feature: COUNT(DISTINCT *) function

  Rule: COUNT(DISTINCT *) counts distinct rows

    Scenario: count distinct star with duplicates
      When query
        """
        SELECT COUNT(DISTINCT *) FROM VALUES (1, 1.0, 'a'), (2, 2.0, 'b'), (1, 1.0, 'a') AS t(a, b, c)
        """
      Then query result
        | count(DISTINCT a, b, c) |
        | 2                       |

    Scenario: count distinct star all same
      When query
        """
        SELECT COUNT(DISTINCT *) FROM VALUES (1, 1), (1, 1), (1, 1) AS t(a, b)
        """
      Then query result
        | count(DISTINCT a, b) |
        | 1                    |

    Scenario: count distinct star all different
      When query
        """
        SELECT COUNT(DISTINCT *) FROM VALUES (1, 1), (2, 2), (3, 3) AS t(a, b)
        """
      Then query result
        | count(DISTINCT a, b) |
        | 3                    |

    Scenario: count distinct star single column
      When query
        """
        SELECT COUNT(DISTINCT *) FROM VALUES (1), (2), (1) AS t(a)
        """
      Then query result
        | count(DISTINCT a) |
        | 2                 |

    Scenario: count distinct star with group by
      When query
        """
        SELECT g, COUNT(DISTINCT *) FROM VALUES ('x', 1), ('x', 1), ('x', 2), ('y', 1) AS t(g, v) GROUP BY g ORDER BY g
        """
      Then query result ordered
        | g | count(DISTINCT g, v) |
        | x | 2                    |
        | y | 1                    |

    Scenario: count distinct star with mixed case column names
      When query
        """
        SELECT COUNT(DISTINCT *) FROM VALUES (1, 2), (1, 2), (3, 4) AS t(MyCol, UPPER_COL)
        """
      Then query result
        | count(DISTINCT MyCol, UPPER_COL) |
        | 2                                |
