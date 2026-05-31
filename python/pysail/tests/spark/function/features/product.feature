Feature: product returns the multiplicative product of all non-null input values

  Rule: product multiplies non-null values

    Scenario: product over a basic group-by from the PySpark doctest
      When query
        """
        SELECT mod3, product(value) AS p
        FROM (SELECT id % 3 AS mod3, id AS value FROM RANGE(10))
        GROUP BY mod3
        ORDER BY mod3
        """
      Then query result ordered
        | mod3 | p    |
        | 0    | 0.0  |
        | 1    | 28.0 |
        | 2    | 80.0 |

    Scenario: product over integer values returns a double
      When query
        """
        SELECT typeof(p) AS t FROM (
          SELECT product(col) AS p
          FROM VALUES (1), (2), (3) AS tab(col)
        )
        """
      Then query result
        | t      |
        | double |

    Scenario: product over float values
      When query
        """
        SELECT product(col) AS p
        FROM VALUES (1.5), (2.0), (4.0) AS tab(col)
        """
      Then query result
        | p    |
        | 12.0 |

  Rule: product handles null inputs

    Scenario: product skips null values
      When query
        """
        SELECT product(col) AS p
        FROM VALUES (2), (CAST(NULL AS INT)), (3), (4) AS tab(col)
        """
      Then query result
        | p    |
        | 24.0 |

    Scenario: product over all-null group returns null
      When query
        """
        SELECT product(col) AS p
        FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS tab(col)
        """
      Then query result
        | p    |
        | NULL |

    Scenario: product over an empty group returns null
      When query
        """
        SELECT product(col) AS p
        FROM (SELECT 1 AS col WHERE false) AS tab
        """
      Then query result
        | p    |
        | NULL |

  Rule: product supports group-by with null groups

    Scenario: product groups including a null-only group
      When query
        """
        SELECT g, product(v) AS p
        FROM VALUES
          ('a', 2),
          ('a', 5),
          ('b', CAST(NULL AS INT)),
          ('b', CAST(NULL AS INT)),
          ('c', 3),
          ('c', CAST(NULL AS INT)),
          ('c', 4)
          AS tab(g, v)
        GROUP BY g
        ORDER BY g
        """
      Then query result ordered
        | g | p    |
        | a | 10.0 |
        | b | NULL |
        | c | 12.0 |
