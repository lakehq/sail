Feature: VALUES relation output schema

  Rule: Column nullability

    Scenario: non-null VALUES literals produce a non-nullable column
      When query
        """
        SELECT id FROM VALUES (1), (2) AS t(id)
        """
      Then query schema
        """
        root
         |-- id: integer (nullable = false)
        """

  Rule: Nested values keep the column type

    Scenario: VALUES rows of arrays of structs
      When query
        """
        SELECT g, x
        FROM VALUES (0, array(named_struct('a', 2))), (1, array(named_struct('a', 1), named_struct('a', 3))) AS t(g, x)
        """
      Then query result ordered
        | g | x          |
        | 0 | [{2}]      |
        | 1 | [{1}, {3}] |

    Scenario: a single VALUES row of an array of structs
      When query
        """
        SELECT x FROM VALUES (array(named_struct('a', 2))) AS t(x)
        """
      Then query result
        | x     |
        | [{2}] |

    Scenario: VALUES rows of arrays of structs with a NULL field
      When query
        """
        SELECT g, x
        FROM VALUES (0, array(named_struct('a', 2))), (1, array(named_struct('a', CAST(NULL AS INT)))) AS t(g, x)
        """
      Then query result ordered
        | g | x        |
        | 0 | [{2}]    |
        | 1 | [{NULL}] |

    Scenario: expressions over VALUES rows of arrays of structs
      When query
        """
        SELECT g, size(x) AS n, x[0].a AS a
        FROM VALUES (0, array(named_struct('a', 2))), (1, array(named_struct('a', 1), named_struct('a', 3))) AS t(g, x)
        ORDER BY g
        """
      Then query result ordered
        | g | n | a |
        | 0 | 1 | 2 |
        | 1 | 2 | 1 |
