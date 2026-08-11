Feature: map output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to map yields the schema Spark declares
      When query
        """
        SELECT map(1.0, '2', 3.0, '4') AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: decimal(2,1)
         |    |-- value: string (valueContainsNull = false)
        """

    @sail-bug
    Scenario: a nullable column input to map stays nullable
      When query
        """
        SELECT map(c, '2', 3.0, '4') AS result FROM VALUES (1.0), (CAST(NULL AS DECIMAL(2,1))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: decimal(2,1)
         |    |-- value: string (valueContainsNull = false)
        """

  Rule: VALUES coercion

    Scenario Outline: VALUES widens a bare NULL to the map type with NULL <position>
      When query
        """
        SELECT id, value, typeof(value) AS type
        FROM VALUES <values> AS t(id, value)
        ORDER BY id
        """
      Then query result
        | id | value       | type         |
        | 1  | NULL        | map<int,int> |
        | 2  | {1 -> NULL} | map<int,int> |
        | 3  | {1 -> 2}    | map<int,int> |

      Examples:
        | position | values                                           |
        | first    | (1, NULL), (2, map(1, NULL)), (3, map(1, 2))     |
        | last     | (2, map(1, NULL)), (3, map(1, 2)), (1, NULL)     |

    Scenario: VALUES ignores an empty map while finding the concrete map type
      When query
        """
        SELECT id, value, typeof(value) AS type
        FROM VALUES
          (1, map()),
          (2, map(1, NULL)),
          (3, map(1, 2)) AS t(id, value)
        ORDER BY id
        """
      Then query result
        | id | value       | type         |
        | 1  | {}          | map<int,int> |
        | 2  | {1 -> NULL} | map<int,int> |
        | 3  | {1 -> 2}    | map<int,int> |

    Scenario Outline: VALUES preserves an empty map type with NULL <position>
      When query
        """
        SELECT id, value, typeof(value) AS type
        FROM VALUES <values> AS t(id, value)
        ORDER BY id
        """
      Then query result
        | id | value | type           |
        | 1  | NULL  | map<void,void> |
        | 2  | {}    | map<void,void> |

      Examples:
        | position | values                 |
        | first    | (1, NULL), (2, map())  |
        | last     | (2, map()), (1, NULL)  |

    Scenario: VALUES recursively widens a NULL nested map value
      When query
        """
        SELECT id, value, typeof(value) AS type
        FROM VALUES
          (1, map(1, map(2, NULL))),
          (2, map(1, map(2, 3))) AS t(id, value)
        ORDER BY id
        """
      Then query result
        | id | value              | type                  |
        | 1  | {1 -> {2 -> NULL}} | map<int,map<int,int>> |
        | 2  | {1 -> {2 -> 3}}    | map<int,map<int,int>> |
