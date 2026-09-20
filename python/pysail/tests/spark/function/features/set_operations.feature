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

  # Note: INTERSECT ALL tests excluded — DataFusion's LogicalPlanBuilder::intersect(is_all=true)
  # produces extra duplicates compared to Spark. Pre-existing upstream bug.
  # https://github.com/apache/datafusion/issues/12955

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

  Rule: A set operation reconciles the column types of both sides

    @sail-bug
    Scenario: the wider type of the two sides is the type of the result
      When query
        """
        SELECT CAST(1 AS INT) AS a UNION ALL SELECT CAST(2 AS BIGINT) AS a
        """
      Then query schema
        """
        root
         |-- a: long (nullable = false)
        """

    @sail-bug
    Scenario: a value that cannot be cast to the reconciled type is rejected
      When query
        """
        SELECT 1 AS a UNION ALL SELECT 'x' AS a
        """
      Then query error CAST_INVALID_INPUT

  Rule: A set operation rejects a map-typed column

    Scenario: selecting distinct rows of a map column is rejected
      When query
        """
        SELECT DISTINCT m FROM (SELECT map('a', 1) AS m UNION ALL SELECT map('a', 1))
        """
      Then query error SET_OPERATION_ON_MAP_TYPE

  Rule: The type in the message is written the way Spark writes a type in SQL

    # Spark renders it with `toSQLType`, which is `DataType.sql`, and not with the upper cased
    # `simpleString` a plan schema is rendered with: a comma is followed by a space, and a struct
    # field keeps the case it was declared with and is quoted when it needs to be.
    Scenario Outline: the type of <case>
      When query
        """
        SELECT <value> AS m UNION SELECT <other>
        """
      Then query error is "<rendered>"\.

      Examples:
        | case             | value                 | other                 | rendered                      |
        | a map            | map('a', 1)           | map('b', 2)           | MAP<STRING, INT>              |
        | a map of maps    | map('a', map('b', 1)) | map('c', map('d', 2)) | MAP<STRING, MAP<STRING, INT>> |
        | an array of maps | array(map('a', 1))    | array(map('b', 2))    | ARRAY<MAP<STRING, INT>>       |

    @sail-bug
    # The rendering is right; what differs is the schema behind it. Spark builds the fields of a
    # struct literal as non-nullable and Sail builds them nullable, so the ` NOT NULL` the type
    # carries never appears. The cause is pinned on its own in
    # `test_a_struct_literal_builds_non_nullable_fields`, and both go green together.
    Scenario Outline: the type of <case> says which of its fields cannot be null
      When query
        """
        SELECT <value> AS m UNION SELECT <other>
        """
      Then query error is "<rendered>"\.

      Examples:
        | case                   | value                                  | other                                  | rendered                                              |
        | a struct of one field  | named_struct('MiCampo', map('a', 1))   | named_struct('MiCampo', map('b', 2))   | STRUCT<MiCampo: MAP<STRING, INT> NOT NULL>            |
        | a struct of two fields | named_struct('x', map('a', 1), 'y', 2) | named_struct('x', map('b', 2), 'y', 3) | STRUCT<x: MAP<STRING, INT> NOT NULL, y: INT NOT NULL> |
        | a field needing quotes | named_struct('mi campo', map('a', 1))  | named_struct('mi campo', map('b', 2))  | STRUCT<`mi campo`: MAP<STRING, INT> NOT NULL>         |

    Scenario Outline: <case> of a map column is rejected
      # Every operation that has to compare whole rows reaches the same check, including the ALL
      # forms, which keep the duplicates but still compare.
      When query
        """
        SELECT m FROM (SELECT map('a', 1) AS m) <operation> SELECT map('a', 1)
        """
      Then query error SET_OPERATION_ON_MAP_TYPE

      Examples:
        | case          | operation     |
        | a union       | UNION         |
        | an intersect  | INTERSECT     |
        | an intersect all | INTERSECT ALL |
        | an except     | EXCEPT        |
        | an except all | EXCEPT ALL    |

    Scenario: a union all of a map column is allowed
      # `UNION ALL` keeps every row as it is and compares nothing, so the map is not a problem.
      When query
        """
        SELECT m FROM (SELECT map('a', 1) AS m) UNION ALL SELECT map('b', 2)
        """
      Then query result
        | m          |
        | {a -> 1}   |
        | {b -> 2}   |

    Scenario: the map is found inside a struct
      When query
        """
        SELECT DISTINCT s FROM (SELECT named_struct('m', map('a', 1)) AS s)
        """
      Then query error SET_OPERATION_ON_MAP_TYPE

    Scenario: the map is found inside an array
      When query
        """
        SELECT DISTINCT a FROM (SELECT array(map('a', 1)) AS a)
        """
      Then query error SET_OPERATION_ON_MAP_TYPE

    Scenario: a map beside the columns that are compared is not a problem
      # Only the columns that are compared have to be ordered, so a map that rides along is fine.
      When query
        """
        SELECT DISTINCT k FROM (SELECT 1 AS k, map('a', 1) AS m)
        """
      Then query result
        | k |
        | 1 |
