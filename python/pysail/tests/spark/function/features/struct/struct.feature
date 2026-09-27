Feature: struct fields and output schema

  Rule: A null struct has null fields

    Scenario: struct field sorting preserves null parents and null children
      When query
        """
        SELECT t.s.a AS field
        FROM VALUES
          (named_struct('a', 2)),
          (CAST(NULL AS STRUCT<a: INT>)),
          (named_struct('a', 1)),
          (named_struct('a', CAST(NULL AS INT))) AS t(s)
        ORDER BY t.s.a ASC NULLS LAST
        """
      Then query result ordered
        | field |
        | 1     |
        | 2     |
        | NULL  |
        | NULL  |

    Scenario: extracting a nested struct or collection preserves parent nulls
      When query
        """
        SELECT s.inner.x AS x, s.items AS items, s.mapping AS mapping
        FROM VALUES
          (CAST(NULL AS STRUCT<inner: STRUCT<x: INT>, items: ARRAY<INT>, mapping: MAP<STRING, INT>>)),
          (named_struct('inner', named_struct('x', 7), 'items', array(1), 'mapping', map('a', 2))) AS t(s)
        ORDER BY x NULLS FIRST
        """
      Then query result ordered
        | x    | items | mapping  |
        | NULL | NULL  | NULL     |
        | 7    | [1]   | {a -> 2} |

    Scenario: extracting a struct field does not evaluate unused fields
      When query
        """
        SELECT named_struct('unused', raise_error('unused'),
                            'selected', named_struct('x', id)).selected.x AS x
        FROM range(1)
        """
      Then query result
        | x |
        | 0 |

    # `CASE WHEN ... THEN s END` sets `s` to NULL but keeps its children,
    # and these functions ignore the NULL map or array read from the NULL struct.
    @sail-bug
    Scenario: looking up a map field of a null struct returns NULL
      When query
        """
        SELECT id, (CASE WHEN id = 0 THEN s END).m['k'] AS value
        FROM VALUES
          (0, named_struct('m', map('k', 1))),
          (1, named_struct('m', map('k', 2))) AS t(id, s)
        ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 0  | 1     |
        | 1  | NULL  |

    @sail-bug
    Scenario: array_max over an array field of a null struct returns NULL
      When query
        """
        SELECT id, array_max((CASE WHEN id = 0 THEN s END).items) AS value
        FROM VALUES
          (0, named_struct('items', array(1))),
          (1, named_struct('items', array(2))) AS t(id, s)
        ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 0  | 1     |
        | 1  | NULL  |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to struct yields the schema Spark declares
      When query
        """
        SELECT struct(1, 2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- col1: integer (nullable = false)
         |    |-- col2: integer (nullable = false)
         |    |-- col3: integer (nullable = false)
        """

    Scenario: a non-null column input to struct yields the schema Spark declares
      When query
        """
        SELECT struct(CAST(id AS INT), 2, 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- col1: integer (nullable = false)
         |    |-- col2: integer (nullable = false)
         |    |-- col3: integer (nullable = false)
        """

    Scenario: a nullable column input to struct stays nullable
      When query
        """
        SELECT struct(c, 2, 3) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- c: integer (nullable = true)
         |    |-- col2: integer (nullable = false)
         |    |-- col3: integer (nullable = false)
        """
