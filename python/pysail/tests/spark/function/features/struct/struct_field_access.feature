Feature: struct field access propagates NULL correctly

  Rule: NULL struct propagation

    Scenario: field access on NULL struct returns NULL
      When query
        """
        SELECT abc.a AS result
        FROM VALUES
          (named_struct('a', 1, 'b', 'hello')),
          (CAST(NULL AS STRUCT<a: INT, b: STRING>)),
          (named_struct('a', 3, 'b', 'world'))
        AS t(abc)
        """
      Then query result
        | result |
        | 1      |
        | NULL   |
        | 3      |

    Scenario: string field access on NULL struct returns NULL
      When query
        """
        SELECT abc.b AS result
        FROM VALUES
          (named_struct('a', 1, 'b', 'hello')),
          (CAST(NULL AS STRUCT<a: INT, b: STRING>)),
          (named_struct('a', 3, 'b', 'world'))
        AS t(abc)
        """
      Then query result
        | result |
        | hello  |
        | NULL   |
        | world  |

    Scenario: nested struct field access on NULL struct returns NULL
      When query
        """
        SELECT abc.x.y AS result
        FROM VALUES
          (named_struct('x', named_struct('y', 1))),
          (CAST(NULL AS STRUCT<x: STRUCT<y: INT>>))
        AS t(abc)
        """
      Then query result
        | result |
        | 1      |
        | NULL   |

    Scenario: NULL field vs NULL struct are both NULL
      When query
        """
        SELECT abc.a AS result
        FROM VALUES
          (named_struct('a', CAST(NULL AS INT), 'b', 'hello')),
          (CAST(NULL AS STRUCT<a: INT, b: STRING>)),
          (named_struct('a', 5, 'b', 'world'))
        AS t(abc)
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |
        | 5      |

  Rule: Edge cases

    Scenario: all rows are NULL structs
      When query
        """
        SELECT abc.a AS result
        FROM VALUES
          (CAST(NULL AS STRUCT<a: INT>)),
          (CAST(NULL AS STRUCT<a: INT>))
        AS t(abc)
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |

    Scenario: boolean field with NULL struct row
      When query
        """
        SELECT abc.flag AS result
        FROM VALUES
          (named_struct('flag', true)),
          (CAST(NULL AS STRUCT<flag: BOOLEAN>)),
          (named_struct('flag', false))
        AS t(abc)
        """
      Then query result
        | result |
        | true   |
        | NULL   |
        | false  |

    Scenario: three levels deep nested field with NULL struct
      When query
        """
        SELECT abc.x.y.z AS result
        FROM VALUES
          (named_struct('x', named_struct('y', named_struct('z', 42)))),
          (CAST(NULL AS STRUCT<x: STRUCT<y: STRUCT<z: INT>>>))
        AS t(abc)
        """
      Then query result
        | result |
        | 42     |
        | NULL   |

    Scenario: middle level struct is NULL
      When query
        """
        SELECT abc.x.y AS result
        FROM VALUES
          (named_struct('x', named_struct('y', 1))),
          (named_struct('x', CAST(NULL AS STRUCT<y: INT>)))
        AS t(abc)
        """
      Then query result
        | result |
        | 1      |
        | NULL   |

    Scenario: array field inside NULL struct returns NULL
      When query
        """
        SELECT abc.arr AS result
        FROM VALUES
          (named_struct('arr', array(1,2,3))),
          (CAST(NULL AS STRUCT<arr: ARRAY<INT>>))
        AS t(abc)
        """
      Then query result
        | result    |
        | [1, 2, 3] |
        | NULL      |

    Scenario: struct field inside NULL struct returns NULL
      When query
        """
        SELECT abc.inner AS result
        FROM VALUES
          (named_struct('inner', named_struct('x', 1))),
          (CAST(NULL AS STRUCT<inner: STRUCT<x: INT>>))
        AS t(abc)
        """
      Then query result
        | result |
        | {1}    |
        | NULL   |

    Scenario: multiple fields extracted from NULL struct are all NULL
      When query
        """
        SELECT abc.a AS ra, abc.b AS rb
        FROM VALUES
          (named_struct('a', 1, 'b', 'x')),
          (CAST(NULL AS STRUCT<a: INT, b: STRING>))
        AS t(abc)
        """
      Then query result
        | ra   | rb   |
        | 1    | x    |
        | NULL | NULL |

    Scenario: empty string field vs NULL struct
      When query
        """
        SELECT abc.s AS result
        FROM VALUES
          (named_struct('s', '')),
          (CAST(NULL AS STRUCT<s: STRING>)),
          (named_struct('s', 'hello'))
        AS t(abc)
        """
      Then query result
        | result |
        |        |
        | NULL   |
        | hello  |
