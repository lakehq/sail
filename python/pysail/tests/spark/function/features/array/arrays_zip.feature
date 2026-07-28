@arrays_zip
Feature: arrays_zip comprehensive tests

  Rule: Basic usage

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT arrays_zip(<args>) AS result <from>
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                     | args                                                           | from                               | result                                                                |
        | arrays_zip two arrays same length        | array(1,2,3), array('a','b','c')                               |                                    | [{1, a}, {2, b}, {3, c}]                                              |
        | arrays_zip three arrays                  | array(1,2), array('a','b'), array(true,false)                  |                                    | [{1, a, true}, {2, b, false}]                                         |
        | arrays_zip single array                  | array(1,2,3)                                                   |                                    | [{1}, {2}, {3}]                                                       |
        | arrays_zip four args asymmetric          | array(1), array('a','b'), array(true, false, NULL), array(1.0) |                                    | [{1, a, true, 1.0}, {NULL, b, false, NULL}, {NULL, NULL, NULL, NULL}] |
        | arrays_zip self-zip same column          | a, a                                                           | FROM VALUES (array(1,2,3)) AS t(a) | [{1, 1}, {2, 2}, {3, 3}]                                              |
        | arrays_zip zero args returns empty array |                                                                |                                    | []                                                                    |

  Rule: Different array lengths

    Scenario Outline: Different lengths: <case>
      When query
        """
        SELECT arrays_zip(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | args                                 | result                                               |
        | arrays_zip first longer pads NULL  | array(1,2,3), array('a','b')         | [{1, a}, {2, b}, {3, NULL}]                          |
        | arrays_zip second longer pads NULL | array(1,2), array('a','b','c')       | [{1, a}, {2, b}, {NULL, c}]                          |
        | arrays_zip one empty array         | array(1,2), array()                  | [{1, NULL}, {2, NULL}]                               |
        | arrays_zip both empty arrays       | array(), array()                     | []                                                   |
        | arrays_zip very asymmetric 1 vs 5  | array(1), array('a','b','c','d','e') | [{1, a}, {NULL, b}, {NULL, c}, {NULL, d}, {NULL, e}] |

    Scenario: arrays_zip four args all empty returns empty
      When query
        """
        SELECT arrays_zip(
          CAST(array() AS ARRAY<INT>),
          CAST(array() AS ARRAY<INT>),
          CAST(array() AS ARRAY<INT>),
          CAST(array() AS ARRAY<INT>)
        ) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: NULL handling

    Scenario Outline: NULL input: <case>
      When query
        """
        SELECT arrays_zip(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                       | args                                                     |
        | arrays_zip untyped NULL array returns NULL | NULL, array(1,2)                                         |
        | arrays_zip typed NULL array returns NULL   | CAST(NULL AS ARRAY<INT>), array(1,2)                     |
        | arrays_zip both args NULL returns NULL     | CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<STRING>)    |
        | arrays_zip NULL and empty returns NULL     | CAST(NULL AS ARRAY<INT>), CAST(array() AS ARRAY<STRING>) |

    Scenario: arrays_zip four NULL args returns NULL
      When query
        """
        SELECT arrays_zip(
          CAST(NULL AS ARRAY<INT>),
          CAST(NULL AS ARRAY<STRING>),
          CAST(NULL AS ARRAY<BOOLEAN>),
          CAST(NULL AS ARRAY<DOUBLE>)
        ) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario Outline: NULL elements: <case>
      When query
        """
        SELECT arrays_zip(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                | args                                | result                       |
        | arrays_zip with NULL elements                       | array(1,NULL,3), array('a','b','c') | [{1, a}, {NULL, b}, {3, c}]  |
        | arrays_zip all NULL elements                        | array(NULL,NULL), array(NULL,NULL)  | [{NULL, NULL}, {NULL, NULL}] |
        | arrays_zip untyped empty array() pads first as NULL | array(), array(1,2)                 | [{NULL, 1}, {NULL, 2}]       |

    # Columnar path: neither column is fully NULL, so the invoke short-circuits do
    # not fire — the combined validity mask makes every row NULL and the result
    # values struct ends up empty. This exercises the flatten build with empty
    # `take` outputs (the path that replaced the removed all-null special case).
    Scenario: arrays_zip all rows NULL via combined validity across columns
      When query
        """
        SELECT arrays_zip(a, b) AS result
        FROM VALUES
          (array(1), CAST(NULL AS ARRAY<INT>)),
          (CAST(NULL AS ARRAY<INT>), array(2))
        AS t(a, b)
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |

    Scenario: arrays_zip mixed valid and NULL rows via a per-row NULL array
      When query
        """
        SELECT arrays_zip(a, b) AS result
        FROM VALUES
          (array(1,2), array('x','y')),
          (array(3), CAST(NULL AS ARRAY<STRING>))
        AS t(a, b)
        """
      Then query result ordered
        | result           |
        | [{1, x}, {2, y}] |
        | NULL             |

  Rule: Columnar multi-row paths (flatten build)
    # Multi-row FROM VALUES columns exercise the flatten kernel's per-row offset
    # and null-pad logic that single-row literals never reach.

    Scenario Outline: Columnar: <case>
      When query
        """
        SELECT arrays_zip(a, b) AS result
        FROM VALUES <values> AS t(a, b)
        """
      Then query result ordered
        | result |
        | <row1> |
        | <row2> |

      Examples:
        | case                                            | values                                                     | row1                           | row2                           |
        | arrays_zip empty rows in a column               | (array(), array()), (array(), array())                     | []                             | []                             |
        | arrays_zip ragged lengths per row in a column   | (array(1,2,3), array('a')), (array(4), array('b','c','d')) | [{1, a}, {2, NULL}, {3, NULL}] | [{4, b}, {NULL, c}, {NULL, d}] |
        | arrays_zip empty and non-empty rows in a column | (array(), array(1)), (array(2), array())                   | [{NULL, 1}]                    | [{2, NULL}]                    |

  Rule: Type variety

    Scenario Outline: Type variety: <case>
      When query
        """
        SELECT arrays_zip(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                            | args                                                                         | result                                       |
        | arrays_zip int and double       | array(1,2), array(1.5,2.5)                                                   | [{1, 1.5}, {2, 2.5}]                         |
        | arrays_zip nested arrays        | array(array(1,2),array(3,4)), array('a','b')                                 | [{[1, 2], a}, {[3, 4], b}]                   |
        | arrays_zip boolean and int      | array(true,false), array(1,2)                                                | [{true, 1}, {false, 2}]                      |
        | arrays_zip struct elements      | array(struct(1 AS a)), array(struct('x' AS b))                               | [{{1}, {x}}]                                 |
        | arrays_zip map elements         | array(map(1,'a')), array(map(2,'b'))                                         | [{{1 -> a}, {2 -> b}}]                       |
        | arrays_zip binary elements      | array(X'01', X'02'), array(X'0A', X'0B')                                     | [{[01], [0A]}, {[02], [0B]}]                 |
        | arrays_zip date elements        | array(DATE'2024-01-01'), array(DATE'2024-12-31')                             | [{2024-01-01, 2024-12-31}]                   |
        | arrays_zip timestamp elements   | array(TIMESTAMP'2024-01-01 00:00:00'), array(TIMESTAMP'2024-12-31 23:59:59') | [{2024-01-01 00:00:00, 2024-12-31 23:59:59}] |
        | arrays_zip deeply nested arrays | array(array(1,2)), array(array(3,4))                                         | [{[1, 2], [3, 4]}]                           |
        | arrays_zip sequence results     | sequence(1, 3), sequence(10, 12)                                             | [{1, 10}, {2, 11}, {3, 12}]                  |

    Scenario: arrays_zip decimal elements
      When query
        """
        SELECT arrays_zip(
          array(CAST(1.5 AS DECIMAL(10,2)), CAST(2.5 AS DECIMAL(10,2))),
          array(1, 2)
        ) AS result
        """
      Then query result
        | result                 |
        | [{1.50, 1}, {2.50, 2}] |

    Scenario: arrays_zip float32 elements
      When query
        """
        SELECT arrays_zip(
          array(CAST(1.5 AS FLOAT)),
          array(CAST(2.5 AS FLOAT))
        ) AS result
        """
      Then query result
        | result       |
        | [{1.5, 2.5}] |

  Rule: Composition

    Scenario: arrays_zip nested in arrays_zip
      When query
        """
        SELECT arrays_zip(arrays_zip(array(1,2), array('a','b')), array(true, false)) AS result
        """
      Then query result
        | result                            |
        | [{{1, a}, true}, {{2, b}, false}] |

    Scenario Outline: arrays_zip element field access position <field>
      When query
        """
        SELECT arrays_zip(array(1,2,3), array('x','y','z'))[1].`<field>` AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | field | result |
        | 0     | 2      |
        | 1     | y      |

    Scenario: arrays_zip to_json roundtrip
      When query
        """
        SELECT to_json(arrays_zip(array(1,2), array('a','b'))) AS result
        """
      Then query result
        | result                            |
        | [{"0":1,"1":"a"},{"0":2,"1":"b"}] |

    Scenario: arrays_zip LATERAL VIEW explode flatten
      When query
        """
        SELECT e.`0` AS x, e.`1` AS y FROM
          (SELECT arrays_zip(array(1,2,3), array('a','b','c')) AS z) LATERAL VIEW explode(z) AS e
        """
      Then query result ordered
        | x | y |
        | 1 | a |
        | 2 | b |
        | 3 | c |

  Rule: Multi-row

    Scenario: arrays_zip multi-row
      When query
        """
        SELECT arrays_zip(a, b) AS result FROM VALUES (array(1,2), array('x','y')), (array(3), array('z','w')), (CAST(NULL AS ARRAY<INT>), array('a')) AS t(a, b)
        """
      Then query result
        | result              |
        | [{1, x}, {2, y}]    |
        | [{3, z}, {NULL, w}] |
        | NULL                |

    Scenario: arrays_zip multi-row uneven lengths per row
      When query
        """
        SELECT arrays_zip(a, b) AS result FROM VALUES
          (array(1,2,3), array('x','y')),
          (array(1), array('a','b','c','d','e')),
          (array(), array('z'))
        AS t(a, b)
        """
      Then query result
        | result                                               |
        | [{1, x}, {2, y}, {3, NULL}]                          |
        | [{1, a}, {NULL, b}, {NULL, c}, {NULL, d}, {NULL, e}] |
        | [{NULL, z}]                                          |

    Scenario: arrays_zip multi-row empty row mixed with NULL
      When query
        """
        SELECT arrays_zip(a, b) AS result FROM VALUES
          (array(), CAST(array() AS ARRAY<STRING>)),
          (array(1), array('x')),
          (CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<STRING>))
        AS t(a, b)
        """
      Then query result
        | result   |
        | []       |
        | [{1, x}] |
        | NULL     |

    Scenario: arrays_zip multi-row all-null column returns all NULL
      When query
        """
        SELECT arrays_zip(a, b) AS result FROM VALUES
          (CAST(NULL AS ARRAY<INT>), array(1, 2)),
          (CAST(NULL AS ARRAY<INT>), array(3, 4)),
          (CAST(NULL AS ARRAY<INT>), array(5, 6))
        AS t(a, b)
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |
        | NULL   |

  Rule: Error conditions

    Scenario Outline: Error: <case>
      When query
        """
        SELECT arrays_zip(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                                        | args                |
        | arrays_zip non-array input errors           | 1, 2                |
        | arrays_zip mixed array and non-array errors | array(1,2), 'hello' |

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: non-null array literals yield a non-nullable array
      When query
        """
        SELECT arrays_zip(array(1, 2), array(3, 4)) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: struct (containsNull = false)
         |    |    |-- 0: integer (nullable = true)
         |    |    |-- 1: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null array column yields a non-nullable array
      When query
        """
        SELECT arrays_zip(array(id), array(id)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: struct (containsNull = false)
         |    |    |-- 0: long (nullable = true)
         |    |    |-- 1: long (nullable = true)
        """

    Scenario: a nullable array column stays nullable
      When query
        """
        SELECT arrays_zip(c, c) AS result FROM VALUES (array(1)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: struct (containsNull = false)
         |    |    |-- c: integer (nullable = true)
         |    |    |-- c: integer (nullable = true)
        """

    @sail-bug
    Scenario: nullable input elements propagate into the struct fields
      When query
        """
        SELECT arrays_zip(c, c) AS result FROM VALUES (array(1, CAST(NULL AS INT))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: struct (containsNull = false)
         |    |    |-- c: integer (nullable = true)
         |    |    |-- c: integer (nullable = true)
        """

  Rule: Result values (migrated from test_arrays_zip.txt doctests)

    Scenario Outline: Result values: <case>
      When query
        """
        SELECT arrays_zip(<cols>) AS <alias> FROM VALUES (<values>) AS t(<cols>)
        """
      Then query result
        | <alias>  |
        | <result> |

      Examples:
        | case                           | cols                 | alias  | values                                              | result                               |
        | arrays_zip doctest #1 (result) | vals1, vals2, vals3  | zipped | array(1L, 2L, 3L), array(2L, 4L, 6L), array(3L, 6L) | [{1, 2, 3}, {2, 4, 6}, {3, 6, NULL}] |
        | arrays_zip doctest #3 (result) | nums, letters        | r      | array(1, 2, 3), array('a', 'b', 'c')                | [{1, a}, {2, b}, {3, c}]             |
        | arrays_zip doctest #4 (result) | nums, letters        | r      | array(1, 2), array('a', 'b', 'c')                   | [{1, a}, {2, b}, {NULL, c}]          |
        | arrays_zip doctest #5 (result) | nums, letters, bools | r      | array(1, 2), array('a', 'b'), array(true, false)    | [{1, a, true}, {2, b, false}]        |
        | arrays_zip doctest #6 (result) | nums, letters        | r      | array(1, 2, NULL), array('a', NULL, 'c')            | [{1, a}, {2, NULL}, {NULL, c}]       |

  Rule: Output schema (migrated from test_arrays_zip.txt printSchema doctests)

    Scenario: arrays_zip doctest #2 (schema)
      When query
        """
        SELECT arrays_zip(vals1, vals2, vals3) AS zipped FROM VALUES (array(1L, 2L, 3L), array(2L, 4L, 6L), array(3L, 6L)) AS t(vals1, vals2, vals3)
        """
      Then query schema
        """
        root
         |-- zipped: array (nullable = true)
         |    |-- element: struct (containsNull = false)
         |    |    |-- vals1: long (nullable = true)
         |    |    |-- vals2: long (nullable = true)
         |    |    |-- vals3: long (nullable = true)
        """
