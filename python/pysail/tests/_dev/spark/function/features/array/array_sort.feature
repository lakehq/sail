Feature: array_sort higher-order function

  Rule: No-comparator form — natural ascending order, NULLs last

    Scenario Outline: No-comparator form: <case>
      When query
        """
        SELECT array_sort(<arr>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                 | arr                                                 | result                     |
        | Sort integers ascending                              | array(3, 1, 2)                                      | [1, 2, 3]                  |
        | Sort integers with a null — null placed last         | array(3, NULL, 1)                                   | [1, 3, NULL]               |
        | Sort strings ascending                               | array('b', 'a', 'c')                                | [a, b, c]                  |
        | Sort doubles with NaN and Infinity — NaN is greatest | array(double('NaN'), 1.0, double('Infinity'), -1.0) | [-1.0, 1.0, Infinity, NaN] |
        | Sort null array returns null                         | CAST(NULL AS ARRAY<INT>)                            | NULL                       |

  Rule: No-comparator form — Spark element ordering

    Scenario Outline: No-comparator element ordering: <case>
      When query
        """
        SELECT array_sort(<arr>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                | arr                                                                                                                                                     | result                                                                           |
        | Struct with a NULL field sorts before other structs | array(named_struct('a', 1), named_struct('a', CAST(NULL AS INT)), NULL)                                                                                 | [{NULL}, {1}, NULL]                                                              |
        | Struct NULL fields are compared field by field      | array(named_struct('a', 1, 'b', CAST(NULL AS STRING)), named_struct('a', 1, 'b', 'x'), named_struct('a', CAST(NULL AS INT), 'b', 'y'))                  | [{NULL, y}, {1, NULL}, {1, x}]                                                   |
        | NULL elements of nested arrays sort first           | array(array(1), array(CAST(NULL AS INT)), array(), array(1, NULL), NULL, array(NULL, 1))                                                                | [[], [NULL], [NULL, 1], [1], [1, NULL], NULL]                                    |
        | Arrays of structs with a NULL field                 | array(array(named_struct('a', 2)), array(named_struct('a', CAST(NULL AS INT))), array(named_struct('a', 1), named_struct('a', CAST(NULL AS INT))))      | [[{NULL}], [{1}, {NULL}], [{2}]]                                                 |
        | Structs with nested arrays                          | array(named_struct('a', array(1, 2)), named_struct('a', array(CAST(NULL AS INT))), named_struct('a', CAST(NULL AS ARRAY<INT>)))                         | [{NULL}, {[NULL]}, {[1, 2]}]                                                     |
        | Negative zero before positive zero keeps its order  | array(-0.0D, 0.0D)                                                                                                                                      | [-0.0, 0.0]                                                                      |
        | Booleans                                            | array(true, NULL, false)                                                                                                                                | [false, true, NULL]                                                              |
        | Strings with an empty string                        | array('b', NULL, 'a', '')                                                                                                                               | [, a, b, NULL]                                                                   |
        | Binary                                              | array(X'02', NULL, X'01', X'')                                                                                                                          | [[], [01], [02], NULL]                                                           |
        | Decimals                                            | array(1.5BD, NULL, -2.25BD)                                                                                                                             | [-2.25, 1.50, NULL]                                                              |
        | Dates                                               | array(DATE'2020-01-02', NULL, DATE'2020-01-01')                                                                                                         | [2020-01-01, 2020-01-02, NULL]                                                   |
        | Timestamps                                          | array(TIMESTAMP'2020-01-02 00:00:00', NULL, TIMESTAMP'2020-01-01 00:00:00')                                                                             | [2020-01-01 00:00:00, 2020-01-02 00:00:00, NULL]                                 |
        | Timestamps without time zone                        | array(TIMESTAMP_NTZ'2020-01-02 00:00:00', NULL, TIMESTAMP_NTZ'2020-01-01 00:00:00')                                                                     | [2020-01-01 00:00:00, 2020-01-02 00:00:00, NULL]                                 |
        | Bigints at the extremes                             | array(9223372036854775807L, NULL, CAST('-9223372036854775808' AS BIGINT), 0L)                                                                           | [-9223372036854775808, 0, 9223372036854775807, NULL]                             |
        | Tinyints                                            | array(CAST(2 AS TINYINT), NULL, CAST(-3 AS TINYINT))                                                                                                    | [-3, 2, NULL]                                                                    |
        | Smallints                                           | array(CAST(2 AS SMALLINT), NULL, CAST(-3 AS SMALLINT))                                                                                                  | [-3, 2, NULL]                                                                    |
        | Floats                                              | array(CAST(2.5 AS FLOAT), NULL, CAST(-3.5 AS FLOAT))                                                                                                    | [-3.5, 2.5, NULL]                                                                |
        | Year-month intervals                                | array(INTERVAL '2' YEAR, NULL, INTERVAL '1-1' YEAR TO MONTH)                                                                                            | [INTERVAL '1-1' YEAR TO MONTH, INTERVAL '2-0' YEAR TO MONTH, NULL]               |
        | Day-time intervals                                  | array(INTERVAL '2 01:00:00' DAY TO SECOND, NULL, INTERVAL '1 02:00:00' DAY TO SECOND)                                                                   | [INTERVAL '1 02:00:00' DAY TO SECOND, INTERVAL '2 01:00:00' DAY TO SECOND, NULL] |
        | Only NULL elements                                  | array(CAST(NULL AS INT), NULL)                                                                                                                          | [NULL, NULL]                                                                     |
        | Empty array                                         | CAST(array() AS ARRAY<INT>)                                                                                                                             | []                                                                               |

    # TODO: Sail orders floats by IEEE 754 total order, so `-0.0` sorts before `0.0`.
    #   Spark treats `-0.0` and `0.0` as equal and its sort is stable, so they keep
    #   their input order. (Spark's own codegen fast path for `sort_array` on non-null
    #   doubles orders `-0.0` first; the expectations follow the stable rule that every
    #   other path uses.) This is part of the NaN and zero ordering follow-up.
    @sail-bug
    Scenario Outline: No-comparator float zero ordering: <case>
      When query
        """
        SELECT array_sort(<arr>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                               | arr                                                                                | result                                  |
        | Positive zero before negative zero keeps its order | array(0.0D, -0.0D)                                                                 | [0.0, -0.0]                             |
        | Double zeros among other values                    | array(1.0D, 0.0D, -0.0D, double('-Infinity'), NULL, -1.0D)                         | [-Infinity, -1.0, 0.0, -0.0, 1.0, NULL] |
        | Float zeros                                        | array(CAST(0.0 AS FLOAT), -CAST(0.0 AS FLOAT), CAST(-1.0 AS FLOAT))                | [-1.0, 0.0, -0.0]                       |
        | Double zero fields of structs                      | array(named_struct('a', 0.0D), named_struct('a', -0.0D), named_struct('a', -1.0D)) | [{-1.0}, {0.0}, {-0.0}]                 |
        | Double zeros in nested arrays                      | array(array(0.0D), array(-0.0D), array(-1.0D))                                     | [[-1.0], [0.0], [-0.0]]                 |

    # TODO: Sail orders floats by IEEE 754 total order, so a NaN with the sign bit set
    #   (e.g. `-double('NaN')`) sorts before every other value. Spark treats every NaN
    #   as equal and larger than any other value. This is part of the NaN and zero
    #   ordering follow-up.
    @sail-bug
    Scenario Outline: No-comparator float NaN ordering: <case>
      When query
        """
        SELECT array_sort(<arr>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                     | arr                                                                                                  | result                                     |
        | Doubles                  | array(double('NaN'), 1.0D, double('-Infinity'), NULL, -double('NaN'), double('Infinity'))            | [-Infinity, 1.0, Infinity, NaN, NaN, NULL] |
        | Floats                   | array(CAST('NaN' AS FLOAT), -CAST('NaN' AS FLOAT), CAST(-1.0 AS FLOAT))                              | [-1.0, NaN, NaN]                           |
        | Double fields of structs | array(named_struct('a', double('NaN')), named_struct('a', -double('NaN')), named_struct('a', -1.0D)) | [{-1.0}, {NaN}, {NaN}]                     |
        | Doubles in nested arrays | array(array(double('NaN')), array(-double('NaN')), array(-1.0D))                                     | [[-1.0], [NaN], [NaN]]                     |

    Scenario: No-comparator element ordering over a column
      When query
        """
        SELECT array_sort(a) AS result
        FROM VALUES
          (array(named_struct('a', 1), named_struct('a', CAST(NULL AS INT)), NULL)),
          (array(named_struct('a', CAST(NULL AS INT)), NULL, named_struct('a', -1))),
          (CAST(NULL AS ARRAY<STRUCT<a: INT>>)) AS t(a)
        """
      Then query result
        | result               |
        | [{NULL}, {1}, NULL]  |
        | [{NULL}, {-1}, NULL] |
        | NULL                 |

    # TODO: Sail orders floats by IEEE 754 total order, so `-0.0` sorts before `0.0`.
    #   Spark treats `-0.0` and `0.0` as equal and its sort is stable, so they keep
    #   their input order. (Spark's own codegen fast path for `sort_array` on non-null
    #   doubles orders `-0.0` first; the expectations follow the stable rule that every
    #   other path uses.) This is part of the NaN and zero ordering follow-up.
    @sail-bug
    Scenario: No-comparator double zero ordering over a column
      When query
        """
        SELECT array_sort(a) AS result
        FROM VALUES
          (array(double('NaN'), 0.0D, CAST(NULL AS DOUBLE), -0.0D, -1.0D)),
          (array(0.0D, -0.0D)) AS t(a)
        """
      Then query result
        | result                       |
        | [-1.0, 0.0, -0.0, NaN, NULL] |
        | [0.0, -0.0]                  |

    # TODO: Sail orders floats by IEEE 754 total order, so a NaN with the sign bit set
    #   (e.g. `-double('NaN')`) sorts before every other value. Spark treats every NaN
    #   as equal and larger than any other value. This is part of the NaN and zero
    #   ordering follow-up.
    @sail-bug
    Scenario: No-comparator double NaN ordering over a column
      When query
        """
        SELECT array_sort(a) AS result
        FROM VALUES
          (array(-double('NaN'), 1.0D, CAST(NULL AS DOUBLE))),
          (array(double('NaN'), -double('NaN'), -1.0D)) AS t(a)
        """
      Then query result
        | result           |
        | [1.0, NaN, NULL] |
        | [-1.0, NaN, NaN] |

  Rule: Comparator form — ascending and descending

    Scenario Outline: Comparator direction: <case>
      When query
        """
        SELECT array_sort(array(5, 6, 1), <cmp>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | cmp                                                            | result    |
        | Ascending comparator with case expression | (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END | [1, 5, 6] |
        | Ascending comparator with subtraction     | (l, r) -> l - r                                                | [1, 5, 6] |
        | Descending comparator with subtraction    | (l, r) -> r - l                                                | [6, 5, 1] |

  Rule: Comparator form — element types

    Scenario: Comparator over array of structs by field
      When query
        """
        SELECT array_sort(array(named_struct('x', 3), named_struct('x', 1), named_struct('x', 2)), (l, r) -> l.x - r.x) AS result
        """
      Then query result
        | result          |
        | [{1}, {2}, {3}] |

    Scenario: Comparator over strings with explicit null handling — nulls first
      When query
        """
        SELECT array_sort(array('bc', 'ab', NULL, 'dc'), (l, r) -> CASE WHEN l IS NULL AND r IS NULL THEN 0 WHEN l IS NULL THEN -1 WHEN r IS NULL THEN 1 WHEN l < r THEN 1 WHEN l > r THEN -1 ELSE 0 END) AS result
        """
      Then query result
        | result             |
        | [NULL, dc, bc, ab] |

  Rule: Comparator form — degenerate inputs

    Scenario Outline: Degenerate input: <case>
      When query
        """
        SELECT array_sort(<arr>, (l, r) -> <cmp>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                       | arr                         | cmp   | result    |
        | Comparator on single-element array         | array(5)                    | l - r | [5]       |
        | Comparator on empty array                  | CAST(array() AS ARRAY<INT>) | l - r | []        |
        | Comparator on null array returns null      | CAST(NULL AS ARRAY<INT>)    | l - r | NULL      |
        | Constant-zero comparator keeps input order | array(3, 1, 2)              | 0     | [3, 1, 2] |

  Rule: Comparator form — multi-row

    Scenario: Comparator applied per row
      When query
        """
        SELECT array_sort(a, (l, r) -> l - r) AS result
        FROM VALUES (array(3, 1, 2)), (array(9, 7, 8)), (CAST(NULL AS ARRAY<INT>)) AS t(a)
        """
      Then query result
        | result    |
        | [1, 2, 3] |
        | [7, 8, 9] |
        | NULL      |

  Rule: Comparator form — errors

    Scenario Outline: Error case: <case>
      When query
        """
        SELECT array_sort(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                                                | args                                         |
        | Comparator returning null raises an error           | array(2, 1), (l, r) -> CAST(NULL AS INT)     |
        | Comparator returning a non-integer type is rejected | array(2, 1), (l, r) -> CAST(l - r AS BIGINT) |
        | Comparator with one parameter is rejected           | array(2, 1), x -> x                          |
        | Comparator with three parameters is rejected        | array(2, 1), (a, b, c) -> 1                  |
        | Non-array first argument is rejected                | 5, (l, r) -> 1                               |
