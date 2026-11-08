Feature: sort_array output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to sort_array yields the schema Spark declares
      When query
        """
        SELECT sort_array(array('b', 'd', null, 'c', 'a'), true) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = true)
        """

    Scenario: a nullable column input to sort_array stays nullable
      When query
        """
        SELECT sort_array(c, true) AS result FROM VALUES (array('b', 'd', null, 'c', 'a')), (CAST(NULL AS ARRAY<STRING>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """

  Rule: Element ordering

    Scenario Outline: sort_array element ordering: <case>
      When query
        """
        SELECT sort_array(<arr>, true) AS asc_result, sort_array(<arr>, false) AS desc_result
        """
      Then query result
        | asc_result   | desc_result   |
        | <asc_result> | <desc_result> |

      Examples:
        | case                               | arr                                                                                                                                                           | asc_result                                            | desc_result                                           |
        | Struct with a NULL field           | array(named_struct('a', 1), named_struct('a', CAST(NULL AS INT)), NULL)                                                                                       | [NULL, {NULL}, {1}]                                   | [{1}, {NULL}, NULL]                                   |
        | NULL elements of nested arrays     | array(array(1), array(CAST(NULL AS INT)), array(), array(1, NULL), NULL, array(NULL, 1))                                                                      | [NULL, [], [NULL], [NULL, 1], [1], [1, NULL]]         | [[1, NULL], [1], [NULL, 1], [NULL], [], NULL]         |
        | Strings with an empty string       | array('b', NULL, 'a', '')                                                                                                                                     | [NULL, , a, b]                                        | [b, a, , NULL]                                        |
        | Booleans                           | array(true, NULL, false)                                                                                                                                      | [NULL, false, true]                                   | [true, false, NULL]                                   |

    # TODO: Sail orders floats by IEEE 754 total order, so `-0.0` sorts before `0.0`.
    #   Spark treats `-0.0` and `0.0` as equal and its sort is stable, so they keep
    #   their input order. (Spark's own codegen fast path for `sort_array` on non-null
    #   doubles orders `-0.0` first; the expectations follow the stable rule that every
    #   other path uses.) This is part of the NaN and zero ordering follow-up.
    @sail-bug
    Scenario Outline: sort_array float zero ordering: <case>
      When query
        """
        SELECT sort_array(<arr>, true) AS asc_result, sort_array(<arr>, false) AS desc_result
        """
      Then query result
        | asc_result   | desc_result   |
        | <asc_result> | <desc_result> |

      Examples:
        | case                               | arr                                                                                                       | asc_result                      | desc_result                     |
        | Positive zero before negative zero | array(0.0D, -0.0D)                                                                                        | [0.0, -0.0]                     | [0.0, -0.0]                     |
        | Negative zero before positive zero | array(-0.0D, 0.0D)                                                                                        | [-0.0, 0.0]                     | [-0.0, 0.0]                     |
        | Double zeros among other values    | array(0.0D, -0.0D, -1.0D, NULL)                                                                           | [NULL, -1.0, 0.0, -0.0]         | [0.0, -0.0, -1.0, NULL]         |
        | Double zero fields of structs      | array(named_struct('a', 0.0D, 'b', 1), named_struct('a', -0.0D, 'b', 1), named_struct('a', 1.0D, 'b', 1)) | [{0.0, 1}, {-0.0, 1}, {1.0, 1}] | [{1.0, 1}, {0.0, 1}, {-0.0, 1}] |

    # TODO: Sail orders floats by IEEE 754 total order, so a NaN with the sign bit set
    #   (e.g. `-double('NaN')`) sorts before every other value. Spark treats every NaN
    #   as equal and larger than any other value. This is part of the NaN and zero
    #   ordering follow-up.
    @sail-bug
    Scenario Outline: sort_array float NaN ordering: <case>
      When query
        """
        SELECT sort_array(<arr>, true) AS asc_result, sort_array(<arr>, false) AS desc_result
        """
      Then query result
        | asc_result   | desc_result   |
        | <asc_result> | <desc_result> |

      Examples:
        | case                     | arr                                                                                                                         | asc_result                       | desc_result                      |
        | Doubles                  | array(double('NaN'), 1.0D, double('-Infinity'), NULL, -double('NaN'))                                                       | [NULL, -Infinity, 1.0, NaN, NaN] | [NaN, NaN, 1.0, -Infinity, NULL] |
        | Double fields of structs | array(named_struct('a', double('NaN'), 'b', 1), named_struct('a', -double('NaN'), 'b', 2), named_struct('a', 1.0D, 'b', 1)) | [{1.0, 1}, {NaN, 1}, {NaN, 2}]   | [{NaN, 2}, {NaN, 1}, {1.0, 1}]   |

    Scenario: sort_array element ordering over a column
      When query
        """
        SELECT sort_array(a) AS asc_result, sort_array(a, false) AS desc_result
        FROM VALUES
          (array(named_struct('a', 1), named_struct('a', CAST(NULL AS INT)), NULL)),
          (CAST(NULL AS ARRAY<STRUCT<a: INT>>)) AS t(a)
        """
      Then query result
        | asc_result          | desc_result         |
        | [NULL, {NULL}, {1}] | [{1}, {NULL}, NULL] |
        | NULL                | NULL                |

    # TODO: Sail orders floats by IEEE 754 total order, so `-0.0` sorts before `0.0`.
    #   Spark treats `-0.0` and `0.0` as equal and its sort is stable, so they keep
    #   their input order. (Spark's own codegen fast path for `sort_array` on non-null
    #   doubles orders `-0.0` first; the expectations follow the stable rule that every
    #   other path uses.) This is part of the NaN and zero ordering follow-up.
    @sail-bug
    Scenario: sort_array double zero ordering over a column
      When query
        """
        SELECT sort_array(a) AS asc_result, sort_array(a, false) AS desc_result
        FROM VALUES
          (array(double('NaN'), 0.0D, CAST(NULL AS DOUBLE), -0.0D, -1.0D)),
          (array(-0.0D, 0.0D)) AS t(a)
        """
      Then query result
        | asc_result                   | desc_result                  |
        | [NULL, -1.0, 0.0, -0.0, NaN] | [NaN, 0.0, -0.0, -1.0, NULL] |
        | [-0.0, 0.0]                  | [-0.0, 0.0]                  |

    # TODO: Sail orders floats by IEEE 754 total order, so a NaN with the sign bit set
    #   (e.g. `-double('NaN')`) sorts before every other value. Spark treats every NaN
    #   as equal and larger than any other value. This is part of the NaN and zero
    #   ordering follow-up.
    @sail-bug
    Scenario: sort_array double NaN ordering over a column
      When query
        """
        SELECT sort_array(a) AS asc_result, sort_array(a, false) AS desc_result
        FROM VALUES
          (array(-double('NaN'), 1.0D, CAST(NULL AS DOUBLE))),
          (array(double('NaN'), -double('NaN'), -1.0D)) AS t(a)
        """
      Then query result
        | asc_result       | desc_result      |
        | [NULL, 1.0, NaN] | [NaN, 1.0, NULL] |
        | [-1.0, NaN, NaN] | [NaN, NaN, -1.0] |
