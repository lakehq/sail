Feature: least output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to least yields the schema Spark declares
      When query
        """
        SELECT least(10, 9, 2, 4, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to least yields the schema Spark declares
      When query
        """
        SELECT least(CAST(id AS INT), 9, 2, 4, 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    @sail-bug
    Scenario: a nullable column input to least stays nullable
      When query
        """
        SELECT least(c, 9, 2, 4, 3) AS result FROM VALUES (10), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

  Rule: Nested values with NULL fields or elements

    Scenario: least treats a NULL struct field as the smallest
      When query
        """
        SELECT least(x, y) AS a, least(y, x) AS b FROM VALUES (named_struct('a', CAST(NULL AS INT), 'b', 5), named_struct('a', 1, 'b', 1)) AS t(x, y)
        """
      Then query result
        | a         | b         |
        | {NULL, 5} | {NULL, 5} |

    Scenario: least treats a NULL array element as the smallest
      When query
        """
        SELECT least(x, y) AS a, least(y, x) AS b FROM VALUES (array(CAST(NULL AS INT), 5), array(1, 1)) AS t(x, y)
        """
      Then query result
        | a         | b         |
        | [NULL, 5] | [NULL, 5] |

    Scenario: least orders a shorter array before one ending with a NULL element
      When query
        """
        SELECT least(x, y) AS a, least(y, x) AS b FROM VALUES (array(1), array(1, CAST(NULL AS INT))) AS t(x, y)
        """
      Then query result
        | a   | b   |
        | [1] | [1] |

    Scenario: least treats a NULL struct as greater than a NULL field
      When query
        """
        SELECT least(x, y, z) AS a, least(z, y, x) AS b FROM VALUES (named_struct('a', CAST(NULL AS INT)), named_struct('a', 1), CAST(NULL AS STRUCT<a: INT>)) AS t(x, y, z)
        """
      Then query result
        | a      | b      |
        | {NULL} | {NULL} |

    Scenario: least with literal and column arguments
      When query
        """
        SELECT least(x, named_struct('a', 1)) AS a, least(named_struct('a', 1), x) AS b, least(named_struct('a', 1), named_struct('a', CAST(NULL AS INT))) AS c FROM VALUES (named_struct('a', CAST(NULL AS INT))) AS t(x)
        """
      Then query result
        | a      | b      | c      |
        | {NULL} | {NULL} | {NULL} |

    Scenario: least treats a NULL field of a struct in an array as the smallest
      When query
        """
        SELECT least(x, y) AS a, least(y, x) AS b FROM VALUES (array(named_struct('a', CAST(NULL AS INT))), array(named_struct('a', 0))) AS t(x, y)
        """
      Then query result
        | a        | b        |
        | [{NULL}] | [{NULL}] |

    Scenario: least treats a NULL nested struct as the smallest field
      When query
        """
        SELECT least(x, y) AS a, least(y, x) AS b FROM VALUES (named_struct('s', CAST(NULL AS STRUCT<a: INT>)), named_struct('s', named_struct('a', 0))) AS t(x, y)
        """
      Then query result
        | a      | b      |
        | {NULL} | {NULL} |

    Scenario: least with NULL arrays and NULL elements
      When query
        """
        SELECT least(x, y) AS a FROM VALUES (CAST(NULL AS ARRAY<INT>), array(CAST(NULL AS INT))), (array(CAST(NULL AS INT)), CAST(NULL AS ARRAY<INT>)), (array(2), array(CAST(NULL AS INT))) AS t(x, y)
        """
      Then query result
        | a      |
        | [NULL] |
        | [NULL] |
        | [NULL] |

    Scenario: greatest treats a NULL struct field as the smallest
      When query
        """
        SELECT greatest(x, y) AS a, greatest(y, x) AS b FROM VALUES (named_struct('a', CAST(NULL AS INT), 'b', 5), named_struct('a', 1, 'b', 1)) AS t(x, y)
        """
      Then query result
        | a      | b      |
        | {1, 1} | {1, 1} |

    Scenario: least and greatest with a NULL string field in a nested struct
      When query
        """
        SELECT least(x, y) AS a, greatest(x, y) AS b FROM VALUES (named_struct('s', named_struct('a', CAST(NULL AS STRING))), named_struct('s', named_struct('a', ''))) AS t(x, y)
        """
      Then query result
        | a        | b    |
        | {{NULL}} | {{}} |

    Scenario: least and greatest with a struct holding an array with a NULL element
      When query
        """
        SELECT least(x, y) AS a, greatest(x, y) AS b FROM VALUES (named_struct('a', array(CAST(NULL AS INT))), named_struct('a', array(-1))) AS t(x, y)
        """
      Then query result
        | a        | b      |
        | {[NULL]} | {[-1]} |

    # TODO: Spark orders -0.0 and 0.0 as equal, so the first argument wins the tie, but Sail orders
    #   -0.0 first. See the NaN and zero ordering follow-up (tmp/nan-central-rule-20261107).
    @sail-bug
    Scenario: least and greatest treat -0.0 and 0.0 struct fields as a tie
      When query
        """
        SELECT least(x, y) AS a, greatest(x, y) AS b FROM VALUES (named_struct('a', CAST('-0.0' AS DOUBLE)), named_struct('a', CAST('0.0' AS DOUBLE))) AS t(x, y)
        """
      Then query result
        | a      | b      |
        | {-0.0} | {-0.0} |

  Rule: DECIMAL with FLOAT or DOUBLE

    Scenario: greatest and least of DECIMAL with FLOAT or DOUBLE are DOUBLE
      When query
        """
        SELECT greatest(CAST(1.5 AS DOUBLE), CAST(1.25 AS DECIMAL(10,2))) AS a, least(CAST(1.5 AS FLOAT), CAST(2 AS DECIMAL(5,0))) AS b, least(CAST(0.5 AS DECIMAL(38,10)), CAST(1.5 AS DOUBLE), CAST(2 AS INT)) AS c, greatest(array(CAST(1.5 AS DOUBLE)), array(CAST(1.25 AS DECIMAL(10,2)))) AS d
        """
      Then query result
        | a   | b   | c   | d     |
        | 1.5 | 1.5 | 0.5 | [1.5] |

    Scenario: greatest and least of DECIMAL with DOUBLE have the DOUBLE type
      When query
        """
        SELECT typeof(greatest(CAST(1.5 AS DOUBLE), CAST(1.25 AS DECIMAL(10,2)))) AS a, typeof(least(CAST(1.5 AS FLOAT), CAST(2 AS DECIMAL(5,0)))) AS b
        """
      Then query result
        | a      | b      |
        | double | double |
