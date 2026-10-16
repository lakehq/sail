Feature: array() type coercion with mixed element types

  Spark's non-ANSI semantics coerce mixed string/numeric arrays to string,
  not to numeric. DataFusion 54 changed `comparison_coercion` to prefer
  numeric, which would otherwise reject `array('a', 1)` at runtime.

  Rule: Homogeneous arrays preserve their element type

    Scenario Outline: array of <case>
      When query
        """
        SELECT array(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case     | args          | result          |
        | integers | 1, 2, 3       | [1, 2, 3]       |
        | strings  | 'a', 'b', 'c' | [a, b, c]       |
        | doubles  | 1.0, 2.5, 3.5 | [1.0, 2.5, 3.5] |

  Rule: Mixed numeric types coerce to the widest numeric type

    Scenario: integer and double coerce to double
      When query
        """
        SELECT array(1, 2.5) AS result
        """
      Then query result
        | result     |
        | [1.0, 2.5] |

  Rule: Mixed string and numeric types coerce to string (Spark non-ANSI)

    Scenario Outline: <case> coerce to string
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT array(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                          | args             | result         |
        | string and integer            | 'a', 1           | [a, 1]         |
        | string and double             | 'a', 1.5         | [a, 1.5]       |
        | multiple strings and numerics | 'a', 1, 2.5, 'b' | [a, 1, 2.5, b] |

    # With ANSI on, the common type is numeric rather than string, so Spark tries to cast the
    # string element and fails at runtime. Sail coerces everything to string in both modes.
    @sail-bug
    Scenario Outline: <case> raises under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT array(<args>) AS result
        """
      Then query error \[CAST_INVALID_INPUT\] The value 'a' of the type "STRING" cannot be cast to "<target>"

      Examples:
        | case                          | args             | target |
        | string and integer            | 'a', 1           | BIGINT |
        | string and double             | 'a', 1.5         | DOUBLE |
        | multiple strings and numerics | 'a', 1, 2.5, 'b' | DOUBLE |

  Rule: NULL elements are preserved during coercion

    Scenario: string, numeric and NULL coerce to string with NULL preserved
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT array('a', 1, NULL, 1.0) AS result
        """
      Then query result
        | result            |
        | [a, 1, NULL, 1.0] |

    @sail-bug
    Scenario: string, numeric and NULL raise under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT array('a', 1, NULL, 1.0) AS result
        """
      Then query error \[CAST_INVALID_INPUT\] The value 'a' of the type "STRING" cannot be cast to "DECIMAL\(21,1\)"

  Rule: A logical type does not mix with its storage type

    # Spark types GEOMETRY and BINARY as different types, so `array` rejects them together
    # (`DATA_DIFF_TYPES`). Sail carries GEOMETRY as BINARY plus field metadata; when the arguments'
    # metadata disagree `array` drops it and accepts the call as `array<binary>`.
    @sail-bug @spark-4.2
    Scenario: array rejects mixing GEOMETRY with BINARY
      When query
        """
        SELECT array(st_geomfromwkb(w), CAST(NULL AS BINARY)) AS result
        FROM VALUES (X'0101000000000000000000F03F0000000000000040') AS t(w)
        """
      Then query error DATA_DIFF_TYPES

  Rule: A logical type survives an argument folded to a literal

    # `array` promises the element field the metadata that carries the Spark logical type, but a
    # call whose arguments are all literals is folded before execution and the folded value comes
    # back without it, so the promise and the result disagree. The same call over a column keeps
    # the type. A release build has the assertion off and loses the type in silence instead.
    @spark-4
    Scenario: array keeps the VARIANT type of a literal element
      When query
        """
        SELECT to_json(array(parse_json('1'))[0]) AS result
        """
      Then query result
        | result |
        | 1      |

    @sail-bug
    @spark-4
    Scenario: array declares an element of VARIANT type for a literal element
      When query
        """
        SELECT array(parse_json('1')) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: variant (containsNull = false)
        """

    @sail-bug
    @spark-4.2
    Scenario: array keeps the GEOMETRY type of a literal element
      When query
        """
        SELECT array(st_geomfromwkb(X'0101000000000000000000F03F0000000000000040')) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: geometry (containsNull = false)
        """
