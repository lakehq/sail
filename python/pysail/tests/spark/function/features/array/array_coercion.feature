@array_coercion
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

  Rule: NULL elements are preserved during coercion

    Scenario: string, numeric and NULL coerce to string with NULL preserved
      When query
        """
        SELECT array('a', 1, NULL, 1.0) AS result
        """
      Then query result
        | result            |
        | [a, 1, NULL, 1.0] |
