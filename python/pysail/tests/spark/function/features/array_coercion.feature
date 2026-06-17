Feature: array() type coercion with mixed element types

  Spark's non-ANSI semantics coerce mixed string/numeric arrays to string,
  not to numeric. DataFusion 54 changed `comparison_coercion` to prefer
  numeric, which would otherwise reject `array('a', 1)` at runtime.

  Rule: Homogeneous arrays preserve their element type

    Scenario: array of integers
      When query
      """
      SELECT array(1, 2, 3) AS result
      """
      Then query result
      | result    |
      | [1, 2, 3] |

    Scenario: array of strings
      When query
      """
      SELECT array('a', 'b', 'c') AS result
      """
      Then query result
      | result    |
      | [a, b, c] |

    Scenario: array of doubles
      When query
      """
      SELECT array(1.0, 2.5, 3.5) AS result
      """
      Then query result
      | result          |
      | [1.0, 2.5, 3.5] |

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

    Scenario: string and integer coerce to string
      When query
      """
      SELECT array('a', 1) AS result
      """
      Then query result
      | result |
      | [a, 1] |

    Scenario: string and double coerce to string
      When query
      """
      SELECT array('a', 1.5) AS result
      """
      Then query result
      | result    |
      | [a, 1.5] |

    Scenario: multiple strings and numerics coerce to string
      When query
      """
      SELECT array('a', 1, 2.5, 'b') AS result
      """
      Then query result
      | result          |
      | [a, 1, 2.5, b]  |

  Rule: NULL elements are preserved during coercion

    Scenario: string, numeric and NULL coerce to string with NULL preserved
      When query
      """
      SELECT array('a', 1, NULL, 1.0) AS result
      """
      Then query result
      | result              |
      | [a, 1, NULL, 1.0]   |
