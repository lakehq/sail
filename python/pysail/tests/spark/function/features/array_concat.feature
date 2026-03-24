@sail-only
Feature: array_concat() concatenates arrays (Sail extension)

  Note: array_concat is a Sail extension not available in standard Spark.
  Use concat() for Spark-compatible array concatenation.

  Rule: Basic concatenation

    @sail-only
    Scenario: array_concat two integer arrays
      When query
      """
      SELECT array_concat(array(1, 2, 3), array(4, 5)) AS result
      """
      Then query result
      | result          |
      | [1, 2, 3, 4, 5] |

    @sail-only
    Scenario: array_concat two string arrays
      When query
      """
      SELECT array_concat(array('a', 'b'), array('c')) AS result
      """
      Then query result
      | result    |
      | [a, b, c] |

  Rule: Empty array handling

    @sail-only
    Scenario: array_concat empty array with typed array
      When query
      """
      SELECT array_concat(array(), array(1, 2, 3)) AS result
      """
      Then query result
      | result    |
      | [1, 2, 3] |

    @sail-only
    Scenario: array_concat typed array with empty array
      When query
      """
      SELECT array_concat(array(1, 2), array()) AS result
      """
      Then query result
      | result |
      | [1, 2] |

  Rule: Null propagation

    @sail-only
    Scenario: array_concat array with null returns null
      When query
      """
      SELECT array_concat(array(1, 2), CAST(NULL AS ARRAY<INT>)) AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-only
    Scenario: array_concat null with array returns null
      When query
      """
      SELECT array_concat(CAST(NULL AS ARRAY<INT>), array(1, 2)) AS result
      """
      Then query result
      | result |
      | NULL   |
