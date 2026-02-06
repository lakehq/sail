Feature: array_concat() concatenates arrays

  Rule: Basic concatenation

    Scenario: concat two integer arrays
      When query
      """
      SELECT array_concat(array(1, 2, 3), array(4, 5)) AS result
      """
      Then query result
      | result          |
      | [1, 2, 3, 4, 5] |

    Scenario: concat two string arrays
      When query
      """
      SELECT array_concat(array('a', 'b'), array('c')) AS result
      """
      Then query result
      | result    |
      | [a, b, c] |

  Rule: Null propagation

    Scenario: concat array with null returns null
      When query
      """
      SELECT array_concat(array(1, 2), CAST(NULL AS ARRAY<INT>)) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: concat null with array returns null
      When query
      """
      SELECT array_concat(CAST(NULL AS ARRAY<INT>), array(1, 2)) AS result
      """
      Then query result
      | result |
      | NULL   |
