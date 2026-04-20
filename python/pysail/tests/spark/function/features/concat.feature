Feature: concat() concatenates arrays

  Rule: Basic concatenation

    Scenario: concat two integer arrays
      When query
      """
      SELECT concat(array(1, 2, 3), array(4, 5)) AS result
      """
      Then query result
      | result          |
      | [1, 2, 3, 4, 5] |

    Scenario: concat two string arrays
      When query
      """
      SELECT concat(array('a', 'b'), array('c')) AS result
      """
      Then query result
      | result    |
      | [a, b, c] |

  Rule: Empty array handling

    Scenario: concat empty array with typed array
      When query
      """
      SELECT concat(array(), array(1, 2, 3)) AS result
      """
      Then query result
      | result    |
      | [1, 2, 3] |

    Scenario: concat typed array with empty array
      When query
      """
      SELECT concat(array(1, 2), array()) AS result
      """
      Then query result
      | result |
      | [1, 2] |

  Rule: Null propagation

    Scenario: concat array with null returns null
      When query
      """
      SELECT concat(array(1, 2), CAST(NULL AS ARRAY<INT>)) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: concat null with array returns null
      When query
      """
      SELECT concat(CAST(NULL AS ARRAY<INT>), array(1, 2)) AS result
      """
      Then query result
      | result |
      | NULL   |
