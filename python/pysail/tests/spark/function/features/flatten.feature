Feature: flatten() flattens nested arrays

  Rule: Basic flattening

    Scenario: flatten nested array of integers
      When query
      """
      SELECT flatten(array(array(1, 2), array(3, 4))) AS result
      """
      Then query result
      | result       |
      | [1, 2, 3, 4] |

    Scenario: flatten nested array of strings
      When query
      """
      SELECT flatten(array(array('a', 'b'), array('c'))) AS result
      """
      Then query result
      | result       |
      | [a, b, c]    |

  Rule: Empty array edge cases

    Scenario: flatten with empty inner arrays
      When query
      """
      SELECT flatten(array(array(), array())) AS result
      """
      Then query result
      | result |
      | []     |

    Scenario: flatten single empty inner array
      When query
      """
      SELECT flatten(array(array())) AS result
      """
      Then query result
      | result |
      | []     |

    Scenario: flatten mixed empty and non-empty arrays
      When query
      """
      SELECT flatten(array(array(1), array(), array(2, 3))) AS result
      """
      Then query result
      | result    |
      | [1, 2, 3] |

  Rule: Null handling

    Scenario: flatten with null element in outer array
      When query
      """
      SELECT flatten(array(array(1, 2), NULL, array(3))) AS result
      """
      Then query result
      | result |
      | NULL   |
