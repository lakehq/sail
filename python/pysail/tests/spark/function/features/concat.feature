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

  Rule: Single-argument identity — concat(x) returns x unchanged

    Scenario: concat of single string literal is identity
      When query
      """
      SELECT concat('hello') AS result
      """
      Then query result
      | result |
      | hello  |

    Scenario: concat of single string column propagates values and nulls
      When query
      """
      SELECT concat(v) AS result FROM VALUES
        ('hello'),
        ('world'),
        (NULL)
      AS t(v) ORDER BY result
      """
      Then query result ordered
      | result |
      | NULL   |
      | hello  |
      | world  |

    Scenario: concat of single array column is identity
      When query
      """
      SELECT concat(v) AS result FROM VALUES
        (array(1, 2, 3)),
        (array(4, 5))
      AS t(v) ORDER BY result
      """
      Then query result ordered
      | result    |
      | [1, 2, 3] |
      | [4, 5]    |

    Scenario: concat of single null string returns null
      When query
      """
      SELECT concat(CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-only
    Scenario: EXPLAIN concat of single column — no spark_concat in plan
      When query
      """
      EXPLAIN SELECT concat(v) AS result FROM VALUES
        ('hello'),
        ('world')
      AS t(v)
      """
      Then query plan matches snapshot

  Rule: Plan snapshot — two-arg concat keeps spark_concat in plan

    Scenario: concat of two array columns returns concatenated arrays
      When query
      """
      SELECT concat(a, b) AS result FROM VALUES
        (array(1, 2), array(3, 4)),
        (array(5), array(6, 7))
      AS t(a, b) ORDER BY result
      """
      Then query result ordered
      | result      |
      | [1, 2, 3, 4] |
      | [5, 6, 7]   |

    @sail-only
    Scenario: EXPLAIN concat of two array columns — spark_concat stays in plan
      When query
      """
      EXPLAIN SELECT concat(a, b) AS result FROM VALUES
        (array(1, 2), array(3, 4)),
        (array(5), array(6, 7))
      AS t(a, b)
      """
      Then query plan matches snapshot
