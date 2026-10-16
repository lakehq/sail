Feature: Native engine edge cases from Apache Comet issues
  These test cases are derived from issues discovered in Apache Comet
  when testing literal argument combinations with constant folding disabled.
  Reference: https://github.com/apache/datafusion-comet/issues

  Rule: Datetime functions with all-scalar inputs (#3336)

    Scenario: hour with literal timestamp
      When query
        """
        SELECT hour(timestamp '2024-01-15 14:30:45') AS result
        """
      Then query result
        | result |
        | 14     |

    Scenario: minute with literal timestamp
      When query
        """
        SELECT minute(timestamp '2024-01-15 14:30:45') AS result
        """
      Then query result
        | result |
        | 30     |

    Scenario: second with literal timestamp
      When query
        """
        SELECT second(timestamp '2024-01-15 14:30:45') AS result
        """
      Then query result
        | result |
        | 45     |

    Scenario: unix_timestamp with literal string
      When query
        """
        SELECT unix_timestamp('2024-01-15 14:30:45') AS result
        """
      Then query result
        | result     |
        | 1705329045 |

  Rule: String functions with all-scalar inputs (#3337)

    Scenario: substring with all literals
      When query
        """
        SELECT substring('hello world', 1, 5) AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: space with literal positive number
      When query
        """
        SELECT concat('a', space(3), 'b') AS result
        """
      Then query result
        | result  |
        | a   b   |

    Scenario: space with negative input (#3326)
      When query
        """
        SELECT space(-1) AS result
        """
      Then query result
        | result |
        |        |

    Scenario: space with zero
      When query
        """
        SELECT space(0) AS result
        """
      Then query result
        | result |
        |        |

  Rule: Array index out of bounds (#3338)
    Spark's array[index] syntax throws error for out-of-bounds.
    Use get() function for null-tolerant access.

    Scenario: array element access with literal index
      When query
        """
        SELECT array(1, 2, 3)[0] AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: array element access out of bounds throws error
      When query
        """
        SELECT array(1, 2, 3)[10] AS result
        """
      Then query error INVALID_ARRAY_INDEX

    @sail-bug
    Scenario: array element access with negative index throws error
      When query
        """
        SELECT array(1, 2, 3)[-1] AS result
        """
      Then query error INVALID_ARRAY_INDEX

    Scenario: get function returns null for out of bounds
      When query
        """
        SELECT get(array(1, 2, 3), 10) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: get function returns null for negative index
      When query
        """
        SELECT get(array(1, 2, 3), -1) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: concat_ws with NULL separator (#3339)

    Scenario: concat_ws with literal NULL separator
      When query
        """
        SELECT concat_ws(NULL, 'a', 'b', 'c') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: concat_ws with NULL values in array
      When query
        """
        SELECT concat_ws(',', 'a', NULL, 'c') AS result
        """
      Then query result
        | result |
        | a,c    |

  Rule: sha2 with literal arguments (#3340)

    Scenario: sha2 with literal string and bit length
      When query
        """
        SELECT sha2('hello', 256) AS result
        """
      Then query result
        | result                                                           |
        | 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824 |

    Scenario: sha2 with NULL input
      When query
        """
        SELECT sha2(NULL, 256) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: bit_count with scalar input (#3341)

    Scenario: bit_count with literal integer
      When query
        """
        SELECT bit_count(7) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: bit_count with zero
      When query
        """
        SELECT bit_count(0) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: bit_count with negative number
      When query
        """
        SELECT bit_count(-1) AS result
        """
      Then query result
        | result |
        | 64     |

  Rule: DateTrunc and TimestampTrunc with literals (#3342)

    Scenario: date_trunc with literal timestamp
      When query
        """
        SELECT date_trunc('month', timestamp '2024-01-15 14:30:45') AS result
        """
      Then query result
        | result              |
        | 2024-01-01 00:00:00 |

    Scenario: date_trunc with literal date
      When query
        """
        SELECT date_trunc('year', date '2024-06-15') AS result
        """
      Then query result
        | result              |
        | 2024-01-01 00:00:00 |

  Rule: RLIKE with all-literal expression (#3343)

    Scenario: rlike with literal string and pattern
      When query
        """
        SELECT 'hello world' RLIKE 'wor.*' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: rlike with no match
      When query
        """
        SELECT 'hello' RLIKE '^world' AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: regexp with literal arguments
      When query
        """
        SELECT regexp('hello123', '[0-9]+') AS result
        """
      Then query result
        | result |
        | true   |

  Rule: replace with empty-string search (#3344)

    Scenario: replace with empty search string
      When query
        """
        SELECT replace('hello', '', 'x') AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: replace with normal arguments
      When query
        """
        SELECT replace('hello world', 'world', 'there') AS result
        """
      Then query result
        | result      |
        | hello there |

  Rule: array_contains edge cases (#3345, #3346)

    @sail-bug
    Scenario: array_contains with literal array and NULL cast
      When query
        """
        SELECT array_contains(array(1, 2, 3), CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: array_contains with empty array
      When query
        """
        SELECT array_contains(array(), 1) AS result
        """
      Then query result
        | result |
        | false  |

    @sail-bug
    Scenario: array_contains with NULL in array searching for missing element
      When query
        """
        SELECT array_contains(array(1, NULL, 3), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: array_contains found element
      When query
        """
        SELECT array_contains(array(1, 2, 3), 2) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: map_from_arrays with NULL literal inputs (#3327)

    Scenario: map_from_arrays with NULL keys array returns null
      When query
        """
        SELECT map_from_arrays(NULL, array(1, 2)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: map_from_arrays with NULL values array returns null
      When query
        """
        SELECT map_from_arrays(array('a', 'b'), NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: map_from_arrays with both NULL returns null
      When query
        """
        SELECT map_from_arrays(NULL, NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: width_bucket type handling (#3331)

    Scenario: width_bucket with integer arguments
      When query
        """
        SELECT width_bucket(5, 0, 10, 5) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: width_bucket with float arguments
      When query
        """
        SELECT width_bucket(5.5, 0.0, 10.0, 5) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: width_bucket below min
      When query
        """
        SELECT width_bucket(-1, 0, 10, 5) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: width_bucket above max
      When query
        """
        SELECT width_bucket(15, 0, 10, 5) AS result
        """
      Then query result
        | result |
        | 6      |

  Rule: GetArrayItem with dynamic index (#3332)

    Scenario: element_at with literal array and index
      When query
        """
        SELECT element_at(array(10, 20, 30), 2) AS result
        """
      Then query result
        | result |
        | 20     |

    @sail-bug
    Scenario: element_at with zero index throws error
      When query
        """
        SELECT element_at(array(10, 20, 30), 0) AS result
        """
      Then query error INVALID_INDEX_OF_ZERO

    Scenario: element_at with negative index
      When query
        """
        SELECT element_at(array(10, 20, 30), -1) AS result
        """
      Then query result
        | result |
        | 30     |
