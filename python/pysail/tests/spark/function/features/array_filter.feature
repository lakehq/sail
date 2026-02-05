Feature: array filter with lambda

  Rule: Filter array elements using lambda predicates

    Scenario: Filter integers greater than a value
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x > 2) AS result
        """
      Then query result
        | result    |
        | [3, 4, 5] |

    Scenario: Filter integers less than a value
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x < 3) AS result
        """
      Then query result
        | result |
        | [1, 2] |

    Scenario: Filter integers greater than or equal to a value
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x >= 3) AS result
        """
      Then query result
        | result    |
        | [3, 4, 5] |

    Scenario: Filter integers less than or equal to a value
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x <= 2) AS result
        """
      Then query result
        | result |
        | [1, 2] |

    Scenario: Filter where all elements match
      When query
        """
        SELECT filter(array(10, 20, 30), x -> x > 5) AS result
        """
      Then query result
        | result       |
        | [10, 20, 30] |

    Scenario: Filter where no elements match
      When query
        """
        SELECT filter(array(1, 2, 3), x -> x > 10) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: Filter with reversed comparison
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> 3 < x) AS result
        """
      Then query result
        | result |
        | [4, 5] |

  Rule: Filter with index argument

    Scenario: Filter using element index - keep elements at even indices
      When query
        """
        SELECT filter(array(10, 20, 30, 40, 50), (x, i) -> i % 2 = 0) AS result
        """
      Then query result
        | result         |
        | [10, 30, 50]   |

    Scenario: Filter using element and index - element greater than index
      When query
        """
        SELECT filter(array(0, 5, 1, 10, 2), (x, i) -> x > i) AS result
        """
      Then query result
        | result    |
        | [5, 10]   |

  Rule: Filter with complex expressions

    Scenario: Filter with AND condition
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x > 1 AND x < 5) AS result
        """
      Then query result
        | result      |
        | [2, 3, 4]   |

    Scenario: Filter with OR condition
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x = 1 OR x = 5) AS result
        """
      Then query result
        | result |
        | [1, 5] |

    Scenario: Filter with modulo function
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5, 6), x -> x % 2 = 0) AS result
        """
      Then query result
        | result      |
        | [2, 4, 6]   |

  Rule: Filter with external column references

    Scenario: Filter using external column as threshold
      When query
        """
        SELECT filter(arr, x -> x > threshold) AS result
        FROM (SELECT array(1, 2, 3, 4, 5) AS arr, 2 AS threshold)
        """
      Then query result
        | result    |
        | [3, 4, 5] |

    Scenario: Filter using multiple external columns
      When query
        """
        SELECT filter(arr, x -> x > min_val AND x < max_val) AS result
        FROM (SELECT array(1, 2, 3, 4, 5) AS arr, 1 AS min_val, 5 AS max_val)
        """
      Then query result
        | result      |
        | [2, 3, 4]   |

    Scenario: Filter with varying thresholds per row
      When query
        """
        SELECT filter(arr, x -> x > threshold) AS result
        FROM VALUES
          (array(1, 2, 3, 4, 5), 2),
          (array(10, 20, 30), 15)
        AS t(arr, threshold)
        """
      Then query result
        | result       |
        | [3, 4, 5]    |
        | [20, 30]     |

  Rule: Filter with null handling

    Scenario: Filter array containing nulls - nulls are excluded by predicate
      When query
        """
        SELECT filter(array(1, NULL, 3, NULL, 5), x -> x > 2) AS result
        """
      Then query result
        | result |
        | [3, 5] |

    # TODO: IS NOT NULL not yet supported in lambda context - lambda variable not resolved
    @skip
    Scenario: Filter with IS NOT NULL predicate
      When query
        """
        SELECT filter(array(1, NULL, 3, NULL, 5), x -> x IS NOT NULL) AS result
        """
      Then query result
        | result    |
        | [1, 3, 5] |

    # TODO: IS NULL not yet supported in lambda context - lambda variable not resolved
    @skip
    Scenario: Filter with IS NULL predicate
      When query
        """
        SELECT filter(array(1, NULL, 3, NULL, 5), x -> x IS NULL) AS result
        """
      Then query result
        | result       |
        | [null, null] |

  Rule: Filter with different data types

    Scenario: Filter empty array
      When query
        """
        SELECT filter(array(), x -> x > 0) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: Filter string array
      When query
        """
        SELECT filter(array('apple', 'banana', 'cherry'), x -> x > 'b') AS result
        """
      Then query result
        | result             |
        | [banana, cherry]   |

    Scenario: Filter string array with length condition
      When query
        """
        SELECT filter(array('a', 'bb', 'ccc', 'dddd'), x -> length(x) > 2) AS result
        """
      Then query result
        | result      |
        | [ccc, dddd] |

    Scenario: Filter with negative numbers
      When query
        """
        SELECT filter(array(-5, -2, 0, 3, 7), x -> x >= 0) AS result
        """
      Then query result
        | result   |
        | [0, 3, 7] |

    Scenario: Filter double array
      When query
        """
        SELECT filter(array(1.5, 2.7, 3.2, 4.8), x -> x > 2.5) AS result
        """
      Then query result
        | result          |
        | [2.7, 3.2, 4.8] |

  Rule: Filter with equality and other operators

    Scenario: Filter with equality
      When query
        """
        SELECT filter(array(1, 2, 3, 2, 1), x -> x = 2) AS result
        """
      Then query result
        | result |
        | [2, 2] |

    Scenario: Filter with not equal
      When query
        """
        SELECT filter(array(1, 2, 3, 2, 1), x -> x <> 2) AS result
        """
      Then query result
        | result    |
        | [1, 3, 1] |

    Scenario: Filter with NOT condition
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> NOT (x > 3)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

  Rule: Filter with operators

    # TODO: BETWEEN not yet supported in lambda context
    @skip
    Scenario: Filter with BETWEEN
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x BETWEEN 2 AND 4) AS result
        """
      Then query result
        | result    |
        | [2, 3, 4] |

  Rule: Filter with functions in predicate

    # TODO: Function calls like abs() not yet supported in lambda context
    @skip
    Scenario: Filter with abs function
      When query
        """
        SELECT filter(array(-3, -1, 0, 2, 4), x -> abs(x) > 1) AS result
        """
      Then query result
        | result      |
        | [-3, 2, 4]  |

    Scenario: Filter with arithmetic in predicate
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x * 2 > 5) AS result
        """
      Then query result
        | result    |
        | [3, 4, 5] |

  Rule: Filter with index and external columns combined

    Scenario: Filter using both index and external column
      When query
        """
        SELECT filter(arr, (x, i) -> x > threshold AND i > 0) AS result
        FROM (SELECT array(1, 5, 2, 8, 3) AS arr, 2 AS threshold)
        """
      Then query result
        | result   |
        | [5, 8, 3] |

    Scenario: Filter with index less than value and external threshold
      When query
        """
        SELECT filter(arr, (x, i) -> x > threshold AND i < 3) AS result
        FROM (SELECT array(1, 5, 2, 8, 3) AS arr, 2 AS threshold)
        """
      Then query result
        | result |
        | [5]    |

  Rule: Filter with multiple rows (batch processing)

    Scenario: Filter multiple rows without external columns
      When query
        """
        SELECT filter(arr, x -> x > 2) AS result
        FROM VALUES
          (array(1, 2, 3)),
          (array(4, 5, 6)),
          (array(1, 1, 1))
        AS t(arr)
        """
      Then query result
        | result    |
        | [3]       |
        | [4, 5, 6] |
        | []        |

    Scenario: Filter multiple rows with different array sizes
      When query
        """
        SELECT filter(arr, x -> x % 2 = 0) AS result
        FROM VALUES
          (array(1, 2)),
          (array(1, 2, 3, 4, 5)),
          (array(7))
        AS t(arr)
        """
      Then query result
        | result |
        | [2]    |
        | [2, 4] |
        | []     |
