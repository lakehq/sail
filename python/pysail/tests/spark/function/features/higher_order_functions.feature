@higher_order_functions
Feature: Higher-order array and map functions

  Rule: filter

    Scenario: filter with single parameter
      When query
        """
        SELECT filter(array(1, 2, 3, 4, 5), x -> x % 2 = 1) AS result
        """
      Then query result
        | result      |
        | [1, 3, 5]   |

    Scenario: filter removes nulls
      When query
        """
        SELECT filter(array(1, NULL, 2, NULL, 3), x -> x IS NOT NULL) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: filter with index parameter
      When query
        """
        SELECT filter(array(0, 2, 3), (x, i) -> x > i) AS result
        """
      Then query result
        | result |
        | [2, 3] |

    Scenario: filter returns empty array
      When query
        """
        SELECT filter(array(1, 2, 3), x -> x > 10) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: transform

    Scenario: transform with single parameter
      When query
        """
        SELECT transform(array(1, 2, 3), x -> x + 1) AS result
        """
      Then query result
        | result    |
        | [2, 3, 4] |

    Scenario: transform with index parameter
      When query
        """
        SELECT transform(array(1, 2, 3), (x, i) -> x + i) AS result
        """
      Then query result
        | result    |
        | [1, 3, 5] |

    Scenario: transform strings
      When query
        """
        SELECT transform(array('a', 'b', 'c'), x -> upper(x)) AS result
        """
      Then query result
        | result         |
        | [A, B, C]      |

  Rule: exists

    Scenario: exists returns true when element matches
      When query
        """
        SELECT exists(array(1, 2, 3), x -> x = 2) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: exists returns false when no element matches
      When query
        """
        SELECT exists(array(1, 2, 3), x -> x > 10) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: exists with null elements
      When query
        """
        SELECT exists(array(0, NULL, 2, 3, NULL), x -> x IS NULL) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: forall

    Scenario: forall returns true when all elements match
      When query
        """
        SELECT forall(array(2, 4, 8), x -> x % 2 = 0) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: forall returns false when not all elements match
      When query
        """
        SELECT forall(array(1, 2, 3), x -> x % 2 = 0) AS result
        """
      Then query result
        | result |
        | false  |

  Rule: aggregate (reduce)

    Scenario: aggregate sum
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 6      |

    Scenario: aggregate with finish function
      When query
        """
        SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10) AS result
        """
      Then query result
        | result |
        | 60     |

    Scenario: reduce alias
      When query
        """
        SELECT reduce(array(1, 2, 3), 0, (acc, x) -> acc + x) AS result
        """
      Then query result
        | result |
        | 6      |

  Rule: zip_with

    Scenario: zip_with two integer arrays
      When query
        """
        SELECT zip_with(array(1, 2), array(3, 4), (x, y) -> x + y) AS result
        """
      Then query result
        | result |
        | [4, 6] |

    Scenario: zip_with string concat
      When query
        """
        SELECT zip_with(array('a', 'b', 'c'), array('d', 'e', 'f'), (x, y) -> concat(x, y)) AS result
        """
      Then query result
        | result         |
        | [ad, be, cf]   |

  Rule: transform_keys

    Scenario: transform_keys doubles integer keys
      When query
        """
        SELECT transform_keys(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), (k, v) -> k + v) AS result
        """
      Then query result
        | result             |
        | {2 -> 1, 4 -> 2, 6 -> 3} |

  Rule: transform_values

    Scenario: transform_values doubles integer values
      When query
        """
        SELECT transform_values(map_from_arrays(array(1, 2, 3), array(1, 2, 3)), (k, v) -> k + v) AS result
        """
      Then query result
        | result             |
        | {1 -> 2, 2 -> 4, 3 -> 6} |

  Rule: map_filter

    Scenario: map_filter keeps entries where key is greater than value
      When query
        """
        SELECT map_filter(map(1, 0, 2, 2, 3, -1), (k, v) -> k > v) AS result
        """
      Then query result
        | result          |
        | {1 -> 0, 3 -> -1} |

  Rule: array_sort with comparator

    Scenario: array_sort with custom comparator descending
      When query
        """
        SELECT array_sort(array(5, 6, 1), (left, right) -> case when left < right then -1 when left > right then 1 else 0 end) AS result
        """
      Then query result
        | result    |
        | [1, 5, 6] |
