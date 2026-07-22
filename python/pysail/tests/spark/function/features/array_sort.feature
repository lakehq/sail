@array_sort @lambda_hof
Feature: array_sort higher-order function

  Rule: No-comparator form — natural ascending order, NULLs last

    Scenario: Sort integers ascending
      When query
        """
        SELECT array_sort(array(3, 1, 2)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: Sort integers with a null — null placed last
      When query
        """
        SELECT array_sort(array(3, NULL, 1)) AS result
        """
      Then query result
        | result          |
        | [1, 3, NULL]    |

    Scenario: Sort strings ascending
      When query
        """
        SELECT array_sort(array('b', 'a', 'c')) AS result
        """
      Then query result
        | result          |
        | [a, b, c]       |

    Scenario: Sort doubles with NaN and Infinity — NaN is greatest
      When query
        """
        SELECT array_sort(array(double('NaN'), 1.0, double('Infinity'), -1.0)) AS result
        """
      Then query result
        | result                     |
        | [-1.0, 1.0, Infinity, NaN] |

    Scenario: Sort null array returns null
      When query
        """
        SELECT array_sort(CAST(NULL AS ARRAY<INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Comparator form — ascending and descending

    Scenario: Ascending comparator with case expression
      When query
        """
        SELECT array_sort(array(5, 6, 1), (l, r) -> CASE WHEN l < r THEN -1 WHEN l > r THEN 1 ELSE 0 END) AS result
        """
      Then query result
        | result    |
        | [1, 5, 6] |

    Scenario: Ascending comparator with subtraction
      When query
        """
        SELECT array_sort(array(5, 6, 1), (l, r) -> l - r) AS result
        """
      Then query result
        | result    |
        | [1, 5, 6] |

    Scenario: Descending comparator with subtraction
      When query
        """
        SELECT array_sort(array(5, 6, 1), (l, r) -> r - l) AS result
        """
      Then query result
        | result    |
        | [6, 5, 1] |

  Rule: Comparator form — element types

    Scenario: Comparator over array of structs by field
      When query
        """
        SELECT array_sort(array(named_struct('x', 3), named_struct('x', 1), named_struct('x', 2)), (l, r) -> l.x - r.x) AS result
        """
      Then query result
        | result                  |
        | [{1}, {2}, {3}]         |

    Scenario: Comparator over strings with explicit null handling — nulls first
      When query
        """
        SELECT array_sort(array('bc', 'ab', NULL, 'dc'), (l, r) -> CASE WHEN l IS NULL AND r IS NULL THEN 0 WHEN l IS NULL THEN -1 WHEN r IS NULL THEN 1 WHEN l < r THEN 1 WHEN l > r THEN -1 ELSE 0 END) AS result
        """
      Then query result
        | result                |
        | [NULL, dc, bc, ab]    |

  Rule: Comparator form — degenerate inputs

    Scenario: Comparator on single-element array
      When query
        """
        SELECT array_sort(array(5), (l, r) -> l - r) AS result
        """
      Then query result
        | result |
        | [5]    |

    Scenario: Comparator on empty array
      When query
        """
        SELECT array_sort(CAST(array() AS ARRAY<INT>), (l, r) -> l - r) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: Comparator on null array returns null
      When query
        """
        SELECT array_sort(CAST(NULL AS ARRAY<INT>), (l, r) -> l - r) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Constant-zero comparator keeps input order
      When query
        """
        SELECT array_sort(array(3, 1, 2), (l, r) -> 0) AS result
        """
      Then query result
        | result    |
        | [3, 1, 2] |

  Rule: Comparator form — multi-row

    Scenario: Comparator applied per row
      When query
        """
        SELECT array_sort(a, (l, r) -> l - r) AS result
        FROM VALUES (array(3, 1, 2)), (array(9, 7, 8)), (CAST(NULL AS ARRAY<INT>)) AS t(a)
        """
      Then query result
        | result    |
        | [1, 2, 3] |
        | [7, 8, 9] |
        | NULL      |

  Rule: Comparator form — errors

    Scenario: Comparator returning null raises an error
      When query
        """
        SELECT array_sort(array(2, 1), (l, r) -> CAST(NULL AS INT)) AS result
        """
      Then query error .*

    Scenario: Comparator returning a non-integer type is rejected
      When query
        """
        SELECT array_sort(array(2, 1), (l, r) -> CAST(l - r AS BIGINT)) AS result
        """
      Then query error .*

    Scenario: Comparator with one parameter is rejected
      When query
        """
        SELECT array_sort(array(2, 1), x -> x) AS result
        """
      Then query error .*

    Scenario: Comparator with three parameters is rejected
      When query
        """
        SELECT array_sort(array(2, 1), (a, b, c) -> 1) AS result
        """
      Then query error .*

    Scenario: Non-array first argument is rejected
      When query
        """
        SELECT array_sort(5, (l, r) -> 1) AS result
        """
      Then query error .*

  Rule: Non-lambda expression in place of the comparator

    # A constant comparator is not a consistent ordering, so only the values that
    # leave the array untouched are pinned here. A comparator that always reports
    # "less" produces a permutation that depends on the sort algorithm, which is
    # not a contract worth asserting.

    Scenario: A constant positive comparator leaves the array untouched
      When query
        """
        SELECT array_sort(array(3, 1, 2), 1) AS result
        """
      Then query result
        | result    |
        | [3, 1, 2] |

    Scenario: A constant zero comparator leaves the array untouched
      When query
        """
        SELECT array_sort(array(3, 1, 2), 0) AS result
        """
      Then query result
        | result    |
        | [3, 1, 2] |

    Scenario: A constant comparator over an empty array
      When query
        """
        SELECT array_sort(array(), 1) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: A constant comparator over a NULL array
      When query
        """
        SELECT array_sort(CAST(NULL AS ARRAY<INT>), 1) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: A constant comparator over an array column resolves per row
      When query
        """
        SELECT array_sort(c, 1) AS result
        FROM VALUES (array(2, 1)), (array()), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query result ordered
        | result |
        | [2, 1] |
        | []     |
        | NULL   |

    @sail-bug
    Scenario: A comparator that does not return INT is still an error
      When query
        """
        SELECT array_sort(array(2, 1), true) AS result
        """
      Then query error requires return "INT" type

  Rule: A NullType array skips the comparator entirely

    Scenario: a comparator that would error is never evaluated for an all-NULL untyped array
      When query
        """
        SELECT array_sort(array(NULL, NULL), CAST(raise_error('boom') AS INT)) AS result
        """
      Then query result
        | result       |
        | [NULL, NULL] |

    Scenario: a comparator is never evaluated for a single-element NULL array
      When query
        """
        SELECT array_sort(array(NULL), CAST(raise_error('boom') AS INT)) AS result
        """
      Then query result
        | result |
        | [NULL] |

    Scenario: a comparator is never evaluated for NULL arrays coming from a column
      When query
        """
        SELECT array_sort(c, CAST(raise_error('boom') AS INT)) AS result FROM VALUES (array(NULL, NULL)), (array(NULL)) AS t(c)
        """
      Then query result ordered
        | result       |
        | [NULL, NULL] |
        | [NULL]       |
