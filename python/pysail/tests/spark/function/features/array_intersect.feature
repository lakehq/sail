@array_intersect
Feature: array_intersect() returns common array elements without duplicates

  Rule: Source-of-truth examples

    Scenario: array_intersect basic usage
      When query
        """
        SELECT sort_array(array_intersect(array('b', 'a', 'c'), array('c', 'd', 'a', 'f'))) AS result
        """
      Then query result
        | result |
        | [a, c] |

    Scenario: array_intersect on array columns preserves first-array order
      When query
        """
        SELECT array_intersect(c1, c2) AS result
        FROM VALUES
          (array('b', 'a', 'c'), array('c', 'd', 'a', 'f'))
        AS t(c1, c2)
        """
      Then query result
        | result |
        | [a, c] |

    Scenario: array_intersect preserves left-array order when left is shorter
      When query
        """
        SELECT array_intersect(c1, c2) AS result
        FROM VALUES
          (array(1, 2), array(2, 1, 3))
        AS t(c1, c2)
        """
      Then query result
        | result |
        | [1, 2] |

    Scenario: array_intersect with no common elements
      When query
        """
        SELECT array_intersect(array('b', 'a', 'c'), array('d', 'e', 'f')) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: array_intersect with all common elements
      When query
        """
        SELECT sort_array(array_intersect(array('a', 'b', 'c'), array('a', 'b', 'c'))) AS result
        """
      Then query result
        | result    |
        | [a, b, c] |

    Scenario: array_intersect with null values
      When query
        """
        SELECT sort_array(array_intersect(array('a', 'b', NULL), array('a', NULL, 'c'))) AS result
        """
      Then query result
        | result    |
        | [NULL, a] |

    Scenario: array_intersect with empty arrays
      When query
        """
        SELECT array_intersect(CAST(array() AS ARRAY<STRING>), array('a', 'b', 'c')) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: Duplicates and multiple rows

    Scenario: array_intersect removes duplicates from the result
      When query
        """
        SELECT sort_array(array_intersect(array(1, 2, 2, 3, 3), array(2, 2, 3, 4))) AS result
        """
      Then query result
        | result |
        | [2, 3] |

    Scenario: array_intersect on identical arrays deduplicates in left-array order
      When query
        """
        SELECT array_intersect(c1, c2) AS result
        FROM VALUES
          (array('b', 'a', 'c', 'c'), array('b', 'a', 'c', 'c'))
        AS t(c1, c2)
        """
      Then query result
        | result  |
        | [b, a, c] |

    Scenario: array_intersect across multiple rows
      When query
        """
        SELECT id, sort_array(array_intersect(left_arr, right_arr)) AS result
        FROM VALUES
          (1, array(1, 2, 3), array(2, 3, 4)),
          (2, array(1, 1, 1), array(1, 2)),
          (3, array(1, 2), array(3, 4)),
          (4, CAST(NULL AS ARRAY<INT>), array(1, 2))
        AS t(id, left_arr, right_arr)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | [2, 3] |
        | 2  | [1]    |
        | 3  | []     |
        | 4  | NULL   |

  Rule: Type-specific behavior

    Scenario: array_intersect with boolean arrays
      When query
        """
        SELECT sort_array(array_intersect(array(true, false, true), array(false, false))) AS result
        """
      Then query result
        | result  |
        | [false] |

    Scenario: array_intersect with double arrays
      When query
        """
        SELECT sort_array(array_intersect(array(1.5D, 2.5D, 3.5D), array(3.5D, 4.5D, 1.5D))) AS result
        """
      Then query result
        | result     |
        | [1.5, 3.5] |

    Scenario: array_intersect with arrays of structs
      When query
        """
        SELECT array_intersect(
          array(named_struct('id', 1, 'name', 'a'), named_struct('id', 2, 'name', 'b')),
          array(named_struct('id', 2, 'name', 'b'), named_struct('id', 3, 'name', 'c'))
        ) AS result
        """
      Then query result
        | result   |
        | [{2, b}] |

    Scenario: array_intersect with array<null> element type
      When query
        """
        SELECT id, array_intersect(left_arr, right_arr) AS result
        FROM VALUES
          (1, array(NULL), array(NULL, NULL)),
          (2, array(NULL), array()),
          (3, array(NULL, NULL), array(NULL))
        AS t(id, left_arr, right_arr)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | [NULL] |
        | 2  | []     |
        | 3  | [NULL] |

    Scenario: array<null> elements do not match typed non-null elements
      When query
        """
        SELECT id, array_intersect(left_arr, right_arr) AS result
        FROM VALUES
          (1, array(NULL, NULL), array('d', 'e', 'f')),
          (2, array(NULL),       CAST(array() AS ARRAY<STRING>)),
          (3, array(),           CAST(array() AS ARRAY<STRING>))
        AS t(id, left_arr, right_arr)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | []     |
        | 2  | []     |
        | 3  | []     |

  Rule: Invalid inputs

    Scenario: array_intersect rejects non-array inputs
      When query
        """
        SELECT array_intersect(1, array(1)) AS result
        """
      Then query error .*

    Scenario: array_intersect rejects incompatible element types
      When query
        """
        SELECT array_intersect(array(1), array('1')) AS result
        """
      Then query error .*
