Feature: array_compact() removes null values from an array

  Rule: Basic usage

    Scenario: array_compact removes null values from integer array
      When query
        """
        SELECT array_compact(array(1, NULL, 2, NULL, 3)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: array_compact removes null values from string array
      When query
        """
        SELECT array_compact(array('a', NULL, 'b', NULL, 'c')) AS result
        """
      Then query result
        | result    |
        | [a, b, c] |

    Scenario: array_compact with no null values returns same array
      When query
        """
        SELECT array_compact(array(1, 2, 3)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: array_compact with all null values returns empty array
      When query
        """
        SELECT array_compact(array(CAST(NULL AS INT), CAST(NULL AS INT))) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: array_compact with null at beginning
      When query
        """
        SELECT array_compact(array(NULL, 1, 2, 3)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: array_compact with null at end
      When query
        """
        SELECT array_compact(array(1, 2, 3, NULL)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

  Rule: Empty array handling

    Scenario: array_compact with empty array returns empty array
      When query
        """
        SELECT array_compact(array()) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: Null input propagation

    Scenario: array_compact with null input returns null
      When query
        """
        SELECT array_compact(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: array_compact with typed null input returns null
      When query
        """
        SELECT array_compact(CAST(NULL AS ARRAY<INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Multiple rows

    Scenario: array_compact across multiple rows
      When query
        """
        SELECT id, array_compact(arr) AS result
        FROM VALUES
          (1, array(1, NULL, 2)),
          (2, array(NULL, NULL)),
          (3, array(3, 4, 5)),
          (4, CAST(NULL AS ARRAY<INT>))
        AS t(id, arr)
        ORDER BY id
        """
      Then query result ordered
        | id | result    |
        | 1  | [1, 2]    |
        | 2  | []        |
        | 3  | [3, 4, 5] |
        | 4  | NULL      |

  Rule: Various data types

    Scenario: array_compact with double values
      When query
        """
        SELECT array_compact(array(1.1, NULL, 2.2, NULL)) AS result
        """
      Then query result
        | result     |
        | [1.1, 2.2] |

    Scenario: array_compact with boolean values
      When query
        """
        SELECT array_compact(array(true, NULL, false)) AS result
        """
      Then query result
        | result       |
        | [true, false] |
