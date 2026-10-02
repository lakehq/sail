Feature: Array functions

  Rule: array constructor

    Scenario: array basic
      When query
        """
        SELECT array(1, 2, 3) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: array with mixed types
      When query
        """
        SELECT array(1, 2.0, 3) AS result
        """
      Then query result
        | result          |
        | [1.0, 2.0, 3.0] |

    Scenario: array with NULLs
      When query
        """
        SELECT array(1, NULL, 3) AS result
        """
      Then query result
        | result       |
        | [1, NULL, 3] |

    Scenario: empty array
      When query
        """
        SELECT array() AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: array of strings
      When query
        """
        SELECT array('a', 'b', 'c') AS result
        """
      Then query result
        | result    |
        | [a, b, c] |

  Rule: array_append

    Scenario: array_append basic
      When query
        """
        SELECT array_append(array(1, 2, 3), 4) AS result
        """
      Then query result
        | result       |
        | [1, 2, 3, 4] |

    Scenario: array_append NULL element
      When query
        """
        SELECT array_append(array(1, 2), NULL) AS result
        """
      Then query result
        | result       |
        | [1, 2, NULL] |

    Scenario: array_append to NULL array
      When query
        """
        SELECT array_append(CAST(NULL AS ARRAY<INT>), 1) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: array_prepend

    Scenario: array_prepend basic
      When query
        """
        SELECT array_prepend(array(2, 3), 1) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: array_prepend to NULL array
      When query
        """
        SELECT array_prepend(CAST(NULL AS ARRAY<INT>), 1) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: array_compact

    Scenario: array_compact removes NULLs
      When query
        """
        SELECT array_compact(array(1, NULL, 2, NULL, 3)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: array_compact no NULLs
      When query
        """
        SELECT array_compact(array(1, 2, 3)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: array_compact all NULLs
      When query
        """
        SELECT array_compact(array(CAST(NULL AS INT), CAST(NULL AS INT))) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: array_contains

    Scenario: array_contains found
      When query
        """
        SELECT array_contains(array(1, 2, 3), 2) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: array_contains not found
      When query
        """
        SELECT array_contains(array(1, 2, 3), 5) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: array_contains NULL array
      When query
        """
        SELECT array_contains(CAST(NULL AS ARRAY<INT>), 1) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: array_contains NULL element errors
      When query
        """
        SELECT array_contains(array(1, NULL, 3), NULL) AS result
        """
      Then query error array_contains

  Rule: array_distinct

    Scenario: array_distinct basic
      When query
        """
        SELECT array_distinct(array(1, 2, 2, 3, 3, 3)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: array_distinct with NULLs
      When query
        """
        SELECT array_distinct(array(1, NULL, 2, NULL)) AS result
        """
      Then query result
        | result       |
        | [1, NULL, 2] |

  Rule: array_except

    Scenario: array_except basic
      When query
        """
        SELECT array_except(array(1, 2, 3, 4), array(2, 4)) AS result
        """
      Then query result
        | result |
        | [1, 3] |

    Scenario: array_except no overlap
      When query
        """
        SELECT array_except(array(1, 2), array(3, 4)) AS result
        """
      Then query result
        | result |
        | [1, 2] |

  Rule: array_intersect

    Scenario: array_intersect basic
      When query
        """
        SELECT array_intersect(array(1, 2, 3), array(2, 3, 4)) AS result
        """
      Then query result
        | result |
        | [2, 3] |

    Scenario: array_intersect no overlap
      When query
        """
        SELECT array_intersect(array(1, 2), array(3, 4)) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: array_union

    Scenario: array_union basic
      When query
        """
        SELECT array_union(array(1, 2, 3), array(3, 4, 5)) AS result
        """
      Then query result
        | result             |
        | [1, 2, 3, 4, 5]   |

    Scenario: array_union with duplicates
      When query
        """
        SELECT array_union(array(1, 1, 2), array(2, 3, 3)) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

  Rule: array_position

    Scenario: array_position found
      When query
        """
        SELECT array_position(array(10, 20, 30), 20) AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: array_position not found
      When query
        """
        SELECT array_position(array(10, 20, 30), 99) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: array_position empty array
      When query
        """
        SELECT array_position(array(), 1) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: array_position NULL array
      When query
        """
        SELECT array_position(CAST(NULL AS ARRAY<INT>), 1) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: array_remove

    Scenario: array_remove basic
      When query
        """
        SELECT array_remove(array(1, 2, 3, 2, 1), 2) AS result
        """
      Then query result
        | result    |
        | [1, 3, 1] |

    Scenario: array_remove element not present
      When query
        """
        SELECT array_remove(array(1, 2, 3), 99) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

  Rule: array_repeat

    Scenario: array_repeat basic
      When query
        """
        SELECT array_repeat(5, 3) AS result
        """
      Then query result
        | result    |
        | [5, 5, 5] |

    Scenario: array_repeat zero times
      When query
        """
        SELECT array_repeat(5, 0) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: array_repeat NULL element
      When query
        """
        SELECT array_repeat(NULL, 3) AS result
        """
      Then query result
        | result             |
        | [NULL, NULL, NULL] |

  Rule: array_size

    Scenario: array_size basic
      When query
        """
        SELECT array_size(array(1, 2, 3)) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: array_size empty
      When query
        """
        SELECT array_size(array()) AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: array_size NULL
      When query
        """
        SELECT array_size(CAST(NULL AS ARRAY<INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: array_insert

    Scenario: array_insert at beginning
      When query
        """
        SELECT array_insert(array(2, 3, 4), 1, 1) AS result
        """
      Then query result
        | result       |
        | [1, 2, 3, 4] |

    Scenario: array_insert at end
      When query
        """
        SELECT array_insert(array(1, 2, 3), 4, 4) AS result
        """
      Then query result
        | result       |
        | [1, 2, 3, 4] |

    Scenario: array_insert in middle
      When query
        """
        SELECT array_insert(array(1, 3, 4), 2, 2) AS result
        """
      Then query result
        | result       |
        | [1, 2, 3, 4] |

    Scenario: array_insert with negative index
      When query
        """
        SELECT array_insert(array(1, 2, 4), -1, 3) AS result
        """
      Then query result
        | result       |
        | [1, 2, 4, 3] |

    Scenario: array_insert at zero index errors
      When query
        """
        SELECT array_insert(array(1, 2, 3), 0, 99) AS result
        """
      Then query error array_insert

    Scenario: array_insert NULL array
      When query
        """
        SELECT array_insert(CAST(NULL AS ARRAY<INT>), 1, 99) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: array_insert beyond end pads with NULLs
      When query
        """
        SELECT array_insert(array(1, 2), 5, 5) AS result
        """
      Then query result
        | result                  |
        | [1, 2, NULL, NULL, 5]  |

  Rule: sort_array

    Scenario: sort_array ascending
      When query
        """
        SELECT sort_array(array(3, 1, 2), true) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: sort_array descending
      When query
        """
        SELECT sort_array(array(3, 1, 2), false) AS result
        """
      Then query result
        | result    |
        | [3, 2, 1] |

    Scenario: sort_array with NULLs ascending
      When query
        """
        SELECT sort_array(array(3, NULL, 1, NULL, 2), true) AS result
        """
      Then query result
        | result                |
        | [NULL, NULL, 1, 2, 3] |

    Scenario: sort_array with NULLs descending
      When query
        """
        SELECT sort_array(array(3, NULL, 1, NULL, 2), false) AS result
        """
      Then query result
        | result                |
        | [3, 2, 1, NULL, NULL] |

  Rule: slice

    Scenario: slice basic
      When query
        """
        SELECT slice(array(1, 2, 3, 4, 5), 2, 3) AS result
        """
      Then query result
        | result    |
        | [2, 3, 4] |

    Scenario: slice from start
      When query
        """
        SELECT slice(array(1, 2, 3, 4, 5), 1, 2) AS result
        """
      Then query result
        | result |
        | [1, 2] |

    Scenario: slice zero length
      When query
        """
        SELECT slice(array(1, 2, 3), 1, 0) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: slice with negative start
      When query
        """
        SELECT slice(array(1, 2, 3, 4, 5), -3, 2) AS result
        """
      Then query result
        | result |
        | [3, 4] |

  Rule: array_join

    Scenario: array_join basic
      When query
        """
        SELECT array_join(array('a', 'b', 'c'), ',') AS result
        """
      Then query result
        | result |
        | a,b,c  |

    Scenario: array_join with NULL replacement
      When query
        """
        SELECT array_join(array('a', NULL, 'c'), ',', 'X') AS result
        """
      Then query result
        | result |
        | a,X,c  |

    Scenario: array_join skip NULLs
      When query
        """
        SELECT array_join(array('a', NULL, 'c'), ',') AS result
        """
      Then query result
        | result |
        | a,c    |

  Rule: get (zero-based index)

    Scenario: get basic
      When query
        """
        SELECT get(array(10, 20, 30), 0) AS r0, get(array(10, 20, 30), 2) AS r2
        """
      Then query result
        | r0 | r2 |
        | 10 | 30 |

    Scenario: get out of bounds
      When query
        """
        SELECT get(array(10, 20, 30), 5) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: get negative index
      When query
        """
        SELECT get(array(10, 20, 30), -1) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: array_max / array_min

    Scenario: array_max basic
      When query
        """
        SELECT array_max(array(3, 1, 5, 2, 4)) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: array_min basic
      When query
        """
        SELECT array_min(array(3, 1, 5, 2, 4)) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: array_max with NULLs
      When query
        """
        SELECT array_max(array(3, NULL, 5, NULL)) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: array_min with NULLs
      When query
        """
        SELECT array_min(array(3, NULL, 1, NULL)) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: array_max NULL array
      When query
        """
        SELECT array_max(CAST(NULL AS ARRAY<INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: array_max empty array
      When query
        """
        SELECT array_max(array()) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: arrays_overlap

    Scenario: arrays_overlap true
      When query
        """
        SELECT arrays_overlap(array(1, 2, 3), array(3, 4, 5)) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: arrays_overlap false
      When query
        """
        SELECT arrays_overlap(array(1, 2), array(3, 4)) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: arrays_overlap NULL array
      When query
        """
        SELECT arrays_overlap(CAST(NULL AS ARRAY<INT>), array(1, 2)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: arrays_overlap both have NULLs
      When query
        """
        SELECT arrays_overlap(array(1, NULL), array(2, NULL)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: flatten

    Scenario: flatten basic
      When query
        """
        SELECT flatten(array(array(1, 2), array(3, 4))) AS result
        """
      Then query result
        | result       |
        | [1, 2, 3, 4] |

    Scenario: flatten with empty inner arrays
      When query
        """
        SELECT flatten(array(array(1, 2), array(), array(3))) AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

  Rule: array_distinct with strings

    Scenario: array_distinct strings
      When query
        """
        SELECT array_distinct(array('a', 'b', 'a', 'c', 'b')) AS result
        """
      Then query result
        | result    |
        | [a, b, c] |

  Rule: sequence

    Scenario: sequence basic
      When query
        """
        SELECT sequence(1, 5) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

    Scenario: sequence with step
      When query
        """
        SELECT sequence(1, 10, 3) AS result
        """
      Then query result
        | result      |
        | [1, 4, 7, 10] |

    Scenario: sequence descending
      When query
        """
        SELECT sequence(5, 1, -1) AS result
        """
      Then query result
        | result          |
        | [5, 4, 3, 2, 1] |

    Scenario: sequence single element
      When query
        """
        SELECT sequence(1, 1) AS result
        """
      Then query result
        | result |
        | [1]    |

  Rule: shuffle

    Scenario: shuffle returns same size
      When query
        """
        SELECT array_size(shuffle(array(1, 2, 3, 4, 5))) AS result
        """
      Then query result
        | result |
        | 5      |

  Rule: arrays_zip

    Scenario: arrays_zip basic
      When query
        """
        SELECT arrays_zip(array(1, 2, 3), array('a', 'b', 'c')) AS result
        """
      Then query result
        | result                                  |
        | [{1, a}, {2, b}, {3, c}]                |

    Scenario: arrays_zip different lengths
      When query
        """
        SELECT arrays_zip(array(1, 2), array('a', 'b', 'c')) AS result
        """
      Then query result
        | result                                  |
        | [{1, a}, {2, b}, {NULL, c}]             |

  Rule: Edge cases and NULLs

    Scenario: nested arrays
      When query
        """
        SELECT array(array(1, 2), array(3, 4)) AS result
        """
      Then query result
        | result          |
        | [[1, 2], [3, 4]] |

    Scenario: array_size of nested
      When query
        """
        SELECT array_size(array(array(1, 2), array(3, 4))) AS result
        """
      Then query result
        | result |
        | 2      |
