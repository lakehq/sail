@lambda_hof
@exists
Feature: exists higher-order function

  Rule: Basic boolean predicate evaluation

    Scenario: predicate matches at least one element returns true
      When query
        """
        SELECT exists(array(1, 2, 3), x -> x > 2) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: predicate matches no elements returns false
      When query
        """
        SELECT exists(array(1, 2, 3), x -> x > 10) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: predicate matches all elements returns true
      When query
        """
        SELECT exists(array(1, 2, 3), x -> x > 0) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: single element array predicate true
      When query
        """
        SELECT exists(array(5), x -> x > 0) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: single element array predicate false
      When query
        """
        SELECT exists(array(5), x -> x > 10) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: empty array always returns false
      When query
        """
        SELECT exists(array(), x -> x > 0) AS result
        """
      Then query result
        | result |
        | false  |

  Rule: NULL array input

    Scenario: typed NULL array input returns NULL
      When query
        """
        SELECT exists(CAST(NULL AS ARRAY<INT>), x -> x > 0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: untyped null literal is a type error
      When query
        """
        SELECT exists(null, x -> x > 0) AS result
        """
      Then query error .*

  Rule: NULL elements in array — three-valued logic

    Scenario: null in array when predicate returns false for null and true exists
      When query
        """
        SELECT exists(array(1, null, 3), x -> x > 2) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: null in array when predicate returns true for some non-null element
      When query
        """
        SELECT exists(array(1, null, 3), x -> x > 0) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: null in array when no non-null element matches and null makes predicate null
      When query
        """
        SELECT exists(array(1, null, 3), x -> x > 5) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: null element matched by IS NULL predicate
      When query
        """
        SELECT exists(array(1, null, 3), x -> x IS NULL) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: IS NOT NULL predicate still true when non-null elements exist
      When query
        """
        SELECT exists(array(1, null, 3), x -> x IS NOT NULL) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: all null array with numeric predicate returns NULL
      When query
        """
        SELECT exists(array(null, null), x -> x > 0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: all null array with IS NULL predicate returns true
      When query
        """
        SELECT exists(array(null, null), x -> x IS NULL) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: single typed null element matched by IS NULL
      When query
        """
        SELECT exists(array(CAST(NULL AS INT)), x -> x IS NULL) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Predicate returning NULL

    Scenario: predicate always returns NULL results in NULL
      When query
        """
        SELECT exists(array(1, 2, 3), x -> CAST(NULL AS BOOLEAN)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: predicate returns true for some elements and NULL for others returns true
      When query
        """
        SELECT exists(array(1, 2, 3), x -> CASE WHEN x = 2 THEN true ELSE CAST(NULL AS BOOLEAN) END) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: predicate returns false for some elements and NULL for others returns NULL
      When query
        """
        SELECT exists(array(1, 2, 3), x -> CASE WHEN x = 2 THEN false ELSE CAST(NULL AS BOOLEAN) END) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Lambda only accepts one parameter

    Scenario: two-parameter lambda is rejected as type error
      When query
        """
        SELECT exists(array(1, 2, 3), (x, i) -> x > i) AS result
        """
      Then query error .*

  Rule: Element type coverage

    Scenario: long array
      When query
        """
        SELECT exists(array(1L, 2L, 3L), x -> x > 2L) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: double array
      When query
        """
        SELECT exists(array(1.0, 2.0, 3.0), x -> x > 2.5) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: decimal array
      When query
        """
        SELECT exists(array(1.5BD, 2.5BD, 3.5BD), x -> x > 3.0BD) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: string array match found
      When query
        """
        SELECT exists(array('a', 'b', 'c'), x -> x = 'b') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: string array no match
      When query
        """
        SELECT exists(array('a', 'b', 'c'), x -> x = 'z') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: boolean array with true element
      When query
        """
        SELECT exists(array(false, false, true), x -> x) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: boolean array all false
      When query
        """
        SELECT exists(array(false, false, false), x -> x) AS result
        """
      Then query result
        | result |
        | false  |

  Rule: Complex predicates

    Scenario: AND predicate
      When query
        """
        SELECT exists(array(1, 2, 3, 4, 5), x -> x > 2 AND x < 5) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: OR predicate
      When query
        """
        SELECT exists(array(1, 2, 3), x -> x < 0 OR x > 2) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: struct array field access
      When query
        """
        SELECT exists(array(named_struct('a', 1, 'b', 2), named_struct('a', 3, 'b', 4)), s -> s.a > 2) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: struct array field access no match
      When query
        """
        SELECT exists(array(named_struct('a', 1, 'b', 2), named_struct('a', 3, 'b', 4)), s -> s.a > 10) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: nested array with inner exists
      When query
        """
        SELECT exists(array(array(1,2), array(3,4)), x -> exists(x, y -> y > 3)) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Outer column capture

    Scenario: predicate references column from outer query
      When query
        """
        SELECT exists(array(1, 2, 3), x -> x > v) AS result FROM (SELECT 2 AS v) t
        """
      Then query result
        | result |
        | true   |

  Rule: ANSI mode inside the predicate

    @sail-bug
    Scenario: short-circuit avoids division by zero under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT exists(array(1, 0, 2), x -> 10 / x > 4) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: short-circuit avoids division by zero under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT exists(array(1, 0, 2), x -> 10 / x > 4) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Predicate must return boolean

    Scenario: non-boolean predicate is a type error
      When query
        """
        SELECT exists(array(1, 2, 3), x -> x + 1) AS result
        """
      Then query error .*

  Rule: Non-lambda expression in place of the lambda

    @sail-bug
    Scenario: a constant true predicate
      When query
        """
        SELECT exists(array(1, 2), true) AS result
        """
      Then query result
        | result |
        | true   |

    @sail-bug
    Scenario: a constant false predicate
      When query
        """
        SELECT exists(array(1, 2), false) AS result
        """
      Then query result
        | result |
        | false  |

    @sail-bug
    Scenario: a constant NULL predicate
      When query
        """
        SELECT exists(array(1, 2), CAST(NULL AS BOOLEAN)) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: a predicate that only references an outer column
      When query
        """
        SELECT exists(array(1, 2), v > 0) AS result FROM (SELECT 5 AS v) t
        """
      Then query result
        | result |
        | true   |

    @sail-bug
    Scenario: the empty array wins over a constant true predicate
      When query
        """
        SELECT exists(array(), true) AS result
        """
      Then query result
        | result |
        | false  |

    @sail-bug
    Scenario: a NULL array wins over a constant true predicate
      When query
        """
        SELECT exists(CAST(NULL AS ARRAY<INT>), true) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: a constant predicate over an array column resolves per row
      When query
        """
        SELECT exists(c, true) AS result
        FROM VALUES (array(1, 2)), (array()), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query result ordered
        | result |
        | true   |
        | false  |
        | NULL   |

    @sail-bug
    Scenario: a non-boolean constant is still a type error
      When query
        """
        SELECT exists(array(1, 2), 1) AS result
        """
      Then query error The second parameter requires the "BOOLEAN" type
