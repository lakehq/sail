@lambda_hof
@forall
Feature: forall higher-order function

  Rule: Basic boolean predicate evaluation

    Scenario: predicate true for all elements returns true
      When query
        """
        SELECT forall(array(1, 2, 3), x -> x > 0) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: predicate false for at least one element returns false
      When query
        """
        SELECT forall(array(1, 2, 3), x -> x > 1) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: predicate true for all when all values satisfy condition
      When query
        """
        SELECT forall(array(2, 3, 4), x -> x > 1) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: single element predicate true
      When query
        """
        SELECT forall(array(5), x -> x > 0) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: single element predicate false
      When query
        """
        SELECT forall(array(5), x -> x > 10) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: empty array is vacuously true
      When query
        """
        SELECT forall(array(), x -> x > 0) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: NULL array input

    Scenario: typed NULL array input returns NULL
      When query
        """
        SELECT forall(CAST(NULL AS ARRAY<INT>), x -> x > 0) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL elements in array — three-valued logic

    Scenario: null in array when some non-null element fails predicate returns false
      When query
        """
        SELECT forall(array(1, null, 3), x -> x > 2) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: null in array when all non-null elements pass predicate returns NULL
      When query
        """
        SELECT forall(array(2, null, 3), x -> x > 1) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: null element with IS NOT NULL predicate returns false
      When query
        """
        SELECT forall(array(1, null, 3), x -> x IS NOT NULL) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: null element with IS NULL predicate on mixed array returns false
      When query
        """
        SELECT forall(array(1, null, 3), x -> x IS NULL) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: all null array with numeric predicate returns NULL
      When query
        """
        SELECT forall(array(null, null), x -> x > 0) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: all null array with IS NULL predicate returns true
      When query
        """
        SELECT forall(array(null, null), x -> x IS NULL) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: single typed null element with IS NULL predicate returns true
      When query
        """
        SELECT forall(array(CAST(NULL AS INT)), x -> x IS NULL) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Predicate returning NULL

    Scenario: predicate always returns NULL results in NULL
      When query
        """
        SELECT forall(array(1, 2, 3), x -> CAST(NULL AS BOOLEAN)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: predicate returns true for some elements and NULL for others returns NULL
      When query
        """
        SELECT forall(array(1, 2, 3), x -> CASE WHEN x = 2 THEN true ELSE CAST(NULL AS BOOLEAN) END) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: predicate returns false for any element overrides NULL and returns false
      When query
        """
        SELECT forall(array(1, 2, 3), x -> CASE WHEN x = 2 THEN false ELSE CAST(NULL AS BOOLEAN) END) AS result
        """
      Then query result
        | result |
        | false  |

  Rule: Lambda only accepts one parameter

    Scenario: two-parameter lambda is rejected as type error
      When query
        """
        SELECT forall(array(1, 2, 3), (x, i) -> x > i) AS result
        """
      Then query error .*

  Rule: Element type coverage

    Scenario: long array all satisfy condition
      When query
        """
        SELECT forall(array(1L, 2L, 3L), x -> x > 0L) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: double array all satisfy condition
      When query
        """
        SELECT forall(array(1.0, 2.0, 3.0), x -> x > 0.5) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: decimal array all satisfy condition
      When query
        """
        SELECT forall(array(1.5BD, 2.5BD, 3.5BD), x -> x > 0.0BD) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: string array not all satisfy condition
      When query
        """
        SELECT forall(array('a', 'b', 'c'), x -> x > 'a') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: string array all satisfy condition
      When query
        """
        SELECT forall(array('b', 'c', 'd'), x -> x > 'a') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: boolean array all true
      When query
        """
        SELECT forall(array(true, true), x -> x) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: boolean array contains false
      When query
        """
        SELECT forall(array(true, false), x -> x) AS result
        """
      Then query result
        | result |
        | false  |

  Rule: Complex predicates

    Scenario: AND predicate all satisfy
      When query
        """
        SELECT forall(array(2, 4, 6), x -> x > 0 AND x % 2 = 0) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: AND predicate not all satisfy
      When query
        """
        SELECT forall(array(2, 3, 6), x -> x > 0 AND x % 2 = 0) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: nested array with inner forall all pass
      When query
        """
        SELECT forall(array(array(2, 4), array(6, 8)), a -> forall(a, x -> x > 1)) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: nested array with inner forall some fail
      When query
        """
        SELECT forall(array(array(2, 4), array(0, 8)), a -> forall(a, x -> x > 1)) AS result
        """
      Then query result
        | result |
        | false  |

  Rule: Outer column capture

    Scenario: predicate references column from outer query
      When query
        """
        SELECT forall(array(1, 2, 3), x -> x > v) AS result FROM (SELECT 0 AS v) t
        """
      Then query result
        | result |
        | true   |

  Rule: ANSI mode inside the predicate

    Scenario: division by zero is reached without short-circuit under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT forall(array(1, 0, 2), x -> 10 / x > 4) AS result
        """
      Then query error .*

    Scenario: division by zero yields a NULL predicate under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT forall(array(1, 0, 2), x -> 10 / x > 4) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Predicate must return boolean

    Scenario: non-boolean predicate is a type error
      When query
        """
        SELECT forall(array(1, 2, 3), x -> x + 1) AS result
        """
      Then query error .*

    @sail-bug
    Scenario: a constant boolean is accepted in place of a lambda
      When query
        """
        SELECT forall(array(1, 2), true) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Array borne by a column rather than a literal

    Scenario: distinct arrays per row are not broadcast from the first row
      When query
        """
        SELECT forall(c, x -> x > 2) AS result
        FROM VALUES (array(5)), (array(1)), (array(3)) AS t(c)
        """
      Then query result ordered
        | result |
        | true   |
        | false  |
        | true   |

    Scenario: non-empty, empty and NULL arrays in the same batch
      When query
        """
        SELECT forall(c, x -> x > 2) AS result
        FROM VALUES (array(3, 4)), (array(1, 2)), (CAST(NULL AS ARRAY<INT>)), (array()) AS t(c)
        """
      Then query result ordered
        | result |
        | true   |
        | false  |
        | NULL   |
        | true   |

    Scenario: three-valued logic resolved per row
      When query
        """
        SELECT forall(c, x -> x > 2) AS result
        FROM VALUES (array(3, NULL)), (array(1, NULL)), (array(NULL)) AS t(c)
        """
      Then query result ordered
        | result |
        | NULL   |
        | false  |
        | NULL   |

    Scenario: every row is a NULL array
      When query
        """
        SELECT forall(c, x -> x > 2) AS result
        FROM VALUES (CAST(NULL AS ARRAY<INT>)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |

    Scenario: every row is an empty array
      When query
        """
        SELECT forall(c, x -> x > 2) AS result
        FROM VALUES (array()), (array()) AS t(c)
        """
      Then query result ordered
        | result |
        | true   |
        | true   |

    Scenario: the captured column changes the predicate per row
      When query
        """
        SELECT forall(c, x -> x > v) AS result
        FROM VALUES (array(1, 2), 0), (array(1, 2), 5) AS t(c, v)
        """
      Then query result ordered
        | result |
        | true   |
        | false  |

  Rule: Short-circuit order under ANSI

    Scenario: a false before the failing element stops evaluation under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT forall(array(100, 0, 2), x -> 10 / x > 4) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: a false before the failing element stops evaluation under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT forall(array(100, 0, 2), x -> 10 / x > 4) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: the failing element comes first so it is evaluated under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT forall(array(0, 100), x -> 10 / x > 4) AS result
        """
      Then query error Division by zero

    Scenario: the failing element comes first so it is evaluated under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT forall(array(0, 100), x -> 10 / x > 4) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: a false only after the failing element does not save it under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT forall(array(1, 0, 100), x -> 10 / x > 4) AS result
        """
      Then query error Division by zero

    Scenario: a false only after the failing element does not save it under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT forall(array(1, 0, 100), x -> 10 / x > 4) AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: one row stops early while another does not under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT forall(c, x -> 10 / x > 4) AS result
        FROM VALUES (array(100, 0)), (array(1, 2)) AS t(c)
        """
      Then query result ordered
        | result |
        | false  |
        | true   |

    Scenario: one row stops early while another does not under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT forall(c, x -> 10 / x > 4) AS result
        FROM VALUES (array(100, 0)), (array(1, 2)) AS t(c)
        """
      Then query result ordered
        | result |
        | false  |
        | true   |

  Rule: Output schema

    @sail-bug
    Scenario: a non-null array literal yields a non-nullable boolean
      When query
        """
        SELECT forall(array(1, 2), x -> x > 1) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a nullable array column yields a nullable boolean
      When query
        """
        SELECT forall(c, x -> x > 1) AS result
        FROM VALUES (array(1)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
