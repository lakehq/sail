@function(lambda)
Feature: forall higher-order function

  Rule: Basic boolean predicate evaluation

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT forall(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                     | arr            | pred   | result |
        | predicate true for all elements returns true             | array(1, 2, 3) | x > 0  | true   |
        | predicate false for at least one element returns false   | array(1, 2, 3) | x > 1  | false  |
        | predicate true for all when all values satisfy condition | array(2, 3, 4) | x > 1  | true   |
        | single element predicate true                            | array(5)       | x > 0  | true   |
        | single element predicate false                           | array(5)       | x > 10 | false  |
        | empty array is vacuously true                            | array()        | x > 0  | true   |

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

    Scenario Outline: Three-valued logic: <case>
      When query
        """
        SELECT forall(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                   | arr                      | pred          | result |
        | null in array when some non-null element fails predicate returns false | array(1, null, 3)        | x > 2         | false  |
        | null in array when all non-null elements pass predicate returns NULL   | array(2, null, 3)        | x > 1         | NULL   |
        | null element with IS NOT NULL predicate returns false                  | array(1, null, 3)        | x IS NOT NULL | false  |
        | null element with IS NULL predicate on mixed array returns false       | array(1, null, 3)        | x IS NULL     | false  |
        | all null array with numeric predicate returns NULL                     | array(null, null)        | x > 0         | NULL   |
        | all null array with IS NULL predicate returns true                     | array(null, null)        | x IS NULL     | true   |
        | single typed null element with IS NULL predicate returns true          | array(CAST(NULL AS INT)) | x IS NULL     | true   |

  Rule: Predicate returning NULL

    Scenario Outline: Predicate returning NULL: <case>
      When query
        """
        SELECT forall(array(1, 2, 3), x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                      | pred                                                      | result |
        | predicate always returns NULL results in NULL                             | CAST(NULL AS BOOLEAN)                                     | NULL   |
        | predicate returns true for some elements and NULL for others returns NULL | CASE WHEN x = 2 THEN true ELSE CAST(NULL AS BOOLEAN) END  | NULL   |
        | predicate returns false for any element overrides NULL and returns false  | CASE WHEN x = 2 THEN false ELSE CAST(NULL AS BOOLEAN) END | false  |

  Rule: Lambda only accepts one parameter

    Scenario: two-parameter lambda is rejected as type error
      When query
        """
        SELECT forall(array(1, 2, 3), (x, i) -> x > i) AS result
        """
      Then query error .*

  Rule: Element type coverage

    Scenario Outline: Element type: <case>
      When query
        """
        SELECT forall(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                   | arr                        | pred      | result |
        | long array all satisfy condition       | array(1L, 2L, 3L)          | x > 0L    | true   |
        | double array all satisfy condition     | array(1.0, 2.0, 3.0)       | x > 0.5   | true   |
        | decimal array all satisfy condition    | array(1.5BD, 2.5BD, 3.5BD) | x > 0.0BD | true   |
        | string array not all satisfy condition | array('a', 'b', 'c')       | x > 'a'   | false  |
        | string array all satisfy condition     | array('b', 'c', 'd')       | x > 'a'   | true   |
        | boolean array all true                 | array(true, true)          | x         | true   |
        | boolean array contains false           | array(true, false)         | x         | false  |

  Rule: Complex predicates

    Scenario Outline: Complex predicate: <case>
      When query
        """
        SELECT forall(<arr>, <lambda>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                     | arr                             | lambda                     | result |
        | AND predicate all satisfy                | array(2, 4, 6)                  | x -> x > 0 AND x % 2 = 0   | true   |
        | AND predicate not all satisfy            | array(2, 3, 6)                  | x -> x > 0 AND x % 2 = 0   | false  |
        | nested array with inner forall all pass  | array(array(2, 4), array(6, 8)) | a -> forall(a, x -> x > 1) | true   |
        | nested array with inner forall some fail | array(array(2, 4), array(0, 8)) | a -> forall(a, x -> x > 1) | false  |

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

    Scenario Outline: every row is <case>
      When query
        """
        SELECT forall(c, x -> x > 2) AS result
        FROM VALUES <values> AS t(c)
        """
      Then query result ordered
        | result   |
        | <result> |
        | <result> |

      Examples:
        | case           | values                                                 | result |
        | a NULL array   | (CAST(NULL AS ARRAY<INT>)), (CAST(NULL AS ARRAY<INT>)) | NULL   |
        | an empty array | (array()), (array())                                   | true   |

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

    Scenario Outline: a false before the failing element stops evaluation under ANSI <mode>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT forall(array(100, 0, 2), x -> 10 / x > 4) AS result
        """
      Then query result
        | result |
        | false  |

      Examples:
        | mode | ansi  |
        | on   | true  |
        | off  | false |

    Scenario Outline: <case> under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT forall(<arr>, x -> 10 / x > 4) AS result
        """
      Then query error Division by zero

      Examples:
        | case                                                    | arr              |
        | the failing element comes first so it is evaluated      | array(0, 100)    |
        | a false only after the failing element does not save it | array(1, 0, 100) |

    Scenario Outline: <case> under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT forall(<arr>, x -> 10 / x > 4) AS result
        """
      Then query result
        | result |
        | false  |

      Examples:
        | case                                                    | arr              |
        | the failing element comes first so it is evaluated      | array(0, 100)    |
        | a false only after the failing element does not save it | array(1, 0, 100) |

    Scenario Outline: one row stops early while another does not under ANSI <mode>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT forall(c, x -> 10 / x > 4) AS result
        FROM VALUES (array(100, 0)), (array(1, 2)) AS t(c)
        """
      Then query result ordered
        | result |
        | false  |
        | true   |

      Examples:
        | mode | ansi  |
        | on   | true  |
        | off  | false |

  @function(nullability)
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

    @sail-bug
    Scenario: a non-null array column yields a non-nullable boolean
      When query
        """
        SELECT forall(array(id), x -> x > 0) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """
