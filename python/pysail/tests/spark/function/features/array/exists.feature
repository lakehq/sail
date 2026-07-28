@lambda_hof
@exists
Feature: exists higher-order function

  Rule: Basic boolean predicate evaluation

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT exists(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                | arr            | pred   | result |
        | predicate matches at least one element returns true | array(1, 2, 3) | x > 2  | true   |
        | predicate matches no elements returns false         | array(1, 2, 3) | x > 10 | false  |
        | predicate matches all elements returns true         | array(1, 2, 3) | x > 0  | true   |
        | single element array predicate true                 | array(5)       | x > 0  | true   |
        | single element array predicate false                | array(5)       | x > 10 | false  |
        | empty array always returns false                    | array()        | x > 0  | false  |

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

    Scenario Outline: Three-valued logic: <case>
      When query
        """
        SELECT exists(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                         | arr                      | pred          | result |
        | null in array when predicate returns false for null and true exists          | array(1, null, 3)        | x > 2         | true   |
        | null in array when predicate returns true for some non-null element          | array(1, null, 3)        | x > 0         | true   |
        | null in array when no non-null element matches and null makes predicate null | array(1, null, 3)        | x > 5         | NULL   |
        | null element matched by IS NULL predicate                                    | array(1, null, 3)        | x IS NULL     | true   |
        | IS NOT NULL predicate still true when non-null elements exist                | array(1, null, 3)        | x IS NOT NULL | true   |
        | all null array with numeric predicate returns NULL                           | array(null, null)        | x > 0         | NULL   |
        | all null array with IS NULL predicate returns true                           | array(null, null)        | x IS NULL     | true   |
        | single typed null element matched by IS NULL                                 | array(CAST(NULL AS INT)) | x IS NULL     | true   |

  Rule: Predicate returning NULL

    Scenario Outline: Predicate returning NULL: <case>
      When query
        """
        SELECT exists(array(1, 2, 3), x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                       | pred                                                      | result |
        | predicate always returns NULL results in NULL                              | CAST(NULL AS BOOLEAN)                                     | NULL   |
        | predicate returns true for some elements and NULL for others returns true  | CASE WHEN x = 2 THEN true ELSE CAST(NULL AS BOOLEAN) END  | true   |
        | predicate returns false for some elements and NULL for others returns NULL | CASE WHEN x = 2 THEN false ELSE CAST(NULL AS BOOLEAN) END | NULL   |

  Rule: Lambda only accepts one parameter

    Scenario: two-parameter lambda is rejected as type error
      When query
        """
        SELECT exists(array(1, 2, 3), (x, i) -> x > i) AS result
        """
      Then query error .*

  Rule: Element type coverage

    Scenario Outline: Element type: <case>
      When query
        """
        SELECT exists(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                            | arr                        | pred      | result |
        | long array                      | array(1L, 2L, 3L)          | x > 2L    | true   |
        | double array                    | array(1.0, 2.0, 3.0)       | x > 2.5   | true   |
        | decimal array                   | array(1.5BD, 2.5BD, 3.5BD) | x > 3.0BD | true   |
        | string array match found        | array('a', 'b', 'c')       | x = 'b'   | true   |
        | string array no match           | array('a', 'b', 'c')       | x = 'z'   | false  |
        | boolean array with true element | array(false, false, true)  | x         | true   |
        | boolean array all false         | array(false, false, false) | x         | false  |

  Rule: Complex predicates

    Scenario Outline: Complex predicate: <case>
      When query
        """
        SELECT exists(<arr>, <lambda>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                               | arr                                                               | lambda                     | result |
        | AND predicate                      | array(1, 2, 3, 4, 5)                                              | x -> x > 2 AND x < 5       | true   |
        | OR predicate                       | array(1, 2, 3)                                                    | x -> x < 0 OR x > 2        | true   |
        | struct array field access          | array(named_struct('a', 1, 'b', 2), named_struct('a', 3, 'b', 4)) | s -> s.a > 2               | true   |
        | struct array field access no match | array(named_struct('a', 1, 'b', 2), named_struct('a', 3, 'b', 4)) | s -> s.a > 10              | false  |
        | nested array with inner exists     | array(array(1,2), array(3,4))                                     | x -> exists(x, y -> y > 3) | true   |

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

    Scenario Outline: short-circuit avoids division by zero under ANSI <mode>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT exists(array(1, 0, 2), x -> 10 / x > 4) AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | mode | ansi  |
        | on   | true  |
        | off  | false |

  Rule: Predicate must return boolean

    Scenario: non-boolean predicate is a type error
      When query
        """
        SELECT exists(array(1, 2, 3), x -> x + 1) AS result
        """
      Then query error .*

    @sail-bug
    Scenario: a constant boolean is accepted in place of a lambda
      When query
        """
        SELECT exists(array(1, 2), true) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Array borne by a column rather than a literal

    Scenario: distinct arrays per row are not broadcast from the first row
      When query
        """
        SELECT exists(c, x -> x > 2) AS result
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
        SELECT exists(c, x -> x > 2) AS result
        FROM VALUES (array(1, 2)), (array(3, 4)), (CAST(NULL AS ARRAY<INT>)), (array()) AS t(c)
        """
      Then query result ordered
        | result |
        | false  |
        | true   |
        | NULL   |
        | false  |

    Scenario: three-valued logic resolved per row
      When query
        """
        SELECT exists(c, x -> x > 2) AS result
        FROM VALUES (array(1, NULL)), (array(3, NULL)), (array(NULL)) AS t(c)
        """
      Then query result ordered
        | result |
        | NULL   |
        | true   |
        | NULL   |

    Scenario Outline: every row is <case>
      When query
        """
        SELECT exists(c, x -> x > 2) AS result
        FROM VALUES <values> AS t(c)
        """
      Then query result ordered
        | result   |
        | <result> |
        | <result> |

      Examples:
        | case           | values                                                 | result |
        | a NULL array   | (CAST(NULL AS ARRAY<INT>)), (CAST(NULL AS ARRAY<INT>)) | NULL   |
        | an empty array | (array()), (array())                                   | false  |

    Scenario: the captured column changes the predicate per row
      When query
        """
        SELECT exists(c, x -> x > v) AS result
        FROM VALUES (array(1, 2), 0), (array(1, 2), 5) AS t(c, v)
        """
      Then query result ordered
        | result |
        | true   |
        | false  |

  Rule: Short-circuit order under ANSI

    Scenario Outline: <case> under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT exists(<arr>, x -> <pred>) AS result
        """
      Then query error Division by zero

      Examples:
        | case                                                | arr            | pred         |
        | an element before the first true is still evaluated | array(0, 1, 2) | 10 / x > 4   |
        | no element is true so every element is evaluated    | array(1, 0, 2) | 10 / x > 100 |
        | a true after the failing element does not save it   | array(5, 0, 1) | 10 / x > 4   |

    Scenario Outline: <case> under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT exists(<arr>, x -> <pred>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                | arr            | pred         | result |
        | an element before the first true is still evaluated | array(0, 1, 2) | 10 / x > 4   | true   |
        | no element is true so every element is evaluated    | array(1, 0, 2) | 10 / x > 100 | NULL   |
        | a true after the failing element does not save it   | array(5, 0, 1) | 10 / x > 4   | true   |

    Scenario Outline: one row short-circuits while another does not under ANSI <mode>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT exists(c, x -> 10 / x > 4) AS result
        FROM VALUES (array(1, 0)), (array(5, 5)) AS t(c)
        """
      Then query result ordered
        | result |
        | true   |
        | false  |

      Examples:
        | mode | ansi  |
        | on   | true  |
        | off  | false |

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null array literal yields a non-nullable boolean
      When query
        """
        SELECT exists(array(1, 2), x -> x > 1) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = false)
        """

    Scenario: a typed NULL array literal yields a nullable boolean
      When query
        """
        SELECT exists(CAST(NULL AS ARRAY<INT>), x -> x > 1) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

    Scenario: a nullable array column yields a nullable boolean
      When query
        """
        SELECT exists(c, x -> x > 1) AS result
        FROM VALUES (array(1)), (CAST(NULL AS ARRAY<INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """
