@function(lambda)
Feature: map_filter with lambda

  Background:
    Given config spark.sql.ansi.enabled = true
    Given config spark.sql.session.timeZone = UTC

  Rule: Predicates retain only entries for which the result is true

    Scenario: Remove null values from a sparse metadata map
      When query
        """
        SELECT map_filter(map('keep', '1', 'drop', CAST(NULL AS STRING)),
                          (k, v) -> v IS NOT NULL) AS result
        """
      Then query result
        | result      |
        | {keep -> 1} |

    Scenario Outline: Lambda argument binding: <case>
      When query
        """
        SELECT map_filter(map(1, 3, 2, 1, 3, CAST(NULL AS INT)),
                          (k, v) -> <predicate>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                         | predicate             | result                        |
        | key only                     | k > 1                 | {2 -> 1, 3 -> NULL}            |
        | value only                   | v > 1                 | {1 -> 3}                      |
        | both parameters              | k > v                 | {2 -> 1}                      |
        | value referenced before key  | v < k                 | {2 -> 1}                      |
        | preserve null values         | v IS NULL             | {3 -> NULL}                   |
        | neither parameter with true  | true                  | {1 -> 3, 2 -> 1, 3 -> NULL}    |
        | neither parameter with false | false                 | {}                            |
        | neither parameter with null  | CAST(NULL AS BOOLEAN) | {}                            |
        | untyped null predicate       | NULL                  | {}                            |

    Scenario: Null and empty maps remain distinct across rows
      When query
        """
        SELECT id, map_filter(m, (k, v) -> v > 1) AS result
        FROM VALUES
          (1, map('a', 1, 'b', 2)),
          (2, CAST(NULL AS MAP<STRING, INT>)),
          (3, CAST(map() AS MAP<STRING, INT>)),
          (4, map('c', 3, 'd', CAST(NULL AS INT)))
        AS t(id, m)
        """
      Then query result
        | id | result   |
        | 1  | {b -> 2} |
        | 2  | NULL     |
        | 3  | {}       |
        | 4  | {c -> 3} |

  Rule: Lambda predicates resolve outer columns and nested scopes

    Scenario: Capture a different threshold for each input row
      When query
        """
        SELECT id, map_filter(m, (k, v) -> v > threshold AND k <> excluded) AS result
        FROM VALUES
          (1, map('a', 1, 'b', 3), 2, 'a'),
          (2, CAST(NULL AS MAP<STRING, INT>), 0, 'a'),
          (3, CAST(map() AS MAP<STRING, INT>), 0, 'a'),
          (4, map('c', 4, 'd', 5), 3, 'd'),
          (5, map('e', 6), CAST(NULL AS INT), 'a')
        AS t(id, m, threshold, excluded)
        """
      Then query result
        | id | result   |
        | 1  | {b -> 3} |
        | 2  | NULL     |
        | 3  | {}       |
        | 4  | {c -> 4} |
        | 5  | {}       |

    Scenario: A predicate can reference only an outer column
      When query
        """
        SELECT id, map_filter(m, (k, v) -> flag) AS result
        FROM VALUES
          (1, map('a', 1), true),
          (2, map('b', 2), false),
          (3, map('c', 3), CAST(NULL AS BOOLEAN)),
          (4, CAST(NULL AS MAP<STRING, INT>), true)
        AS t(id, m, flag)
        """
      Then query result
        | id | result   |
        | 1  | {a -> 1} |
        | 2  | {}       |
        | 3  | {}       |
        | 4  | NULL     |

    Scenario: Lambda parameters shadow columns and resolve without case sensitivity
      When query
        """
        SELECT map_filter(m, (k, v) -> K < V) AS result
        FROM VALUES (map(1, 3, 2, 1), 100, 0) AS t(m, k, v)
        """
      Then query result
        | result   |
        | {1 -> 3} |

    Scenario: A map filter can consume another map filter
      When query
        """
        SELECT map_filter(map_filter(map(1, 1, 2, 2, 3, 3), (k, v) -> v > 1),
                          (k, v) -> k < 3) AS result
        """
      Then query result
        | result   |
        | {2 -> 2} |

    Scenario: An inner array lambda captures both map parameters
      When query
        """
        SELECT map_filter(map(1, 3, 4, 2),
                          (k, v) -> size(filter(array(1, 2, 3), x -> x > k AND x < v)) > 0) AS result
        """
      Then query result
        | result   |
        | {1 -> 3} |

    Scenario: A map filter captures an enclosing array lambda
      When query
        """
        SELECT transform(array(1, 2), threshold ->
                 map_filter(map('a', 1, 'b', 2), (k, v) -> v > threshold)) AS result
        """
      Then query result
        | result         |
        | [{b -> 2}, {}] |

    Scenario: Nested map lambda parameters shadow the enclosing parameters
      When query
        """
        SELECT map_filter(map('a', map(1, 1), 'b', map(2, 3)),
                          (k, v) -> size(map_filter(v, (k, v) -> v > k)) > 0) AS result
        """
      Then query result
        | result            |
        | {b -> {2 -> 3}}   |

  Rule: Filtering preserves original key and value types

    Scenario Outline: Preserve map entries: <case>
      When query
        """
        SELECT map_filter(<map>, (k, v) -> <predicate>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case           | map                                                                  | predicate   | result          |
        | array values   | map('a', array(1), 'b', array(2, 3))                                  | size(v) > 1 | {b -> [2, 3]}   |
        | struct values  | map('a', named_struct('n', 1), 'b', named_struct('n', 2))             | v.n > 1     | {b -> {2}}      |
        | decimal values | map(1, CAST(1.25 AS DECIMAL(4, 2)), 2, CAST(2.50 AS DECIMAL(4, 2)))    | k = 2       | {2 -> 2.50}     |
        | boolean values | map('a', true, 'b', false, 'c', CAST(NULL AS BOOLEAN))                 | v           | {a -> true}     |

  Rule: Ordinary Boolean expressions are accepted as predicates

    Scenario Outline: Ordinary constant predicate: <predicate>
      When query
        """
        SELECT map_filter(map('a', 1, 'b', CAST(NULL AS INT)), <predicate>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | predicate | result                |
        | true      | {a -> 1, b -> NULL}    |
        | false     | {}                    |
        | NULL      | {}                    |

    Scenario Outline: Ordinary predicates coerce null-typed map expressions: <map>
      When query
        """
        SELECT map_filter(<map>, true) AS result
        """
      Then query result
        | result |
        | NULL   |
      Then query schema
        """
        root
         |-- result: map (nullable = true)
         |    |-- key: void
         |    |-- value: void (valueContainsNull = true)
        """

      Examples:
        | map                 |
        | NULL                |
        | raise_error('boom') |

    Scenario: An ordinary predicate accepts a null-typed map column
      When query
        """
        SELECT id, map_filter(m, flag) AS result
        FROM VALUES
          (1, NULL, true),
          (2, NULL, false),
          (3, NULL, CAST(NULL AS BOOLEAN))
        AS t(id, m, flag)
        """
      Then query result
        | id | result |
        | 1  | NULL   |
        | 2  | NULL   |
        | 3  | NULL   |

    Scenario: An ordinary predicate captures a Boolean column
      When query
        """
        SELECT id, map_filter(m, flag) AS result
        FROM VALUES
          (1, map('a', 1), true),
          (2, map('b', 2), false),
          (3, map('c', 3), CAST(NULL AS BOOLEAN)),
          (4, CAST(NULL AS MAP<STRING, INT>), true)
        AS t(id, m, flag)
        """
      Then query result
        | id | result   |
        | 1  | {a -> 1} |
        | 2  | {}       |
        | 3  | {}       |
        | 4  | NULL     |

    Scenario Outline: Ordinary predicates retain enclosing parameter names: <parameter>
      When query
        """
        SELECT transform(array(true, false), <parameter> ->
                 map_filter(map('a', 1), <parameter>)) AS result
        """
      Then query result
        | result         |
        | [{a -> 1}, {}] |

      Examples:
        | parameter   |
        | __map_key   |
        | __map_value |

  Rule: Null-typed predicates are coerced without evaluation

    Scenario Outline: Null-typed predicate: <case>
      When query
        """
        SELECT map_filter(map(1, 2), <predicate>) AS result
        """
      Then query result
        | result |
        | {}     |

      Examples:
        | case          | predicate                                                     |
        | ordinary      | raise_error('boom')                                            |
        | lambda        | (k, v) -> raise_error(CAST(v AS STRING))                        |
        | nested lambda | (k, v) -> assert_true(exists(array(v), x -> x < 0))             |

    Scenario Outline: Unused null-typed predicate captures are pruned: <case>
      When query
        """
        SELECT map_filter(map(1, 2), <predicate>) AS result
        FROM (SELECT raise_error('boom') AS err) t
        """
      Then query result
        | result |
        | {}     |

      Examples:
        | case     | predicate     |
        | ordinary | err           |
        | lambda   | (k, v) -> err |

    Scenario Outline: Boolean-typed errors still raise: <case>
      When query
        """
        SELECT map_filter(map(1, 2), <predicate>) AS result
        """
      Then query error boom

      Examples:
        | case     | predicate                                    |
        | ordinary | CAST(raise_error('boom') AS BOOLEAN)           |
        | lambda   | (k, v) -> CAST(raise_error('boom') AS BOOLEAN)  |

  Rule: Lambda evaluation respects ANSI mode and null input

    Scenario: Division by zero in the predicate raises under ANSI mode
      When query
        """
        SELECT map_filter(map('a', 0), (k, v) -> 1 / v > 0) AS result
        """
      Then query error (?i)by zero

    Scenario: Division by zero produces a null predicate with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT map_filter(map('a', 0, 'b', 1), (k, v) -> 1 / v > 0) AS result
        """
      Then query result
        | result   |
        | {b -> 1} |

    # Shared Boolean CASE evaluation can evaluate division in a skipped branch.
    @sail-bug
    Scenario: A Boolean CASE predicate skips division for zero-valued entries
      When query
        """
        SELECT id, map_filter(m, (k, v) ->
          CASE WHEN v = 0 THEN false ELSE 1 / v > 0 END) AS result
        FROM VALUES
          (1, map('a', 0, 'b', 2)),
          (2, CAST(NULL AS MAP<STRING, INT>))
        AS t(id, m)
        """
      Then query result
        | id | result   |
        | 1  | {b -> 2} |
        | 2  | NULL     |

    Scenario: Null and empty maps do not evaluate the predicate
      When query
        """
        SELECT id, map_filter(m, (k, v) -> 1 / divisor > 0) AS result
        FROM VALUES
          (1, map('a', 1), 1),
          (2, CAST(NULL AS MAP<STRING, INT>), 0),
          (3, CAST(map() AS MAP<STRING, INT>), 0),
          (4, map('b', 2), 2)
        AS t(id, m, divisor)
        """
      Then query result
        | id | result   |
        | 1  | {a -> 1} |
        | 2  | NULL     |
        | 3  | {}       |
        | 4  | {b -> 2} |

    Scenario: A batch with no map entries does not evaluate the predicate
      When query
        """
        SELECT id, map_filter(m, (k, v) -> 1 / divisor > 0) AS result
        FROM VALUES
          (1, CAST(NULL AS MAP<STRING, INT>), 0),
          (2, CAST(map() AS MAP<STRING, INT>), 0)
        AS t(id, m, divisor)
        """
      Then query result
        | id | result |
        | 1  | NULL   |
        | 2  | {}     |

    Scenario: Repeated captured expressions are evaluated only for map entries
      When query
        """
        SELECT id,
               map_filter(m, (k, v) -> 1 / divisor > 0) AS positive,
               map_filter(m, (k, v) -> 1 / divisor < 0) AS negative
        FROM VALUES
          (1, CAST(map() AS MAP<STRING, INT>), 0),
          (2, map('a', 1), 1)
        AS t(id, m, divisor)
        """
      Then query result
        | id | positive | negative |
        | 1  | {}       | {}       |
        | 2  | {a -> 1} | {}       |

  Rule: Output schema preserves map nullability

    # Sail's existing map constructor declares this non-null map nullable.
    # map_filter preserves its input nullability; fix the constructor separately.
    @sail-bug
    Scenario: Map filter preserves non-nullable literal map schema
      When query
        """
        SELECT map_filter(map(1, 2), (k, v) -> true) AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: integer
         |    |-- value: integer (valueContainsNull = false)
        """

  Rule: Invalid map and lambda arguments are rejected

    Scenario Outline: Invalid map filter argument: <case>
      When query
        """
        SELECT map_filter(<arguments>) AS result
        """
      Then query error .*

      Examples:
        | case                       | arguments                         |
        | non-map input              | array(1), (k, v) -> true          |
        | untyped null with lambda   | NULL, (k, v) -> true              |
        | one lambda parameter       | map(1, 2), k -> true              |
        | three lambda parameters    | map(1, 2), (k, v, i) -> true      |
        | non-boolean predicate      | map(1, 2), (k, v) -> v            |
        | missing predicate          | map(1, 2)                        |
        | duplicate lambda parameter | map(1, 2), (k, k) -> true         |
        | wrong arity with null body | map(1, 2), k -> raise_error('boom')           |
        | bare nested lambda         | map(1, 2), (k, v) -> (x -> NULL)              |
        | lambda in scalar predicate | map(1, 2), (k, v) -> coalesce(x -> NULL, NULL) |

  Rule: Null-typed operands retain scalar type validation

    Scenario Outline: Reject an invalid null-typed operand: <case>
      When query
        """
        SELECT map_filter(<arguments>) AS result
        """
      Then query error (?i)boolean

      Examples:
        | case               | arguments                                                 |
        | lambda CASE        | map(1, 2), (k, v) -> CASE WHEN array(1) THEN NULL END       |
        | ordinary CASE      | map(1, 2), CASE WHEN array(1) THEN NULL END                 |
        | map CASE           | CASE WHEN array(1) THEN NULL END, true                     |
        | lambda IF          | map(1, 2), (k, v) -> IF(array(1), NULL, NULL)               |
        | lambda assert_true | map(1, 2), (k, v) -> assert_true(array(1))                  |
        | map IF             | IF(array(1), NULL, NULL), true                             |
        | map assert_true    | assert_true(array(1)), true                                |

    Scenario Outline: Null-typed operands retain cast validation: <case>
      When query
        """
        SELECT map_filter(<arguments>) AS result
        """
      Then query error (?i)cast

      Examples:
        | case              | arguments                                                            |
        | lambda CAST       | map(1, 2), (k, v) -> CASE WHEN CAST(array(v) AS BOOLEAN) THEN NULL END |
        | ordinary TRY_CAST | map(1, 2), CASE WHEN TRY_CAST(array(1) AS BOOLEAN) THEN NULL END       |
        | map CAST          | CASE WHEN CAST(array(1) AS BOOLEAN) THEN NULL END, true               |
        | direct CAST       | map(1, 2), (k, v) -> CAST(array(v) AS VOID)                            |

    Scenario Outline: Null-typed predicates discard unchecked higher-order returns: <case>
      When query
        """
        SELECT map_filter(map(1, 2), (k, v) ->
          CASE WHEN <condition> THEN NULL END) AS result
        """
      Then query result
        | result |
        | {}     |

      Examples:
        | case          | condition                                                                                         |
        | map entries   | map_entries(map_filter(map(1, 2), (a, b) -> b)) IS NOT NULL                                         |
        | nested lambda | transform(array(1), x -> map_filter(map(1, 2), (a, b) -> b)) IS NOT NULL                             |
        | runtime cast  | CAST('x' AS BOOLEAN)                                                                               |
