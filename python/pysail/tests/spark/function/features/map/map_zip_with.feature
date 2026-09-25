@function(lambda)
Feature: map_zip_with merges the union of keys

  Background:
    Given config spark.sql.ansi.enabled = true
    Given config spark.sql.mapZipWithUsesJavaCollections = true
    Given config spark.sql.caseSensitive = false

  Rule: Key unions preserve first occurrence order and missing values are null

    Scenario Outline: Map zip lambda with <case>
      When query
        """
        SELECT map_zip_with(map('a', 1, 'b', 2), map('b', 3, 'c', 4),
                            (k, v1, v2) -> <body>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                          | body                           | result                              |
        | both value parameters         | coalesce(v1, 0) + coalesce(v2, 0) | {a -> 1, b -> 5, c -> 4}           |
        | null missing values           | v1 + v2                        | {a -> NULL, b -> 5, c -> NULL}      |
        | only key parameter            | k                              | {a -> a, b -> b, c -> c}            |
        | only left value parameter     | v1                             | {a -> 1, b -> 2, c -> NULL}         |
        | only right value parameter    | v2                             | {a -> NULL, b -> 3, c -> 4}         |
        | right value before left value | coalesce(v2, v1)                | {a -> 1, b -> 3, c -> 4}            |
        | no parameters                 | 7                              | {a -> 7, b -> 7, c -> 7}            |
        | null result                   | NULL                           | {a -> NULL, b -> NULL, c -> NULL}   |

    Scenario: Value types are independent and lambda result type can change
      When query
        """
        SELECT map_zip_with(map('a', 1), map('a', 'x', 'b', 'y'),
                            (k, v1, v2) -> concat_ws(':', k, v1, v2)) AS result
        """
      Then query result
        | result                    |
        | {a -> a:1:x, b -> b:y}    |

    Scenario: Empty and null maps remain distinct
      When query
        """
        SELECT id, map_zip_with(a, b, (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)) AS result
        FROM VALUES
          (1, map('a', 1), CAST(map() AS MAP<STRING, INT>)),
          (2, CAST(NULL AS MAP<STRING, INT>), map('b', 2)),
          (3, CAST(map() AS MAP<STRING, INT>), map('b', 2)),
          (4, CAST(map() AS MAP<STRING, INT>), CAST(map() AS MAP<STRING, INT>)),
          (5, map('a', 1), CAST(NULL AS MAP<STRING, INT>)) AS t(id, a, b)
        """
      Then query result
        | id | result   |
        | 1  | {a -> 1} |
        | 2  | NULL     |
        | 3  | {b -> 2} |
        | 4  | {}       |
        | 5  | NULL     |

    Scenario: An untyped empty map adopts the other map key type
      When query
        """
        SELECT map_zip_with(map(), map('a', 2), (k, v1, v2) -> v2) AS result
        """
      Then query result
        | result   |
        | {a -> 2} |

  Rule: Map lambdas resolve outer columns and nested scope

    Scenario: Captures retain original rows after null maps are skipped
      When query
        """
        SELECT id, map_zip_with(a, b, (k, v1, v2) -> coalesce(v1, v2) + id) AS result
        FROM VALUES
          (1, map('a', 1), map('b', 2)),
          (2, CAST(NULL AS MAP<STRING, INT>), map('a', 0)),
          (3, CAST(map() AS MAP<STRING, INT>), CAST(map() AS MAP<STRING, INT>)),
          (4, map('c', 3), map('d', 4)) AS t(id, a, b)
        """
      Then query result
        | id | result            |
        | 1  | {a -> 2, b -> 3}  |
        | 2  | NULL              |
        | 3  | {}                |
        | 4  | {c -> 7, d -> 8}  |

    Scenario: Nested zip lambda captures all three map lambda parameters
      When query
        """
        SELECT map_zip_with(map(1, 2), map(1, 3), (k, v1, v2) ->
                 zip_with(array(10), array(20), (x, y) -> k + v1 + v2 + x + y)) AS result
        """
      Then query result
        | result      |
        | {1 -> [36]} |

    Scenario: Enclosing lambda is captured by map inputs and merge lambda
      When query
        """
        SELECT transform(array(1, 2), x ->
                 map_zip_with(map('a', x), map('a', 10), (k, v1, v2) -> v1 + v2 + x)) AS result
        """
      Then query result
        | result                    |
        | [{a -> 12}, {a -> 14}]    |

  Rule: Key coercion and equality match Spark maps

    Scenario: Integral keys widen before lambda binding
      When query
        """
        SELECT map_zip_with(map(1, 'a'), map(1L, 'b', 2L, 'c'),
                            (k, v1, v2) -> concat_ws(':', typeof(k), v1, v2)) AS result
        """
      Then query result
        | result                             |
        | {1 -> bigint:a:b, 2 -> bigint:c}    |

    Scenario: Binary keys compare by content
      When query
        """
        SELECT map_values(map_zip_with(map(unhex('01'), 1), map(unhex('01'), 2),
                                       (k, v1, v2) -> v1 + v2)) AS result
        """
      Then query result
        | result |
        | [3]    |

    Scenario Outline: Composite <case> keys compare using Spark equality
      When query
        """
        SELECT map_values(map_zip_with(map(<left>, 1), map(<right>, 2),
                                       (k, v1, v2) -> v1 + v2)) AS result
        """
      Then query result
        | result |
        | [3]    |

      Examples:
        | case             | left                       | right                     |
        | array            | array(1, NULL)             | array(1, NULL)            |
        | struct           | named_struct('x', 1)       | named_struct('x', 1)      |
        | nested zero      | array(-0.0D)               | array(0.0D)               |
        | nested NaN       | array(double('NaN'))       | array(double('NaN'))      |

    Scenario: Legacy mode widens numeric keys to strings
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT map_zip_with(map(1, 2), map('1', 3), (k, v1, v2) -> v1 + v2) AS result
        """
      Then query result
        | result   |
        | {1 -> 5} |

  Rule: Unreachable maps and lambda bodies do not execute

    Scenario: Null left map skips an erroring right map
      When query
        """
        SELECT map_zip_with(CAST(NULL AS MAP<STRING, INT>), map('a', CAST(raise_error('boom') AS INT)),
                            (k, v1, v2) -> v1 + v2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Empty maps skip an erroring lambda
      When query
        """
        SELECT map_zip_with(map(), map(), (k, v1, v2) -> raise_error('boom')) AS result
        """
      Then query result
        | result |
        | {}     |

  Rule: Invalid lambda and key types fail at analysis

    Scenario Outline: Invalid map zip argument <case>
      When query
        """
        SELECT map_zip_with(<arguments>)
        """
      Then query error (?i)<error>

      Examples:
        | case                 | arguments                                         | error      |
        | too few parameters   | map(1, 2), map(1, 3), (k, v) -> v                  | lambda     |
        | too many parameters  | map(1, 2), map(1, 3), (k, v1, v2, x) -> x          | lambda     |
        | non-map input        | array(1), map(1, 3), (k, v1, v2) -> v2             | map        |
        | unsafe ANSI key cast | map(1, 2), map('1', 3), (k, v1, v2) -> v1 + v2    | key\|types |

    Scenario: Plain map zip body permits an untyped null collection
      When query
        """
        SELECT map_zip_with(NULL, map(1, 2), 7) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: The legacy Scala map-key equality mode retains its compatibility behavior

    @sail-bug
    Scenario: Legacy map zip mode preserves distinct NaN keys
      Given config spark.sql.mapZipWithUsesJavaCollections = false
      When query
        """
        SELECT map_values(map_zip_with(map(double('NaN'), 1), map(double('NaN'), 2),
                 (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0))) AS result
        """
      Then query result
        | result |
        | [1, 2] |

  Rule: Struct map keys follow the configured name resolver

    Scenario: Case-insensitive map keys adopt the left field name
      When query
        """
        SELECT map_values(map_zip_with(map(named_struct('a', 1), 2), map(named_struct('A', 1), 3),
                                       (k, v1, v2) -> v1 + v2)) AS result
        """
      Then query result
        | result |
        | [5]    |

    Scenario: Case-sensitive map keys reject differently named fields
      Given config spark.sql.caseSensitive = true
      When query
        """
        SELECT map_zip_with(map(named_struct('a', 1), 2), map(named_struct('A', 1), 3),
                            (k, v1, v2) -> v1 + v2)
        """
      Then query error (?i)key|types

  Rule: Decimal key widening retains integral digits before fractional digits

    Scenario: Decimal key coercion bounds precision without losing integral range
      When query
        """
        SELECT map_values(map_zip_with(map(CAST(1 AS DECIMAL(38, 0)), 1),
                                       map(CAST(1 AS DECIMAL(38, 10)), 2),
                                       (k, v1, v2) -> typeof(k))) AS result
        """
      Then query result
        | result          |
        | [decimal(38,0)] |

    Scenario: Decimal keys retain large integral values while widening
      When query
        """
        SELECT map_values(map_zip_with(
                 map(CAST('12345678901234567890123456789012345678' AS DECIMAL(38, 0)), 1),
                 map(CAST(1 AS DECIMAL(38, 10)), 2),
                 (k, v1, v2) -> coalesce(v1, v2))) AS result
        """
      Then query result
        | result |
        | [1, 2] |

  Rule: Temporal map keys follow Spark's null-safe cast restrictions

    Scenario: Date keys widen to timestamp keys
      When query
        """
        SELECT map_values(map_zip_with(map(DATE'2020-01-01', 1),
                                       map(TIMESTAMP'2020-01-01 00:00:00', 2),
                                       (k, v1, v2) -> v1 + v2)) AS result
        """
      Then query result
        | result |
        | [3]    |

    Scenario Outline: Date and timestamp without timezone keys are incompatible in <order> order
      When query
        """
        SELECT map_zip_with(map(<left>, 1), map(<right>, 2), (k, v1, v2) -> v1 + v2)
        """
      Then query error (?i)key|types

      Examples:
        | order      | left                                 | right                                |
        | date first | DATE'2020-01-01'                     | TIMESTAMP_NTZ'2020-01-01 00:00:00'    |
        | date last  | TIMESTAMP_NTZ'2020-01-01 00:00:00'    | DATE'2020-01-01'                     |

    Scenario Outline: Nested temporal keys can widen with nullable fields in <collection> keys
      When query
        """
        SELECT map_values(map_zip_with(map(<left>, 1), map(<right>, 2),
                                       (k, v1, v2) -> v1 + v2)) AS result
        """
      Then query result
        | result |
        | [3]    |

      Examples:
        | collection | left                                  | right                                                       |
        | array      | array(DATE'2020-01-01')                | array(TIMESTAMP_NTZ'2020-01-01 00:00:00')                     |
        | struct     | named_struct('d', DATE'2020-01-01')     | named_struct('d', TIMESTAMP_NTZ'2020-01-01 00:00:00')         |
