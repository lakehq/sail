Feature: nvl2 output schema

  Rule: Constant evaluation

    Scenario Outline: nvl2 can supply a constant identifier: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT IDENTIFIER(<expression>) AS result FROM VALUES (4) AS t(id1)
        """
      Then query result
        | result |
        | 4      |

      Examples:
        | case                    | expression                                                   |
        | null tested argument    | nvl2(NULL, 'missing', 'id1')                                   |
        | nonnull tested argument | nvl2(1, 'id1', 'missing')                                      |
        | nested conditional      | nvl2(NULL, 'missing', nvl2(1, 'id1', 'missing'))                |
        | invalid unselected cast | concat('id', CAST(nvl2(NULL, 'bad', 1) AS STRING))              |

    Scenario Outline: Constant identifier evaluation keeps unselected <function> casts lazy
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT IDENTIFIER(<expression>) AS result FROM VALUES (7) AS t(id)
        """
      Then query result
        | result |
        | 7      |

      Examples:
        | function   | expression                                                                          |
        | IF then    | IF(TRUE, 'id', CAST(CAST('invalid' AS INT) AS STRING))                                |
        | IF else    | IF(FALSE, CAST(CAST('invalid' AS INT) AS STRING), 'id')                               |
        | CASE then  | CASE WHEN TRUE THEN 'id' ELSE CAST(CAST('invalid' AS INT) AS STRING) END              |
        | CASE else  | CASE WHEN FALSE THEN CAST(CAST('invalid' AS INT) AS STRING) ELSE 'id' END             |

    # TODO: Reject table function arguments that Spark does not treat as foldable.
    #  Lowering NVL2, NVL and IFNULL to CASE lets constant evaluation accept them.
    @sail-bug
    Scenario Outline: range rejects a non-foldable <function> argument
      When query
        """
        SELECT count(*) AS n FROM range(<expression>)
        """
      Then query error NON_FOLDABLE_ARGUMENT

      Examples:
        | function | expression        |
        | nvl2     | nvl2(1, 2, 3)     |
        | nvl      | nvl(NULL, 2)      |
        | ifnull   | ifnull(NULL, 2)   |

  @function(nullability)
  Rule: Output schema

    Scenario: NVL2 with an untyped NULL default retains nullable numeric output
      When query
        """
        SELECT nvl2(id, id, NULL) AS result FROM range(2) ORDER BY id
        """
      Then query result ordered
        | result |
        | 0 |
        | 1 |
      And query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: NVL2 with two untyped NULL branches retains VOID output
      When query
        """
        SELECT nvl2(id, NULL, NULL) AS result FROM range(2)
        """
      Then query result
        | result |
        | NULL |
        | NULL |
      And query schema
        """
        root
         |-- result: void (nullable = true)
        """

    @sail-bug
    Scenario: NVL2 includes DATE to TIMESTAMP_NTZ cast nullability
      # The shared DATE cast currently reports non-nullability in Sail.
      When query
        """
        SELECT nvl2(nullif(id, 0), DATE '2020-01-01', TIMESTAMP_NTZ '2020-01-02 03:04:05') AS result
        FROM range(2) ORDER BY id
        """
      Then query result
        | result              |
        | 2020-01-02 03:04:05 |
        | 2020-01-01 00:00:00 |
      And query schema
        """
        root
         |-- result: timestamp_ntz (nullable = true)
        """

    Scenario: a non-null literal input to nvl2 yields the schema Spark declares
      When query
        """
        SELECT nvl2(NULL, 2, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

    Scenario: nvl2 preserves a nullable result branch when the tested value is a non-null literal
      When query
        """
        SELECT nvl2(1, 2, CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | 2      |
      And query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: nvl2 preserves the declared nullability of its result branches
      When query
        """
        SELECT nvl2(x, x, 0) AS result
        FROM VALUES (1), (CAST(NULL AS INT)) AS t(x)
        ORDER BY result
        """
      Then query result
        | result |
        | 0      |
        | 1      |
      And query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: nvl2 preserves non-nullability when legacy temporal branches become strings
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT nvl2(NULL, DATE '2024-01-01', '2024-02-03') AS result
        """
      Then query result
        | result     |
        | 2024-02-03 |
      And query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: nvl2 preserves nullable temporal branches when converting them to strings
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT nvl2(id, d, '2024-02-03') AS result
        FROM VALUES
          (1, CAST(NULL AS DATE)),
          (CAST(NULL AS INT), DATE '2024-01-01')
        AS t(id, d)
        """
      Then query result
        | result     |
        | NULL       |
        | 2024-02-03 |
      And query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Result type

    Scenario: nvl2 is typed by its result arguments when the tested argument is a widened CASE
      When query
        """
        SELECT
          id,
          nvl2(CASE WHEN id = 0 THEN 1 WHEN id = 1 THEN CAST(2 AS BIGINT) END, 1, 0) AS result,
          typeof(nvl2(CASE WHEN id = 0 THEN 1 WHEN id = 1 THEN CAST(2 AS BIGINT) END, 1, 0)) AS result_type
        FROM VALUES (0), (1), (2) AS t(id)
        """
      Then query result
        | id | result | result_type |
        | 0  | 1      | int         |
        | 1  | 1      | int         |
        | 2  | 0      | int         |

    Scenario Outline: nvl2 exposes its common nonnumeric result type: <case>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(nvl2(NULL, <first_branch>, <second_branch>)) AS result_type
        """
      Then query result
        | result_type   |
        | <result_type> |

      Examples:
        | case                  | ansi  | first_branch                       | second_branch                      | result_type   |
        | DATE and TIMESTAMP_NTZ | false | DATE '2024-01-01'                  | TIMESTAMP_NTZ '2024-02-03 04:05:06' | timestamp_ntz |
        | DATE and TIMESTAMP_NTZ | true  | DATE '2024-01-01'                  | TIMESTAMP_NTZ '2024-02-03 04:05:06' | timestamp_ntz |
        | TIMESTAMP_NTZ and LTZ  | false | TIMESTAMP_NTZ '2024-01-01 00:00:00' | TIMESTAMP_LTZ '2024-02-03 04:05:06' | timestamp     |
        | TIMESTAMP_NTZ and LTZ  | true  | TIMESTAMP_NTZ '2024-01-01 00:00:00' | TIMESTAMP_LTZ '2024-02-03 04:05:06' | timestamp     |
        | STRING and BINARY     | true  | 'a'                                | X'62'                              | binary        |

    # TODO: Reject NVL2 result branches without a Spark wider common type.
    #  NVL2 shares the permissive CASE coercion that IF and CASE already use.
    @sail-bug
    Scenario Outline: nvl2 rejects result branches without a common type: <case>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT nvl2(nullif(id, 0), <first_branch>, <second_branch>) AS result FROM range(2)
        """
      Then query error DATATYPE_MISMATCH

      Examples:
        | case                              | ansi  | first_branch              | second_branch    |
        | INT and STRING map keys           | true  | map(CAST(id AS INT), 'v') | map('k', 'w')    |
        | BIGINT and STRING map keys        | true  | map(id, 'v')              | map('k', 'w')    |
        | BINARY and STRING                 | false | X'61'                     | 'b'              |
        | year-month and day-time intervals | true  | INTERVAL '1' YEAR         | INTERVAL '1' DAY |

    Scenario: nvl2 returns a DATE branch as TIMESTAMP_NTZ
      When query
        """
        SELECT nvl2(1, DATE '2024-01-01', TIMESTAMP_NTZ '2024-02-03 04:05:06') AS result
        """
      Then query result collected
        | result              |
        | 2024-01-01 00:00:00 |

    Scenario: nvl2 declares the common timestamp type in its output schema
      When query
        """
        SELECT nvl2(
          NULL,
          TIMESTAMP_NTZ '2024-01-01 00:00:00',
          TIMESTAMP_LTZ '2024-02-03 04:05:06'
        ) AS result
        """
      Then query result
        | result              |
        | 2024-02-03 04:05:06 |
      And query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

  Rule: Row evaluation

    Scenario Outline: A NULL-typed error in an unselected NVL2 branch stays lazy
      When query
        """
        SELECT nvl2(<tested>, <then>, <else>) AS result FROM range(2) ORDER BY id
        """
      Then query result ordered
        | result |
        | 0 |
        | 1 |

      Examples:
        | tested | then | else |
        | id | id | raise_error('unselected error') |
        | NULL | raise_error('unselected error') | id |

    Scenario Outline: A NULL-typed error in a selected NVL2 branch is retained
      When query
        """
        SELECT nvl2(<tested>, <then>, <else>) AS result FROM range(2)
        """
      Then query error selected error

      Examples:
        | tested | then | else |
        | NULL | id | raise_error('selected error') |
        | id | raise_error('selected error') | id |

    Scenario: NVL2 retains invalid constant tested casts with two NULL branches
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT nvl2(CAST('bad' AS INT), NULL, NULL) AS result FROM range(2)
        """
      Then query error (?i)(CAST_INVALID_INPUT|cast error|cannot cast|can't cast)

    # TODO: Match Spark's elimination of deterministic tested expressions when
    # both branches are identical; shared optimization still evaluates raise_error.
    @sail-bug
    Scenario: NVL2 can discard a tested error when both branches are NULL
      When query
        """
        SELECT nvl2(raise_error('tested error'), NULL, NULL) AS result FROM range(2)
        """
      Then query result
        | result |
        | NULL |
        | NULL |

    Scenario Outline: nvl2 projected through a view stays row dependent in IN lists
      Given final statement
        """
        DROP VIEW IF EXISTS nvl2_in_input
        """
      And statement
        """
        CREATE OR REPLACE TEMP VIEW nvl2_in_input AS
        SELECT id, nvl2(x, <non_null>, <null>) AS v
        FROM VALUES (0, 1), (1, CAST(NULL AS INT)), (2, 2) AS t(id, x)
        """
      When query
        """
        SELECT id, id IN (v, 7, 8, 9) AS included,
          id NOT IN (v, 7, 8, 9) AS excluded
        FROM nvl2_in_input
        ORDER BY id
        """
      Then query result ordered
        | id | included | excluded |
        | 0  | <first>  | <not_first> |
        | 1  | <second> | <not_second> |
        | 2  | <third>  | <not_third> |

      Examples:
        | non_null | null | first | not_first | second | not_second | third | not_third |
        | x        | 0    | false | true      | false  | true       | true  | false     |
        | 0        | x    | true  | false     | NULL   | NULL       | false | true      |
        | 0        | 1    | true  | false     | true   | false      | false | true      |

    Scenario: nvl2 inside an IN list is evaluated for each row
      When query
        """
        SELECT k
        FROM VALUES (0, 'a'), (1, 'b'), (2, CAST(NULL AS STRING)) AS t(k, s)
        WHERE k IN (nvl2(s, 0, 2), 5, 6, 7)
        ORDER BY k
        """
      Then query result ordered
        | k |
        | 0 |
        | 2 |

    Scenario: nvl2 in an IN list evaluates its nullable column branch for each row
      When query
        """
        SELECT id, nvl2(x, x, 0) AS result
        FROM VALUES (0, 1), (1, CAST(NULL AS INT)), (2, 2) AS t(id, x)
        WHERE id IN (nvl2(x, x, 0), 7, 8, 9)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 2  | 2      |
      And query schema
        """
        root
         |-- id: integer (nullable = false)
         |-- result: integer (nullable = true)
        """

    Scenario: nvl2 in an IN list preserves a scalar branch beside a nonnullable column branch
      When query
        """
        SELECT k
        FROM VALUES (0, 'a'), (1, 'b'), (2, CAST(NULL AS STRING)) AS t(k, s)
        WHERE k IN (nvl2(s, 0, k), 5, 6, 7)
        ORDER BY k
        """
      Then query result ordered
        | k |
        | 0 |
        | 2 |

    Scenario: nvl2 in an IN list evaluates a scalar subquery branch for each row
      When query
        """
        SELECT id
        FROM VALUES (0, 1), (1, 1), (2, CAST(NULL AS INT)) AS t(id, x)
        WHERE id IN (
          nvl2(x, (SELECT max(v) FROM VALUES (0), (1) AS q(v)), 2), 5, 6, 7
        )
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: nvl2 with a scalar non-null result and nullable column null result stays row-dependent
      When query
        """
        SELECT k
        FROM VALUES (0, 0, CAST(NULL AS INT)), (1, 0, 1), (2, 1, 2) AS t(k, id, x)
        WHERE id IN (nvl2(x, 0, x), 7, 8, 9)
        ORDER BY k
        """
      Then query result ordered
        | k |
        | 1 |

  Rule: Persistent views

    Scenario: nvl2 in an ANSI persistent view retains its DATE result with ANSI disabled
      Given config spark.sql.ansi.enabled = true
      And final statement
        """
        DROP VIEW IF EXISTS nvl2_ansi_date_view
        """
      And statement
        """
        CREATE OR REPLACE VIEW nvl2_ansi_date_view AS
        SELECT id, nvl2(nullif(id, 0), DATE '2024-01-01', '2024-02-03') AS v
        FROM range(2)
        """
      And config spark.sql.ansi.enabled = false
      When query
        """
        SELECT id, v, typeof(v) AS v_type FROM nvl2_ansi_date_view ORDER BY id
        """
      Then query result ordered
        | id | v          | v_type |
        | 0  | 2024-02-03 | date   |
        | 1  | 2024-01-01 | date   |

  # Spark folds NVL2's `If(IsNotNull(expr1), expr2, expr3)` replacement when the
  # tested argument is constant or non-nullable (NullPropagation and SimplifyConditionals).
  Rule: Constant tested argument

    Scenario: nvl2 with a constant or non-nullable tested argument selects a branch
      When query
        """
        SELECT id, nvl2(id, a, b) AS r1, nvl2(1, a, b) AS r2, nvl2(NULL, a, b) AS r3
        FROM VALUES (1, 10, 20), (2, 30, 40) AS t(id, a, b)
        ORDER BY id
        """
      Then query result ordered
        | id | r1 | r2 | r3 |
        | 1  | 10 | 10 | 20 |
        | 2  | 30 | 30 | 40 |
      And query schema
        """
        root
         |-- id: integer (nullable = false)
         |-- r1: integer (nullable = false)
         |-- r2: integer (nullable = false)
         |-- r3: integer (nullable = false)
        """

    Scenario Outline: nvl2 with a constant tested argument filters on the selected branch: <case>
      When query
        """
        SELECT id FROM VALUES (1, 10, 20), (2, 30, 40) AS t(id, a, b)
        WHERE <predicate>
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 2  |

      Examples:
        | case              | predicate                 |
        | non-null constant | nvl2(1, a, b) > 15        |
        | null constant     | nvl2(NULL, a, b) > 25     |

    @sail-only
    Scenario: EXPLAIN nvl2 with a non-null constant tested argument projects the selected column
      When query
        """
        EXPLAIN SELECT nvl2(1, a, b) AS r FROM VALUES (1, 10, 20) AS t(id, a, b)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: EXPLAIN nvl2 with a non-null constant tested argument filters on the selected column
      When query
        """
        EXPLAIN SELECT id FROM VALUES (1, 10, 20) AS t(id, a, b) WHERE nvl2(1, a, b) > 15
        """
      Then query plan matches snapshot
