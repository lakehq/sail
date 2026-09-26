Feature: IN subquery support

  Rule: Single-column IN subquery

    Scenario: basic IN subquery with range
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW people AS
        SELECT * FROM VALUES (2, 'Alice'), (5, 'Bob'), (8, 'Mike') AS t(age, name)
        """
      When query
        """
        SELECT age, name FROM people WHERE age IN (SELECT id FROM range(6)) ORDER BY age
        """
      Then query result ordered
        | age | name  |
        | 2   | Alice |
        | 5   | Bob   |

    Scenario: IN subquery with no matches
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW people AS
        SELECT * FROM VALUES (10, 'Alice'), (20, 'Bob') AS t(age, name)
        """
      When query
        """
        SELECT age, name FROM people WHERE age IN (SELECT id FROM range(3))
        """
      Then query result
        | age | name |

    Scenario: NOT IN subquery
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW people AS
        SELECT * FROM VALUES (2, 'Alice'), (5, 'Bob'), (8, 'Mike') AS t(age, name)
        """
      When query
        """
        SELECT age, name FROM people WHERE age NOT IN (SELECT id FROM range(6)) ORDER BY age
        """
      Then query result ordered
        | age | name |
        | 8   | Mike |

    Scenario: IN subquery with values
      When query
        """
        SELECT * FROM VALUES (1), (2), (3), (4), (5) AS t(x)
        WHERE x IN (SELECT * FROM VALUES (2), (4) AS s(y))
        ORDER BY x
        """
      Then query result ordered
        | x |
        | 2 |
        | 4 |

  Rule: Multi-column IN subquery

    Scenario: multi-column IN subquery
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW people AS
        SELECT * FROM VALUES (2, 'Alice'), (5, 'Bob'), (8, 'Mike') AS t(age, name)
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW lookup AS
        SELECT * FROM VALUES (2, 'Alice'), (8, 'Mike') AS t(id, label)
        """
      When query
        """
        SELECT age, name FROM people
        WHERE (age, name) IN (SELECT id, label FROM lookup)
        ORDER BY age
        """
      Then query result ordered
        | age | name  |
        | 2   | Alice |
        | 8   | Mike  |

    Scenario: multi-column IN subquery with expressions
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW data AS
        SELECT * FROM VALUES (1, 10), (2, 20), (3, 30) AS t(a, b)
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW targets AS
        SELECT * FROM VALUES (3, 19), (4, 29) AS t(x, y)
        """
      When query
        """
        SELECT a, b FROM data
        WHERE (a + 1, b - 1) IN (SELECT x, y FROM targets)
        ORDER BY a
        """
      Then query result ordered
        | a | b  |
        | 2 | 20 |
        | 3 | 30 |

    Scenario: multi-column NOT IN subquery
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW people AS
        SELECT * FROM VALUES (2, 'Alice'), (5, 'Bob'), (8, 'Mike') AS t(age, name)
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW lookup AS
        SELECT * FROM VALUES (2, 'Alice'), (8, 'Mike') AS t(id, label)
        """
      When query
        """
        SELECT age, name FROM people
        WHERE (age, name) NOT IN (SELECT id, label FROM lookup)
        ORDER BY age
        """
      Then query result ordered
        | age | name |
        | 5   | Bob  |

  Rule: Projected uncorrelated IN subquery

    Scenario: projected IN preserves input rows and ignores duplicate matches
      When query
        """
        SELECT id, id IN (SELECT x FROM VALUES (1), (1), (2) t(x)) AS present
        FROM VALUES (1), (2), (3), (3) u(id)
        ORDER BY id
        """
      Then query result ordered
        | id | present |
        | 1  | true    |
        | 2  | true    |
        | 3  | false   |
        | 3  | false   |
      Then query schema
        """
        root
         |-- id: integer (nullable = false)
         |-- present: boolean (nullable = false)
        """

    Scenario: projected IN and NOT IN account for null candidates and nested negation
      When query
        """
        SELECT id,
          id IN (SELECT x FROM VALUES (1), (NULL) t(x)) AS present,
          id NOT IN (SELECT x FROM VALUES (1), (NULL) t(x)) AS absent,
          NOT (id IN (SELECT x FROM VALUES (1), (NULL) t(x))) AS negated,
          NOT (id NOT IN (SELECT x FROM VALUES (1), (NULL) t(x))) AS double_negated,
          NOT (NOT (id IN (SELECT x FROM VALUES (1), (NULL) t(x)))) AS nested_not
        FROM VALUES (1), (2), (NULL) u(id)
        ORDER BY id NULLS LAST
        """
      Then query result ordered
        | id   | present | absent | negated | double_negated | nested_not |
        | 1    | true    | false  | false   | true           | true       |
        | 2    | false   | false  | false   | false          | false      |
        | NULL | false   | false  | false   | false          | false      |
      Then query schema
        """
        root
         |-- id: integer (nullable = true)
         |-- present: boolean (nullable = true)
         |-- absent: boolean (nullable = true)
         |-- negated: boolean (nullable = true)
         |-- double_negated: boolean (nullable = true)
         |-- nested_not: boolean (nullable = true)
        """

    Scenario: projected NOT IN rejects null input against a nonempty subquery
      When query
        """
        SELECT id,
          id IN (SELECT x FROM VALUES (1), (2) t(x)) AS present,
          id NOT IN (SELECT x FROM VALUES (1), (2) t(x)) AS absent
        FROM VALUES (1), (3), (NULL) u(id)
        ORDER BY id NULLS LAST
        """
      Then query result ordered
        | id   | present | absent |
        | 1    | true    | false  |
        | 3    | false   | true   |
        | NULL | false   | false  |

    Scenario: projected IN and NOT IN use empty subquery semantics even for null input
      When query
        """
        SELECT id,
          id IN (SELECT x FROM VALUES (1) t(x) WHERE false) AS present,
          id NOT IN (SELECT x FROM VALUES (1) t(x) WHERE false) AS absent
        FROM VALUES (1), (NULL) u(id)
        ORDER BY id NULLS LAST
        """
      Then query result ordered
        | id   | present | absent |
        | 1    | false   | true   |
        | NULL | false   | true   |

    Scenario: projected IN supports expressions with comparison coercion inside CASE
      When query
        """
        SELECT id,
          CASE WHEN id + 1 IN (SELECT CAST(x AS BIGINT) FROM VALUES (2), (4) t(x))
            THEN 'hit' ELSE 'miss' END AS label
        FROM VALUES (1), (2), (3) u(id)
        ORDER BY id
        """
      Then query result ordered
        | id | label |
        | 1  | hit   |
        | 2  | miss  |
        | 3  | hit   |

    Scenario: projected IN compares aggregate results
      When query
        """
        SELECT
          SUM(id) IN (SELECT MAX(x) FROM VALUES (2), (3) t(x)) AS present,
          SUM(id) NOT IN (SELECT MAX(x) FROM VALUES (2) t(x)) AS absent
        FROM VALUES (1), (2) u(id)
        """
      Then query result
        | present | absent |
        | true    | true   |

    Scenario: projected IN keeps a scalar aggregate row from an empty input
      When query
        """
        SELECT id,
          id IN (SELECT MAX(x) FROM VALUES (1) t(x) WHERE false) AS present,
          id NOT IN (SELECT MAX(x) FROM VALUES (1) t(x) WHERE false) AS absent
        FROM VALUES (1), (NULL) u(id)
        ORDER BY id NULLS LAST
        """
      Then query result ordered
        | id   | present | absent |
        | 1    | false   | false  |
        | NULL | false   | false  |

    Scenario Outline: projected IN propagates literal null operands for a nonempty subquery
      When query
        """
        SELECT
          <value> IN (SELECT x FROM VALUES (1) t(x)) AS present,
          <value> NOT IN (SELECT x FROM VALUES (1) t(x)) AS absent,
          NOT (<value> IN (SELECT x FROM VALUES (1) t(x))) AS negated,
          NOT (NOT (<value> IN (SELECT x FROM VALUES (1) t(x)))) AS nested_not,
          (<value> IN (SELECT x FROM VALUES (1) t(x))) IS NULL AS unknown
        """
      Then query result
        | present | absent | negated | nested_not | unknown |
        | NULL    | NULL   | NULL    | NULL       | true    |
      Then query schema
        """
        root
         |-- present: boolean (nullable = true)
         |-- absent: boolean (nullable = true)
         |-- negated: boolean (nullable = true)
         |-- nested_not: boolean (nullable = true)
         |-- unknown: boolean (nullable = false)
        """

      Examples:
        | value                                 |
        | NULL                                  |
        | CAST(NULL AS INT)                      |
        | CAST(CAST(NULL AS SMALLINT) AS BIGINT)  |
        | STRUCT(NULL AS x)                     |

    Scenario: projected literal null IN distinguishes an empty subquery from an empty scalar aggregate
      When query
        """
        SELECT
          NULL IN (SELECT x FROM VALUES (1) t(x) WHERE false) AS empty_present,
          NULL NOT IN (SELECT x FROM VALUES (1) t(x) WHERE false) AS empty_absent,
          NULL IN (SELECT MAX(x) FROM VALUES (1) t(x) WHERE false) AS aggregate_present,
          NULL NOT IN (SELECT MAX(x) FROM VALUES (1) t(x) WHERE false) AS aggregate_absent
        """
      Then query result
        | empty_present | empty_absent | aggregate_present | aggregate_absent |
        | false         | true         | NULL              | NULL             |

    Scenario: projected literal null IN still rejects incompatible operand types
      When query
        """
        SELECT CAST(NULL AS INT) IN (SELECT array(1)) AS present
        """
      Then query error (?i)(cannot infer common argument type|can.t cast|data.?type.?mismatch)

    Scenario: projected literal null IN still evaluates a nonempty subquery
      When query
        """
        SELECT CAST(NULL AS INT) IN (
          SELECT CAST(x AS INT) FROM VALUES ('invalid') t(x)
        ) AS present
        """
      Then query error (?i)(cast_invalid_input|cannot cast string)

    Scenario Outline: projected IN propagates folded null operands before decorrelation
      When query
        """
        SELECT
          <value> IN (SELECT x FROM VALUES (1) t(x)) AS present,
          <value> NOT IN (SELECT x FROM VALUES (1) t(x)) AS absent
        FROM (SELECT CAST(NULL AS INT) AS id) u
        """
      Then query result
        | present | absent |
        | NULL    | NULL   |

      Examples:
        | value                         |
        | NULLIF(1, 1)                  |
        | TRY_CAST('invalid' AS INT)    |
        | id                            |

    Scenario Outline: projected IN normalizes indirect negation before decorrelation
      When query
        """
        SELECT id, <predicate> AS absent
        FROM VALUES (1), (2), (NULL) u(id)
        ORDER BY id NULLS LAST
        """
      Then query result ordered
        | id   | absent |
        | 1    | false  |
        | 2    | false  |
        | NULL | false  |

      Examples:
        | predicate                                                          |
        | NOT CAST(id IN (SELECT x FROM VALUES (1), (NULL) t(x)) AS BOOLEAN) |
        | (id IN (SELECT x FROM VALUES (1), (NULL) t(x))) = FALSE            |
        | (id IN (SELECT x FROM VALUES (1), (NULL) t(x))) <> TRUE            |
        | NOT (FALSE OR (id IN (SELECT x FROM VALUES (1), (NULL) t(x))))     |
        | NOT (TRUE AND (id IN (SELECT x FROM VALUES (1), (NULL) t(x))))     |

    Scenario: folded projected IN stays null when filtered through its result alias
      When query
        """
        SELECT present
        FROM (SELECT NULLIF(1, 1) IN (SELECT 1) AS present) t
        WHERE present IS NULL
        """
      Then query result
        | present |
        | NULL    |

    Scenario: an aggregate counts folded projected IN rows selected through their result alias
      When query
        """
        SELECT COUNT(*) AS n
        FROM (SELECT NULLIF(1, 1) IN (SELECT 1) AS present) t
        WHERE present IS NULL
        """
      Then query result
        | n |
        | 1 |

    Scenario: projected IN propagates constants through multiple named projections
      When query
        """
        WITH a AS (SELECT id, NULLIF(1, 1) AS x FROM range(3)),
          b AS (SELECT id, x AS y FROM a),
          c AS (SELECT id, y AS z FROM b)
        SELECT id, z IN (SELECT 1) AS present, z NOT IN (SELECT 1) AS absent
        FROM c ORDER BY id
        """
      Then query result ordered
        | id | present | absent |
        | 0  | NULL    | NULL   |
        | 1  | NULL    | NULL   |
        | 2  | NULL    | NULL   |

    Scenario: projected IN folds union branches independently
      When query
        """
        SELECT tag, x IN (SELECT 1) AS present
        FROM (
          SELECT 0 AS tag, NULLIF(1, 1) AS x
          UNION ALL
          SELECT 1, 1
        ) t
        ORDER BY tag
        """
      Then query result ordered
        | tag | present |
        | 0   | NULL    |
        | 1   | true    |

    Scenario: projected IN folds union branches through multiple named projections
      When query
        """
        WITH a AS (
          SELECT 0 AS tag, NULLIF(1, 1) AS x
          UNION ALL
          SELECT 1, 1
        ),
          b AS (SELECT tag, x AS y FROM a),
          c AS (SELECT tag, y AS z FROM b)
        SELECT tag, z IN (SELECT 1) AS present FROM c ORDER BY tag
        """
      Then query result ordered
        | tag | present |
        | 0   | NULL    |
        | 1   | true    |

    Scenario: projected IN does not propagate constants from an outer join nullable side
      When query
        """
        SELECT a.id, b.x IN (SELECT 1) AS present
        FROM range(3) a
        LEFT JOIN (SELECT id, NULLIF(1, 1) AS x FROM range(2)) b ON a.id = b.id
        ORDER BY a.id
        """
      Then query result ordered
        | id | present |
        | 0  | false   |
        | 1  | false   |
        | 2  | false   |

    Scenario Outline: projected IN propagates constants after a filter eliminates an outer join
      When query
        """
        SELECT a.id,
          b.x IN (SELECT 1) AS present,
          b.x NOT IN (SELECT 1) AS absent
        FROM range(3) a
        LEFT JOIN (SELECT id, NULLIF(1, 1) AS x FROM range(2)) b ON a.id = b.id
        WHERE <predicate>
        ORDER BY a.id
        """
      Then query result ordered
        | id | present | absent |
        | 0  | NULL    | NULL   |
        | 1  | NULL    | NULL   |

      Examples:
        | predicate                                       |
        | b.id IS NOT NULL                                |
        | NOT (b.id IS NULL)                              |
        | CASE WHEN b.id IS NULL THEN false ELSE true END |

    Scenario Outline: projected IN preserves local values after a filter eliminates an outer join
      When query
        """
        SELECT a.id,
          b.x IN (SELECT 1) AS present,
          b.x NOT IN (SELECT 1) AS absent
        FROM range(3) a
        LEFT JOIN (SELECT col1 AS id, NULLIF(1, 1) AS x FROM VALUES (0), (1)) b
          ON a.id = b.id
        WHERE <predicate>
        ORDER BY a.id
        """
      Then query result ordered
        | id | present | absent |
        | 0  | false   | false  |
        | 1  | false   | false  |

      Examples:
        | predicate                                       |
        | b.id IS NOT NULL                                |
        | NOT (b.id IS NULL)                              |
        | CASE WHEN b.id IS NULL THEN false ELSE true END |

    Scenario Outline: projected IN preserves materialized local producer values
      When query
        """
        SELECT id, x IN (SELECT 1) AS present, x NOT IN (SELECT 1) AS absent
        FROM (<producer>) producer
        ORDER BY id
        """
      Then query result ordered
        | id | present | absent |
        | 1  | false   | false  |
        | 2  | false   | false  |

      Examples:
        | producer                                                                                                                              |
        | SELECT col1 AS id, NULLIF(1, 1) AS x FROM VALUES (1), (2)                                                                             |
        | WITH a AS (SELECT col1 AS id FROM VALUES (1), (2)), b AS (SELECT id AS renamed FROM a) SELECT renamed AS id, NULLIF(1, 1) AS x FROM b |
        | SELECT id, NULLIF(1, 1) AS x FROM (SELECT col1 AS id FROM VALUES (1), (2) WHERE col1 > 0) a                                           |
        | SELECT col1 AS id, NULLIF(1, 1) AS x FROM VALUES (1), (2) ORDER BY col1 DESC                                                          |
        | SELECT col1 AS id, NULLIF(1, 1) AS x, rand(1) AS unused FROM VALUES (1), (2)                                                          |
        | SELECT col1 AS id, NULLIF(1, 1) AS x FROM VALUES (1) UNION ALL SELECT col1 AS id, NULLIF(1, 1) AS x FROM VALUES (2)                   |

    Scenario Outline: projected IN propagates constants beyond local materialization boundaries
      When query
        """
        SELECT id, x IN (SELECT 1) AS present, x NOT IN (SELECT 1) AS absent
        FROM (<producer>) producer
        ORDER BY id
        """
      Then query result ordered
        | id | present | absent |
        | 1  | NULL    | NULL   |
        | 2  | NULL    | NULL   |

      Examples:
        | producer                                                                                                                |
        | SELECT id, NULLIF(1, 1) AS x FROM (SELECT col1 AS id FROM VALUES (1), (2) ORDER BY col1 DESC) a                         |
        | SELECT id, NULLIF(1, 1) AS x FROM (SELECT col1 AS id, COUNT(*) AS n FROM VALUES (1), (1), (2) GROUP BY col1) a          |
        | SELECT col1 AS id, NULLIF(1, 1) AS x, (SELECT MAX(id) FROM range(3)) AS unused FROM VALUES (1), (2)                     |
        | SELECT col1 AS id, NULLIF(1, 1) AS x FROM VALUES (1), (2) WHERE col1 IN (SELECT id FROM range(3))                       |
        | SELECT a.col1 AS id, NULLIF(1, 1) AS x FROM VALUES (1), (2) a JOIN VALUES (1), (2) b ON a.col1 = b.col1                 |
        | WITH v AS (SELECT col1 AS id FROM VALUES (1) UNION ALL SELECT col1 FROM VALUES (2)) SELECT id, NULLIF(1, 1) AS x FROM v |

    Scenario: projected IN preserves local producer values through a literal limit
      When query
        """
        SELECT id, x IN (SELECT 1) AS present, x NOT IN (SELECT 1) AS absent
        FROM (
          SELECT id, NULLIF(1, 1) AS x
          FROM (SELECT col1 AS id FROM VALUES (1), (2) LIMIT 1) a
        ) producer
        """
      Then query result
        | id | present | absent |
        | 1  | false   | false  |

    Scenario: projected IN preserves column semantics for an empty outer join side
      When query
        """
        SELECT a.id,
          b.x IN (SELECT 1) AS present,
          b.x NOT IN (SELECT 1) AS absent
        FROM range(2) a
        LEFT JOIN (SELECT id, 1 AS x FROM range(1) WHERE false) b ON a.id = b.id
        ORDER BY a.id
        """
      Then query result ordered
        | id | present | absent |
        | 0  | false   | false  |
        | 1  | false   | false  |

    Scenario: projected IN uses one query time while folding stable expressions
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          current_timestamp() = current_timestamp() AS same_time,
          CASE WHEN current_timestamp() = current_timestamp()
            THEN NULLIF(1, 1) ELSE 1 END IN (SELECT 1) AS present
        """
      Then query result
        | same_time | present |
        | true      | NULL    |

    Scenario: projected folded null IN preserves subquery cardinality
      When query
        """
        SELECT
          NULLIF(1, 1) IN (SELECT id FROM range(0)) AS empty_present,
          NULLIF(1, 1) NOT IN (SELECT id FROM range(0)) AS empty_absent,
          NULLIF(1, 1) IN (SELECT MAX(id) FROM range(0)) AS aggregate_present,
          NULLIF(1, 1) NOT IN (SELECT MAX(id) FROM range(0)) AS aggregate_absent
        """
      Then query result
        | empty_present | empty_absent | aggregate_present | aggregate_absent |
        | false         | true         | NULL              | NULL             |

  Rule: Struct constructors supply IN subquery values

    Scenario Outline: SQL struct constructors and quoted struct functions retain their value shapes
      When query
        """
        SELECT id FROM VALUES (1), (2) t(id)
        WHERE <value> <operator> (SELECT <candidate> FROM VALUES (1) u(x))
        """
      Then query result
        | id       |
        | <result> |

      Examples:
        | value          | operator | candidate       | result |
        | STRUCT(id)     | IN       | x               | 1      |
        | (STRUCT(id))   | NOT IN   | x               | 2      |
        | `struct`(id)   | IN       | STRUCT(x AS id) | 1      |
        | (`struct`(id)) | NOT IN   | STRUCT(x AS id) | 2      |

    Scenario Outline: a quoted struct function remains one projected IN value
      When query
        """
        SELECT <value> IN (SELECT <candidate>) AS present
        """
      Then query result
        | present |
        | true    |

      Examples:
        | value          | candidate    |
        | `struct`(1)    | STRUCT(1)    |
        | `struct`(1, 2) | STRUCT(1, 2) |
        | `struct`()     | STRUCT()     |

    Scenario Outline: an empty SQL struct constructor supplies no IN values
      When query
        """
        SELECT <value> IN (SELECT STRUCT()) AS present
        """
      Then query error (?i)(head of empty list|column count mismatch|empty IN subquery values)

      Examples:
        | value        |
        | STRUCT()     |
        | ((STRUCT())) |

    Scenario Outline: a nested struct constructor supplies exactly one IN value
      When query
        """
        SELECT <value> IN (SELECT <candidate>) AS present
        """
      Then query result
        | present |
        | true    |

      Examples:
        | value                    | candidate    |
        | STRUCT(STRUCT(1))        | STRUCT(1)    |
        | STRUCT(STRUCT(1, 2))     | STRUCT(1, 2) |
        | (STRUCT(STRUCT(1, 2)))   | STRUCT(1, 2) |
        | STRUCT(`struct`(1, 2))   | STRUCT(1, 2) |
        | STRUCT(STRUCT())        | STRUCT()     |

    Scenario: a quoted struct function cannot supply a scalar IN value
      When query
        """
        SELECT `struct`(1) IN (SELECT 1) AS present
        """
      Then query error (?i)(cannot infer common argument type|can.t cast|data.?type.?mismatch)

    Scenario: a single-field struct supplies one IN subquery value
      When query
        """
        WITH structs AS (
          SELECT STRUCT(x AS x, y AS y) AS s
          FROM VALUES (CAST(1 AS BIGINT), CAST(2 AS BIGINT)), (2, 3) t(x, y)
        )
        SELECT
          STRUCT(STRUCT(1 AS x, 2 AS y) AS s) IN (SELECT s FROM structs) AS present,
          STRUCT(STRUCT(9 AS x, 9 AS y) AS s) IN (SELECT s FROM structs) AS missing
        """
      Then query result
        | present | missing |
        | true    | false   |

    Scenario: a single-field struct supplies one filtered IN subquery value
      When query
        """
        SELECT id FROM VALUES (1), (2), (3) t(id)
        WHERE STRUCT(id) IN (SELECT id FROM VALUES (1), (3) u(id))
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 3  |

  Rule: Filters referencing projected IN aliases

    Scenario Outline: negating a projected IN alias uses a null-aware predicate
      When query
        """
        SELECT id
        FROM (
          SELECT id, id IN (SELECT x FROM VALUES (1), (NULL) t(x)) AS present
          FROM VALUES (1), (2), (NULL) u(id)
        ) q
        WHERE <predicate>
        """
      Then query result
        | id |

      Examples:
        | predicate       |
        | NOT present     |
        | present = FALSE |

    Scenario: negating a projected NOT IN alias restores the positive predicate
      When query
        """
        SELECT id
        FROM (
          SELECT id, id NOT IN (SELECT x FROM VALUES (1), (NULL) t(x)) AS absent
          FROM VALUES (1), (2), (NULL) u(id)
        ) q
        WHERE NOT absent
        """
      Then query result
        | id |
        | 1  |

    Scenario: Boolean comparison aliases remain available to predicate pushdown
      When query
        """
        SELECT id
        FROM (SELECT id, (id IN (SELECT 1)) = TRUE AS present FROM range(3)) q
        WHERE present
        """
      Then query result
        | id |
        | 1  |

    Scenario: unused Boolean comparisons of projected IN can be pruned
      When query
        """
        SELECT id
        FROM (SELECT id, (id IN (SELECT 1)) = TRUE AS present FROM range(3)) q
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 0  |
        | 1  |
        | 2  |

    Scenario: a limit keeps an IN alias materialized below its outer filter
      When query
        """
        SELECT id
        FROM (
          SELECT id, id IN (SELECT x FROM VALUES (1), (NULL) t(x)) AS present
          FROM VALUES (1), (2), (NULL) u(id)
          LIMIT 2
        ) q
        WHERE NOT present
        """
      Then query result
        | id |
        | 2  |

    Scenario: Boolean equality and null-safe tests retain different IN null semantics
      When query
        """
        SELECT id,
          (id IN (SELECT x FROM VALUES (1), (NULL) t(x))) = TRUE AS present,
          (id IN (SELECT x FROM VALUES (1), (NULL) t(x))) <> FALSE AS also_present,
          (id IN (SELECT x FROM VALUES (1), (NULL) t(x))) = FALSE AS absent,
          (id IN (SELECT x FROM VALUES (1), (NULL) t(x))) IS FALSE AS is_false,
          (id IN (SELECT x FROM VALUES (1), (NULL) t(x))) <=> FALSE AS null_safe_false
        FROM VALUES (1), (2), (NULL) u(id)
        ORDER BY id NULLS LAST
        """
      Then query result ordered
        | id   | present | also_present | absent | is_false | null_safe_false |
        | 1    | true    | true         | false  | false    | false           |
        | 2    | false   | false        | false  | true     | true            |
        | NULL | false   | false        | false  | true     | true            |

    Scenario: unused conditional negation of projected IN can be pruned
      When query
        """
        SELECT id
        FROM (
          SELECT id, NOT (CASE WHEN id = 2
            THEN id IN (SELECT x FROM VALUES (1), (NULL) t(x))
            ELSE FALSE END) AS present
          FROM range(3)
        ) q
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 0  |
        | 1  |
        | 2  |

    Scenario: pruning unused conditional IN preserves materialized local null operands
      When query
        """
        SELECT present
        FROM (
          SELECT x IN (SELECT 1) AS present,
            NOT (CASE WHEN id = 2
              THEN id IN (SELECT value FROM VALUES (1), (NULL) candidates(value))
              ELSE FALSE END) AS unused
          FROM (SELECT col1 AS id, NULLIF(1, 1) AS x FROM VALUES (1), (2)) producer
        ) q
        """
      Then query result
        | present |
        | false   |
        | false   |

    Scenario: unused projected IN does not execute its right side
      When query
        """
        SELECT id
        FROM (
          SELECT id, id IN (
            SELECT CAST(CASE WHEN id = 1 THEN 'invalid' ELSE '1' END AS INT)
            FROM range(3)
          ) AS unused
          FROM range(3)
        ) q
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 0  |
        | 1  |
        | 2  |

    Scenario: unused literal null IN does not execute its right-side filter
      When query
        """
        SELECT id
        FROM (
          SELECT id, NULL IN (
            SELECT id FROM range(3)
            WHERE CAST(CASE WHEN id = 1 THEN 'invalid' ELSE 'false' END AS BOOLEAN)
          ) AS unused
          FROM range(3)
        ) q
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 0  |
        | 1  |
        | 2  |

    Scenario Outline: unused projected IN preserves constant subquery errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id
        FROM (
          SELECT id, <operand> IN (SELECT CAST('invalid' AS INT)) AS unused
          FROM range(3)
        ) q
        """
      Then query error (?i)(cast_invalid_input|cannot cast string)

      Examples:
        | operand |
        | id      |
        | NULL    |

    Scenario: an unused projected IN preserves required RHS constant errors beside conditional IN
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id
        FROM (
          SELECT id, id IN (
            SELECT bad FROM (
              SELECT CAST('invalid' AS INT) AS bad,
                NOT (CASE WHEN id = 2
                  THEN id IN (SELECT x FROM VALUES (1), (NULL) t(x))
                  ELSE FALSE END) AS unused
              FROM range(3)
            ) rhs
          ) AS unused
          FROM range(3)
        ) q
        """
      Then query error (?i)(cast_invalid_input|cannot cast string)

    Scenario: projected IN prunes unused constant RHS siblings before folding
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id
        FROM (
          SELECT id, id IN (
            SELECT value FROM (
              SELECT id AS value, CAST('invalid' AS INT) AS bad,
                NOT (CASE WHEN id = 2
                  THEN id IN (SELECT x FROM VALUES (1), (NULL) t(x))
                  ELSE FALSE END) AS unused
              FROM range(3)
            ) rhs
          ) AS unused
          FROM range(3)
        ) q
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 0  |
        | 1  |
        | 2  |
