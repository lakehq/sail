Feature: Division by zero behavior

  Rule: All division by zero returns NULL when ANSI mode is disabled (Spark 4.x behavior)
    Scenario Outline: ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                         | expr                                                    |
        | Float divided by zero returns NULL           | 1.0 / 0.0                                               |
        | Negative float divided by zero returns NULL  | -1.0 / 0.0                                              |
        | Zero divided by zero returns NULL            | 0.0 / 0.0                                               |
        | Integer divided by integer zero returns NULL | 1 / 0                                                   |
        | Integer divided by float zero returns NULL   | 1 / 0.0                                                 |
        | Decimal divided by decimal zero returns NULL | CAST(1.0 AS DECIMAL(10,2)) / CAST(0.0 AS DECIMAL(10,2)) |
        | Decimal divided by integer zero returns NULL | CAST(100.50 AS DECIMAL(10,2)) / 0                       |

  Rule: Division by zero throws error when ANSI mode is enabled
    Scenario Outline: ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query error <error>

      Examples:
        | case                                                             | expr                                                    | error                |
        | Integer divided by zero throws error in ANSI mode                | 1 / 0                                                   | (?i)divide.*zero     |
        | Float divided by zero throws error in ANSI mode                  | 1.0 / 0.0                                               | (?i)divide.*zero     |
        | Decimal divided by decimal zero throws error in ANSI mode        | CAST(1.0 AS DECIMAL(10,2)) / CAST(0.0 AS DECIMAL(10,2)) | (?i)divide.*zero     |
        | Decimal divided by integer zero throws error in ANSI mode        | CAST(100.50 AS DECIMAL(10,2)) / 0                       | (?i)divide.*zero     |
        | DIV by literal zero throws error in ANSI mode                    | 10 DIV 0                                                | (?i)divide.*zero     |
        | Modulo by literal zero throws error in ANSI mode                 | 10 % 0                                                  | (?i)remainder.*zero  |
        | Computed expression evaluating to zero throws error in ANSI mode | 1 / (1 - 1)                                             | (?i)divi(?:de\|sion).*zero |

    Scenario Outline: an unused literal-zero division does not raise: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                 | expr                                                                | result |
        | unused THEN branch   | CASE WHEN false THEN 1 / 0 ELSE 7 END                                | 7.0    |
        | NULL selected branch | CASE WHEN true THEN CAST(NULL AS DOUBLE) ELSE 1 / 0 END               | NULL   |
        | NULL dividend        | CAST(NULL AS DOUBLE) / 0                                             | NULL   |

  Rule: Empty-input division respects ANSI mode
    # The ANSI=true cases where Spark folds an unused division are tested with
    # explicit engine expectations in test_math.py::test_division_by_zero_in_unevaluated_expressions.
    Scenario Outline: foldable literal-zero division before empty input with ANSI <ansi>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expr> AS result <input>
        """
      Then query <expectation>
        | result |

      Examples:
        | case                      | expr                                                  | input                             | ansi  | expectation                      |
        | empty range               | 1 / 0                                                 | FROM range(0)                     | false | result                           |
        | false filter              | 1 / 0                                                 | WHERE false                       | false | result                           |
        | nonfoldable parent        | id + (1 / 0)                                          | FROM range(0)                     | false | result                           |
        | boolean parent            | (1 / 0 > 0) AND id=0                                  | FROM range(0)                     | false | result                           |
        | scalar parent of CASE     | abs(CASE WHEN true THEN 1 / 0 ELSE id END)            | FROM range(0)                     | false | result                           |
        | arithmetic parent of CASE | 1 + (CASE WHEN true THEN 1 / 0 ELSE id END)           | FROM range(0)                     | false | result                           |
        | cast parent of CASE       | CAST(CASE WHEN true THEN 1 / 0 ELSE id END AS STRING) | FROM range(0)                     | false | result                           |
        | pruned false CASE branch  | CASE WHEN false THEN id WHEN true THEN 1 / 0 END      | FROM range(0)                     | false | result                           |
        | pruned later CASE branch  | CASE WHEN true THEN 1 / 0 WHEN false THEN id END      | FROM range(0)                     | false | result                           |
        | redundant zero limit      | 1 / 0                                                 | FROM range(0) LIMIT 0             | false | result                           |
        | redundant filtered limit  | 1 / 0                                                 | FROM range(1) WHERE false LIMIT 0 | false | result                           |
        | redundant grouped limit   | 1 / 0                                                 | FROM range(0) GROUP BY id LIMIT 0 | false | result                           |

    Scenario Outline: foldable literal-zero division with one result row and ANSI <ansi>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expr> AS result <input>
        """
      Then query <expectation>
        | result   |
        | <result> |

      Examples:
        | case                     | expr                           | input         | ansi  | expectation                      | result |
        | empty higher-order input | transform(array(), x -> 1 / 0) |               | false | result                           | []     |

    # TODO: Support SUM of untyped NULL; this ANSI=false typing gap predates the PR.
    @sail-bug
    Scenario: SUM of literal-zero division returns NULL with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT sum(1 / 0) AS result FROM range(0)
        """
      Then query result
        | result |
        | NULL   |

    Scenario Outline: empty input preserves conditional and NULL suppression: <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expr> AS result FROM range(0)
        """
      Then query result
        | result |

      Examples:
        | case                      | expr                                                                   | ansi  |
        | nonfoldable dividend      | id / 0                                                                 | true  |
        | nonfoldable dividend      | id / 0                                                                 | false |
        | NULL dividend             | CAST(NULL AS DOUBLE) / 0                                               | true  |
        | NULL dividend             | CAST(NULL AS DOUBLE) / 0                                               | false |
        | NULL parent               | CAST(NULL AS DOUBLE) + (1 / 0)                                         | true  |
        | NULL parent               | CAST(NULL AS DOUBLE) + (1 / 0)                                         | false |
        | dynamic CASE branch       | CASE WHEN id < 0 THEN 1 / 0 ELSE 7 END                                 | true  |
        | dynamic CASE branch       | CASE WHEN id < 0 THEN 1 / 0 ELSE 7 END                                 | false |
        | coalesce branch           | coalesce(id, 1 / 0)                                                    | true  |
        | coalesce branch           | coalesce(id, 1 / 0)                                                    | false |
        | NULL parent of CASE       | CAST(NULL AS DOUBLE) + CASE WHEN true THEN 1 / 0 ELSE id END           | true  |
        | NULL parent of CASE       | CAST(NULL AS DOUBLE) + CASE WHEN true THEN 1 / 0 ELSE id END           | false |
        | NULL parent of eager CASE | CAST(NULL AS DOUBLE) + abs(CASE WHEN true THEN 1 / 0 ELSE id END)      | true  |
        | NULL parent of eager CASE | CAST(NULL AS DOUBLE) + abs(CASE WHEN true THEN 1 / 0 ELSE id END)      | false |
        | nested dynamic CASE       | CASE WHEN id < 0 THEN CASE WHEN true THEN 1 / 0 ELSE id END ELSE 7 END | true  |
        | nested dynamic CASE       | CASE WHEN id < 0 THEN CASE WHEN true THEN 1 / 0 ELSE id END ELSE 7 END | false |
        | unreachable CASE branch   | CASE WHEN true THEN 7 ELSE 1 / 0 + id END                              | true  |
        | unreachable CASE branch   | CASE WHEN true THEN 7 ELSE 1 / 0 + id END                              | false |

    Scenario Outline: empty set operation preserves conditional suppression: <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expr> AS result FROM range(0)
        <operation> SELECT CAST(id AS DOUBLE) AS result FROM range(0)
        """
      Then query result
        | result |

      Examples:
        | case              | expr                                                              | operation | ansi  |
        | dynamic CASE      | CASE WHEN id < 0 THEN 1 / 0 ELSE 7 END                            | UNION ALL | true  |
        | dynamic CASE      | CASE WHEN id < 0 THEN 1 / 0 ELSE 7 END                            | UNION ALL | false |
        | NULL eager parent | CAST(NULL AS DOUBLE) + abs(CASE WHEN true THEN 1 / 0 ELSE id END) | UNION ALL | true  |
        | NULL eager parent | CAST(NULL AS DOUBLE) + abs(CASE WHEN true THEN 1 / 0 ELSE id END) | UNION ALL | false |
        | distinct barrier  | CASE WHEN true THEN 1 / 0 ELSE id END                             | UNION     | true  |
        | distinct barrier  | CASE WHEN true THEN 1 / 0 ELSE id END                             | UNION     | false |

    Scenario Outline: zero limit suppresses division before evaluating its input: <input> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT 1 / 0 AS result <input> LIMIT 0
        """
      Then query result
        | result |

      Examples:
        | input         | ansi  |
        | FROM range(1) | true  |
        | FROM range(1) | false |
        |               | true  |
        |               | false |

    Scenario Outline: zero limit suppresses division over a global aggregate with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT count(*) + (1 / 0) AS result FROM range(0) LIMIT 0
        """
      Then query result
        | result |

      Examples:
        | ansi  |
        | true  |
        | false |

  Rule: Array lambdas evaluate only existing elements
    Scenario Outline: mixed NULL and empty arrays skip lambda division <expr> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT id, transform(a, x -> <expr>) AS result
        FROM VALUES (0, CAST(NULL AS ARRAY<INT>)), (1, CAST(array() AS ARRAY<INT>)) AS t(id, a)
        ORDER BY id
        """
      Then query result
        | id | result |
        | 0  | NULL   |
        | 1  | []     |

      Examples:
        | expr  | ansi  |
        | 1 / 0 | true  |
        | 1 / 0 | false |
        | 1 / x | true  |
        | 1 / x | false |

    # A NULL element still invokes the lambda; a NULL array has no elements to visit.
    Scenario Outline: a NULL array element evaluates constant lambda division with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT transform(array(CAST(NULL AS INT)), x -> 1 / 0) AS result
        """
      Then query <expectation>
        | result   |
        | <result> |

      Examples:
        | ansi  | expectation                      | result |
        | true  | error (?i)divi(?:de\|sion).*zero |        |
        | false | result                           | [NULL] |

  Rule: Division raises only when it is evaluated (documented divergence from Spark)
    # Spark's optimizer folds constant expressions before execution, so a literal-zero division
    # over an input that produces no rows fails in Spark unless another optimizer rule removes
    # the expression first. Sail evaluates divisions only on rows that exist.
    # `query result unless Spark raises` records Spark's expectation next to Sail's result.
    Scenario Outline: a constant division that never runs does not raise: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result FROM <from>
        """
      Then query result unless Spark raises (?i)divi(?:de|sion).*zero
        | result |

      Examples:
        | case                               | expr                                 | from                              |
        | TRY_CAST zero over an empty range  | 1 / TRY_CAST(0 AS DOUBLE)            | range(0)                          |
        | computed zero over an empty range  | 1 / (1 - 1)                          | range(0)                          |
        | rows removed by a column filter    | 1 / 0                                | range(5) WHERE id > 10            |
        | alias reused by an outer query     | r + 1                                | (SELECT 1 / 0 AS r FROM range(0)) |
        | constant CASE selecting the branch | CASE WHEN true THEN 1 / 0 ELSE 7 END | range(0)                          |
        | first coalesce argument            | coalesce(1 / 0, 1)                   | range(0)                          |
        | DIV by literal zero                | 10 DIV 0                             | range(0)                          |

    Scenario: a constant remainder that never runs does not raise
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 10 % 0 AS result FROM range(0)
        """
      Then query result unless Spark raises (?i)remainder.*zero
        | result |

    Scenario Outline: a NULL operand removes the division before it runs: <case>
      # Spark evaluates the divisor before the dividend and the left operand of an addition
      # first, so a computed NULL on the other side does not stop it from raising. Sail's
      # simplifier replaces `NULL op x` by NULL before any evaluation.
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query result unless Spark raises (?i)divi(?:de|sion).*zero
        | result   |
        | <result> |

      Examples:
        | case                              | expr                   | result |
        | computed NULL dividend            | nullif(1, 1) / (1 / 0) | NULL   |
        | computed NULL right operand       | (1 / 0) + nullif(1, 1) | NULL   |
        | constant true right operand of OR | (1 / 0 > 1) OR true    | true   |

    Scenario Outline: a NULL operand evaluated first suppresses the division in Spark too: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                       | expr                           |
        | computed NULL divisor      | (1 / 0) / nullif(1, 1)         |
        | computed NULL left operand | nullif(1, 1) + (1 / 0)         |
        | literal NULL right operand | (1 / 0) + CAST(NULL AS DOUBLE) |

  Rule: DIV and modulo by a literal zero follow the same rules as division
    Scenario Outline: literal zero divisor in an unevaluated DIV or modulo: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | expr                                     | result |
        | unused DIV branch     | CASE WHEN false THEN 10 DIV 0 ELSE 7 END | 7      |
        | unused modulo branch  | CASE WHEN false THEN 10 % 0 ELSE 7 END   | 7      |
        | NULL parent of DIV    | CAST(NULL AS BIGINT) + (10 DIV 0)        | NULL   |
        | NULL parent of modulo | CAST(NULL AS INT) + (10 % 0)             | NULL   |

  Rule: Dynamic divisor division by zero raises error in ANSI mode
    Scenario Outline: Dynamic divisor: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result FROM <from>
        """
      Then query error <error>

      Examples:
        | case                                                      | expr        | from                  | error                |
        | Integer divided by dynamic zero raises error in ANSI mode | 10 / id     | (VALUES (0)) AS t(id) | (?i)divi(?:de\|sion).*zero |
        | DIV by dynamic zero raises error in ANSI mode             | 10 DIV id   | (VALUES (0)) AS t(id) | (?i)divi(?:de\|sion).*zero |
        | Modulo by dynamic zero raises error in ANSI mode          | 10 % id     | (VALUES (0)) AS t(id) | (?i)remainder.*zero  |
        | mod function with dynamic zero raises error in ANSI mode  | mod(10, id) | (VALUES (0)) AS t(id) | (?i)remainder.*zero  |
        | Division by zero in range raises error in ANSI mode       | 1 / id      | range(2)              | (?i)divi(?:de\|sion).*zero |

    Scenario Outline: Dynamic divisor (typed): <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        FROM (VALUES (0)) AS t(id)
        """
      Then query error (?i)divi(?:de|sion).*zero

      Examples:
        | case                                                      | expr                                                    |
        | Decimal divided by dynamic zero raises error in ANSI mode | CAST(10.5 AS DECIMAL(10,2)) / CAST(id AS DECIMAL(10,2)) |
        | Double divided by dynamic zero raises error in ANSI mode  | CAST(1 AS DOUBLE) / CAST(id AS DOUBLE)                  |

    Scenario Outline: Two-column: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT a, b, a <op> b AS result
        FROM (VALUES (-2, -1), (-1, 0), (0, 1), (1, 2), (2, 3)) AS t(a, b)
        ORDER BY a
        """
      Then query error <error>

      Examples:
        | case                                                            | op | error                |
        | Two-column division with zero divisor raises error in ANSI mode | /  | (?i)divi(?:de\|sion).*zero |
        | Two-column modulo with zero divisor raises error in ANSI mode   | %  | (?i)remainder.*zero  |

  Rule: Dynamic divisor division by zero returns NULL in non-ANSI mode
    Scenario Outline: Dynamic divisor non-ANSI: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result FROM (VALUES (0)) AS t(id)
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                                          | expr      |
        | Integer divided by dynamic zero returns NULL in non-ANSI mode | 10 / id   |
        | DIV by dynamic zero returns NULL in non-ANSI mode             | 10 DIV id |
        | Modulo by dynamic zero returns NULL in non-ANSI mode          | 10 % id   |

    Scenario: Decimal divided by dynamic zero returns NULL in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(10.5 AS DECIMAL(10,2)) / CAST(id AS DECIMAL(10,2)) AS result
        FROM (VALUES (0)) AS t(id)
        """
      Then query result
        | result |
        | NULL   |

  Rule: DIV and modulo by literal zero returns NULL in non-ANSI mode
    Scenario Outline: Literal zero: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                                                 | expr        |
        | DIV by literal zero returns NULL in non-ANSI mode                    | 10 DIV 0    |
        | Modulo by literal zero returns NULL in non-ANSI mode                 | 10 % 0      |
        | Computed expression evaluating to zero returns NULL in non-ANSI mode | 1 / (1 - 1) |

    Scenario: Division by zero in range returns NULL in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT 1 / id AS result FROM range(2)
        """
      Then query result
        | result |
        | NULL   |
        | 1.0    |

    Scenario: Two-column division with zero divisor returns NULL in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT a, b, a / b AS result
        FROM (VALUES (-2, -1), (-1, 0), (0, 1), (1, 2), (2, 3)) AS t(a, b)
        ORDER BY a
        """
      Then query result
        | a  | b  | result             |
        | -2 | -1 | 2.0                |
        | -1 | 0  | NULL               |
        | 0  | 1  | 0.0                |
        | 1  | 2  | 0.5                |
        | 2  | 3  | 0.6666666666666666 |

    Scenario: Two-column modulo with zero divisor returns NULL in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT a, b, a % b AS result
        FROM (VALUES (-2, -1), (-1, 0), (0, 1), (1, 2), (2, 3)) AS t(a, b)
        ORDER BY a
        """
      Then query result
        | a  | b  | result |
        | -2 | -1 | 0      |
        | -1 | 0  | NULL   |
        | 0  | 1  | 0      |
        | 1  | 2  | 1      |
        | 2  | 3  | 2      |

  Rule: Non-zero dynamic divisors work normally
    Scenario: Integer divided by non-zero dynamic divisor works in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 10 / id AS result FROM (VALUES (2)) AS t(id)
        """
      Then query result
        | result |
        | 5.0    |
