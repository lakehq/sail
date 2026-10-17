Feature: Timestamp comparison planning preserves query-time values

  Scenario Outline: Mixed timestamps share the query clock in <timezone>
    Given config spark.sql.session.timeZone = <timezone>
    When query
      """
      SELECT ts = current_timestamp() AS direct,
             current_timestamp() = ts AS reverse,
             ts = now() AS alias
      FROM (SELECT localtimestamp() AS ts)
      """
    Then query result
      | direct | reverse | alias |
      | true   | true    | true  |

    Examples:
      | timezone       |
      | UTC            |
      | Asia/Kathmandu |

  Scenario: Nested current-time expressions are evaluated with the query clock
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT ts < date_trunc('day', current_timestamp()) AS truncated,
             ts < to_timestamp(unix_timestamp()) AS epoch_seconds
      FROM VALUES (TIMESTAMP_NTZ '2000-01-01 00:00:00') AS t(ts)
      """
    Then query result
      | truncated | epoch_seconds |
      | true      | true          |

  Scenario: Nested temporal coercion handles an unrelated null field
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(a[0].t AS STRING) AS first, CAST(a[1].t AS STRING) AS second,
             a[0].s IS NULL AS null_sibling, a[1].s AS string_sibling
      FROM (
        SELECT array(
          named_struct('t', TIMESTAMP_NTZ '2024-03-10 02:30:00', 's', NULL),
          named_struct('t', TIMESTAMP '2024-03-10 03:30:00', 's', 'x')
        ) AS a
      )
      """
    Then query result
      | first               | second              | null_sibling | string_sibling |
      | 2024-03-10 03:30:00 | 2024-03-10 03:30:00 | true         | x              |

  Scenario: Nested temporal coercion preserves trailing and all-null siblings
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(a[0].t AS STRING) AS first, CAST(a[1].t AS STRING) AS second,
             a[0].s AS string_sibling, a[1].s IS NULL AS null_sibling,
             a[0].n IS NULL AND a[1].n IS NULL AS all_null,
             typeof(a[0].n) AS null_type
      FROM (
        SELECT array(
          named_struct('t', TIMESTAMP_NTZ '2024-03-10 02:30:00', 's', 'x', 'n', NULL),
          named_struct('t', TIMESTAMP '2024-03-10 03:30:00', 's', NULL, 'n', NULL)
        ) AS a
      )
      """
    Then query result
      | first               | second              | string_sibling | null_sibling | all_null | null_type |
      | 2024-03-10 03:30:00 | 2024-03-10 03:30:00 | x              | true         | true     | void      |

  Scenario: Nested temporal coercion widens numeric siblings alongside nulls
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(a[0].t AS STRING) AS timestamp_value, a[0].n IS NULL AS null_sibling,
             a[1].n AS int_sibling, a[2].n AS long_sibling, typeof(a[1].n) AS number_type
      FROM (
        SELECT array(
          named_struct('t', TIMESTAMP_NTZ '2024-03-10 02:30:00', 'n', NULL),
          named_struct('t', TIMESTAMP '2024-03-10 03:30:00', 'n', 1),
          named_struct('t', TIMESTAMP '2024-03-10 03:30:00', 'n', 2147483648L)
        ) AS a
      )
      """
    Then query result
      | timestamp_value     | null_sibling | int_sibling | long_sibling | number_type |
      | 2024-03-10 03:30:00 | true         | 1           | 2147483648   | bigint      |

  @sail-bug
  Scenario: Nested timestamp casts handle an unrelated string to integer cast
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(a.t AS STRING) AS timestamp_value, a.s AS number
      FROM (
        SELECT CAST(
          named_struct('t', TIMESTAMP_NTZ '2024-03-10 02:30:00', 's', '1')
          AS STRUCT<t: TIMESTAMP, s: INT>
        ) AS a
      )
      """
    Then query result
      | timestamp_value     | number |
      | 2024-03-10 03:30:00 | 1      |

  Scenario: Temporal struct casts preserve repeated field positions
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(value.a AS STRING) AS first, CAST(value.b AS STRING) AS second
      FROM (
        SELECT CAST(CAST(
          named_struct('t', TIMESTAMP_NTZ '2020-01-01 01:00:00',
                       't', TIMESTAMP_NTZ '2020-01-02 02:00:00')
          AS STRUCT<t: TIMESTAMP, t: TIMESTAMP>)
          AS STRUCT<a: TIMESTAMP, b: TIMESTAMP>) AS value
      )
      """
    Then query result
      | first               | second              |
      | 2020-01-01 01:00:00 | 2020-01-02 02:00:00 |

  Scenario Outline: Temporal <conversion> evaluates a volatile struct once
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT count(*) AS inconsistent
      FROM (
        SELECT CAST(<conversion>(
          CASE WHEN rand() < 0.5
            THEN named_struct('t', TIMESTAMP_NTZ '2000-01-01', 'n', 0)
            ELSE named_struct('t', TIMESTAMP_NTZ '2001-01-01', 'n', 1)
          END <target>) AS STRING) AS value
        FROM range(1000)
      )
      WHERE value NOT IN ('{2000-01-01 00:00:00, 0}', '{2001-01-01 00:00:00, 1}')
      """
    Then query result
      | inconsistent |
      | 0            |

    Examples:
      | conversion | target                                       |
      | CAST       | AS STRUCT<t: TIMESTAMP, n: INT>               |
      | coalesce   | , CAST(NULL AS STRUCT<t: TIMESTAMP, n: INT>)   |

  Scenario Outline: Folded timestamp gap comparisons retain instant semantics for <source>
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT ts = TIMESTAMP '2020-03-08 03:30:00' AS equal FROM <source>
      """
    Then query result
      | equal |
      | true  |

    Examples:
      | source                                                                        |
      | VALUES (TIMESTAMP_NTZ '2020-03-08 02:30:00') AS t(ts)                           |
      | (SELECT TIMESTAMP_NTZ '2020-03-08 02:30:00' AS ts FROM range(1))                 |

  Scenario: Explicit timestamp cast comparisons distinguish overlap instants
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id,
             CAST(ts AS TIMESTAMP_NTZ) = TIMESTAMP_NTZ '2020-11-01 01:30:00' AS equal,
             TIMESTAMP_NTZ '2020-11-01 01:30:00' = CAST(ts AS TIMESTAMP_NTZ) AS reverse,
             CAST(CAST(CAST(ts AS TIMESTAMP_NTZ) AS TIMESTAMP) AS TIMESTAMP_NTZ)
               = TIMESTAMP_NTZ '2020-11-01 01:30:00' AS nested
      FROM (
        SELECT id, CASE WHEN id < 2
          THEN TIMESTAMP '2020-11-01 08:30:00Z' + id * INTERVAL '1' HOUR END AS ts
        FROM range(3)
      )
      ORDER BY id
      """
    Then query result
      | id | equal | reverse | nested |
      | 0  | true  | true    | true   |
      | 1  | false | false   | false  |
      | 2  | NULL  | NULL    | NULL   |

  Scenario: Nullif uses the same timestamp comparison as equality
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id, ts = TIMESTAMP '2020-03-08 03:30:00' AS equal,
             CAST(nullif(ts, TIMESTAMP '2020-03-08 03:30:00') AS STRING) AS value
      FROM (
        SELECT id, CAST(CASE WHEN id = 0 THEN '2020-03-08 02:30:00'
          ELSE '2020-03-08 03:30:00' END AS TIMESTAMP_NTZ) AS ts
        FROM range(2)
      )
      ORDER BY id
      """
    Then query result
      | id | equal | value               |
      | 0  | false | 2020-03-08 02:30:00 |
      | 1  | true  | NULL                |

  Scenario: Timestamp comparisons in lambda bodies unwrap a valid bound
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT values[0] AS gap, values[1] AS normal
      FROM (
        SELECT transform(
          array(TIMESTAMP_NTZ '2020-03-08 02:30:00', TIMESTAMP_NTZ '2020-03-08 03:30:00'),
          t -> t = TIMESTAMP '2020-03-08 03:30:00'
        ) AS values
      )
      """
    Then query result
      | gap   | normal |
      | false | true   |

  Scenario: Timestamp comparison rewriting preserves intervening date casts
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id,
             CAST(CAST(CAST(ts AS TIMESTAMP_NTZ) AS DATE) AS TIMESTAMP_NTZ)
               = TIMESTAMP_NTZ '2020-11-01 00:00:00' AS equal
      FROM (
        SELECT id, TIMESTAMP '2020-11-01 08:30:00Z' + id * INTERVAL '1' HOUR AS ts
        FROM range(2)
      )
      ORDER BY id
      """
    Then query result
      | id | equal |
      | 0  | true  |
      | 1  | true  |

  Scenario Outline: Rewritten skipped-day comparisons retain <grouping> output names
    Given config spark.sql.session.timeZone = Pacific/Apia
    When query
      """
      SELECT d = TIMESTAMP '2011-12-31 00:00:00' AS equal, count(*) AS n
      FROM (SELECT date_add(DATE '2011-12-29', CAST(id AS INT)) AS d FROM range(4))
      GROUP BY <grouping>
      ORDER BY equal
      """
    Then query result
      | equal | n |
      | false | 3 |
      | true  | 1 |

    Examples:
      | grouping                                                     |
      | d = TIMESTAMP '2011-12-31 00:00:00'                            |
      | GROUPING SETS ((d = TIMESTAMP '2011-12-31 00:00:00'))           |

  Scenario Outline: Mixed timestamp IN folds a single-use conditional source before equality
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id, ts IN (<bounds>) AS result FROM (
        SELECT id, CAST(CASE WHEN id = 0 THEN '2020-03-08 02:30:00'
          ELSE '2020-03-08 03:30:00' END AS TIMESTAMP_NTZ) AS ts
        FROM range(2)
      ) ORDER BY id
      """
    Then query result
      | id | result |
      | 0  | true   |
      | 1  | true   |

    Examples:
      | bounds                                                                   |
      | TIMESTAMP '2020-03-08 03:30:00'                                           |
      | TIMESTAMP '2020-03-08 03:30:00', TIMESTAMP '2020-03-08 03:30:00'            |

  Scenario: Mixed timestamp IN retains a conditional source reused by another output
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT CAST(ts AS STRING) AS source,
             ts IN (TIMESTAMP '2020-03-08 03:30:00') AS result
      FROM (
        SELECT CAST(CASE WHEN id = 0 THEN '2020-03-08 02:30:00'
          ELSE '2020-03-08 03:30:00' END AS TIMESTAMP_NTZ) AS ts
        FROM range(1)
      )
      """
    Then query result
      | source              | result |
      | 2020-03-08 02:30:00 | false  |

  Scenario: A comparison pushed into a union unwraps before evaluating its local branch
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id, ts = TIMESTAMP '2020-03-08 03:30:00' AS result FROM (
        SELECT 0 AS id, ts FROM VALUES (TIMESTAMP_NTZ '2020-03-08 02:30:00') AS t(ts)
        UNION ALL
        SELECT id + 1 AS id, CAST(CASE WHEN id = 0 THEN '2020-03-08 02:30:00'
          ELSE '2020-03-08 03:30:00' END AS TIMESTAMP_NTZ) AS ts FROM range(2)
      ) ORDER BY id
      """
    Then query result
      | id | result |
      | 0  | false  |
      | 1  | false  |
      | 2  | true   |

  Scenario Outline: Nested timestamp casts fold literal conditional branches before <operator>
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id,
             CAST(CAST(CAST(CASE WHEN id = 0 THEN '2020-03-08 02:30:00'
               ELSE '2020-03-08 03:30:00' END AS TIMESTAMP_NTZ)
               AS TIMESTAMP) AS TIMESTAMP_NTZ)
               <operator> (TIMESTAMP '2020-03-08 03:30:00') AS result
      FROM range(2) ORDER BY id
      """
    Then query result
      | id | result   |
      | 0  | <result> |
      | 1  | <result> |

    Examples:
      | operator | result |
      | =        | true   |
      | IN       | true   |
      | NOT IN   | false  |

  Scenario Outline: Temporal struct casts retain <rows> rows with null parents
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT count(*) AS total,
             count(CAST(source AS STRUCT<t: TIMESTAMP>)) AS present
      FROM (
        SELECT CASE WHEN id < 2 THEN CAST(NULL AS STRUCT<t: TIMESTAMP_NTZ>)
          ELSE named_struct('t', TIMESTAMP_NTZ '2020-03-08 02:30:00') END AS source
        FROM range(<rows>)
      )
      """
    Then query result
      | total  | present |
      | <rows> | 0       |

    Examples:
      | rows |
      | 0    |
      | 2    |

  Scenario: A single-use timestamp alias preserves distinct overlap instants
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT x = TIMESTAMP_NTZ '2020-11-01 01:30:00' AS equal FROM (
        SELECT CAST(TIMESTAMP '2020-11-01 08:30:00Z' + id * INTERVAL '1' HOUR AS TIMESTAMP_NTZ) AS x
        FROM range(2)
      )
      """
    Then query result
      | equal |
      | true  |
      | false |

  Scenario Outline: Nested timestamp casts compare <branches> CASE branches at an overlap
    Given config spark.sql.ansi.enabled = true
    And config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id, CAST(CAST(CASE WHEN id = 0 THEN TIMESTAMP '2020-11-01 08:30:00Z'
        ELSE <alternative> END AS TIMESTAMP_NTZ) AS TIMESTAMP)
        = TIMESTAMP '2020-11-01 08:30:00Z' AS result FROM range(2) ORDER BY id
      """
    Then query result ordered
      | id | result          |
      | 0  | true            |
      | 1  | <second_result> |

    Examples:
      | branches        | alternative                                                 | second_result |
      | partly foldable | TIMESTAMP '2020-11-01 08:30:00Z' + id * INTERVAL '1' HOUR       | false         |
      | literal         | TIMESTAMP '2020-11-01 09:30:00Z'                              | true          |
