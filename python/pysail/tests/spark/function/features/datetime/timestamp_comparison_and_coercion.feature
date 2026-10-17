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
