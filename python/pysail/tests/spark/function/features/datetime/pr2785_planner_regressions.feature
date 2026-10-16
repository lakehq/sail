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

  @sail-bug
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
