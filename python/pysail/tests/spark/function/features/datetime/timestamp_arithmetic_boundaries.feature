Feature: Timestamp arithmetic rejects overflow and retains supported wide dates

  Background:
    Given config spark.sql.session.timeZone = UTC

  @sail-bug
  Scenario Outline: Integer epoch constructors reject <function> overflow with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT unix_micros(<function>(<value>))
      """
    Then query error (?i)overflow|out of range|cast

    Examples:
      | function          | value                | ansi  |
      | timestamp_seconds | 9223372036855L       | true  |
      | timestamp_seconds | -9223372036855L      | true  |
      | timestamp_millis  | 9223372036854776L    | true  |
      | timestamp_millis  | -9223372036854776L   | true  |
      | timestamp_seconds | 9223372036855L       | false |
      | timestamp_seconds | -9223372036855L      | false |
      | timestamp_millis  | 9223372036854776L    | false |
      | timestamp_millis  | -9223372036854776L   | false |

  Scenario Outline: Timestamp arithmetic rejects <expression> overflow with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT unix_micros(<expression>) FROM range(1)
      """
    Then query error (?i)overflow|out of range

    Examples:
      | expression                                                                                   | ansi  |
      | timestampadd(SECOND, id + 9223372036855L, TIMESTAMP '1970-01-01')                               | true  |
      | timestampadd(MILLISECOND, id + 9223372036854776L, TIMESTAMP '1970-01-01')                       | true  |
      | timestampadd(MICROSECOND, id + 1L, timestamp_micros(9223372036854775807L))                      | true  |
      | timestampadd(MICROSECOND, id - 1L, timestamp_micros(CAST('-9223372036854775808' AS BIGINT)))    | true  |
      | timestamp_micros(id + 9223372036854775807L) + INTERVAL 1 MICROSECOND                            | true  |
      | timestamp_micros(id + 9223372036854775807L) + INTERVAL 1 DAY                                    | true  |
      | timestampadd(SECOND, id + 9223372036855L, TIMESTAMP '1970-01-01')                               | false |
      | timestampadd(MILLISECOND, id + 9223372036854776L, TIMESTAMP '1970-01-01')                       | false |
      | timestampadd(MICROSECOND, id + 1L, timestamp_micros(9223372036854775807L))                      | false |
      | timestampadd(MICROSECOND, id - 1L, timestamp_micros(CAST('-9223372036854775808' AS BIGINT)))    | false |
      | timestamp_micros(id + 9223372036854775807L) + INTERVAL 1 MICROSECOND                            | false |
      | timestamp_micros(id + 9223372036854775807L) + INTERVAL 1 DAY                                    | false |

  Scenario: NULL timestamps suppress quantity overflow within mixed batches
    When query
      """
      SELECT id, unix_micros(timestampadd(SECOND, quantity, ts)) AS result
      FROM (
        SELECT id, CASE WHEN id = 0 THEN 9223372036854775807L ELSE 1L END AS quantity,
               CASE WHEN id = 0 THEN CAST(NULL AS TIMESTAMP)
                    ELSE TIMESTAMP '1970-01-01' END AS ts
        FROM range(0, 2, 1, 1)
      )
      ORDER BY id
      """
    Then query result ordered
      | id | result  |
      | 0  | NULL    |
      | 1  | 1000000 |

  Scenario: Named-zone day arithmetic retains both timestamp extremes in columns
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT id, unix_micros(ts + INTERVAL 1 DAY) AS added,
             unix_micros(ts - INTERVAL 1 DAY) AS subtracted
      FROM (
        SELECT id, timestamp_micros(CASE WHEN id = 0 THEN -9000000000000000000L
                                         ELSE 9000000000000000000L END) AS ts
        FROM range(0, 2, 1, 1)
      )
      ORDER BY id
      """
    Then query result ordered
      | id | added                | subtracted           |
      | 0  | -8999999913600000000 | -9000000086400000000 |
      | 1  | 9000000086400000000  | 8999999913600000000  |

  Scenario Outline: Checked elapsed arithmetic preserves supported session alias <zone>
    Given config spark.sql.session.timeZone = <zone>
    When query
      """
      SELECT unix_micros(timestampadd(HOUR, id + 1, timestamp_micros(0))) AS result
      FROM range(1)
      """
    Then query result
      | result     |
      | 3600000000 |

    Examples:
      | zone |
      | Z    |
      | UT   |
      | PST  |

  Scenario: Wide named-zone day arithmetic can return to an ordinary summer date
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT unix_micros(timestamp_micros(9000000000000000000L)
                        + make_dt_interval(-104148186)) AS result
      """
    Then query result
      | result           |
      | 1596729600000000 |

  @sail-bug
  Scenario: Calendar days retain recurring DST transitions beyond Chrono's range
    # FIXME: The preexisting elapsed-day fallback does not apply future recurring timezone rules.
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT unix_micros(timestamp_micros(8837530219800000000L) + INTERVAL 1 DAY) AS result
      """
    Then query result
      | result              |
      | 8837530302600000000 |
