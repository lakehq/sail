Feature: the date difference as an arithmetic operand, vs Spark 4.2.0

  # Target matrix for `fix/interval`. Spark's `date - date` is `DayTimeIntervalType(DAY)`
  # (`datetimeExpressions.scala:3616`), so the difference is an INTERVAL operand; in PR #2526 it is an
  # INT day count, which lands in different cells. Every verdict below was measured on the Spark 4.2
  # JVM (`SPARK_TESTING=1 SPARK_REMOTE=local`) with `df.schema`, the analysis lens: 274 cells,
  # 92 resolve and 182 are refused. The operand is written as `dd` in the case names.
  # No verdict changes with ANSI, measured rather than assumed; both modes are kept anyway.
  # The verdict is the plan lens only. The `answers` outlines pin what each resolving cell returns,
  # type and value: `*` and `/` fall back to DAY TO SECOND, `+` and `-` widen the range, and with ANSI
  # on the three STRING cells resolve but fail when they run.

  Rule: with ANSI off

    Scenario Outline: ansi-off: the date difference resolves: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(<expression>) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case         | expression                                                                      |
        | dd * bigint  | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS BIGINT))                     |
        | dd * dec     | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS DECIMAL(10,2)))              |
        | dd * double  | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS DOUBLE))                     |
        | dd * int     | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS INT))                        |
        | dd * null    | (DATE'2024-01-15' - DATE'2024-01-01') * (NULL)                                  |
        | dd + date    | (DATE'2024-01-15' - DATE'2024-01-01') + (DATE'2024-01-15')                      |
        | dd + dd      | (DATE'2024-01-15' - DATE'2024-01-01') + ((DATE'2024-01-15' - DATE'2024-01-01')) |
        | dd + null    | (DATE'2024-01-15' - DATE'2024-01-01') + (NULL)                                  |
        | dd - dd      | (DATE'2024-01-15' - DATE'2024-01-01') - ((DATE'2024-01-15' - DATE'2024-01-01')) |
        | dd - null    | (DATE'2024-01-15' - DATE'2024-01-01') - (NULL)                                  |
        | dd / bigint  | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS BIGINT))                     |
        | dd / dec     | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS DECIMAL(10,2)))              |
        | dd / double  | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS DOUBLE))                     |
        | dd / int     | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS INT))                        |
        | dd / null    | (DATE'2024-01-15' - DATE'2024-01-01') / (NULL)                                  |
        | dd / str     | (DATE'2024-01-15' - DATE'2024-01-01') / ('2')                                   |
        | bigint * dd  | (CAST(2 AS BIGINT)) * (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | dec * dd     | (CAST(2 AS DECIMAL(10,2))) * (DATE'2024-01-15' - DATE'2024-01-01')              |
        | double * dd  | (CAST(2 AS DOUBLE)) * (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | int * dd     | (CAST(2 AS INT)) * (DATE'2024-01-15' - DATE'2024-01-01')                        |
        | null * dd    | (NULL) * (DATE'2024-01-15' - DATE'2024-01-01')                                  |
        | date + dd    | (DATE'2024-01-15') + (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | null + dd    | (NULL) + (DATE'2024-01-15' - DATE'2024-01-01')                                  |
        | date - dd    | (DATE'2024-01-15') - (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | null - dd    | (NULL) - (DATE'2024-01-15' - DATE'2024-01-01')                                  |
        | +dd          | +(DATE'2024-01-15' - DATE'2024-01-01')                                          |
        | -dd          | -(DATE'2024-01-15' - DATE'2024-01-01')                                          |
        | dd * str     | (DATE'2024-01-15' - DATE'2024-01-01') * ('2')                                   |
        | dd + ival_d  | (DATE'2024-01-15' - DATE'2024-01-01') + (INTERVAL '2' DAY)                      |
        | dd + ival_dt | (DATE'2024-01-15' - DATE'2024-01-01') + (INTERVAL '25' HOUR)                    |
        | dd + str     | (DATE'2024-01-15' - DATE'2024-01-01') + ('2')                                   |
        | dd + ts      | (DATE'2024-01-15' - DATE'2024-01-01') + (TIMESTAMP'2024-01-15 12:00:00')        |
        | dd + ts_ntz  | (DATE'2024-01-15' - DATE'2024-01-01') + (TIMESTAMP_NTZ'2024-01-15 12:00:00')    |
        | dd - ival_d  | (DATE'2024-01-15' - DATE'2024-01-01') - (INTERVAL '2' DAY)                      |
        | dd - ival_dt | (DATE'2024-01-15' - DATE'2024-01-01') - (INTERVAL '25' HOUR)                    |
        | str * dd     | ('2') * (DATE'2024-01-15' - DATE'2024-01-01')                                   |
        | ival_d + dd  | (INTERVAL '2' DAY) + (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | ival_dt + dd | (INTERVAL '25' HOUR) + (DATE'2024-01-15' - DATE'2024-01-01')                    |
        | str + dd     | ('2') + (DATE'2024-01-15' - DATE'2024-01-01')                                   |
        | ts + dd      | (TIMESTAMP'2024-01-15 12:00:00') + (DATE'2024-01-15' - DATE'2024-01-01')        |
        | ts_ntz + dd  | (TIMESTAMP_NTZ'2024-01-15 12:00:00') + (DATE'2024-01-15' - DATE'2024-01-01')    |
        | ival_d - dd  | (INTERVAL '2' DAY) - (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | ival_dt - dd | (INTERVAL '25' HOUR) - (DATE'2024-01-15' - DATE'2024-01-01')                    |
        | str - dd     | ('2') - (DATE'2024-01-15' - DATE'2024-01-01')                                   |
        | ts - dd      | (TIMESTAMP'2024-01-15 12:00:00') - (DATE'2024-01-15' - DATE'2024-01-01')        |
        | ts_ntz - dd  | (TIMESTAMP_NTZ'2024-01-15 12:00:00') - (DATE'2024-01-15' - DATE'2024-01-01')    |

    # What each resolving cell answers, type and value, measured on the Spark 4.2 JVM. The verdict
    # above only says the plan resolves; this pins the interval Spark makes of it.
    Scenario Outline: ansi-off: the date difference answers: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT typeof(<expression>) AS t, CAST(<expression> AS STRING) AS v
        """
      Then query result
        | t      | v       |
        | <type> | <value> |

      Examples:
        | case         | expression                                                                      | type                   | value                                |
        | date + dd    | (DATE'2024-01-15') + (DATE'2024-01-15' - DATE'2024-01-01')                      | date                   | 2024-01-29                           |
        | date - dd    | (DATE'2024-01-15') - (DATE'2024-01-15' - DATE'2024-01-01')                      | date                   | 2024-01-01                           |
        | dd + date    | (DATE'2024-01-15' - DATE'2024-01-01') + (DATE'2024-01-15')                      | date                   | 2024-01-29                           |
        | +dd          | +(DATE'2024-01-15' - DATE'2024-01-01')                                          | interval day           | INTERVAL '14' DAY                    |
        | -dd          | -(DATE'2024-01-15' - DATE'2024-01-01')                                          | interval day           | INTERVAL '-14' DAY                   |
        | bigint * dd  | (CAST(2 AS BIGINT)) * (DATE'2024-01-15' - DATE'2024-01-01')                     | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | dd * bigint  | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS BIGINT))                     | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | dd * dec     | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS DECIMAL(10,2)))              | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | dd * double  | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS DOUBLE))                     | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | dd * int     | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS INT))                        | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | dd * null    | (DATE'2024-01-15' - DATE'2024-01-01') * (NULL)                                  | interval day to second | NULL                                 |
        | dd * str     | (DATE'2024-01-15' - DATE'2024-01-01') * ('2')                                   | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | dd + dd      | (DATE'2024-01-15' - DATE'2024-01-01') + ((DATE'2024-01-15' - DATE'2024-01-01')) | interval day           | INTERVAL '28' DAY                    |
        | dd + ival_d  | (DATE'2024-01-15' - DATE'2024-01-01') + (INTERVAL '2' DAY)                      | interval day           | INTERVAL '16' DAY                    |
        | dd + ival_dt | (DATE'2024-01-15' - DATE'2024-01-01') + (INTERVAL '25' HOUR)                    | interval day to hour   | INTERVAL '15 01' DAY TO HOUR         |
        | dd + null    | (DATE'2024-01-15' - DATE'2024-01-01') + (NULL)                                  | interval day           | NULL                                 |
        | dd + str     | (DATE'2024-01-15' - DATE'2024-01-01') + ('2')                                   | string                 | NULL                                 |
        | dd + ts      | (DATE'2024-01-15' - DATE'2024-01-01') + (TIMESTAMP'2024-01-15 12:00:00')        | timestamp              | 2024-01-29 12:00:00                  |
        | dd + ts_ntz  | (DATE'2024-01-15' - DATE'2024-01-01') + (TIMESTAMP_NTZ'2024-01-15 12:00:00')    | timestamp_ntz          | 2024-01-29 12:00:00                  |
        | dd - dd      | (DATE'2024-01-15' - DATE'2024-01-01') - ((DATE'2024-01-15' - DATE'2024-01-01')) | interval day           | INTERVAL '0' DAY                     |
        | dd - ival_d  | (DATE'2024-01-15' - DATE'2024-01-01') - (INTERVAL '2' DAY)                      | interval day           | INTERVAL '12' DAY                    |
        | dd - ival_dt | (DATE'2024-01-15' - DATE'2024-01-01') - (INTERVAL '25' HOUR)                    | interval day to hour   | INTERVAL '12 23' DAY TO HOUR         |
        | dd - null    | (DATE'2024-01-15' - DATE'2024-01-01') - (NULL)                                  | interval day           | NULL                                 |
        | dd / bigint  | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS BIGINT))                     | interval day to second | INTERVAL '7 00:00:00' DAY TO SECOND  |
        | dd / dec     | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS DECIMAL(10,2)))              | interval day to second | INTERVAL '7 00:00:00' DAY TO SECOND  |
        | dd / double  | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS DOUBLE))                     | interval day to second | INTERVAL '7 00:00:00' DAY TO SECOND  |
        | dd / int     | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS INT))                        | interval day to second | INTERVAL '7 00:00:00' DAY TO SECOND  |
        | dd / null    | (DATE'2024-01-15' - DATE'2024-01-01') / (NULL)                                  | interval day to second | NULL                                 |
        | dd / str     | (DATE'2024-01-15' - DATE'2024-01-01') / ('2')                                   | interval day to second | INTERVAL '7 00:00:00' DAY TO SECOND  |
        | dec * dd     | (CAST(2 AS DECIMAL(10,2))) * (DATE'2024-01-15' - DATE'2024-01-01')              | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | double * dd  | (CAST(2 AS DOUBLE)) * (DATE'2024-01-15' - DATE'2024-01-01')                     | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | int * dd     | (CAST(2 AS INT)) * (DATE'2024-01-15' - DATE'2024-01-01')                        | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | ival_d + dd  | (INTERVAL '2' DAY) + (DATE'2024-01-15' - DATE'2024-01-01')                      | interval day           | INTERVAL '16' DAY                    |
        | ival_d - dd  | (INTERVAL '2' DAY) - (DATE'2024-01-15' - DATE'2024-01-01')                      | interval day           | INTERVAL '-12' DAY                   |
        | ival_dt + dd | (INTERVAL '25' HOUR) + (DATE'2024-01-15' - DATE'2024-01-01')                    | interval day to hour   | INTERVAL '15 01' DAY TO HOUR         |
        | ival_dt - dd | (INTERVAL '25' HOUR) - (DATE'2024-01-15' - DATE'2024-01-01')                    | interval day to hour   | INTERVAL '-12 23' DAY TO HOUR        |
        | null * dd    | (NULL) * (DATE'2024-01-15' - DATE'2024-01-01')                                  | interval day to second | NULL                                 |
        | null + dd    | (NULL) + (DATE'2024-01-15' - DATE'2024-01-01')                                  | interval day           | NULL                                 |
        | null - dd    | (NULL) - (DATE'2024-01-15' - DATE'2024-01-01')                                  | interval day           | NULL                                 |
        | str * dd     | ('2') * (DATE'2024-01-15' - DATE'2024-01-01')                                   | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | str + dd     | ('2') + (DATE'2024-01-15' - DATE'2024-01-01')                                   | string                 | NULL                                 |
        | str - dd     | ('2') - (DATE'2024-01-15' - DATE'2024-01-01')                                   | string                 | NULL                                 |
        | ts + dd      | (TIMESTAMP'2024-01-15 12:00:00') + (DATE'2024-01-15' - DATE'2024-01-01')        | timestamp              | 2024-01-29 12:00:00                  |
        | ts - dd      | (TIMESTAMP'2024-01-15 12:00:00') - (DATE'2024-01-15' - DATE'2024-01-01')        | timestamp              | 2024-01-01 12:00:00                  |
        | ts_ntz + dd  | (TIMESTAMP_NTZ'2024-01-15 12:00:00') + (DATE'2024-01-15' - DATE'2024-01-01')    | timestamp_ntz          | 2024-01-29 12:00:00                  |
        | ts_ntz - dd  | (TIMESTAMP_NTZ'2024-01-15 12:00:00') - (DATE'2024-01-15' - DATE'2024-01-01')    | timestamp_ntz          | 2024-01-01 12:00:00                  |

    Scenario Outline: ansi-off: the date difference is refused: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expression> AS v
        """
      Then query error (?i)cannot resolve

      Examples:
        | case          | expression                                                                      |
        | dd % bigint   | (DATE'2024-01-15' - DATE'2024-01-01') % (CAST(2 AS BIGINT))                     |
        | dd % calendar | (DATE'2024-01-15' - DATE'2024-01-01') % (make_interval(0,1,0,1,0,0,0))          |
        | dd % date     | (DATE'2024-01-15' - DATE'2024-01-01') % (DATE'2024-01-15')                      |
        | dd % dd       | (DATE'2024-01-15' - DATE'2024-01-01') % ((DATE'2024-01-15' - DATE'2024-01-01')) |
        | dd % dec      | (DATE'2024-01-15' - DATE'2024-01-01') % (CAST(2 AS DECIMAL(10,2)))              |
        | dd % double   | (DATE'2024-01-15' - DATE'2024-01-01') % (CAST(2 AS DOUBLE))                     |
        | dd % int      | (DATE'2024-01-15' - DATE'2024-01-01') % (CAST(2 AS INT))                        |
        | dd % ival_d   | (DATE'2024-01-15' - DATE'2024-01-01') % (INTERVAL '2' DAY)                      |
        | dd % ival_dt  | (DATE'2024-01-15' - DATE'2024-01-01') % (INTERVAL '25' HOUR)                    |
        | dd % ival_ym  | (DATE'2024-01-15' - DATE'2024-01-01') % (INTERVAL '1-2' YEAR TO MONTH)          |
        | dd % null     | (DATE'2024-01-15' - DATE'2024-01-01') % (NULL)                                  |
        | dd % str      | (DATE'2024-01-15' - DATE'2024-01-01') % ('2')                                   |
        | dd % ts       | (DATE'2024-01-15' - DATE'2024-01-01') % (TIMESTAMP'2024-01-15 12:00:00')        |
        | dd % ts_ntz   | (DATE'2024-01-15' - DATE'2024-01-01') % (TIMESTAMP_NTZ'2024-01-15 12:00:00')    |
        | dd * calendar | (DATE'2024-01-15' - DATE'2024-01-01') * (make_interval(0,1,0,1,0,0,0))          |
        | dd * date     | (DATE'2024-01-15' - DATE'2024-01-01') * (DATE'2024-01-15')                      |
        | dd * dd       | (DATE'2024-01-15' - DATE'2024-01-01') * ((DATE'2024-01-15' - DATE'2024-01-01')) |
        | dd * ival_d   | (DATE'2024-01-15' - DATE'2024-01-01') * (INTERVAL '2' DAY)                      |
        | dd * ival_dt  | (DATE'2024-01-15' - DATE'2024-01-01') * (INTERVAL '25' HOUR)                    |
        | dd * ival_ym  | (DATE'2024-01-15' - DATE'2024-01-01') * (INTERVAL '1-2' YEAR TO MONTH)          |
        | dd * ts       | (DATE'2024-01-15' - DATE'2024-01-01') * (TIMESTAMP'2024-01-15 12:00:00')        |
        | dd * ts_ntz   | (DATE'2024-01-15' - DATE'2024-01-01') * (TIMESTAMP_NTZ'2024-01-15 12:00:00')    |
        | dd + bigint   | (DATE'2024-01-15' - DATE'2024-01-01') + (CAST(2 AS BIGINT))                     |
        | dd + calendar | (DATE'2024-01-15' - DATE'2024-01-01') + (make_interval(0,1,0,1,0,0,0))          |
        | dd + dec      | (DATE'2024-01-15' - DATE'2024-01-01') + (CAST(2 AS DECIMAL(10,2)))              |
        | dd + double   | (DATE'2024-01-15' - DATE'2024-01-01') + (CAST(2 AS DOUBLE))                     |
        | dd + int      | (DATE'2024-01-15' - DATE'2024-01-01') + (CAST(2 AS INT))                        |
        | dd + ival_ym  | (DATE'2024-01-15' - DATE'2024-01-01') + (INTERVAL '1-2' YEAR TO MONTH)          |
        | dd - bigint   | (DATE'2024-01-15' - DATE'2024-01-01') - (CAST(2 AS BIGINT))                     |
        | dd - calendar | (DATE'2024-01-15' - DATE'2024-01-01') - (make_interval(0,1,0,1,0,0,0))          |
        | dd - date     | (DATE'2024-01-15' - DATE'2024-01-01') - (DATE'2024-01-15')                      |
        | dd - dec      | (DATE'2024-01-15' - DATE'2024-01-01') - (CAST(2 AS DECIMAL(10,2)))              |
        | dd - double   | (DATE'2024-01-15' - DATE'2024-01-01') - (CAST(2 AS DOUBLE))                     |
        | dd - int      | (DATE'2024-01-15' - DATE'2024-01-01') - (CAST(2 AS INT))                        |
        | dd - ival_ym  | (DATE'2024-01-15' - DATE'2024-01-01') - (INTERVAL '1-2' YEAR TO MONTH)          |
        | dd - str      | (DATE'2024-01-15' - DATE'2024-01-01') - ('2')                                   |
        | dd - ts       | (DATE'2024-01-15' - DATE'2024-01-01') - (TIMESTAMP'2024-01-15 12:00:00')        |
        | dd - ts_ntz   | (DATE'2024-01-15' - DATE'2024-01-01') - (TIMESTAMP_NTZ'2024-01-15 12:00:00')    |
        | dd / calendar | (DATE'2024-01-15' - DATE'2024-01-01') / (make_interval(0,1,0,1,0,0,0))          |
        | dd / date     | (DATE'2024-01-15' - DATE'2024-01-01') / (DATE'2024-01-15')                      |
        | dd / dd       | (DATE'2024-01-15' - DATE'2024-01-01') / ((DATE'2024-01-15' - DATE'2024-01-01')) |
        | dd / ival_d   | (DATE'2024-01-15' - DATE'2024-01-01') / (INTERVAL '2' DAY)                      |
        | dd / ival_dt  | (DATE'2024-01-15' - DATE'2024-01-01') / (INTERVAL '25' HOUR)                    |
        | dd / ival_ym  | (DATE'2024-01-15' - DATE'2024-01-01') / (INTERVAL '1-2' YEAR TO MONTH)          |
        | dd / ts       | (DATE'2024-01-15' - DATE'2024-01-01') / (TIMESTAMP'2024-01-15 12:00:00')        |
        | dd / ts_ntz   | (DATE'2024-01-15' - DATE'2024-01-01') / (TIMESTAMP_NTZ'2024-01-15 12:00:00')    |
        | bigint % dd   | (CAST(2 AS BIGINT)) % (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | calendar % dd | (make_interval(0,1,0,1,0,0,0)) % (DATE'2024-01-15' - DATE'2024-01-01')          |
        | date % dd     | (DATE'2024-01-15') % (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | dec % dd      | (CAST(2 AS DECIMAL(10,2))) % (DATE'2024-01-15' - DATE'2024-01-01')              |
        | double % dd   | (CAST(2 AS DOUBLE)) % (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | int % dd      | (CAST(2 AS INT)) % (DATE'2024-01-15' - DATE'2024-01-01')                        |
        | ival_d % dd   | (INTERVAL '2' DAY) % (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | ival_dt % dd  | (INTERVAL '25' HOUR) % (DATE'2024-01-15' - DATE'2024-01-01')                    |
        | ival_ym % dd  | (INTERVAL '1-2' YEAR TO MONTH) % (DATE'2024-01-15' - DATE'2024-01-01')          |
        | null % dd     | (NULL) % (DATE'2024-01-15' - DATE'2024-01-01')                                  |
        | str % dd      | ('2') % (DATE'2024-01-15' - DATE'2024-01-01')                                   |
        | ts % dd       | (TIMESTAMP'2024-01-15 12:00:00') % (DATE'2024-01-15' - DATE'2024-01-01')        |
        | ts_ntz % dd   | (TIMESTAMP_NTZ'2024-01-15 12:00:00') % (DATE'2024-01-15' - DATE'2024-01-01')    |
        | calendar * dd | (make_interval(0,1,0,1,0,0,0)) * (DATE'2024-01-15' - DATE'2024-01-01')          |
        | date * dd     | (DATE'2024-01-15') * (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | ival_d * dd   | (INTERVAL '2' DAY) * (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | ival_dt * dd  | (INTERVAL '25' HOUR) * (DATE'2024-01-15' - DATE'2024-01-01')                    |
        | ival_ym * dd  | (INTERVAL '1-2' YEAR TO MONTH) * (DATE'2024-01-15' - DATE'2024-01-01')          |
        | ts * dd       | (TIMESTAMP'2024-01-15 12:00:00') * (DATE'2024-01-15' - DATE'2024-01-01')        |
        | ts_ntz * dd   | (TIMESTAMP_NTZ'2024-01-15 12:00:00') * (DATE'2024-01-15' - DATE'2024-01-01')    |
        | bigint + dd   | (CAST(2 AS BIGINT)) + (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | calendar + dd | (make_interval(0,1,0,1,0,0,0)) + (DATE'2024-01-15' - DATE'2024-01-01')          |
        | dec + dd      | (CAST(2 AS DECIMAL(10,2))) + (DATE'2024-01-15' - DATE'2024-01-01')              |
        | double + dd   | (CAST(2 AS DOUBLE)) + (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | int + dd      | (CAST(2 AS INT)) + (DATE'2024-01-15' - DATE'2024-01-01')                        |
        | ival_ym + dd  | (INTERVAL '1-2' YEAR TO MONTH) + (DATE'2024-01-15' - DATE'2024-01-01')          |
        | bigint - dd   | (CAST(2 AS BIGINT)) - (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | calendar - dd | (make_interval(0,1,0,1,0,0,0)) - (DATE'2024-01-15' - DATE'2024-01-01')          |
        | dec - dd      | (CAST(2 AS DECIMAL(10,2))) - (DATE'2024-01-15' - DATE'2024-01-01')              |
        | double - dd   | (CAST(2 AS DOUBLE)) - (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | int - dd      | (CAST(2 AS INT)) - (DATE'2024-01-15' - DATE'2024-01-01')                        |
        | ival_ym - dd  | (INTERVAL '1-2' YEAR TO MONTH) - (DATE'2024-01-15' - DATE'2024-01-01')          |
        | bigint / dd   | (CAST(2 AS BIGINT)) / (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | calendar / dd | (make_interval(0,1,0,1,0,0,0)) / (DATE'2024-01-15' - DATE'2024-01-01')          |
        | date / dd     | (DATE'2024-01-15') / (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | dec / dd      | (CAST(2 AS DECIMAL(10,2))) / (DATE'2024-01-15' - DATE'2024-01-01')              |
        | double / dd   | (CAST(2 AS DOUBLE)) / (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | int / dd      | (CAST(2 AS INT)) / (DATE'2024-01-15' - DATE'2024-01-01')                        |
        | ival_d / dd   | (INTERVAL '2' DAY) / (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | ival_dt / dd  | (INTERVAL '25' HOUR) / (DATE'2024-01-15' - DATE'2024-01-01')                    |
        | ival_ym / dd  | (INTERVAL '1-2' YEAR TO MONTH) / (DATE'2024-01-15' - DATE'2024-01-01')          |
        | null / dd     | (NULL) / (DATE'2024-01-15' - DATE'2024-01-01')                                  |
        | str / dd      | ('2') / (DATE'2024-01-15' - DATE'2024-01-01')                                   |
        | ts / dd       | (TIMESTAMP'2024-01-15 12:00:00') / (DATE'2024-01-15' - DATE'2024-01-01')        |
        | ts_ntz / dd   | (TIMESTAMP_NTZ'2024-01-15 12:00:00') / (DATE'2024-01-15' - DATE'2024-01-01')    |

  Rule: with ANSI on

    Scenario Outline: ansi-on: the date difference resolves: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(<expression>) IS NOT NULL AS resolved
        """
      Then query result
        | resolved |
        | true     |

      Examples:
        | case         | expression                                                                      |
        | dd * bigint  | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS BIGINT))                     |
        | dd * dec     | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS DECIMAL(10,2)))              |
        | dd * double  | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS DOUBLE))                     |
        | dd * int     | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS INT))                        |
        | dd * null    | (DATE'2024-01-15' - DATE'2024-01-01') * (NULL)                                  |
        | dd + date    | (DATE'2024-01-15' - DATE'2024-01-01') + (DATE'2024-01-15')                      |
        | dd + dd      | (DATE'2024-01-15' - DATE'2024-01-01') + ((DATE'2024-01-15' - DATE'2024-01-01')) |
        | dd + null    | (DATE'2024-01-15' - DATE'2024-01-01') + (NULL)                                  |
        | dd - dd      | (DATE'2024-01-15' - DATE'2024-01-01') - ((DATE'2024-01-15' - DATE'2024-01-01')) |
        | dd - null    | (DATE'2024-01-15' - DATE'2024-01-01') - (NULL)                                  |
        | dd / bigint  | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS BIGINT))                     |
        | dd / dec     | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS DECIMAL(10,2)))              |
        | dd / double  | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS DOUBLE))                     |
        | dd / int     | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS INT))                        |
        | dd / null    | (DATE'2024-01-15' - DATE'2024-01-01') / (NULL)                                  |
        | dd / str     | (DATE'2024-01-15' - DATE'2024-01-01') / ('2')                                   |
        | bigint * dd  | (CAST(2 AS BIGINT)) * (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | dec * dd     | (CAST(2 AS DECIMAL(10,2))) * (DATE'2024-01-15' - DATE'2024-01-01')              |
        | double * dd  | (CAST(2 AS DOUBLE)) * (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | int * dd     | (CAST(2 AS INT)) * (DATE'2024-01-15' - DATE'2024-01-01')                        |
        | null * dd    | (NULL) * (DATE'2024-01-15' - DATE'2024-01-01')                                  |
        | date + dd    | (DATE'2024-01-15') + (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | null + dd    | (NULL) + (DATE'2024-01-15' - DATE'2024-01-01')                                  |
        | date - dd    | (DATE'2024-01-15') - (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | null - dd    | (NULL) - (DATE'2024-01-15' - DATE'2024-01-01')                                  |
        | +dd          | +(DATE'2024-01-15' - DATE'2024-01-01')                                          |
        | -dd          | -(DATE'2024-01-15' - DATE'2024-01-01')                                          |
        | dd * str     | (DATE'2024-01-15' - DATE'2024-01-01') * ('2')                                   |
        | dd + ival_d  | (DATE'2024-01-15' - DATE'2024-01-01') + (INTERVAL '2' DAY)                      |
        | dd + ival_dt | (DATE'2024-01-15' - DATE'2024-01-01') + (INTERVAL '25' HOUR)                    |
        | dd + str     | (DATE'2024-01-15' - DATE'2024-01-01') + ('2')                                   |
        | dd + ts      | (DATE'2024-01-15' - DATE'2024-01-01') + (TIMESTAMP'2024-01-15 12:00:00')        |
        | dd + ts_ntz  | (DATE'2024-01-15' - DATE'2024-01-01') + (TIMESTAMP_NTZ'2024-01-15 12:00:00')    |
        | dd - ival_d  | (DATE'2024-01-15' - DATE'2024-01-01') - (INTERVAL '2' DAY)                      |
        | dd - ival_dt | (DATE'2024-01-15' - DATE'2024-01-01') - (INTERVAL '25' HOUR)                    |
        | str * dd     | ('2') * (DATE'2024-01-15' - DATE'2024-01-01')                                   |
        | ival_d + dd  | (INTERVAL '2' DAY) + (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | ival_dt + dd | (INTERVAL '25' HOUR) + (DATE'2024-01-15' - DATE'2024-01-01')                    |
        | str + dd     | ('2') + (DATE'2024-01-15' - DATE'2024-01-01')                                   |
        | ts + dd      | (TIMESTAMP'2024-01-15 12:00:00') + (DATE'2024-01-15' - DATE'2024-01-01')        |
        | ts_ntz + dd  | (TIMESTAMP_NTZ'2024-01-15 12:00:00') + (DATE'2024-01-15' - DATE'2024-01-01')    |
        | ival_d - dd  | (INTERVAL '2' DAY) - (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | ival_dt - dd | (INTERVAL '25' HOUR) - (DATE'2024-01-15' - DATE'2024-01-01')                    |
        | str - dd     | ('2') - (DATE'2024-01-15' - DATE'2024-01-01')                                   |
        | ts - dd      | (TIMESTAMP'2024-01-15 12:00:00') - (DATE'2024-01-15' - DATE'2024-01-01')        |
        | ts_ntz - dd  | (TIMESTAMP_NTZ'2024-01-15 12:00:00') - (DATE'2024-01-15' - DATE'2024-01-01')    |

    # What each resolving cell answers, type and value, measured on the Spark 4.2 JVM. The verdict
    # above only says the plan resolves; this pins the interval Spark makes of it.
    Scenario Outline: ansi-on: the date difference answers: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT typeof(<expression>) AS t, CAST(<expression> AS STRING) AS v
        """
      Then query result
        | t      | v       |
        | <type> | <value> |

      Examples:
        | case         | expression                                                                      | type                   | value                                |
        | date + dd    | (DATE'2024-01-15') + (DATE'2024-01-15' - DATE'2024-01-01')                      | date                   | 2024-01-29                           |
        | date - dd    | (DATE'2024-01-15') - (DATE'2024-01-15' - DATE'2024-01-01')                      | date                   | 2024-01-01                           |
        | dd + date    | (DATE'2024-01-15' - DATE'2024-01-01') + (DATE'2024-01-15')                      | date                   | 2024-01-29                           |
        | +dd          | +(DATE'2024-01-15' - DATE'2024-01-01')                                          | interval day           | INTERVAL '14' DAY                    |
        | -dd          | -(DATE'2024-01-15' - DATE'2024-01-01')                                          | interval day           | INTERVAL '-14' DAY                   |
        | bigint * dd  | (CAST(2 AS BIGINT)) * (DATE'2024-01-15' - DATE'2024-01-01')                     | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | dd * bigint  | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS BIGINT))                     | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | dd * dec     | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS DECIMAL(10,2)))              | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | dd * double  | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS DOUBLE))                     | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | dd * int     | (DATE'2024-01-15' - DATE'2024-01-01') * (CAST(2 AS INT))                        | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | dd * null    | (DATE'2024-01-15' - DATE'2024-01-01') * (NULL)                                  | interval day to second | NULL                                 |
        | dd * str     | (DATE'2024-01-15' - DATE'2024-01-01') * ('2')                                   | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | dd + dd      | (DATE'2024-01-15' - DATE'2024-01-01') + ((DATE'2024-01-15' - DATE'2024-01-01')) | interval day           | INTERVAL '28' DAY                    |
        | dd + ival_d  | (DATE'2024-01-15' - DATE'2024-01-01') + (INTERVAL '2' DAY)                      | interval day           | INTERVAL '16' DAY                    |
        | dd + ival_dt | (DATE'2024-01-15' - DATE'2024-01-01') + (INTERVAL '25' HOUR)                    | interval day to hour   | INTERVAL '15 01' DAY TO HOUR         |
        | dd + null    | (DATE'2024-01-15' - DATE'2024-01-01') + (NULL)                                  | interval day           | NULL                                 |
        | dd + ts      | (DATE'2024-01-15' - DATE'2024-01-01') + (TIMESTAMP'2024-01-15 12:00:00')        | timestamp              | 2024-01-29 12:00:00                  |
        | dd + ts_ntz  | (DATE'2024-01-15' - DATE'2024-01-01') + (TIMESTAMP_NTZ'2024-01-15 12:00:00')    | timestamp_ntz          | 2024-01-29 12:00:00                  |
        | dd - dd      | (DATE'2024-01-15' - DATE'2024-01-01') - ((DATE'2024-01-15' - DATE'2024-01-01')) | interval day           | INTERVAL '0' DAY                     |
        | dd - ival_d  | (DATE'2024-01-15' - DATE'2024-01-01') - (INTERVAL '2' DAY)                      | interval day           | INTERVAL '12' DAY                    |
        | dd - ival_dt | (DATE'2024-01-15' - DATE'2024-01-01') - (INTERVAL '25' HOUR)                    | interval day to hour   | INTERVAL '12 23' DAY TO HOUR         |
        | dd - null    | (DATE'2024-01-15' - DATE'2024-01-01') - (NULL)                                  | interval day           | NULL                                 |
        | dd / bigint  | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS BIGINT))                     | interval day to second | INTERVAL '7 00:00:00' DAY TO SECOND  |
        | dd / dec     | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS DECIMAL(10,2)))              | interval day to second | INTERVAL '7 00:00:00' DAY TO SECOND  |
        | dd / double  | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS DOUBLE))                     | interval day to second | INTERVAL '7 00:00:00' DAY TO SECOND  |
        | dd / int     | (DATE'2024-01-15' - DATE'2024-01-01') / (CAST(2 AS INT))                        | interval day to second | INTERVAL '7 00:00:00' DAY TO SECOND  |
        | dd / null    | (DATE'2024-01-15' - DATE'2024-01-01') / (NULL)                                  | interval day to second | NULL                                 |
        | dd / str     | (DATE'2024-01-15' - DATE'2024-01-01') / ('2')                                   | interval day to second | INTERVAL '7 00:00:00' DAY TO SECOND  |
        | dec * dd     | (CAST(2 AS DECIMAL(10,2))) * (DATE'2024-01-15' - DATE'2024-01-01')              | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | double * dd  | (CAST(2 AS DOUBLE)) * (DATE'2024-01-15' - DATE'2024-01-01')                     | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | int * dd     | (CAST(2 AS INT)) * (DATE'2024-01-15' - DATE'2024-01-01')                        | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | ival_d + dd  | (INTERVAL '2' DAY) + (DATE'2024-01-15' - DATE'2024-01-01')                      | interval day           | INTERVAL '16' DAY                    |
        | ival_d - dd  | (INTERVAL '2' DAY) - (DATE'2024-01-15' - DATE'2024-01-01')                      | interval day           | INTERVAL '-12' DAY                   |
        | ival_dt + dd | (INTERVAL '25' HOUR) + (DATE'2024-01-15' - DATE'2024-01-01')                    | interval day to hour   | INTERVAL '15 01' DAY TO HOUR         |
        | ival_dt - dd | (INTERVAL '25' HOUR) - (DATE'2024-01-15' - DATE'2024-01-01')                    | interval day to hour   | INTERVAL '-12 23' DAY TO HOUR        |
        | null * dd    | (NULL) * (DATE'2024-01-15' - DATE'2024-01-01')                                  | interval day to second | NULL                                 |
        | null + dd    | (NULL) + (DATE'2024-01-15' - DATE'2024-01-01')                                  | interval day           | NULL                                 |
        | null - dd    | (NULL) - (DATE'2024-01-15' - DATE'2024-01-01')                                  | interval day           | NULL                                 |
        | str * dd     | ('2') * (DATE'2024-01-15' - DATE'2024-01-01')                                   | interval day to second | INTERVAL '28 00:00:00' DAY TO SECOND |
        | ts + dd      | (TIMESTAMP'2024-01-15 12:00:00') + (DATE'2024-01-15' - DATE'2024-01-01')        | timestamp              | 2024-01-29 12:00:00                  |
        | ts - dd      | (TIMESTAMP'2024-01-15 12:00:00') - (DATE'2024-01-15' - DATE'2024-01-01')        | timestamp              | 2024-01-01 12:00:00                  |
        | ts_ntz + dd  | (TIMESTAMP_NTZ'2024-01-15 12:00:00') + (DATE'2024-01-15' - DATE'2024-01-01')    | timestamp_ntz          | 2024-01-29 12:00:00                  |
        | ts_ntz - dd  | (TIMESTAMP_NTZ'2024-01-15 12:00:00') - (DATE'2024-01-15' - DATE'2024-01-01')    | timestamp_ntz          | 2024-01-01 12:00:00                  |

    # These resolve, but the string operand is read as a TIMESTAMP when the query runs, and '2' is not
    # one: with ANSI on that is an error rather than a NULL.
    Scenario Outline: ansi-on: the date difference fails when it runs: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expression> AS v
        """
      Then query error <error>

      Examples:
        | case     | expression                                    | error              |
        | dd + str | (DATE'2024-01-15' - DATE'2024-01-01') + ('2') | CAST_INVALID_INPUT |
        | str + dd | ('2') + (DATE'2024-01-15' - DATE'2024-01-01') | CAST_INVALID_INPUT |
        | str - dd | ('2') - (DATE'2024-01-15' - DATE'2024-01-01') | CAST_INVALID_INPUT |

    Scenario Outline: ansi-on: the date difference is refused: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expression> AS v
        """
      Then query error (?i)cannot resolve

      Examples:
        | case          | expression                                                                      |
        | dd % bigint   | (DATE'2024-01-15' - DATE'2024-01-01') % (CAST(2 AS BIGINT))                     |
        | dd % calendar | (DATE'2024-01-15' - DATE'2024-01-01') % (make_interval(0,1,0,1,0,0,0))          |
        | dd % date     | (DATE'2024-01-15' - DATE'2024-01-01') % (DATE'2024-01-15')                      |
        | dd % dd       | (DATE'2024-01-15' - DATE'2024-01-01') % ((DATE'2024-01-15' - DATE'2024-01-01')) |
        | dd % dec      | (DATE'2024-01-15' - DATE'2024-01-01') % (CAST(2 AS DECIMAL(10,2)))              |
        | dd % double   | (DATE'2024-01-15' - DATE'2024-01-01') % (CAST(2 AS DOUBLE))                     |
        | dd % int      | (DATE'2024-01-15' - DATE'2024-01-01') % (CAST(2 AS INT))                        |
        | dd % ival_d   | (DATE'2024-01-15' - DATE'2024-01-01') % (INTERVAL '2' DAY)                      |
        | dd % ival_dt  | (DATE'2024-01-15' - DATE'2024-01-01') % (INTERVAL '25' HOUR)                    |
        | dd % ival_ym  | (DATE'2024-01-15' - DATE'2024-01-01') % (INTERVAL '1-2' YEAR TO MONTH)          |
        | dd % null     | (DATE'2024-01-15' - DATE'2024-01-01') % (NULL)                                  |
        | dd % str      | (DATE'2024-01-15' - DATE'2024-01-01') % ('2')                                   |
        | dd % ts       | (DATE'2024-01-15' - DATE'2024-01-01') % (TIMESTAMP'2024-01-15 12:00:00')        |
        | dd % ts_ntz   | (DATE'2024-01-15' - DATE'2024-01-01') % (TIMESTAMP_NTZ'2024-01-15 12:00:00')    |
        | dd * calendar | (DATE'2024-01-15' - DATE'2024-01-01') * (make_interval(0,1,0,1,0,0,0))          |
        | dd * date     | (DATE'2024-01-15' - DATE'2024-01-01') * (DATE'2024-01-15')                      |
        | dd * dd       | (DATE'2024-01-15' - DATE'2024-01-01') * ((DATE'2024-01-15' - DATE'2024-01-01')) |
        | dd * ival_d   | (DATE'2024-01-15' - DATE'2024-01-01') * (INTERVAL '2' DAY)                      |
        | dd * ival_dt  | (DATE'2024-01-15' - DATE'2024-01-01') * (INTERVAL '25' HOUR)                    |
        | dd * ival_ym  | (DATE'2024-01-15' - DATE'2024-01-01') * (INTERVAL '1-2' YEAR TO MONTH)          |
        | dd * ts       | (DATE'2024-01-15' - DATE'2024-01-01') * (TIMESTAMP'2024-01-15 12:00:00')        |
        | dd * ts_ntz   | (DATE'2024-01-15' - DATE'2024-01-01') * (TIMESTAMP_NTZ'2024-01-15 12:00:00')    |
        | dd + bigint   | (DATE'2024-01-15' - DATE'2024-01-01') + (CAST(2 AS BIGINT))                     |
        | dd + calendar | (DATE'2024-01-15' - DATE'2024-01-01') + (make_interval(0,1,0,1,0,0,0))          |
        | dd + dec      | (DATE'2024-01-15' - DATE'2024-01-01') + (CAST(2 AS DECIMAL(10,2)))              |
        | dd + double   | (DATE'2024-01-15' - DATE'2024-01-01') + (CAST(2 AS DOUBLE))                     |
        | dd + int      | (DATE'2024-01-15' - DATE'2024-01-01') + (CAST(2 AS INT))                        |
        | dd + ival_ym  | (DATE'2024-01-15' - DATE'2024-01-01') + (INTERVAL '1-2' YEAR TO MONTH)          |
        | dd - bigint   | (DATE'2024-01-15' - DATE'2024-01-01') - (CAST(2 AS BIGINT))                     |
        | dd - calendar | (DATE'2024-01-15' - DATE'2024-01-01') - (make_interval(0,1,0,1,0,0,0))          |
        | dd - date     | (DATE'2024-01-15' - DATE'2024-01-01') - (DATE'2024-01-15')                      |
        | dd - dec      | (DATE'2024-01-15' - DATE'2024-01-01') - (CAST(2 AS DECIMAL(10,2)))              |
        | dd - double   | (DATE'2024-01-15' - DATE'2024-01-01') - (CAST(2 AS DOUBLE))                     |
        | dd - int      | (DATE'2024-01-15' - DATE'2024-01-01') - (CAST(2 AS INT))                        |
        | dd - ival_ym  | (DATE'2024-01-15' - DATE'2024-01-01') - (INTERVAL '1-2' YEAR TO MONTH)          |
        | dd - str      | (DATE'2024-01-15' - DATE'2024-01-01') - ('2')                                   |
        | dd - ts       | (DATE'2024-01-15' - DATE'2024-01-01') - (TIMESTAMP'2024-01-15 12:00:00')        |
        | dd - ts_ntz   | (DATE'2024-01-15' - DATE'2024-01-01') - (TIMESTAMP_NTZ'2024-01-15 12:00:00')    |
        | dd / calendar | (DATE'2024-01-15' - DATE'2024-01-01') / (make_interval(0,1,0,1,0,0,0))          |
        | dd / date     | (DATE'2024-01-15' - DATE'2024-01-01') / (DATE'2024-01-15')                      |
        | dd / dd       | (DATE'2024-01-15' - DATE'2024-01-01') / ((DATE'2024-01-15' - DATE'2024-01-01')) |
        | dd / ival_d   | (DATE'2024-01-15' - DATE'2024-01-01') / (INTERVAL '2' DAY)                      |
        | dd / ival_dt  | (DATE'2024-01-15' - DATE'2024-01-01') / (INTERVAL '25' HOUR)                    |
        | dd / ival_ym  | (DATE'2024-01-15' - DATE'2024-01-01') / (INTERVAL '1-2' YEAR TO MONTH)          |
        | dd / ts       | (DATE'2024-01-15' - DATE'2024-01-01') / (TIMESTAMP'2024-01-15 12:00:00')        |
        | dd / ts_ntz   | (DATE'2024-01-15' - DATE'2024-01-01') / (TIMESTAMP_NTZ'2024-01-15 12:00:00')    |
        | bigint % dd   | (CAST(2 AS BIGINT)) % (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | calendar % dd | (make_interval(0,1,0,1,0,0,0)) % (DATE'2024-01-15' - DATE'2024-01-01')          |
        | date % dd     | (DATE'2024-01-15') % (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | dec % dd      | (CAST(2 AS DECIMAL(10,2))) % (DATE'2024-01-15' - DATE'2024-01-01')              |
        | double % dd   | (CAST(2 AS DOUBLE)) % (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | int % dd      | (CAST(2 AS INT)) % (DATE'2024-01-15' - DATE'2024-01-01')                        |
        | ival_d % dd   | (INTERVAL '2' DAY) % (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | ival_dt % dd  | (INTERVAL '25' HOUR) % (DATE'2024-01-15' - DATE'2024-01-01')                    |
        | ival_ym % dd  | (INTERVAL '1-2' YEAR TO MONTH) % (DATE'2024-01-15' - DATE'2024-01-01')          |
        | null % dd     | (NULL) % (DATE'2024-01-15' - DATE'2024-01-01')                                  |
        | str % dd      | ('2') % (DATE'2024-01-15' - DATE'2024-01-01')                                   |
        | ts % dd       | (TIMESTAMP'2024-01-15 12:00:00') % (DATE'2024-01-15' - DATE'2024-01-01')        |
        | ts_ntz % dd   | (TIMESTAMP_NTZ'2024-01-15 12:00:00') % (DATE'2024-01-15' - DATE'2024-01-01')    |
        | calendar * dd | (make_interval(0,1,0,1,0,0,0)) * (DATE'2024-01-15' - DATE'2024-01-01')          |
        | date * dd     | (DATE'2024-01-15') * (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | ival_d * dd   | (INTERVAL '2' DAY) * (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | ival_dt * dd  | (INTERVAL '25' HOUR) * (DATE'2024-01-15' - DATE'2024-01-01')                    |
        | ival_ym * dd  | (INTERVAL '1-2' YEAR TO MONTH) * (DATE'2024-01-15' - DATE'2024-01-01')          |
        | ts * dd       | (TIMESTAMP'2024-01-15 12:00:00') * (DATE'2024-01-15' - DATE'2024-01-01')        |
        | ts_ntz * dd   | (TIMESTAMP_NTZ'2024-01-15 12:00:00') * (DATE'2024-01-15' - DATE'2024-01-01')    |
        | bigint + dd   | (CAST(2 AS BIGINT)) + (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | calendar + dd | (make_interval(0,1,0,1,0,0,0)) + (DATE'2024-01-15' - DATE'2024-01-01')          |
        | dec + dd      | (CAST(2 AS DECIMAL(10,2))) + (DATE'2024-01-15' - DATE'2024-01-01')              |
        | double + dd   | (CAST(2 AS DOUBLE)) + (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | int + dd      | (CAST(2 AS INT)) + (DATE'2024-01-15' - DATE'2024-01-01')                        |
        | ival_ym + dd  | (INTERVAL '1-2' YEAR TO MONTH) + (DATE'2024-01-15' - DATE'2024-01-01')          |
        | bigint - dd   | (CAST(2 AS BIGINT)) - (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | calendar - dd | (make_interval(0,1,0,1,0,0,0)) - (DATE'2024-01-15' - DATE'2024-01-01')          |
        | dec - dd      | (CAST(2 AS DECIMAL(10,2))) - (DATE'2024-01-15' - DATE'2024-01-01')              |
        | double - dd   | (CAST(2 AS DOUBLE)) - (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | int - dd      | (CAST(2 AS INT)) - (DATE'2024-01-15' - DATE'2024-01-01')                        |
        | ival_ym - dd  | (INTERVAL '1-2' YEAR TO MONTH) - (DATE'2024-01-15' - DATE'2024-01-01')          |
        | bigint / dd   | (CAST(2 AS BIGINT)) / (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | calendar / dd | (make_interval(0,1,0,1,0,0,0)) / (DATE'2024-01-15' - DATE'2024-01-01')          |
        | date / dd     | (DATE'2024-01-15') / (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | dec / dd      | (CAST(2 AS DECIMAL(10,2))) / (DATE'2024-01-15' - DATE'2024-01-01')              |
        | double / dd   | (CAST(2 AS DOUBLE)) / (DATE'2024-01-15' - DATE'2024-01-01')                     |
        | int / dd      | (CAST(2 AS INT)) / (DATE'2024-01-15' - DATE'2024-01-01')                        |
        | ival_d / dd   | (INTERVAL '2' DAY) / (DATE'2024-01-15' - DATE'2024-01-01')                      |
        | ival_dt / dd  | (INTERVAL '25' HOUR) / (DATE'2024-01-15' - DATE'2024-01-01')                    |
        | ival_ym / dd  | (INTERVAL '1-2' YEAR TO MONTH) / (DATE'2024-01-15' - DATE'2024-01-01')          |
        | null / dd     | (NULL) / (DATE'2024-01-15' - DATE'2024-01-01')                                  |
        | str / dd      | ('2') / (DATE'2024-01-15' - DATE'2024-01-01')                                   |
        | ts / dd       | (TIMESTAMP'2024-01-15 12:00:00') / (DATE'2024-01-15' - DATE'2024-01-01')        |
        | ts_ntz / dd   | (TIMESTAMP_NTZ'2024-01-15 12:00:00') / (DATE'2024-01-15' - DATE'2024-01-01')    |
