# SPDX-License-Identifier: Apache-2.0

Feature: Date functions read TIMESTAMP values in the session time zone

  Scenario: Date functions read TIMESTAMP values in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        trunc(timestamp_micros(1704078000000000), 'MM') AS trunc_month,
        trunc(timestamp_micros(1704078000000000), 'YEAR') AS trunc_year,
        timestampdiff(DAY, TIMESTAMP_NTZ '2020-01-01 23:00:00', TIMESTAMP_NTZ '2020-01-02 01:00:00') AS ntz_days,
        timestampdiff(DAY, TIMESTAMP_NTZ '2020-01-02 01:00:00', TIMESTAMP_NTZ '2020-01-01 23:00:00') AS ntz_days_reversed,
        timestampdiff(DAY, timestamp_micros(1703991600000000), timestamp_micros(1704078000000000)) AS days,
        datediff(timestamp_micros(1704078000000000), timestamp_micros(1703991600000000)) AS date_days,
        months_between(timestamp_micros(1706756400000000), timestamp_micros(1703991600000000)) AS months,
        CAST(timestamp_micros(1704078000000000) AS DATE) = DATE '2023-12-31' AS date_equals,
        CAST(timestamp_micros(1704078000000000) AS DATE) < DATE '2024-01-01' AS date_less_than
      """
    Then query result
      | trunc_month | trunc_year | ntz_days | ntz_days_reversed | days | date_days | months     | date_equals | date_less_than |
      | 2023-12-01  | 2023-01-01 | 0        | 0                 | 1    | 1         | 1.03225806 | true        | true           |

  Scenario: Date functions read TIMESTAMP values in the session time zone (Asia/Shanghai)
    Given config spark.sql.session.timeZone = Asia/Shanghai
    When query
      """
      SELECT
        trunc(timestamp_micros(1704078000000000), 'MM') AS trunc_month,
        trunc(timestamp_micros(1704078000000000), 'YEAR') AS trunc_year,
        timestampdiff(DAY, TIMESTAMP_NTZ '2020-01-01 23:00:00', TIMESTAMP_NTZ '2020-01-02 01:00:00') AS ntz_days,
        timestampdiff(DAY, TIMESTAMP_NTZ '2020-01-02 01:00:00', TIMESTAMP_NTZ '2020-01-01 23:00:00') AS ntz_days_reversed,
        timestampdiff(DAY, timestamp_micros(1703991600000000), timestamp_micros(1704078000000000)) AS days,
        datediff(timestamp_micros(1704078000000000), timestamp_micros(1703991600000000)) AS date_days,
        months_between(timestamp_micros(1706756400000000), timestamp_micros(1703991600000000)) AS months,
        CAST(timestamp_micros(1704078000000000) AS DATE) = DATE '2023-12-31' AS date_equals,
        CAST(timestamp_micros(1704078000000000) AS DATE) < DATE '2024-01-01' AS date_less_than
      """
    Then query result
      | trunc_month | trunc_year | ntz_days | ntz_days_reversed | days | date_days | months     | date_equals | date_less_than |
      | 2024-01-01  | 2024-01-01 | 0        | 0                 | 1    | 1         | 1.03225806 | false       | false          |

  Scenario: Date functions count a DST day in the session time zone (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        months_between(TIMESTAMP '2020-03-08 12:00:00', TIMESTAMP '2020-03-07 12:00:00') AS months,
        timestampdiff(HOUR, TIMESTAMP '2020-03-08 01:00:00', TIMESTAMP '2020-03-08 04:00:00') AS hours
      """
    Then query result
      | months     | hours |
      | 0.03091398 | 3     |

  Scenario: Date functions count fixed units in a fixed-offset session time zone
    Given config spark.sql.session.timeZone = +05:30
    When query
      """
      SELECT
        timestampdiff(HOUR, TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 05:30:00') AS hours,
        timestampdiff(MINUTE, timestamp_micros(1577836800000000), TIMESTAMP '2020-01-01 05:30:00') AS minutes,
        timestampdiff(SECOND, TIMESTAMP '2020-01-01 00:00:00', timestamp_micros(1577836800000000)) AS seconds,
        timestampdiff(WEEK, TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-15 00:00:00') AS weeks,
        timestampdiff(HOUR, TIMESTAMP_NTZ '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 05:30:00') AS mixed_hours
      """
    Then query result
      | hours | minutes | seconds | weeks | mixed_hours |
      | 5     | 0       | 19800   | 2     | 5           |

  Scenario: Formatting dates and wall-clock timestamps uses the session offset at that instant
    Given config spark.sql.session.timeZone = Europe/Amsterdam
    When query
      """
      SELECT date_format(DATE '2020-01-01', 'HH:mm XXX') AS midnight,
             date_format(TIMESTAMP_NTZ '2020-03-29 02:30:00', 'HH:mm XXX') AS gap,
             hour(TIMESTAMP_NTZ '2020-03-29 02:30:00') AS ntz_hour
      """
    Then query result
      | midnight     | gap          | ntz_hour |
      | 00:00 +01:00 | 03:30 +02:00 | 2        |

  Scenario: Casting and rendering an instant during a daylight-saving overlap
    Given config spark.sql.session.timeZone = Europe/Amsterdam
    When query
      """
      SELECT string(ts) AS displayed, string(named_struct('t', ts)) AS nested,
             unix_micros(timestamp(ts)) AS instant, typeof(timestamp(ts)) AS ts_type,
             to_timestamp_ntz(ts) AS local_ts, to_timestamp_ntz(ts, 'yyyy') AS formatted_ntz,
             unix_micros(to_timestamp_ltz(ts, CAST(NULL AS STRING))) AS formatted_ltz,
             to_json(to_variant_object(named_struct('t', ts))) AS json
      FROM (SELECT timestamp_micros(1603589400000000L) AS ts)
      """
    Then query result
      | displayed           | nested                | instant          | ts_type   | local_ts            | formatted_ntz       | formatted_ltz    | json                               |
      | 2020-10-25 02:30:00 | {2020-10-25 02:30:00} | 1603589400000000 | timestamp | 2020-10-25 02:30:00 | 2020-10-25 01:30:00 | 1603589400000000 | {"t":"2020-10-25 02:30:00+01:00"} |

  Scenario: Try interval arithmetic counts local days across the spring transition
    Given config spark.sql.session.timeZone = Europe/Amsterdam
    When query
      """
      SELECT date_format(try_add(ts, INTERVAL 1 DAY), 'HH:mm XXX') AS tomorrow,
             typeof(try_add(ts, INTERVAL 1 DAY)) AS result_type,
             unix_micros(try_subtract(try_add(ts, INTERVAL 1 DAY), INTERVAL 1 DAY)) = unix_micros(ts) AS restored,
             try_add(timestamp_micros(9223372036854775807L), INTERVAL 1 MICROSECOND) IS NULL AS overflow
      FROM (SELECT TIMESTAMP '2020-03-28 12:00:00' AS ts)
      """
    Then query result
      | tomorrow     | result_type | restored | overflow |
      | 12:00 +02:00 | timestamp   | true     | true     |

  Scenario: Functions implicitly converting an instant to a string use the session time zone
    Given config spark.sql.session.timeZone = Europe/Amsterdam
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT upper(ts) AS rendered, coalesce(ts, 'missing') AS fallback
      FROM (SELECT timestamp_micros(1577835000000000L) AS ts)
      """
    Then query result
      | rendered            | fallback            |
      | 2020-01-01 00:30:00 | 2020-01-01 00:30:00 |

  Scenario: Implicit timestamp arguments use the session zone for extraction and elapsed time
    Given config spark.sql.session.timeZone = Europe/Amsterdam
    When query
      """
      SELECT hour('2019-12-31 23:30:00Z') AS local_hour,
             day('2019-12-31 23:30:00Z') AS calendar_day,
             months_between(TIMESTAMP_NTZ '2020-03-29 12:00:00', TIMESTAMP_NTZ '2020-03-28 12:00:00') AS months
      """
    Then query result
      | local_hour | calendar_day | months     |
      | 0          | 31           | 0.03091398 |

  Scenario: Extracting fields in a fixed offset with seconds
    Given config spark.sql.session.timeZone = +05:30:45
    When query
      """
      SELECT hour(ts) AS h, minute(ts) AS m, second(ts) AS s, extract(SECOND FROM ts) AS fractional_s,
             date_format(ts, 'HH:mm:ss') AS clock
      FROM (SELECT timestamp_micros(0L) AS ts)
      """
    Then query result
      | h | m  | s  | fractional_s | clock    |
      | 5 | 30 | 45 | 45.000000    | 05:30:45 |

  Scenario: Formatted conversions of typed inputs follow Spark's stored microsecond semantics
    Given config spark.sql.session.timeZone = Europe/Amsterdam
    When query
      """
      SELECT to_date(ts, 'yyyy') AS formatted_date, to_date(ts) AS local_date,
             to_timestamp_ntz(DATE '2020-01-01', 'yyyy') AS midnight_instant,
             extract(hour FROM '2019-12-31 23:30:00Z') AS instant_hour
      FROM (SELECT TIMESTAMP_NTZ '2019-12-31 23:30:00' AS ts)
      """
    Then query result
      | formatted_date | local_date | midnight_instant    | instant_hour |
      | 2020-01-01     | 2019-12-31 | 2019-12-31 23:00:00 | 0            |

  Scenario: A JSON time zone override applies only to instant timestamps
    Given config spark.sql.session.timeZone = Europe/Amsterdam
    When query
      """
      SELECT to_json(named_struct('instant', timestamp_micros(0L), 'wall', TIMESTAMP_NTZ '1970-01-01 00:00:00'),
                     map('timeZone', '+05:45', 'timestampFormat', 'HH:mm XXX', 'timestampNTZFormat', 'HH:mm')) AS json
      """
    Then query result
      | json                                    |
      | {"instant":"05:45 +05:45","wall":"00:00"} |

  Scenario: A date function uses the written date of a string containing a time zone
    Given config spark.sql.session.timeZone = Europe/Amsterdam
    When query
      """
      SELECT day(s) AS d, year(s) AS y, date_part('day', s) AS part, last_day(s) AS last,
             datediff(s, DATE '2019-12-31') AS days, hour(s) AS local_hour
      FROM (SELECT '2019-12-31 23:30:00-05:00' AS s)
      """
    Then query result
      | d  | y    | part | last       | days | local_hour |
      | 31 | 2019 | 31   | 2019-12-31 | 0    | 5          |

  Scenario: A subminute session offset carries an instant into the next year
    Given config spark.sql.session.timeZone = +05:30:45
    When query
      """
      SELECT string(ts) AS local_time, to_date(ts) AS local_date, year(ts) AS y,
             date_format(date_trunc('DAY', ts), 'HH:mm:ss') AS midnight,
             unix_micros(to_timestamp('2020-01-01 00:00:15', 'yyyy-MM-dd HH:mm:ss')) = unix_micros(ts) AS parsed
      FROM (SELECT timestamp_micros(1577816970000000L) AS ts)
      """
    Then query result
      | local_time          | local_date | y    | midnight | parsed |
      | 2020-01-01 00:00:15 | 2020-01-01 | 2020 | 00:00:00 | true   |

  Scenario: JSON parsing honors a time zone override for instant timestamps
    Given config spark.sql.session.timeZone = Europe/Amsterdam
    When query
      """
      SELECT unix_micros(v.t) AS instant, string(v.wall) AS wall FROM (
        SELECT from_json('{"t":"1970-01-01 05:45:00","wall":"01/01/1970 05:45"}',
                         't TIMESTAMP, wall TIMESTAMP_NTZ',
                         map('timeZone', '+05:45', 'timestampFormat', 'yyyy-MM-dd HH:mm:ss',
                             'timestampNTZFormat', 'dd/MM/yyyy HH:mm')) AS v)
      """
    Then query result
      | instant | wall                |
      | 0       | 1970-01-01 05:45:00 |
