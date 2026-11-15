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
