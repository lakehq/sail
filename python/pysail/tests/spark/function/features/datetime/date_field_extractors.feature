Feature: date field extractors: month, day, dayofmonth, quarter, dayofyear, dayofweek, weekday, weekofyear

  # One Scenario Outline per group of leaves of Spark's `GetDateField` family (`datetimeExpressions.scala:471-497`), over the eight
  # functions. The expected values are the answers of the Spark JVM 4.2 (see the holocron: flujo_sail/datetime_extractors/familia_getdatefield).
  # The rows that fail on Sail are in a second Outline with `@sail-bug`; every one of them was run on `main` and fails there too.
  # The harness ignores a tag on an `Examples` block, so the tag goes on the `Scenario Outline`.

  Rule: Formula edges

    Scenario Outline: <function>: Edge: <case>
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <function>(DATE '<date>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | case                                  | date       | result |
        | day        | leap day 2024-02-29                   | 2024-02-29 | 29     |
        | day        | day before leap day                   | 2024-02-28 | 28     |
        | day        | day after leap day                    | 2024-03-01 | 1      |
        | day        | non-leap Feb 28 2023                  | 2023-02-28 | 28     |
        | day        | non-leap Mar 1 2023                   | 2023-03-01 | 1      |
        | day        | century leap 2000-02-29               | 2000-02-29 | 29     |
        | day        | century non-leap 1900-02-28           | 1900-02-28 | 28     |
        | day        | 1900-03-01                            | 1900-03-01 | 1      |
        | day        | 2100-03-01                            | 2100-03-01 | 1      |
        | day        | 31 Dec leap year 2024-12-31           | 2024-12-31 | 31     |
        | day        | 31 Dec non-leap 2023-12-31 (Sunday)   | 2023-12-31 | 31     |
        | day        | 1 Jan 2024 (Monday)                   | 2024-01-01 | 1      |
        | day        | ISO week 53: 2020-12-31               | 2020-12-31 | 31     |
        | day        | ISO week 53 spills: 2021-01-03        | 2021-01-03 | 3      |
        | day        | ISO week 1: 2021-01-04                | 2021-01-04 | 4      |
        | day        | week 1 of next year: 2024-12-30       | 2024-12-30 | 30     |
        | day        | week 1 of next year: 2018-12-31       | 2018-12-31 | 31     |
        | day        | week 53 of prior year: 2016-01-01     | 2016-01-01 | 1      |
        | day        | week 53 of prior year: 2005-01-02     | 2005-01-02 | 2      |
        | day        | 2026-01-01                            | 2026-01-01 | 1      |
        | day        | 2015-12-31 (W53)                      | 2015-12-31 | 31     |
        | day        | 2009-01-01 (W1)                       | 2009-01-01 | 1      |
        | day        | Sunday 2024-03-17                     | 2024-03-17 | 17     |
        | day        | Monday 2024-03-18                     | 2024-03-18 | 18     |
        | day        | Saturday 2024-03-16                   | 2024-03-16 | 16     |
        | day        | Q1 end 2024-03-31                     | 2024-03-31 | 31     |
        | day        | Q2 start 2024-04-01                   | 2024-04-01 | 1      |
        | day        | Q2 end 2024-06-30                     | 2024-06-30 | 30     |
        | day        | Q3 start 2024-07-01                   | 2024-07-01 | 1      |
        | day        | Q3 end 2024-09-30                     | 2024-09-30 | 30     |
        | day        | Q4 start 2024-10-01                   | 2024-10-01 | 1      |
        | day        | month end 31 Jan                      | 2024-01-31 | 31     |
        | day        | month end 30 Apr                      | 2024-04-30 | 30     |
        | day        | epoch 1970-01-01 (Thursday)           | 1970-01-01 | 1      |
        | day        | before epoch 1969-12-31               | 1969-12-31 | 31     |
        | day        | 0001-01-01 (Monday)                   | 0001-01-01 | 1      |
        | day        | 9999-12-31 (Friday)                   | 9999-12-31 | 31     |
        | day        | 1582-10-04                            | 1582-10-04 | 4      |
        | day        | 1582-10-15                            | 1582-10-15 | 15     |
        | day        | day 256 of a non-leap year 2023-09-13 | 2023-09-13 | 13     |
        | day        | day 366 2024-12-31 (again via doy)    | 2024-12-31 | 31     |
        | day        | 1999-12-31                            | 1999-12-31 | 31     |
        | day        | 2000-01-01                            | 2000-01-01 | 1      |
        | day        | Sunday 0001-01-07                     | 0001-01-07 | 7      |
        | dayofmonth | leap day 2024-02-29                   | 2024-02-29 | 29     |
        | dayofmonth | day before leap day                   | 2024-02-28 | 28     |
        | dayofmonth | day after leap day                    | 2024-03-01 | 1      |
        | dayofmonth | non-leap Feb 28 2023                  | 2023-02-28 | 28     |
        | dayofmonth | non-leap Mar 1 2023                   | 2023-03-01 | 1      |
        | dayofmonth | century leap 2000-02-29               | 2000-02-29 | 29     |
        | dayofmonth | century non-leap 1900-02-28           | 1900-02-28 | 28     |
        | dayofmonth | 1900-03-01                            | 1900-03-01 | 1      |
        | dayofmonth | 2100-03-01                            | 2100-03-01 | 1      |
        | dayofmonth | 31 Dec leap year 2024-12-31           | 2024-12-31 | 31     |
        | dayofmonth | 31 Dec non-leap 2023-12-31 (Sunday)   | 2023-12-31 | 31     |
        | dayofmonth | 1 Jan 2024 (Monday)                   | 2024-01-01 | 1      |
        | dayofmonth | ISO week 53: 2020-12-31               | 2020-12-31 | 31     |
        | dayofmonth | ISO week 53 spills: 2021-01-03        | 2021-01-03 | 3      |
        | dayofmonth | ISO week 1: 2021-01-04                | 2021-01-04 | 4      |
        | dayofmonth | week 1 of next year: 2024-12-30       | 2024-12-30 | 30     |
        | dayofmonth | week 1 of next year: 2018-12-31       | 2018-12-31 | 31     |
        | dayofmonth | week 53 of prior year: 2016-01-01     | 2016-01-01 | 1      |
        | dayofmonth | week 53 of prior year: 2005-01-02     | 2005-01-02 | 2      |
        | dayofmonth | 2026-01-01                            | 2026-01-01 | 1      |
        | dayofmonth | 2015-12-31 (W53)                      | 2015-12-31 | 31     |
        | dayofmonth | 2009-01-01 (W1)                       | 2009-01-01 | 1      |
        | dayofmonth | Sunday 2024-03-17                     | 2024-03-17 | 17     |
        | dayofmonth | Monday 2024-03-18                     | 2024-03-18 | 18     |
        | dayofmonth | Saturday 2024-03-16                   | 2024-03-16 | 16     |
        | dayofmonth | Q1 end 2024-03-31                     | 2024-03-31 | 31     |
        | dayofmonth | Q2 start 2024-04-01                   | 2024-04-01 | 1      |
        | dayofmonth | Q2 end 2024-06-30                     | 2024-06-30 | 30     |
        | dayofmonth | Q3 start 2024-07-01                   | 2024-07-01 | 1      |
        | dayofmonth | Q3 end 2024-09-30                     | 2024-09-30 | 30     |
        | dayofmonth | Q4 start 2024-10-01                   | 2024-10-01 | 1      |
        | dayofmonth | month end 31 Jan                      | 2024-01-31 | 31     |
        | dayofmonth | month end 30 Apr                      | 2024-04-30 | 30     |
        | dayofmonth | epoch 1970-01-01 (Thursday)           | 1970-01-01 | 1      |
        | dayofmonth | before epoch 1969-12-31               | 1969-12-31 | 31     |
        | dayofmonth | 0001-01-01 (Monday)                   | 0001-01-01 | 1      |
        | dayofmonth | 9999-12-31 (Friday)                   | 9999-12-31 | 31     |
        | dayofmonth | 1582-10-04                            | 1582-10-04 | 4      |
        | dayofmonth | 1582-10-15                            | 1582-10-15 | 15     |
        | dayofmonth | day 256 of a non-leap year 2023-09-13 | 2023-09-13 | 13     |
        | dayofmonth | day 366 2024-12-31 (again via doy)    | 2024-12-31 | 31     |
        | dayofmonth | 1999-12-31                            | 1999-12-31 | 31     |
        | dayofmonth | 2000-01-01                            | 2000-01-01 | 1      |
        | dayofmonth | Sunday 0001-01-07                     | 0001-01-07 | 7      |
        | dayofyear  | leap day 2024-02-29                   | 2024-02-29 | 60     |
        | dayofyear  | day before leap day                   | 2024-02-28 | 59     |
        | dayofyear  | day after leap day                    | 2024-03-01 | 61     |
        | dayofyear  | non-leap Feb 28 2023                  | 2023-02-28 | 59     |
        | dayofyear  | non-leap Mar 1 2023                   | 2023-03-01 | 60     |
        | dayofyear  | century leap 2000-02-29               | 2000-02-29 | 60     |
        | dayofyear  | century non-leap 1900-02-28           | 1900-02-28 | 59     |
        | dayofyear  | 1900-03-01                            | 1900-03-01 | 60     |
        | dayofyear  | 2100-03-01                            | 2100-03-01 | 60     |
        | dayofyear  | 31 Dec leap year 2024-12-31           | 2024-12-31 | 366    |
        | dayofyear  | 31 Dec non-leap 2023-12-31 (Sunday)   | 2023-12-31 | 365    |
        | dayofyear  | 1 Jan 2024 (Monday)                   | 2024-01-01 | 1      |
        | dayofyear  | ISO week 53: 2020-12-31               | 2020-12-31 | 366    |
        | dayofyear  | ISO week 53 spills: 2021-01-03        | 2021-01-03 | 3      |
        | dayofyear  | ISO week 1: 2021-01-04                | 2021-01-04 | 4      |
        | dayofyear  | week 1 of next year: 2024-12-30       | 2024-12-30 | 365    |
        | dayofyear  | week 1 of next year: 2018-12-31       | 2018-12-31 | 365    |
        | dayofyear  | week 53 of prior year: 2016-01-01     | 2016-01-01 | 1      |
        | dayofyear  | week 53 of prior year: 2005-01-02     | 2005-01-02 | 2      |
        | dayofyear  | 2026-01-01                            | 2026-01-01 | 1      |
        | dayofyear  | 2015-12-31 (W53)                      | 2015-12-31 | 365    |
        | dayofyear  | 2009-01-01 (W1)                       | 2009-01-01 | 1      |
        | dayofyear  | Sunday 2024-03-17                     | 2024-03-17 | 77     |
        | dayofyear  | Monday 2024-03-18                     | 2024-03-18 | 78     |
        | dayofyear  | Saturday 2024-03-16                   | 2024-03-16 | 76     |
        | dayofyear  | Q1 end 2024-03-31                     | 2024-03-31 | 91     |
        | dayofyear  | Q2 start 2024-04-01                   | 2024-04-01 | 92     |
        | dayofyear  | Q2 end 2024-06-30                     | 2024-06-30 | 182    |
        | dayofyear  | Q3 start 2024-07-01                   | 2024-07-01 | 183    |
        | dayofyear  | Q3 end 2024-09-30                     | 2024-09-30 | 274    |
        | dayofyear  | Q4 start 2024-10-01                   | 2024-10-01 | 275    |
        | dayofyear  | month end 31 Jan                      | 2024-01-31 | 31     |
        | dayofyear  | month end 30 Apr                      | 2024-04-30 | 121    |
        | dayofyear  | epoch 1970-01-01 (Thursday)           | 1970-01-01 | 1      |
        | dayofyear  | before epoch 1969-12-31               | 1969-12-31 | 365    |
        | dayofyear  | 0001-01-01 (Monday)                   | 0001-01-01 | 1      |
        | dayofyear  | 9999-12-31 (Friday)                   | 9999-12-31 | 365    |
        | dayofyear  | 1582-10-04                            | 1582-10-04 | 277    |
        | dayofyear  | 1582-10-15                            | 1582-10-15 | 288    |
        | dayofyear  | day 256 of a non-leap year 2023-09-13 | 2023-09-13 | 256    |
        | dayofyear  | day 366 2024-12-31 (again via doy)    | 2024-12-31 | 366    |
        | dayofyear  | 1999-12-31                            | 1999-12-31 | 365    |
        | dayofyear  | 2000-01-01                            | 2000-01-01 | 1      |
        | dayofyear  | Sunday 0001-01-07                     | 0001-01-07 | 7      |
        | dayofweek  | leap day 2024-02-29                   | 2024-02-29 | 5      |
        | dayofweek  | day before leap day                   | 2024-02-28 | 4      |
        | dayofweek  | day after leap day                    | 2024-03-01 | 6      |
        | dayofweek  | non-leap Feb 28 2023                  | 2023-02-28 | 3      |
        | dayofweek  | non-leap Mar 1 2023                   | 2023-03-01 | 4      |
        | dayofweek  | century leap 2000-02-29               | 2000-02-29 | 3      |
        | dayofweek  | century non-leap 1900-02-28           | 1900-02-28 | 4      |
        | dayofweek  | 1900-03-01                            | 1900-03-01 | 5      |
        | dayofweek  | 2100-03-01                            | 2100-03-01 | 2      |
        | dayofweek  | 31 Dec leap year 2024-12-31           | 2024-12-31 | 3      |
        | dayofweek  | 31 Dec non-leap 2023-12-31 (Sunday)   | 2023-12-31 | 1      |
        | dayofweek  | 1 Jan 2024 (Monday)                   | 2024-01-01 | 2      |
        | dayofweek  | ISO week 53: 2020-12-31               | 2020-12-31 | 5      |
        | dayofweek  | ISO week 53 spills: 2021-01-03        | 2021-01-03 | 1      |
        | dayofweek  | ISO week 1: 2021-01-04                | 2021-01-04 | 2      |
        | dayofweek  | week 1 of next year: 2024-12-30       | 2024-12-30 | 2      |
        | dayofweek  | week 1 of next year: 2018-12-31       | 2018-12-31 | 2      |
        | dayofweek  | week 53 of prior year: 2016-01-01     | 2016-01-01 | 6      |
        | dayofweek  | week 53 of prior year: 2005-01-02     | 2005-01-02 | 1      |
        | dayofweek  | 2026-01-01                            | 2026-01-01 | 5      |
        | dayofweek  | 2015-12-31 (W53)                      | 2015-12-31 | 5      |
        | dayofweek  | 2009-01-01 (W1)                       | 2009-01-01 | 5      |
        | dayofweek  | Sunday 2024-03-17                     | 2024-03-17 | 1      |
        | dayofweek  | Monday 2024-03-18                     | 2024-03-18 | 2      |
        | dayofweek  | Saturday 2024-03-16                   | 2024-03-16 | 7      |
        | dayofweek  | Q1 end 2024-03-31                     | 2024-03-31 | 1      |
        | dayofweek  | Q2 start 2024-04-01                   | 2024-04-01 | 2      |
        | dayofweek  | Q2 end 2024-06-30                     | 2024-06-30 | 1      |
        | dayofweek  | Q3 start 2024-07-01                   | 2024-07-01 | 2      |
        | dayofweek  | Q3 end 2024-09-30                     | 2024-09-30 | 2      |
        | dayofweek  | Q4 start 2024-10-01                   | 2024-10-01 | 3      |
        | dayofweek  | month end 31 Jan                      | 2024-01-31 | 4      |
        | dayofweek  | month end 30 Apr                      | 2024-04-30 | 3      |
        | dayofweek  | epoch 1970-01-01 (Thursday)           | 1970-01-01 | 5      |
        | dayofweek  | before epoch 1969-12-31               | 1969-12-31 | 4      |
        | dayofweek  | 0001-01-01 (Monday)                   | 0001-01-01 | 2      |
        | dayofweek  | 9999-12-31 (Friday)                   | 9999-12-31 | 6      |
        | dayofweek  | 1582-10-04                            | 1582-10-04 | 2      |
        | dayofweek  | 1582-10-15                            | 1582-10-15 | 6      |
        | dayofweek  | day 256 of a non-leap year 2023-09-13 | 2023-09-13 | 4      |
        | dayofweek  | day 366 2024-12-31 (again via doy)    | 2024-12-31 | 3      |
        | dayofweek  | 1999-12-31                            | 1999-12-31 | 6      |
        | dayofweek  | 2000-01-01                            | 2000-01-01 | 7      |
        | dayofweek  | Sunday 0001-01-07                     | 0001-01-07 | 1      |
        | weekday    | leap day 2024-02-29                   | 2024-02-29 | 3      |
        | weekday    | day before leap day                   | 2024-02-28 | 2      |
        | weekday    | day after leap day                    | 2024-03-01 | 4      |
        | weekday    | non-leap Feb 28 2023                  | 2023-02-28 | 1      |
        | weekday    | non-leap Mar 1 2023                   | 2023-03-01 | 2      |
        | weekday    | century leap 2000-02-29               | 2000-02-29 | 1      |
        | weekday    | century non-leap 1900-02-28           | 1900-02-28 | 2      |
        | weekday    | 1900-03-01                            | 1900-03-01 | 3      |
        | weekday    | 2100-03-01                            | 2100-03-01 | 0      |
        | weekday    | 31 Dec leap year 2024-12-31           | 2024-12-31 | 1      |
        | weekday    | 1 Jan 2024 (Monday)                   | 2024-01-01 | 0      |
        | weekday    | ISO week 53: 2020-12-31               | 2020-12-31 | 3      |
        | weekday    | ISO week 1: 2021-01-04                | 2021-01-04 | 0      |
        | weekday    | week 1 of next year: 2024-12-30       | 2024-12-30 | 0      |
        | weekday    | week 1 of next year: 2018-12-31       | 2018-12-31 | 0      |
        | weekday    | week 53 of prior year: 2016-01-01     | 2016-01-01 | 4      |
        | weekday    | 2026-01-01                            | 2026-01-01 | 3      |
        | weekday    | 2015-12-31 (W53)                      | 2015-12-31 | 3      |
        | weekday    | 2009-01-01 (W1)                       | 2009-01-01 | 3      |
        | weekday    | Monday 2024-03-18                     | 2024-03-18 | 0      |
        | weekday    | Saturday 2024-03-16                   | 2024-03-16 | 5      |
        | weekday    | Q2 start 2024-04-01                   | 2024-04-01 | 0      |
        | weekday    | Q3 start 2024-07-01                   | 2024-07-01 | 0      |
        | weekday    | Q3 end 2024-09-30                     | 2024-09-30 | 0      |
        | weekday    | Q4 start 2024-10-01                   | 2024-10-01 | 1      |
        | weekday    | month end 31 Jan                      | 2024-01-31 | 2      |
        | weekday    | month end 30 Apr                      | 2024-04-30 | 1      |
        | weekday    | epoch 1970-01-01 (Thursday)           | 1970-01-01 | 3      |
        | weekday    | before epoch 1969-12-31               | 1969-12-31 | 2      |
        | weekday    | 0001-01-01 (Monday)                   | 0001-01-01 | 0      |
        | weekday    | 9999-12-31 (Friday)                   | 9999-12-31 | 4      |
        | weekday    | 1582-10-04                            | 1582-10-04 | 0      |
        | weekday    | 1582-10-15                            | 1582-10-15 | 4      |
        | weekday    | day 256 of a non-leap year 2023-09-13 | 2023-09-13 | 2      |
        | weekday    | day 366 2024-12-31 (again via doy)    | 2024-12-31 | 1      |
        | weekday    | 1999-12-31                            | 1999-12-31 | 4      |
        | weekday    | 2000-01-01                            | 2000-01-01 | 5      |
        | weekofyear | leap day 2024-02-29                   | 2024-02-29 | 9      |
        | weekofyear | day before leap day                   | 2024-02-28 | 9      |
        | weekofyear | day after leap day                    | 2024-03-01 | 9      |
        | weekofyear | non-leap Feb 28 2023                  | 2023-02-28 | 9      |
        | weekofyear | non-leap Mar 1 2023                   | 2023-03-01 | 9      |
        | weekofyear | century leap 2000-02-29               | 2000-02-29 | 9      |
        | weekofyear | century non-leap 1900-02-28           | 1900-02-28 | 9      |
        | weekofyear | 1900-03-01                            | 1900-03-01 | 9      |
        | weekofyear | 2100-03-01                            | 2100-03-01 | 9      |
        | weekofyear | 31 Dec leap year 2024-12-31           | 2024-12-31 | 1      |
        | weekofyear | 31 Dec non-leap 2023-12-31 (Sunday)   | 2023-12-31 | 52     |
        | weekofyear | 1 Jan 2024 (Monday)                   | 2024-01-01 | 1      |
        | weekofyear | ISO week 53: 2020-12-31               | 2020-12-31 | 53     |
        | weekofyear | ISO week 53 spills: 2021-01-03        | 2021-01-03 | 53     |
        | weekofyear | ISO week 1: 2021-01-04                | 2021-01-04 | 1      |
        | weekofyear | week 1 of next year: 2024-12-30       | 2024-12-30 | 1      |
        | weekofyear | week 1 of next year: 2018-12-31       | 2018-12-31 | 1      |
        | weekofyear | week 53 of prior year: 2016-01-01     | 2016-01-01 | 53     |
        | weekofyear | week 53 of prior year: 2005-01-02     | 2005-01-02 | 53     |
        | weekofyear | 2026-01-01                            | 2026-01-01 | 1      |
        | weekofyear | 2015-12-31 (W53)                      | 2015-12-31 | 53     |
        | weekofyear | 2009-01-01 (W1)                       | 2009-01-01 | 1      |
        | weekofyear | Sunday 2024-03-17                     | 2024-03-17 | 11     |
        | weekofyear | Monday 2024-03-18                     | 2024-03-18 | 12     |
        | weekofyear | Saturday 2024-03-16                   | 2024-03-16 | 11     |
        | weekofyear | Q1 end 2024-03-31                     | 2024-03-31 | 13     |
        | weekofyear | Q2 start 2024-04-01                   | 2024-04-01 | 14     |
        | weekofyear | Q2 end 2024-06-30                     | 2024-06-30 | 26     |
        | weekofyear | Q3 start 2024-07-01                   | 2024-07-01 | 27     |
        | weekofyear | Q3 end 2024-09-30                     | 2024-09-30 | 40     |
        | weekofyear | Q4 start 2024-10-01                   | 2024-10-01 | 40     |
        | weekofyear | month end 31 Jan                      | 2024-01-31 | 5      |
        | weekofyear | month end 30 Apr                      | 2024-04-30 | 18     |
        | weekofyear | epoch 1970-01-01 (Thursday)           | 1970-01-01 | 1      |
        | weekofyear | before epoch 1969-12-31               | 1969-12-31 | 1      |
        | weekofyear | 0001-01-01 (Monday)                   | 0001-01-01 | 1      |
        | weekofyear | 9999-12-31 (Friday)                   | 9999-12-31 | 52     |
        | weekofyear | 1582-10-04                            | 1582-10-04 | 40     |
        | weekofyear | 1582-10-15                            | 1582-10-15 | 41     |
        | weekofyear | day 256 of a non-leap year 2023-09-13 | 2023-09-13 | 37     |
        | weekofyear | day 366 2024-12-31 (again via doy)    | 2024-12-31 | 1      |
        | weekofyear | 1999-12-31                            | 1999-12-31 | 52     |
        | weekofyear | 2000-01-01                            | 2000-01-01 | 52     |
        | weekofyear | Sunday 0001-01-07                     | 0001-01-07 | 1      |

    # TODO(known gap): weekday of a Sunday gives -1 in Sail (it computes `date_part('dow') - 1`); Spark's `WeekDay` is 0 (Monday) to 6 (Sunday).
    #   Every row fails on `main` too (checked).
    @sail-bug
    Scenario Outline: <function>: Edge: <case> (known gap)
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <function>(DATE '<date>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function | case                                | date       | result |
        | weekday  | 31 Dec non-leap 2023-12-31 (Sunday) | 2023-12-31 | 6      |
        | weekday  | ISO week 53 spills: 2021-01-03      | 2021-01-03 | 6      |
        | weekday  | week 53 of prior year: 2005-01-02   | 2005-01-02 | 6      |
        | weekday  | Sunday 2024-03-17                   | 2024-03-17 | 6      |
        | weekday  | Q1 end 2024-03-31                   | 2024-03-31 | 6      |
        | weekday  | Q2 end 2024-06-30                   | 2024-06-30 | 6      |
        | weekday  | Sunday 0001-01-07                   | 0001-01-07 | 6      |

  Rule: Input type to DATE

    Scenario Outline: <function>: Input type: <case>
      Given config spark.sql.session.timeZone = <zone>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <function>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | case                                    | zone                | ansi  | input                                 | result |
        | day        | TIMESTAMP crosses midnight, LA          | America/Los_Angeles | false | TIMESTAMP '2024-01-01 03:00:00+00:00' | 31     |
        | day        | TIMESTAMP crosses midnight, LA, ANSI on | America/Los_Angeles | true  | TIMESTAMP '2024-01-01 03:00:00+00:00' | 31     |
        | day        | TIMESTAMP same instant, UTC             | UTC                 | false | TIMESTAMP '2024-01-01 03:00:00+00:00' | 1      |
        | dayofmonth | TIMESTAMP crosses midnight, LA          | America/Los_Angeles | false | TIMESTAMP '2024-01-01 03:00:00+00:00' | 31     |
        | dayofmonth | TIMESTAMP crosses midnight, LA, ANSI on | America/Los_Angeles | true  | TIMESTAMP '2024-01-01 03:00:00+00:00' | 31     |
        | dayofmonth | TIMESTAMP same instant, UTC             | UTC                 | false | TIMESTAMP '2024-01-01 03:00:00+00:00' | 1      |
        | dayofyear  | TIMESTAMP crosses midnight, LA          | America/Los_Angeles | false | TIMESTAMP '2024-01-01 03:00:00+00:00' | 365    |
        | dayofyear  | TIMESTAMP crosses midnight, LA, ANSI on | America/Los_Angeles | true  | TIMESTAMP '2024-01-01 03:00:00+00:00' | 365    |
        | dayofyear  | TIMESTAMP same instant, UTC             | UTC                 | false | TIMESTAMP '2024-01-01 03:00:00+00:00' | 1      |
        | dayofweek  | TIMESTAMP crosses midnight, LA          | America/Los_Angeles | false | TIMESTAMP '2024-01-01 03:00:00+00:00' | 1      |
        | dayofweek  | TIMESTAMP crosses midnight, LA, ANSI on | America/Los_Angeles | true  | TIMESTAMP '2024-01-01 03:00:00+00:00' | 1      |
        | dayofweek  | TIMESTAMP same instant, UTC             | UTC                 | false | TIMESTAMP '2024-01-01 03:00:00+00:00' | 2      |
        | weekday    | TIMESTAMP same instant, UTC             | UTC                 | false | TIMESTAMP '2024-01-01 03:00:00+00:00' | 0      |
        | weekofyear | NTZ ignores the zone, LA                | America/Los_Angeles | false | TIMESTAMP_NTZ '2024-01-01 03:00:00'   | 1      |
        | weekofyear | NTZ, ANSI on                            | America/Los_Angeles | true  | TIMESTAMP_NTZ '2024-01-01 03:00:00'   | 1      |
        | day        | NTZ ignores the zone, LA                | America/Los_Angeles | false | TIMESTAMP_NTZ '2024-01-01 03:00:00'   | 1      |
        | day        | NTZ, ANSI on                            | America/Los_Angeles | true  | TIMESTAMP_NTZ '2024-01-01 03:00:00'   | 1      |
        | dayofmonth | NTZ ignores the zone, LA                | America/Los_Angeles | false | TIMESTAMP_NTZ '2024-01-01 03:00:00'   | 1      |
        | dayofmonth | NTZ, ANSI on                            | America/Los_Angeles | true  | TIMESTAMP_NTZ '2024-01-01 03:00:00'   | 1      |
        | dayofyear  | NTZ ignores the zone, LA                | America/Los_Angeles | false | TIMESTAMP_NTZ '2024-01-01 03:00:00'   | 1      |
        | dayofyear  | NTZ, ANSI on                            | America/Los_Angeles | true  | TIMESTAMP_NTZ '2024-01-01 03:00:00'   | 1      |
        | dayofweek  | NTZ ignores the zone, LA                | America/Los_Angeles | false | TIMESTAMP_NTZ '2024-01-01 03:00:00'   | 2      |
        | dayofweek  | NTZ, ANSI on                            | America/Los_Angeles | true  | TIMESTAMP_NTZ '2024-01-01 03:00:00'   | 2      |
        | weekday    | NTZ ignores the zone, LA                | America/Los_Angeles | false | TIMESTAMP_NTZ '2024-01-01 03:00:00'   | 0      |
        | weekday    | NTZ, ANSI on                            | America/Los_Angeles | true  | TIMESTAMP_NTZ '2024-01-01 03:00:00'   | 0      |
        | weekofyear | TIMESTAMP crosses midnight, LA          | America/Los_Angeles | false | TIMESTAMP '2024-01-01 03:00:00+00:00' | 52     |
        | weekofyear | TIMESTAMP crosses midnight, LA, ANSI on | America/Los_Angeles | true  | TIMESTAMP '2024-01-01 03:00:00+00:00' | 52     |
        | weekofyear | TIMESTAMP same instant, UTC             | UTC                 | false | TIMESTAMP '2024-01-01 03:00:00+00:00' | 1      |

    # TODO(known gap): weekday of a TIMESTAMP that lands on a Sunday in the session zone gives -1 (same formula as the Edge group).
    #   Every row fails on `main` too (checked).
    @sail-bug
    Scenario Outline: <function>: Input type: <case> (known gap)
      Given config spark.sql.session.timeZone = <zone>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <function>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function | case                                    | zone                | ansi  | input                                 | result |
        | weekday  | TIMESTAMP crosses midnight, LA          | America/Los_Angeles | false | TIMESTAMP '2024-01-01 03:00:00+00:00' | 6      |
        | weekday  | TIMESTAMP crosses midnight, LA, ANSI on | America/Los_Angeles | true  | TIMESTAMP '2024-01-01 03:00:00+00:00' | 6      |

  Rule: String argument (result rows)

    Scenario Outline: <function>: String: <case> with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <function>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | case             | ansi  | input                 | result |
        | weekofyear | with a time part | false | '2024-03-17 10:00:00' | 11     |
        | weekofyear | with a time part | true  | '2024-03-17 10:00:00' | 11     |
        | weekofyear | T separator      | false | '2024-03-17T10:00:00' | 11     |
        | weekofyear | T separator      | true  | '2024-03-17T10:00:00' | 11     |
        | weekofyear | leap Feb 29      | false | '2024-02-29'          | 9      |
        | weekofyear | leap Feb 29      | true  | '2024-02-29'          | 9      |
        | day        | with a time part | false | '2024-03-17 10:00:00' | 17     |
        | dayofmonth | with a time part | false | '2024-03-17 10:00:00' | 17     |
        | dayofyear  | with a time part | false | '2024-03-17 10:00:00' | 77     |
        | dayofweek  | with a time part | false | '2024-03-17 10:00:00' | 1      |
        | day        | with a time part | true  | '2024-03-17 10:00:00' | 17     |
        | dayofmonth | with a time part | true  | '2024-03-17 10:00:00' | 17     |
        | dayofyear  | with a time part | true  | '2024-03-17 10:00:00' | 77     |
        | dayofweek  | with a time part | true  | '2024-03-17 10:00:00' | 1      |
        | day        | T separator      | false | '2024-03-17T10:00:00' | 17     |
        | dayofmonth | T separator      | false | '2024-03-17T10:00:00' | 17     |
        | dayofyear  | T separator      | false | '2024-03-17T10:00:00' | 77     |
        | dayofweek  | T separator      | false | '2024-03-17T10:00:00' | 1      |
        | day        | T separator      | true  | '2024-03-17T10:00:00' | 17     |
        | dayofmonth | T separator      | true  | '2024-03-17T10:00:00' | 17     |
        | dayofyear  | T separator      | true  | '2024-03-17T10:00:00' | 77     |
        | dayofweek  | T separator      | true  | '2024-03-17T10:00:00' | 1      |
        | day        | leap Feb 29      | false | '2024-02-29'          | 29     |
        | dayofmonth | leap Feb 29      | false | '2024-02-29'          | 29     |
        | dayofyear  | leap Feb 29      | false | '2024-02-29'          | 60     |
        | dayofweek  | leap Feb 29      | false | '2024-02-29'          | 5      |
        | weekday    | leap Feb 29      | false | '2024-02-29'          | 3      |
        | day        | leap Feb 29      | true  | '2024-02-29'          | 29     |
        | dayofmonth | leap Feb 29      | true  | '2024-02-29'          | 29     |
        | dayofyear  | leap Feb 29      | true  | '2024-02-29'          | 60     |
        | dayofweek  | leap Feb 29      | true  | '2024-02-29'          | 5      |
        | weekday    | leap Feb 29      | true  | '2024-02-29'          | 3      |

    # TODO(known gap): `date_part` reads the string itself and not with Spark's `stringToDate` (`SparkDateTimeUtils.scala:395-458`): it rejects the lenient `2024-3-7`, blanks around, a year or a year and month, trailing text after a space and a signed year, and it raises where Spark gives NULL.
    #   Every row fails on `main` too (checked).
    @sail-bug
    Scenario Outline: <function>: String: <case> with ANSI <ansi> (known gap)
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <function>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | case                        | ansi  | input                 | result |
        | day        | lenient 2024-3-7            | false | '2024-3-7'            | 7      |
        | dayofmonth | lenient 2024-3-7            | false | '2024-3-7'            | 7      |
        | dayofyear  | lenient 2024-3-7            | false | '2024-3-7'            | 67     |
        | dayofweek  | lenient 2024-3-7            | false | '2024-3-7'            | 5      |
        | weekday    | lenient 2024-3-7            | false | '2024-3-7'            | 3      |
        | weekofyear | lenient 2024-3-7            | false | '2024-3-7'            | 10     |
        | day        | lenient 2024-3-7            | true  | '2024-3-7'            | 7      |
        | dayofmonth | lenient 2024-3-7            | true  | '2024-3-7'            | 7      |
        | dayofyear  | lenient 2024-3-7            | true  | '2024-3-7'            | 67     |
        | dayofweek  | lenient 2024-3-7            | true  | '2024-3-7'            | 5      |
        | weekday    | lenient 2024-3-7            | true  | '2024-3-7'            | 3      |
        | weekofyear | lenient 2024-3-7            | true  | '2024-3-7'            | 10     |
        | weekday    | with a time part            | false | '2024-03-17 10:00:00' | 6      |
        | weekday    | with a time part            | true  | '2024-03-17 10:00:00' | 6      |
        | day        | padded blanks               | false | '  2024-03-17  '      | 17     |
        | dayofmonth | padded blanks               | false | '  2024-03-17  '      | 17     |
        | dayofyear  | padded blanks               | false | '  2024-03-17  '      | 77     |
        | dayofweek  | padded blanks               | false | '  2024-03-17  '      | 1      |
        | weekday    | padded blanks               | false | '  2024-03-17  '      | 6      |
        | weekofyear | padded blanks               | false | '  2024-03-17  '      | 11     |
        | day        | padded blanks               | true  | '  2024-03-17  '      | 17     |
        | dayofmonth | padded blanks               | true  | '  2024-03-17  '      | 17     |
        | dayofyear  | padded blanks               | true  | '  2024-03-17  '      | 77     |
        | dayofweek  | padded blanks               | true  | '  2024-03-17  '      | 1      |
        | weekday    | padded blanks               | true  | '  2024-03-17  '      | 6      |
        | weekofyear | padded blanks               | true  | '  2024-03-17  '      | 11     |
        | day        | year only                   | false | '2024'                | 1      |
        | dayofmonth | year only                   | false | '2024'                | 1      |
        | dayofyear  | year only                   | false | '2024'                | 1      |
        | dayofweek  | year only                   | false | '2024'                | 2      |
        | weekday    | year only                   | false | '2024'                | 0      |
        | weekofyear | year only                   | false | '2024'                | 1      |
        | day        | year only                   | true  | '2024'                | 1      |
        | dayofmonth | year only                   | true  | '2024'                | 1      |
        | dayofyear  | year only                   | true  | '2024'                | 1      |
        | dayofweek  | year only                   | true  | '2024'                | 2      |
        | weekday    | year only                   | true  | '2024'                | 0      |
        | weekofyear | year only                   | true  | '2024'                | 1      |
        | day        | year-month                  | false | '2024-03'             | 1      |
        | dayofmonth | year-month                  | false | '2024-03'             | 1      |
        | dayofyear  | year-month                  | false | '2024-03'             | 61     |
        | dayofweek  | year-month                  | false | '2024-03'             | 6      |
        | weekday    | year-month                  | false | '2024-03'             | 4      |
        | weekofyear | year-month                  | false | '2024-03'             | 9      |
        | day        | year-month                  | true  | '2024-03'             | 1      |
        | dayofmonth | year-month                  | true  | '2024-03'             | 1      |
        | dayofyear  | year-month                  | true  | '2024-03'             | 61     |
        | dayofweek  | year-month                  | true  | '2024-03'             | 6      |
        | weekday    | year-month                  | true  | '2024-03'             | 4      |
        | weekofyear | year-month                  | true  | '2024-03'             | 9      |
        | weekday    | T separator                 | false | '2024-03-17T10:00:00' | 6      |
        | weekday    | T separator                 | true  | '2024-03-17T10:00:00' | 6      |
        | day        | trailing text after a space | false | '2024-03-17 basura'   | 17     |
        | dayofmonth | trailing text after a space | false | '2024-03-17 basura'   | 17     |
        | dayofyear  | trailing text after a space | false | '2024-03-17 basura'   | 77     |
        | dayofweek  | trailing text after a space | false | '2024-03-17 basura'   | 1      |
        | weekday    | trailing text after a space | false | '2024-03-17 basura'   | 6      |
        | weekofyear | trailing text after a space | false | '2024-03-17 basura'   | 11     |
        | day        | trailing text after a space | true  | '2024-03-17 basura'   | 17     |
        | dayofmonth | trailing text after a space | true  | '2024-03-17 basura'   | 17     |
        | dayofyear  | trailing text after a space | true  | '2024-03-17 basura'   | 77     |
        | dayofweek  | trailing text after a space | true  | '2024-03-17 basura'   | 1      |
        | weekday    | trailing text after a space | true  | '2024-03-17 basura'   | 6      |
        | weekofyear | trailing text after a space | true  | '2024-03-17 basura'   | 11     |
        | day        | negative year               | false | '-0001-12-31'         | 31     |
        | dayofmonth | negative year               | false | '-0001-12-31'         | 31     |
        | dayofyear  | negative year               | false | '-0001-12-31'         | 365    |
        | dayofweek  | negative year               | false | '-0001-12-31'         | 6      |
        | weekday    | negative year               | false | '-0001-12-31'         | 4      |
        | weekofyear | negative year               | false | '-0001-12-31'         | 52     |
        | day        | negative year               | true  | '-0001-12-31'         | 31     |
        | dayofmonth | negative year               | true  | '-0001-12-31'         | 31     |
        | dayofyear  | negative year               | true  | '-0001-12-31'         | 365    |
        | dayofweek  | negative year               | true  | '-0001-12-31'         | 6      |
        | weekday    | negative year               | true  | '-0001-12-31'         | 4      |
        | weekofyear | negative year               | true  | '-0001-12-31'         | 52     |
        | day        | plus year                   | false | '+10000-01-01'        | 1      |
        | dayofmonth | plus year                   | false | '+10000-01-01'        | 1      |
        | dayofyear  | plus year                   | false | '+10000-01-01'        | 1      |
        | dayofweek  | plus year                   | false | '+10000-01-01'        | 7      |
        | weekday    | plus year                   | false | '+10000-01-01'        | 5      |
        | weekofyear | plus year                   | false | '+10000-01-01'        | 52     |
        | day        | plus year                   | true  | '+10000-01-01'        | 1      |
        | dayofmonth | plus year                   | true  | '+10000-01-01'        | 1      |
        | dayofyear  | plus year                   | true  | '+10000-01-01'        | 1      |
        | dayofweek  | plus year                   | true  | '+10000-01-01'        | 7      |
        | weekday    | plus year                   | true  | '+10000-01-01'        | 5      |
        | weekofyear | plus year                   | true  | '+10000-01-01'        | 52     |
        | day        | malformed                   | false | 'bad'                 | NULL   |
        | dayofmonth | malformed                   | false | 'bad'                 | NULL   |
        | dayofyear  | malformed                   | false | 'bad'                 | NULL   |
        | dayofweek  | malformed                   | false | 'bad'                 | NULL   |
        | weekday    | malformed                   | false | 'bad'                 | NULL   |
        | weekofyear | malformed                   | false | 'bad'                 | NULL   |
        | day        | empty                       | false | ''                    | NULL   |
        | dayofmonth | empty                       | false | ''                    | NULL   |
        | dayofyear  | empty                       | false | ''                    | NULL   |
        | dayofweek  | empty                       | false | ''                    | NULL   |
        | weekday    | empty                       | false | ''                    | NULL   |
        | weekofyear | empty                       | false | ''                    | NULL   |
        | day        | month 13                    | false | '2024-13-01'          | NULL   |
        | dayofmonth | month 13                    | false | '2024-13-01'          | NULL   |
        | dayofyear  | month 13                    | false | '2024-13-01'          | NULL   |
        | dayofweek  | month 13                    | false | '2024-13-01'          | NULL   |
        | weekday    | month 13                    | false | '2024-13-01'          | NULL   |
        | weekofyear | month 13                    | false | '2024-13-01'          | NULL   |
        | day        | non-leap Feb 29             | false | '2023-02-29'          | NULL   |
        | dayofmonth | non-leap Feb 29             | false | '2023-02-29'          | NULL   |
        | dayofyear  | non-leap Feb 29             | false | '2023-02-29'          | NULL   |
        | dayofweek  | non-leap Feb 29             | false | '2023-02-29'          | NULL   |
        | weekday    | non-leap Feb 29             | false | '2023-02-29'          | NULL   |
        | weekofyear | non-leap Feb 29             | false | '2023-02-29'          | NULL   |
        | day        | slashes                     | false | '2024/03/17'          | NULL   |
        | dayofmonth | slashes                     | false | '2024/03/17'          | NULL   |
        | dayofyear  | slashes                     | false | '2024/03/17'          | NULL   |
        | dayofweek  | slashes                     | false | '2024/03/17'          | NULL   |
        | weekday    | slashes                     | false | '2024/03/17'          | NULL   |
        | weekofyear | slashes                     | false | '2024/03/17'          | NULL   |
        | day        | trailing letters            | false | '2024-03-17abc'       | NULL   |
        | dayofmonth | trailing letters            | false | '2024-03-17abc'       | NULL   |
        | dayofyear  | trailing letters            | false | '2024-03-17abc'       | NULL   |
        | dayofweek  | trailing letters            | false | '2024-03-17abc'       | NULL   |
        | weekday    | trailing letters            | false | '2024-03-17abc'       | NULL   |
        | weekofyear | trailing letters            | false | '2024-03-17abc'       | NULL   |
        | day        | 3-digit year                | false | '999-01-01'           | NULL   |
        | dayofmonth | 3-digit year                | false | '999-01-01'           | NULL   |
        | dayofyear  | 3-digit year                | false | '999-01-01'           | NULL   |
        | dayofweek  | 3-digit year                | false | '999-01-01'           | NULL   |
        | weekday    | 3-digit year                | false | '999-01-01'           | NULL   |
        | weekofyear | 3-digit year                | false | '999-01-01'           | NULL   |
        | day        | 8-digit year                | false | '12345678-01-01'      | NULL   |
        | dayofmonth | 8-digit year                | false | '12345678-01-01'      | NULL   |
        | dayofyear  | 8-digit year                | false | '12345678-01-01'      | NULL   |
        | dayofweek  | 8-digit year                | false | '12345678-01-01'      | NULL   |
        | weekday    | 8-digit year                | false | '12345678-01-01'      | NULL   |
        | weekofyear | 8-digit year                | false | '12345678-01-01'      | NULL   |

  Rule: String argument (errors, ANSI on)

    # TODO(known gap): a malformed string raises the text of the kernel; Spark raises `CAST_INVALID_INPUT` with ANSI on and gives NULL with ANSI off.
    #   Every row fails on `main` too (checked).
    @sail-bug
    Scenario Outline: <function>: String error: <case> (known gap)
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <function>(<input>) AS result
        """
      Then query error <class>.*The value <input> of the type "STRING" cannot be cast to "DATE"

      Examples:
        | function   | case             | input            | class              |
        | day        | malformed        | 'bad'            | CAST_INVALID_INPUT |
        | dayofmonth | malformed        | 'bad'            | CAST_INVALID_INPUT |
        | dayofyear  | malformed        | 'bad'            | CAST_INVALID_INPUT |
        | dayofweek  | malformed        | 'bad'            | CAST_INVALID_INPUT |
        | weekday    | malformed        | 'bad'            | CAST_INVALID_INPUT |
        | weekofyear | malformed        | 'bad'            | CAST_INVALID_INPUT |
        | day        | empty            | ''               | CAST_INVALID_INPUT |
        | dayofmonth | empty            | ''               | CAST_INVALID_INPUT |
        | dayofyear  | empty            | ''               | CAST_INVALID_INPUT |
        | dayofweek  | empty            | ''               | CAST_INVALID_INPUT |
        | weekday    | empty            | ''               | CAST_INVALID_INPUT |
        | weekofyear | empty            | ''               | CAST_INVALID_INPUT |
        | day        | month 13         | '2024-13-01'     | CAST_INVALID_INPUT |
        | dayofmonth | month 13         | '2024-13-01'     | CAST_INVALID_INPUT |
        | dayofyear  | month 13         | '2024-13-01'     | CAST_INVALID_INPUT |
        | dayofweek  | month 13         | '2024-13-01'     | CAST_INVALID_INPUT |
        | weekday    | month 13         | '2024-13-01'     | CAST_INVALID_INPUT |
        | weekofyear | month 13         | '2024-13-01'     | CAST_INVALID_INPUT |
        | day        | non-leap Feb 29  | '2023-02-29'     | CAST_INVALID_INPUT |
        | dayofmonth | non-leap Feb 29  | '2023-02-29'     | CAST_INVALID_INPUT |
        | dayofyear  | non-leap Feb 29  | '2023-02-29'     | CAST_INVALID_INPUT |
        | dayofweek  | non-leap Feb 29  | '2023-02-29'     | CAST_INVALID_INPUT |
        | weekday    | non-leap Feb 29  | '2023-02-29'     | CAST_INVALID_INPUT |
        | weekofyear | non-leap Feb 29  | '2023-02-29'     | CAST_INVALID_INPUT |
        | day        | slashes          | '2024/03/17'     | CAST_INVALID_INPUT |
        | dayofmonth | slashes          | '2024/03/17'     | CAST_INVALID_INPUT |
        | dayofyear  | slashes          | '2024/03/17'     | CAST_INVALID_INPUT |
        | dayofweek  | slashes          | '2024/03/17'     | CAST_INVALID_INPUT |
        | weekday    | slashes          | '2024/03/17'     | CAST_INVALID_INPUT |
        | weekofyear | slashes          | '2024/03/17'     | CAST_INVALID_INPUT |
        | day        | trailing letters | '2024-03-17abc'  | CAST_INVALID_INPUT |
        | dayofmonth | trailing letters | '2024-03-17abc'  | CAST_INVALID_INPUT |
        | dayofyear  | trailing letters | '2024-03-17abc'  | CAST_INVALID_INPUT |
        | dayofweek  | trailing letters | '2024-03-17abc'  | CAST_INVALID_INPUT |
        | weekday    | trailing letters | '2024-03-17abc'  | CAST_INVALID_INPUT |
        | weekofyear | trailing letters | '2024-03-17abc'  | CAST_INVALID_INPUT |
        | day        | 3-digit year     | '999-01-01'      | CAST_INVALID_INPUT |
        | dayofmonth | 3-digit year     | '999-01-01'      | CAST_INVALID_INPUT |
        | dayofyear  | 3-digit year     | '999-01-01'      | CAST_INVALID_INPUT |
        | dayofweek  | 3-digit year     | '999-01-01'      | CAST_INVALID_INPUT |
        | weekday    | 3-digit year     | '999-01-01'      | CAST_INVALID_INPUT |
        | weekofyear | 3-digit year     | '999-01-01'      | CAST_INVALID_INPUT |
        | day        | 8-digit year     | '12345678-01-01' | CAST_INVALID_INPUT |
        | dayofmonth | 8-digit year     | '12345678-01-01' | CAST_INVALID_INPUT |
        | dayofyear  | 8-digit year     | '12345678-01-01' | CAST_INVALID_INPUT |
        | dayofweek  | 8-digit year     | '12345678-01-01' | CAST_INVALID_INPUT |
        | weekday    | 8-digit year     | '12345678-01-01' | CAST_INVALID_INPUT |
        | weekofyear | 8-digit year     | '12345678-01-01' | CAST_INVALID_INPUT |

  Rule: NULL handling

    Scenario Outline: <function>: NULL: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <function>(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | function   | case                       | ansi  | input                   |
        | day        | typed NULL DATE            | false | CAST(NULL AS DATE)      |
        | dayofmonth | typed NULL DATE            | false | CAST(NULL AS DATE)      |
        | dayofyear  | typed NULL DATE            | false | CAST(NULL AS DATE)      |
        | dayofweek  | typed NULL DATE            | false | CAST(NULL AS DATE)      |
        | weekday    | typed NULL DATE            | false | CAST(NULL AS DATE)      |
        | weekofyear | typed NULL DATE            | false | CAST(NULL AS DATE)      |
        | weekofyear | untyped NULL               | false | NULL                    |
        | day        | typed NULL STRING          | false | CAST(NULL AS STRING)    |
        | dayofmonth | typed NULL STRING          | false | CAST(NULL AS STRING)    |
        | dayofyear  | typed NULL STRING          | false | CAST(NULL AS STRING)    |
        | dayofweek  | typed NULL STRING          | false | CAST(NULL AS STRING)    |
        | weekday    | typed NULL STRING          | false | CAST(NULL AS STRING)    |
        | weekofyear | typed NULL STRING          | false | CAST(NULL AS STRING)    |
        | day        | typed NULL TIMESTAMP       | false | CAST(NULL AS TIMESTAMP) |
        | dayofmonth | typed NULL TIMESTAMP       | false | CAST(NULL AS TIMESTAMP) |
        | dayofyear  | typed NULL TIMESTAMP       | false | CAST(NULL AS TIMESTAMP) |
        | dayofweek  | typed NULL TIMESTAMP       | false | CAST(NULL AS TIMESTAMP) |
        | weekday    | typed NULL TIMESTAMP       | false | CAST(NULL AS TIMESTAMP) |
        | weekofyear | typed NULL TIMESTAMP       | false | CAST(NULL AS TIMESTAMP) |
        | day        | typed NULL STRING, ANSI on | true  | CAST(NULL AS STRING)    |
        | dayofmonth | typed NULL STRING, ANSI on | true  | CAST(NULL AS STRING)    |
        | dayofyear  | typed NULL STRING, ANSI on | true  | CAST(NULL AS STRING)    |
        | dayofweek  | typed NULL STRING, ANSI on | true  | CAST(NULL AS STRING)    |
        | weekday    | typed NULL STRING, ANSI on | true  | CAST(NULL AS STRING)    |
        | weekofyear | typed NULL STRING, ANSI on | true  | CAST(NULL AS STRING)    |

    # TODO(known gap): month, day, dayofmonth, quarter, dayofyear, dayofweek and weekday of an untyped NULL raise a compute error; Spark gives NULL.
    #   Every row fails on `main` too (checked).
    @sail-bug
    Scenario Outline: <function>: NULL: <case> (known gap)
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <function>(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | function   | case         | ansi  | input |
        | day        | untyped NULL | false | NULL  |
        | dayofmonth | untyped NULL | false | NULL  |
        | dayofyear  | untyped NULL | false | NULL  |
        | dayofweek  | untyped NULL | false | NULL  |
        | weekday    | untyped NULL | false | NULL  |

  Rule: Special datetime strings

    # TODO(known gap): Spark folds a foldable string such as `'epoch'` or `'today'` into a date (`finishAnalysis.scala:179-190`); Sail does not know the special words.
    #   Every row fails on `main` too (checked).
    @sail-bug
    Scenario Outline: <function>: Special word: <case> (known gap)
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <function>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | case                 | ansi  | input                | result |
        | day        | literal 'epoch'      | false | 'epoch'              | 1      |
        | dayofmonth | literal 'epoch'      | false | 'epoch'              | 1      |
        | dayofyear  | literal 'epoch'      | false | 'epoch'              | 1      |
        | dayofweek  | literal 'epoch'      | false | 'epoch'              | 5      |
        | weekday    | literal 'epoch'      | false | 'epoch'              | 3      |
        | weekofyear | literal 'epoch'      | false | 'epoch'              | 1      |
        | day        | literal 'epoch'      | true  | 'epoch'              | 1      |
        | dayofmonth | literal 'epoch'      | true  | 'epoch'              | 1      |
        | dayofyear  | literal 'epoch'      | true  | 'epoch'              | 1      |
        | dayofweek  | literal 'epoch'      | true  | 'epoch'              | 5      |
        | weekday    | literal 'epoch'      | true  | 'epoch'              | 3      |
        | weekofyear | literal 'epoch'      | true  | 'epoch'              | 1      |
        | day        | literal 'EPOCH'      | false | 'EPOCH'              | 1      |
        | dayofmonth | literal 'EPOCH'      | false | 'EPOCH'              | 1      |
        | dayofyear  | literal 'EPOCH'      | false | 'EPOCH'              | 1      |
        | dayofweek  | literal 'EPOCH'      | false | 'EPOCH'              | 5      |
        | weekday    | literal 'EPOCH'      | false | 'EPOCH'              | 3      |
        | weekofyear | literal 'EPOCH'      | false | 'EPOCH'              | 1      |
        | day        | literal 'EPOCH'      | true  | 'EPOCH'              | 1      |
        | dayofmonth | literal 'EPOCH'      | true  | 'EPOCH'              | 1      |
        | dayofyear  | literal 'EPOCH'      | true  | 'EPOCH'              | 1      |
        | dayofweek  | literal 'EPOCH'      | true  | 'EPOCH'              | 5      |
        | weekday    | literal 'EPOCH'      | true  | 'EPOCH'              | 3      |
        | weekofyear | literal 'EPOCH'      | true  | 'EPOCH'              | 1      |
        | day        | literal ' epoch '    | false | ' epoch '            | 1      |
        | dayofmonth | literal ' epoch '    | false | ' epoch '            | 1      |
        | dayofyear  | literal ' epoch '    | false | ' epoch '            | 1      |
        | dayofweek  | literal ' epoch '    | false | ' epoch '            | 5      |
        | weekday    | literal ' epoch '    | false | ' epoch '            | 3      |
        | weekofyear | literal ' epoch '    | false | ' epoch '            | 1      |
        | day        | literal ' epoch '    | true  | ' epoch '            | 1      |
        | dayofmonth | literal ' epoch '    | true  | ' epoch '            | 1      |
        | dayofyear  | literal ' epoch '    | true  | ' epoch '            | 1      |
        | dayofweek  | literal ' epoch '    | true  | ' epoch '            | 5      |
        | weekday    | literal ' epoch '    | true  | ' epoch '            | 3      |
        | weekofyear | literal ' epoch '    | true  | ' epoch '            | 1      |
        | day        | literal 'epoch UTC'  | false | 'epoch UTC'          | 1      |
        | dayofmonth | literal 'epoch UTC'  | false | 'epoch UTC'          | 1      |
        | dayofyear  | literal 'epoch UTC'  | false | 'epoch UTC'          | 1      |
        | dayofweek  | literal 'epoch UTC'  | false | 'epoch UTC'          | 5      |
        | weekday    | literal 'epoch UTC'  | false | 'epoch UTC'          | 3      |
        | weekofyear | literal 'epoch UTC'  | false | 'epoch UTC'          | 1      |
        | day        | literal 'epoch UTC'  | true  | 'epoch UTC'          | 1      |
        | dayofmonth | literal 'epoch UTC'  | true  | 'epoch UTC'          | 1      |
        | dayofyear  | literal 'epoch UTC'  | true  | 'epoch UTC'          | 1      |
        | dayofweek  | literal 'epoch UTC'  | true  | 'epoch UTC'          | 5      |
        | weekday    | literal 'epoch UTC'  | true  | 'epoch UTC'          | 3      |
        | weekofyear | literal 'epoch UTC'  | true  | 'epoch UTC'          | 1      |
        | day        | foldable concat('ep' | false | concat('ep','och')   | 1      |
        | dayofmonth | foldable concat('ep' | false | concat('ep','och')   | 1      |
        | dayofyear  | foldable concat('ep' | false | concat('ep','och')   | 1      |
        | dayofweek  | foldable concat('ep' | false | concat('ep','och')   | 5      |
        | weekday    | foldable concat('ep' | false | concat('ep','och')   | 3      |
        | weekofyear | foldable concat('ep' | false | concat('ep','och')   | 1      |
        | day        | nullif is not folded | false | nullif('epoch', 'x') | NULL   |
        | dayofmonth | nullif is not folded | false | nullif('epoch', 'x') | NULL   |
        | dayofyear  | nullif is not folded | false | nullif('epoch', 'x') | NULL   |
        | dayofweek  | nullif is not folded | false | nullif('epoch', 'x') | NULL   |
        | weekday    | nullif is not folded | false | nullif('epoch', 'x') | NULL   |
        | weekofyear | nullif is not folded | false | nullif('epoch', 'x') | NULL   |

    # TODO(known gap): Spark folds a string literal into a date only when it is foldable; a column holding `epoch` is not special, so
    #   it is NULL. Sail raises a parse error (the shared `date_part` kernel reads the string itself).
    @sail-bug
    Scenario Outline: <function>: Special word: a column holding 'epoch' is not special, with ANSI off
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <function>(s) AS result FROM VALUES ('epoch') AS t(s)
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | function   |
        | day        |
        | dayofmonth |
        | dayofyear  |
        | dayofweek  |
        | weekday    |
        | weekofyear |

    # TODO(known gap): the error class of a malformed string is `CAST_INVALID_INPUT` in Spark; Sail raises the text of the kernel.
    @sail-bug
    Scenario Outline: <function>: Special word: a column holding 'epoch' raises with ANSI on
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <function>(s) AS result FROM VALUES ('epoch') AS t(s)
        """
      Then query error .*CAST_INVALID_INPUT.*

      Examples:
        | function   |
        | day        |
        | dayofmonth |
        | dayofyear  |
        | dayofweek  |
        | weekday    |
        | weekofyear |

  Rule: Range

    Scenario Outline: <function>: Range: <case>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT <function>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | case                            | zone | input                                     | result |
        | day        | DATE min 0001-01-01             | UTC  | DATE'0001-01-01'                          | 1      |
        | dayofmonth | DATE min 0001-01-01             | UTC  | DATE'0001-01-01'                          | 1      |
        | dayofyear  | DATE min 0001-01-01             | UTC  | DATE'0001-01-01'                          | 1      |
        | dayofweek  | DATE min 0001-01-01             | UTC  | DATE'0001-01-01'                          | 2      |
        | weekday    | DATE min 0001-01-01             | UTC  | DATE'0001-01-01'                          | 0      |
        | day        | DATE max 9999-12-31             | UTC  | DATE'9999-12-31'                          | 31     |
        | dayofmonth | DATE max 9999-12-31             | UTC  | DATE'9999-12-31'                          | 31     |
        | dayofyear  | DATE max 9999-12-31             | UTC  | DATE'9999-12-31'                          | 365    |
        | dayofweek  | DATE max 9999-12-31             | UTC  | DATE'9999-12-31'                          | 6      |
        | weekday    | DATE max 9999-12-31             | UTC  | DATE'9999-12-31'                          | 4      |
        | day        | year 0000-01-01                 | UTC  | date_from_unix_date(-719528)              | 1      |
        | dayofmonth | year 0000-01-01                 | UTC  | date_from_unix_date(-719528)              | 1      |
        | dayofyear  | year 0000-01-01                 | UTC  | date_from_unix_date(-719528)              | 1      |
        | dayofweek  | year 0000-01-01                 | UTC  | date_from_unix_date(-719528)              | 7      |
        | weekday    | year 0000-01-01                 | UTC  | date_from_unix_date(-719528)              | 5      |
        | day        | year -1 last day                | UTC  | date_from_unix_date(-719529)              | 31     |
        | dayofmonth | year -1 last day                | UTC  | date_from_unix_date(-719529)              | 31     |
        | dayofyear  | year -1 last day                | UTC  | date_from_unix_date(-719529)              | 365    |
        | dayofweek  | year -1 last day                | UTC  | date_from_unix_date(-719529)              | 6      |
        | weekday    | year -1 last day                | UTC  | date_from_unix_date(-719529)              | 4      |
        | day        | Julian/Gregorian gap 1582-10-04 | UTC  | DATE'1582-10-04'                          | 4      |
        | dayofmonth | Julian/Gregorian gap 1582-10-04 | UTC  | DATE'1582-10-04'                          | 4      |
        | dayofyear  | Julian/Gregorian gap 1582-10-04 | UTC  | DATE'1582-10-04'                          | 277    |
        | dayofweek  | Julian/Gregorian gap 1582-10-04 | UTC  | DATE'1582-10-04'                          | 2      |
        | weekday    | Julian/Gregorian gap 1582-10-04 | UTC  | DATE'1582-10-04'                          | 0      |
        | day        | 1582-10-15                      | UTC  | DATE'1582-10-15'                          | 15     |
        | dayofmonth | 1582-10-15                      | UTC  | DATE'1582-10-15'                          | 15     |
        | dayofyear  | 1582-10-15                      | UTC  | DATE'1582-10-15'                          | 288    |
        | dayofweek  | 1582-10-15                      | UTC  | DATE'1582-10-15'                          | 6      |
        | weekday    | 1582-10-15                      | UTC  | DATE'1582-10-15'                          | 4      |
        | day        | epoch day 0                     | UTC  | date_from_unix_date(0)                    | 1      |
        | dayofmonth | epoch day 0                     | UTC  | date_from_unix_date(0)                    | 1      |
        | dayofyear  | epoch day 0                     | UTC  | date_from_unix_date(0)                    | 1      |
        | dayofweek  | epoch day 0                     | UTC  | date_from_unix_date(0)                    | 5      |
        | weekday    | epoch day 0                     | UTC  | date_from_unix_date(0)                    | 3      |
        | day        | day -1                          | UTC  | date_from_unix_date(-1)                   | 31     |
        | dayofmonth | day -1                          | UTC  | date_from_unix_date(-1)                   | 31     |
        | dayofyear  | day -1                          | UTC  | date_from_unix_date(-1)                   | 365    |
        | dayofweek  | day -1                          | UTC  | date_from_unix_date(-1)                   | 4      |
        | weekday    | day -1                          | UTC  | date_from_unix_date(-1)                   | 2      |
        | day        | TIMESTAMP 9999-12-31 23:59:59   | UTC  | timestamp_seconds(253402300799)           | 31     |
        | dayofmonth | TIMESTAMP 9999-12-31 23:59:59   | UTC  | timestamp_seconds(253402300799)           | 31     |
        | dayofyear  | TIMESTAMP 9999-12-31 23:59:59   | UTC  | timestamp_seconds(253402300799)           | 365    |
        | dayofweek  | TIMESTAMP 9999-12-31 23:59:59   | UTC  | timestamp_seconds(253402300799)           | 6      |
        | weekday    | TIMESTAMP 9999-12-31 23:59:59   | UTC  | timestamp_seconds(253402300799)           | 4      |
        | weekofyear | NTZ 0001-01-01                  | UTC  | TIMESTAMP_NTZ'0001-01-01 00:00:00'        | 1      |
        | weekofyear | NTZ 9999-12-31 23:59:59.999999  | UTC  | TIMESTAMP_NTZ'9999-12-31 23:59:59.999999' | 52     |
        | weekofyear | DATE min 0001-01-01             | UTC  | DATE'0001-01-01'                          | 1      |
        | weekofyear | DATE max 9999-12-31             | UTC  | DATE'9999-12-31'                          | 52     |
        | weekofyear | year 0000-01-01                 | UTC  | date_from_unix_date(-719528)              | 52     |
        | weekofyear | year -1 last day                | UTC  | date_from_unix_date(-719529)              | 52     |
        | weekofyear | Julian/Gregorian gap 1582-10-04 | UTC  | DATE'1582-10-04'                          | 40     |
        | weekofyear | 1582-10-15                      | UTC  | DATE'1582-10-15'                          | 41     |
        | weekofyear | epoch day 0                     | UTC  | date_from_unix_date(0)                    | 1      |
        | weekofyear | day -1                          | UTC  | date_from_unix_date(-1)                   | 1      |
        | day        | NTZ 0001-01-01                  | UTC  | TIMESTAMP_NTZ'0001-01-01 00:00:00'        | 1      |
        | dayofmonth | NTZ 0001-01-01                  | UTC  | TIMESTAMP_NTZ'0001-01-01 00:00:00'        | 1      |
        | dayofyear  | NTZ 0001-01-01                  | UTC  | TIMESTAMP_NTZ'0001-01-01 00:00:00'        | 1      |
        | dayofweek  | NTZ 0001-01-01                  | UTC  | TIMESTAMP_NTZ'0001-01-01 00:00:00'        | 2      |
        | weekday    | NTZ 0001-01-01                  | UTC  | TIMESTAMP_NTZ'0001-01-01 00:00:00'        | 0      |
        | day        | NTZ 9999-12-31 23:59:59.999999  | UTC  | TIMESTAMP_NTZ'9999-12-31 23:59:59.999999' | 31     |
        | dayofmonth | NTZ 9999-12-31 23:59:59.999999  | UTC  | TIMESTAMP_NTZ'9999-12-31 23:59:59.999999' | 31     |
        | dayofyear  | NTZ 9999-12-31 23:59:59.999999  | UTC  | TIMESTAMP_NTZ'9999-12-31 23:59:59.999999' | 365    |
        | dayofweek  | NTZ 9999-12-31 23:59:59.999999  | UTC  | TIMESTAMP_NTZ'9999-12-31 23:59:59.999999' | 6      |
        | weekday    | NTZ 9999-12-31 23:59:59.999999  | UTC  | TIMESTAMP_NTZ'9999-12-31 23:59:59.999999' | 4      |

    # TODO(known gap): the extremes of DATE and TIMESTAMP give NULL or an error in Sail where Spark has a value.
    #   Every row fails on `main` too (checked).
    @sail-bug
    Scenario Outline: <function>: Range: <case> (known gap)
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT <function>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | case                                        | zone                | input                                  | result |
        | day        | int min days                                | UTC                 | date_from_unix_date(-2147483648)       | 23     |
        | dayofmonth | int min days                                | UTC                 | date_from_unix_date(-2147483648)       | 23     |
        | dayofyear  | int min days                                | UTC                 | date_from_unix_date(-2147483648)       | 174    |
        | dayofweek  | int min days                                | UTC                 | date_from_unix_date(-2147483648)       | 3      |
        | weekday    | int min days                                | UTC                 | date_from_unix_date(-2147483648)       | 1      |
        | weekofyear | int min days                                | UTC                 | date_from_unix_date(-2147483648)       | 26     |
        | day        | int max days                                | UTC                 | date_from_unix_date(2147483647)        | 11     |
        | dayofmonth | int max days                                | UTC                 | date_from_unix_date(2147483647)        | 11     |
        | dayofyear  | int max days                                | UTC                 | date_from_unix_date(2147483647)        | 193    |
        | dayofweek  | int max days                                | UTC                 | date_from_unix_date(2147483647)        | 6      |
        | weekday    | int max days                                | UTC                 | date_from_unix_date(2147483647)        | 4      |
        | weekofyear | int max days                                | UTC                 | date_from_unix_date(2147483647)        | 28     |
        | day        | TIMESTAMP micros max                        | UTC                 | timestamp_micros(9223372036854775807)  | 10     |
        | dayofmonth | TIMESTAMP micros max                        | UTC                 | timestamp_micros(9223372036854775807)  | 10     |
        | dayofyear  | TIMESTAMP micros max                        | UTC                 | timestamp_micros(9223372036854775807)  | 10     |
        | dayofweek  | TIMESTAMP micros max                        | UTC                 | timestamp_micros(9223372036854775807)  | 1      |
        | weekday    | TIMESTAMP micros max                        | UTC                 | timestamp_micros(9223372036854775807)  | 6      |
        | weekofyear | TIMESTAMP micros max                        | UTC                 | timestamp_micros(9223372036854775807)  | 1      |
        | day        | TIMESTAMP micros min                        | UTC                 | timestamp_micros(-9223372036854775808) | 21     |
        | dayofmonth | TIMESTAMP micros min                        | UTC                 | timestamp_micros(-9223372036854775808) | 21     |
        | dayofyear  | TIMESTAMP micros min                        | UTC                 | timestamp_micros(-9223372036854775808) | 356    |
        | dayofweek  | TIMESTAMP micros min                        | UTC                 | timestamp_micros(-9223372036854775808) | 1      |
        | weekday    | TIMESTAMP micros min                        | UTC                 | timestamp_micros(-9223372036854775808) | 6      |
        | weekofyear | TIMESTAMP micros min                        | UTC                 | timestamp_micros(-9223372036854775808) | 51     |
        | weekofyear | TIMESTAMP 9999-12-31 23:59:59               | UTC                 | timestamp_seconds(253402300799)        | 52     |
        | day        | TIMESTAMP 0001-01-01 00:00:00Z, LA (year 0) | America/Los_Angeles | timestamp_seconds(-62135596800)        | 31     |
        | dayofmonth | TIMESTAMP 0001-01-01 00:00:00Z, LA (year 0) | America/Los_Angeles | timestamp_seconds(-62135596800)        | 31     |
        | dayofyear  | TIMESTAMP 0001-01-01 00:00:00Z, LA (year 0) | America/Los_Angeles | timestamp_seconds(-62135596800)        | 366    |
        | dayofweek  | TIMESTAMP 0001-01-01 00:00:00Z, LA (year 0) | America/Los_Angeles | timestamp_seconds(-62135596800)        | 1      |
        | weekday    | TIMESTAMP 0001-01-01 00:00:00Z, LA (year 0) | America/Los_Angeles | timestamp_seconds(-62135596800)        | 6      |
        | weekofyear | TIMESTAMP 0001-01-01 00:00:00Z, LA (year 0) | America/Los_Angeles | timestamp_seconds(-62135596800)        | 52     |
        | day        | TIMESTAMP micros max, LA                    | America/Los_Angeles | timestamp_micros(9223372036854775807)  | 9      |
        | dayofmonth | TIMESTAMP micros max, LA                    | America/Los_Angeles | timestamp_micros(9223372036854775807)  | 9      |
        | dayofyear  | TIMESTAMP micros max, LA                    | America/Los_Angeles | timestamp_micros(9223372036854775807)  | 9      |
        | dayofweek  | TIMESTAMP micros max, LA                    | America/Los_Angeles | timestamp_micros(9223372036854775807)  | 7      |
        | weekday    | TIMESTAMP micros max, LA                    | America/Los_Angeles | timestamp_micros(9223372036854775807)  | 5      |
        | weekofyear | TIMESTAMP micros max, LA                    | America/Los_Angeles | timestamp_micros(9223372036854775807)  | 1      |
        | day        | TIMESTAMP micros min, LA                    | America/Los_Angeles | timestamp_micros(-9223372036854775808) | 21     |
        | dayofmonth | TIMESTAMP micros min, LA                    | America/Los_Angeles | timestamp_micros(-9223372036854775808) | 21     |
        | dayofyear  | TIMESTAMP micros min, LA                    | America/Los_Angeles | timestamp_micros(-9223372036854775808) | 356    |
        | dayofweek  | TIMESTAMP micros min, LA                    | America/Los_Angeles | timestamp_micros(-9223372036854775808) | 1      |
        | weekday    | TIMESTAMP micros min, LA                    | America/Los_Angeles | timestamp_micros(-9223372036854775808) | 6      |
        | weekofyear | TIMESTAMP micros min, LA                    | America/Los_Angeles | timestamp_micros(-9223372036854775808) | 51     |

  Rule: Session time zone

    Scenario Outline: <function>: Zone <zone>: <case>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT <function>(timestamp_seconds(<seconds>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | zone                | case                  | seconds    | result |
        | day        | America/Los_Angeles | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofmonth | America/Los_Angeles | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofyear  | America/Los_Angeles | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofweek  | America/Los_Angeles | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | weekday    | America/Los_Angeles | 19:00Z Mon 2024-01-01 | 1704135600 | 0      |
        | day        | America/Los_Angeles | Sun 2023-12-31 23:30Z | 1704065400 | 31     |
        | dayofmonth | America/Los_Angeles | Sun 2023-12-31 23:30Z | 1704065400 | 31     |
        | dayofyear  | America/Los_Angeles | Sun 2023-12-31 23:30Z | 1704065400 | 365    |
        | dayofweek  | America/Los_Angeles | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | day        | PST                 | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofmonth | PST                 | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofyear  | PST                 | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofweek  | PST                 | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | weekday    | PST                 | 19:00Z Mon 2024-01-01 | 1704135600 | 0      |
        | day        | PST                 | Sun 2023-12-31 23:30Z | 1704065400 | 31     |
        | dayofmonth | PST                 | Sun 2023-12-31 23:30Z | 1704065400 | 31     |
        | dayofyear  | PST                 | Sun 2023-12-31 23:30Z | 1704065400 | 365    |
        | dayofweek  | PST                 | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | day        | EST                 | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofmonth | EST                 | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofyear  | EST                 | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofweek  | EST                 | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | weekday    | EST                 | 19:00Z Mon 2024-01-01 | 1704135600 | 0      |
        | day        | EST                 | Sun 2023-12-31 23:30Z | 1704065400 | 31     |
        | dayofmonth | EST                 | Sun 2023-12-31 23:30Z | 1704065400 | 31     |
        | dayofyear  | EST                 | Sun 2023-12-31 23:30Z | 1704065400 | 365    |
        | dayofweek  | EST                 | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | day        | Asia/Kolkata        | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofmonth | Asia/Kolkata        | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofyear  | Asia/Kolkata        | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofweek  | Asia/Kolkata        | 03:00Z Mon 2024-01-01 | 1704078000 | 2      |
        | weekday    | Asia/Kolkata        | 03:00Z Mon 2024-01-01 | 1704078000 | 0      |
        | day        | +05:30              | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofmonth | +05:30              | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofyear  | +05:30              | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofweek  | +05:30              | 03:00Z Mon 2024-01-01 | 1704078000 | 2      |
        | weekday    | +05:30              | 03:00Z Mon 2024-01-01 | 1704078000 | 0      |
        | day        | GMT+8               | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofmonth | GMT+8               | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofyear  | GMT+8               | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofweek  | GMT+8               | 03:00Z Mon 2024-01-01 | 1704078000 | 2      |
        | weekday    | GMT+8               | 03:00Z Mon 2024-01-01 | 1704078000 | 0      |
        | day        | UTC+8               | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofmonth | UTC+8               | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofyear  | UTC+8               | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofweek  | UTC+8               | 03:00Z Mon 2024-01-01 | 1704078000 | 2      |
        | weekday    | UTC+8               | 03:00Z Mon 2024-01-01 | 1704078000 | 0      |
        | day        | Z                   | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofmonth | Z                   | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofyear  | Z                   | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofweek  | Z                   | 03:00Z Mon 2024-01-01 | 1704078000 | 2      |
        | weekday    | Z                   | 03:00Z Mon 2024-01-01 | 1704078000 | 0      |
        | day        | Z                   | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofmonth | Z                   | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofyear  | Z                   | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofweek  | Z                   | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | weekday    | Z                   | 19:00Z Mon 2024-01-01 | 1704135600 | 0      |
        | day        | Z                   | Sun 2023-12-31 23:30Z | 1704065400 | 31     |
        | dayofmonth | Z                   | Sun 2023-12-31 23:30Z | 1704065400 | 31     |
        | dayofyear  | Z                   | Sun 2023-12-31 23:30Z | 1704065400 | 365    |
        | dayofweek  | Z                   | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | day        | Etc/GMT+12          | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofmonth | Etc/GMT+12          | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofyear  | Etc/GMT+12          | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofweek  | Etc/GMT+12          | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | weekday    | Etc/GMT+12          | 19:00Z Mon 2024-01-01 | 1704135600 | 0      |
        | day        | Etc/GMT+12          | Sun 2023-12-31 23:30Z | 1704065400 | 31     |
        | dayofmonth | Etc/GMT+12          | Sun 2023-12-31 23:30Z | 1704065400 | 31     |
        | dayofyear  | Etc/GMT+12          | Sun 2023-12-31 23:30Z | 1704065400 | 365    |
        | dayofweek  | Etc/GMT+12          | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | day        | Pacific/Kiritimati  | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofmonth | Pacific/Kiritimati  | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofyear  | Pacific/Kiritimati  | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofweek  | Pacific/Kiritimati  | 03:00Z Mon 2024-01-01 | 1704078000 | 2      |
        | weekday    | Pacific/Kiritimati  | 03:00Z Mon 2024-01-01 | 1704078000 | 0      |
        | day        | America/St_Johns    | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofmonth | America/St_Johns    | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofyear  | America/St_Johns    | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | dayofweek  | America/St_Johns    | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | weekday    | America/St_Johns    | 19:00Z Mon 2024-01-01 | 1704135600 | 0      |
        | day        | America/St_Johns    | Sun 2023-12-31 23:30Z | 1704065400 | 31     |
        | dayofmonth | America/St_Johns    | Sun 2023-12-31 23:30Z | 1704065400 | 31     |
        | dayofyear  | America/St_Johns    | Sun 2023-12-31 23:30Z | 1704065400 | 365    |
        | dayofweek  | America/St_Johns    | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | day        | Australia/Lord_Howe | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofmonth | Australia/Lord_Howe | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofyear  | Australia/Lord_Howe | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | dayofweek  | Australia/Lord_Howe | 03:00Z Mon 2024-01-01 | 1704078000 | 2      |
        | weekday    | Australia/Lord_Howe | 03:00Z Mon 2024-01-01 | 1704078000 | 0      |
        | weekofyear | America/Los_Angeles | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | weekofyear | America/Los_Angeles | Sun 2023-12-31 23:30Z | 1704065400 | 52     |
        | weekofyear | PST                 | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | weekofyear | PST                 | Sun 2023-12-31 23:30Z | 1704065400 | 52     |
        | weekofyear | EST                 | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | weekofyear | EST                 | Sun 2023-12-31 23:30Z | 1704065400 | 52     |
        | weekofyear | Asia/Kolkata        | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | weekofyear | Asia/Kolkata        | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | weekofyear | +05:30              | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | weekofyear | +05:30              | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | weekofyear | GMT+8               | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | weekofyear | GMT+8               | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | weekofyear | UTC+8               | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | weekofyear | UTC+8               | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | weekofyear | Z                   | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | weekofyear | Z                   | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | weekofyear | Z                   | Sun 2023-12-31 23:30Z | 1704065400 | 52     |
        | weekofyear | Etc/GMT+12          | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | weekofyear | Etc/GMT+12          | Sun 2023-12-31 23:30Z | 1704065400 | 52     |
        | weekofyear | Pacific/Kiritimati  | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | weekofyear | Pacific/Kiritimati  | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | weekofyear | America/St_Johns    | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | weekofyear | America/St_Johns    | Sun 2023-12-31 23:30Z | 1704065400 | 52     |
        | weekofyear | Australia/Lord_Howe | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | weekofyear | Australia/Lord_Howe | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |

    # TODO(known gap): the TIMESTAMP is read with the zone of its Arrow type, not with the session zone, so an instant near midnight gives the day of another zone (month 1 instead of 12 under PST).
    #   Every row fails on `main` too (checked).
    @sail-bug
    Scenario Outline: <function>: Zone <zone>: <case> (known gap)
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT <function>(timestamp_seconds(<seconds>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | zone                | case                  | seconds    | result |
        | day        | America/Los_Angeles | 03:00Z Mon 2024-01-01 | 1704078000 | 31     |
        | dayofmonth | America/Los_Angeles | 03:00Z Mon 2024-01-01 | 1704078000 | 31     |
        | dayofyear  | America/Los_Angeles | 03:00Z Mon 2024-01-01 | 1704078000 | 365    |
        | dayofweek  | America/Los_Angeles | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | weekday    | America/Los_Angeles | 03:00Z Mon 2024-01-01 | 1704078000 | 6      |
        | weekofyear | America/Los_Angeles | 03:00Z Mon 2024-01-01 | 1704078000 | 52     |
        | weekday    | America/Los_Angeles | Sun 2023-12-31 23:30Z | 1704065400 | 6      |
        | day        | PST                 | 03:00Z Mon 2024-01-01 | 1704078000 | 31     |
        | dayofmonth | PST                 | 03:00Z Mon 2024-01-01 | 1704078000 | 31     |
        | dayofyear  | PST                 | 03:00Z Mon 2024-01-01 | 1704078000 | 365    |
        | dayofweek  | PST                 | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | weekday    | PST                 | 03:00Z Mon 2024-01-01 | 1704078000 | 6      |
        | weekofyear | PST                 | 03:00Z Mon 2024-01-01 | 1704078000 | 52     |
        | weekday    | PST                 | Sun 2023-12-31 23:30Z | 1704065400 | 6      |
        | day        | EST                 | 03:00Z Mon 2024-01-01 | 1704078000 | 31     |
        | dayofmonth | EST                 | 03:00Z Mon 2024-01-01 | 1704078000 | 31     |
        | dayofyear  | EST                 | 03:00Z Mon 2024-01-01 | 1704078000 | 365    |
        | dayofweek  | EST                 | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | weekday    | EST                 | 03:00Z Mon 2024-01-01 | 1704078000 | 6      |
        | weekofyear | EST                 | 03:00Z Mon 2024-01-01 | 1704078000 | 52     |
        | weekday    | EST                 | Sun 2023-12-31 23:30Z | 1704065400 | 6      |
        | day        | Asia/Kolkata        | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofmonth | Asia/Kolkata        | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofyear  | Asia/Kolkata        | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofweek  | Asia/Kolkata        | 19:00Z Mon 2024-01-01 | 1704135600 | 3      |
        | weekday    | Asia/Kolkata        | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | day        | Asia/Kolkata        | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofmonth | Asia/Kolkata        | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofyear  | Asia/Kolkata        | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofweek  | Asia/Kolkata        | Sun 2023-12-31 23:30Z | 1704065400 | 2      |
        | weekday    | Asia/Kolkata        | Sun 2023-12-31 23:30Z | 1704065400 | 0      |
        | weekofyear | Asia/Kolkata        | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | day        | +05:30              | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofmonth | +05:30              | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofyear  | +05:30              | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofweek  | +05:30              | 19:00Z Mon 2024-01-01 | 1704135600 | 3      |
        | weekday    | +05:30              | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | day        | +05:30              | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofmonth | +05:30              | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofyear  | +05:30              | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofweek  | +05:30              | Sun 2023-12-31 23:30Z | 1704065400 | 2      |
        | weekday    | +05:30              | Sun 2023-12-31 23:30Z | 1704065400 | 0      |
        | weekofyear | +05:30              | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | day        | GMT+8               | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofmonth | GMT+8               | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofyear  | GMT+8               | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofweek  | GMT+8               | 19:00Z Mon 2024-01-01 | 1704135600 | 3      |
        | weekday    | GMT+8               | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | day        | GMT+8               | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofmonth | GMT+8               | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofyear  | GMT+8               | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofweek  | GMT+8               | Sun 2023-12-31 23:30Z | 1704065400 | 2      |
        | weekday    | GMT+8               | Sun 2023-12-31 23:30Z | 1704065400 | 0      |
        | weekofyear | GMT+8               | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | day        | UTC+8               | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofmonth | UTC+8               | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofyear  | UTC+8               | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofweek  | UTC+8               | 19:00Z Mon 2024-01-01 | 1704135600 | 3      |
        | weekday    | UTC+8               | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | day        | UTC+8               | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofmonth | UTC+8               | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofyear  | UTC+8               | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofweek  | UTC+8               | Sun 2023-12-31 23:30Z | 1704065400 | 2      |
        | weekday    | UTC+8               | Sun 2023-12-31 23:30Z | 1704065400 | 0      |
        | weekofyear | UTC+8               | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | weekday    | Z                   | Sun 2023-12-31 23:30Z | 1704065400 | 6      |
        | day        | Etc/GMT+12          | 03:00Z Mon 2024-01-01 | 1704078000 | 31     |
        | dayofmonth | Etc/GMT+12          | 03:00Z Mon 2024-01-01 | 1704078000 | 31     |
        | dayofyear  | Etc/GMT+12          | 03:00Z Mon 2024-01-01 | 1704078000 | 365    |
        | dayofweek  | Etc/GMT+12          | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | weekday    | Etc/GMT+12          | 03:00Z Mon 2024-01-01 | 1704078000 | 6      |
        | weekofyear | Etc/GMT+12          | 03:00Z Mon 2024-01-01 | 1704078000 | 52     |
        | weekday    | Etc/GMT+12          | Sun 2023-12-31 23:30Z | 1704065400 | 6      |
        | day        | Pacific/Kiritimati  | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofmonth | Pacific/Kiritimati  | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofyear  | Pacific/Kiritimati  | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofweek  | Pacific/Kiritimati  | 19:00Z Mon 2024-01-01 | 1704135600 | 3      |
        | weekday    | Pacific/Kiritimati  | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | day        | Pacific/Kiritimati  | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofmonth | Pacific/Kiritimati  | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofyear  | Pacific/Kiritimati  | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofweek  | Pacific/Kiritimati  | Sun 2023-12-31 23:30Z | 1704065400 | 2      |
        | weekday    | Pacific/Kiritimati  | Sun 2023-12-31 23:30Z | 1704065400 | 0      |
        | weekofyear | Pacific/Kiritimati  | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | day        | America/St_Johns    | 03:00Z Mon 2024-01-01 | 1704078000 | 31     |
        | dayofmonth | America/St_Johns    | 03:00Z Mon 2024-01-01 | 1704078000 | 31     |
        | dayofyear  | America/St_Johns    | 03:00Z Mon 2024-01-01 | 1704078000 | 365    |
        | dayofweek  | America/St_Johns    | 03:00Z Mon 2024-01-01 | 1704078000 | 1      |
        | weekday    | America/St_Johns    | 03:00Z Mon 2024-01-01 | 1704078000 | 6      |
        | weekofyear | America/St_Johns    | 03:00Z Mon 2024-01-01 | 1704078000 | 52     |
        | weekday    | America/St_Johns    | Sun 2023-12-31 23:30Z | 1704065400 | 6      |
        | day        | Australia/Lord_Howe | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofmonth | Australia/Lord_Howe | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofyear  | Australia/Lord_Howe | 19:00Z Mon 2024-01-01 | 1704135600 | 2      |
        | dayofweek  | Australia/Lord_Howe | 19:00Z Mon 2024-01-01 | 1704135600 | 3      |
        | weekday    | Australia/Lord_Howe | 19:00Z Mon 2024-01-01 | 1704135600 | 1      |
        | day        | Australia/Lord_Howe | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofmonth | Australia/Lord_Howe | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofyear  | Australia/Lord_Howe | Sun 2023-12-31 23:30Z | 1704065400 | 1      |
        | dayofweek  | Australia/Lord_Howe | Sun 2023-12-31 23:30Z | 1704065400 | 2      |
        | weekday    | Australia/Lord_Howe | Sun 2023-12-31 23:30Z | 1704065400 | 0      |
        | weekofyear | Australia/Lord_Howe | Sun 2023-12-31 23:30Z | 1704065400 | 1      |

  Rule: Rejections

    # TODO(known gap): a wrong arity leaks DataFusion's text and a type Spark rejects (INT, BIGINT, DOUBLE, DECIMAL, BOOLEAN, BINARY, ...) is not rejected with `WRONG_NUM_ARGS` / `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` at analysis.
    #   Every row fails on `main` too (checked).
    @sail-bug
    Scenario Outline: <function>: Rejection: <case> (known gap)
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <function>(<args>) AS result
        """
      Then query error <message>

      Examples:
        | function   | case                | ansi  | args                   | message                                                                                                                                                                                         |
        | day        | arity 0             | false |                        | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `day` requires 1 parameters but the actual number is 0.                                                                                                  |
        | dayofmonth | arity 0             | false |                        | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `dayofmonth` requires 1 parameters but the actual number is 0.                                                                                           |
        | dayofyear  | arity 0             | false |                        | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `dayofyear` requires 1 parameters but the actual number is 0.                                                                                            |
        | dayofweek  | arity 0             | false |                        | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `dayofweek` requires 1 parameters but the actual number is 0.                                                                                            |
        | weekday    | arity 0             | false |                        | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `weekday` requires 1 parameters but the actual number is 0.                                                                                              |
        | weekofyear | arity 0             | false |                        | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `weekofyear` requires 1 parameters but the actual number is 0.                                                                                           |
        | day        | arity 2             | false | DATE'2024-03-17', 1    | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `day` requires 1 parameters but the actual number is 2.                                                                                                  |
        | dayofmonth | arity 2             | false | DATE'2024-03-17', 1    | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `dayofmonth` requires 1 parameters but the actual number is 2.                                                                                           |
        | dayofyear  | arity 2             | false | DATE'2024-03-17', 1    | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `dayofyear` requires 1 parameters but the actual number is 2.                                                                                            |
        | dayofweek  | arity 2             | false | DATE'2024-03-17', 1    | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `dayofweek` requires 1 parameters but the actual number is 2.                                                                                            |
        | weekday    | arity 2             | false | DATE'2024-03-17', 1    | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `weekday` requires 1 parameters but the actual number is 2.                                                                                              |
        | weekofyear | arity 2             | false | DATE'2024-03-17', 1    | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `weekofyear` requires 1 parameters but the actual number is 2.                                                                                           |
        | day        | arity 3             | false | DATE'2024-03-17', 1, 2 | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `day` requires 1 parameters but the actual number is 3.                                                                                                  |
        | dayofmonth | arity 3             | false | DATE'2024-03-17', 1, 2 | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `dayofmonth` requires 1 parameters but the actual number is 3.                                                                                           |
        | dayofyear  | arity 3             | false | DATE'2024-03-17', 1, 2 | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `dayofyear` requires 1 parameters but the actual number is 3.                                                                                            |
        | dayofweek  | arity 3             | false | DATE'2024-03-17', 1, 2 | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `dayofweek` requires 1 parameters but the actual number is 3.                                                                                            |
        | weekday    | arity 3             | false | DATE'2024-03-17', 1, 2 | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `weekday` requires 1 parameters but the actual number is 3.                                                                                              |
        | weekofyear | arity 3             | false | DATE'2024-03-17', 1, 2 | WRONG_NUM_ARGS.WITHOUT_SUGGESTION. The `weekofyear` requires 1 parameters but the actual number is 3.                                                                                           |
        | day        | INT                 | false | 1                      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .day.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .INT..                       |
        | dayofmonth | INT                 | false | 1                      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofmonth.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .INT..                |
        | dayofyear  | INT                 | false | 1                      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofyear.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .INT..                 |
        | dayofweek  | INT                 | false | 1                      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofweek.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .INT..                 |
        | weekday    | INT                 | false | 1                      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekday.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .INT..                   |
        | weekofyear | INT                 | false | 1                      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekofyear.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .INT..                |
        | day        | BIGINT              | false | 1L                     | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .day.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .BIGINT..                    |
        | dayofmonth | BIGINT              | false | 1L                     | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofmonth.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .BIGINT..             |
        | dayofyear  | BIGINT              | false | 1L                     | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofyear.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .BIGINT..              |
        | dayofweek  | BIGINT              | false | 1L                     | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofweek.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .BIGINT..              |
        | weekday    | BIGINT              | false | 1L                     | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekday.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .BIGINT..                |
        | weekofyear | BIGINT              | false | 1L                     | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekofyear.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .BIGINT..             |
        | day        | DOUBLE              | false | 1.5D                   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .day.1.5.. due to data type mismatch: The first parameter requires the .DATE. type, however .1.5. has the type .DOUBLE..                |
        | dayofmonth | DOUBLE              | false | 1.5D                   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofmonth.1.5.. due to data type mismatch: The first parameter requires the .DATE. type, however .1.5. has the type .DOUBLE..         |
        | dayofyear  | DOUBLE              | false | 1.5D                   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofyear.1.5.. due to data type mismatch: The first parameter requires the .DATE. type, however .1.5. has the type .DOUBLE..          |
        | dayofweek  | DOUBLE              | false | 1.5D                   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofweek.1.5.. due to data type mismatch: The first parameter requires the .DATE. type, however .1.5. has the type .DOUBLE..          |
        | weekday    | DOUBLE              | false | 1.5D                   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekday.1.5.. due to data type mismatch: The first parameter requires the .DATE. type, however .1.5. has the type .DOUBLE..            |
        | weekofyear | DOUBLE              | false | 1.5D                   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekofyear.1.5.. due to data type mismatch: The first parameter requires the .DATE. type, however .1.5. has the type .DOUBLE..         |
        | day        | DECIMAL             | false | 1.5BD                  | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .day.1.5.. due to data type mismatch: The first parameter requires the .DATE. type, however .1.5. has the type .DECIMAL.2,1...          |
        | dayofmonth | DECIMAL             | false | 1.5BD                  | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofmonth.1.5.. due to data type mismatch: The first parameter requires the .DATE. type, however .1.5. has the type .DECIMAL.2,1...   |
        | dayofyear  | DECIMAL             | false | 1.5BD                  | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofyear.1.5.. due to data type mismatch: The first parameter requires the .DATE. type, however .1.5. has the type .DECIMAL.2,1...    |
        | dayofweek  | DECIMAL             | false | 1.5BD                  | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofweek.1.5.. due to data type mismatch: The first parameter requires the .DATE. type, however .1.5. has the type .DECIMAL.2,1...    |
        | weekday    | DECIMAL             | false | 1.5BD                  | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekday.1.5.. due to data type mismatch: The first parameter requires the .DATE. type, however .1.5. has the type .DECIMAL.2,1...      |
        | weekofyear | DECIMAL             | false | 1.5BD                  | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekofyear.1.5.. due to data type mismatch: The first parameter requires the .DATE. type, however .1.5. has the type .DECIMAL.2,1...   |
        | day        | BOOLEAN             | false | true                   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .day.true.. due to data type mismatch: The first parameter requires the .DATE. type, however .true. has the type .BOOLEAN..             |
        | dayofmonth | BOOLEAN             | false | true                   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofmonth.true.. due to data type mismatch: The first parameter requires the .DATE. type, however .true. has the type .BOOLEAN..      |
        | dayofyear  | BOOLEAN             | false | true                   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofyear.true.. due to data type mismatch: The first parameter requires the .DATE. type, however .true. has the type .BOOLEAN..       |
        | dayofweek  | BOOLEAN             | false | true                   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofweek.true.. due to data type mismatch: The first parameter requires the .DATE. type, however .true. has the type .BOOLEAN..       |
        | weekday    | BOOLEAN             | false | true                   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekday.true.. due to data type mismatch: The first parameter requires the .DATE. type, however .true. has the type .BOOLEAN..         |
        | weekofyear | BOOLEAN             | false | true                   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekofyear.true.. due to data type mismatch: The first parameter requires the .DATE. type, however .true. has the type .BOOLEAN..      |
        | day        | BINARY              | false | X'01'                  | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .day.X'01'.. due to data type mismatch: The first parameter requires the .DATE. type, however .X'01'. has the type .BINARY..            |
        | dayofmonth | BINARY              | false | X'01'                  | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofmonth.X'01'.. due to data type mismatch: The first parameter requires the .DATE. type, however .X'01'. has the type .BINARY..     |
        | dayofyear  | BINARY              | false | X'01'                  | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofyear.X'01'.. due to data type mismatch: The first parameter requires the .DATE. type, however .X'01'. has the type .BINARY..      |
        | dayofweek  | BINARY              | false | X'01'                  | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofweek.X'01'.. due to data type mismatch: The first parameter requires the .DATE. type, however .X'01'. has the type .BINARY..      |
        | weekday    | BINARY              | false | X'01'                  | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekday.X'01'.. due to data type mismatch: The first parameter requires the .DATE. type, however .X'01'. has the type .BINARY..        |
        | weekofyear | BINARY              | false | X'01'                  | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekofyear.X'01'.. due to data type mismatch: The first parameter requires the .DATE. type, however .X'01'. has the type .BINARY..     |
        | day        | INTERVAL day-time   | false | INTERVAL '1' DAY       | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .day.INTERVAL '1' DAY.. due to data type mismatch: The first parameter requires the .DATE. type, however .INTERVAL '1' DAY. has the typ |
        | dayofmonth | INTERVAL day-time   | false | INTERVAL '1' DAY       | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofmonth.INTERVAL '1' DAY.. due to data type mismatch: The first parameter requires the .DATE. type, however .INTERVAL '1' DAY. has  |
        | dayofyear  | INTERVAL day-time   | false | INTERVAL '1' DAY       | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofyear.INTERVAL '1' DAY.. due to data type mismatch: The first parameter requires the .DATE. type, however .INTERVAL '1' DAY. has t |
        | dayofweek  | INTERVAL day-time   | false | INTERVAL '1' DAY       | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofweek.INTERVAL '1' DAY.. due to data type mismatch: The first parameter requires the .DATE. type, however .INTERVAL '1' DAY. has t |
        | weekday    | INTERVAL day-time   | false | INTERVAL '1' DAY       | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekday.INTERVAL '1' DAY.. due to data type mismatch: The first parameter requires the .DATE. type, however .INTERVAL '1' DAY. has the |
        | weekofyear | INTERVAL day-time   | false | INTERVAL '1' DAY       | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekofyear.INTERVAL '1' DAY.. due to data type mismatch: The first parameter requires the .DATE. type, however .INTERVAL '1' DAY. has  |
        | day        | INTERVAL year-month | false | INTERVAL '1' YEAR      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .day.INTERVAL '1' YEAR.. due to data type mismatch: The first parameter requires the .DATE. type, however .INTERVAL '1' YEAR. has the t |
        | dayofmonth | INTERVAL year-month | false | INTERVAL '1' YEAR      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofmonth.INTERVAL '1' YEAR.. due to data type mismatch: The first parameter requires the .DATE. type, however .INTERVAL '1' YEAR. ha |
        | dayofyear  | INTERVAL year-month | false | INTERVAL '1' YEAR      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofyear.INTERVAL '1' YEAR.. due to data type mismatch: The first parameter requires the .DATE. type, however .INTERVAL '1' YEAR. has |
        | dayofweek  | INTERVAL year-month | false | INTERVAL '1' YEAR      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofweek.INTERVAL '1' YEAR.. due to data type mismatch: The first parameter requires the .DATE. type, however .INTERVAL '1' YEAR. has |
        | weekday    | INTERVAL year-month | false | INTERVAL '1' YEAR      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekday.INTERVAL '1' YEAR.. due to data type mismatch: The first parameter requires the .DATE. type, however .INTERVAL '1' YEAR. has t |
        | weekofyear | INTERVAL year-month | false | INTERVAL '1' YEAR      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekofyear.INTERVAL '1' YEAR.. due to data type mismatch: The first parameter requires the .DATE. type, however .INTERVAL '1' YEAR. ha |
        | day        | ARRAY               | false | array(1)               | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .day.array.1... due to data type mismatch: The first parameter requires the .DATE. type, however .array.1.. has the type .ARRAY<INT>..  |
        | dayofmonth | ARRAY               | false | array(1)               | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofmonth.array.1... due to data type mismatch: The first parameter requires the .DATE. type, however .array.1.. has the type .ARRAY< |
        | dayofyear  | ARRAY               | false | array(1)               | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofyear.array.1... due to data type mismatch: The first parameter requires the .DATE. type, however .array.1.. has the type .ARRAY<I |
        | dayofweek  | ARRAY               | false | array(1)               | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofweek.array.1... due to data type mismatch: The first parameter requires the .DATE. type, however .array.1.. has the type .ARRAY<I |
        | weekday    | ARRAY               | false | array(1)               | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekday.array.1... due to data type mismatch: The first parameter requires the .DATE. type, however .array.1.. has the type .ARRAY<INT |
        | weekofyear | ARRAY               | false | array(1)               | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekofyear.array.1... due to data type mismatch: The first parameter requires the .DATE. type, however .array.1.. has the type .ARRAY< |
        | day        | MAP                 | false | map(1, 2)              | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .day.map.1, 2... due to data type mismatch: The first parameter requires the .DATE. type, however .map.1, 2.. has the type .MAP<INT, IN |
        | dayofmonth | MAP                 | false | map(1, 2)              | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofmonth.map.1, 2... due to data type mismatch: The first parameter requires the .DATE. type, however .map.1, 2.. has the type .MAP< |
        | dayofyear  | MAP                 | false | map(1, 2)              | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofyear.map.1, 2... due to data type mismatch: The first parameter requires the .DATE. type, however .map.1, 2.. has the type .MAP<I |
        | dayofweek  | MAP                 | false | map(1, 2)              | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofweek.map.1, 2... due to data type mismatch: The first parameter requires the .DATE. type, however .map.1, 2.. has the type .MAP<I |
        | weekday    | MAP                 | false | map(1, 2)              | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekday.map.1, 2... due to data type mismatch: The first parameter requires the .DATE. type, however .map.1, 2.. has the type .MAP<INT |
        | weekofyear | MAP                 | false | map(1, 2)              | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekofyear.map.1, 2... due to data type mismatch: The first parameter requires the .DATE. type, however .map.1, 2.. has the type .MAP< |
        | day        | STRUCT              | false | named_struct('a', 1)   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .day.named_struct.a, 1... due to data type mismatch: The first parameter requires the .DATE. type, however .named_struct.a, 1.. has the |
        | dayofmonth | STRUCT              | false | named_struct('a', 1)   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofmonth.named_struct.a, 1... due to data type mismatch: The first parameter requires the .DATE. type, however .named_struct.a, 1..  |
        | dayofyear  | STRUCT              | false | named_struct('a', 1)   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofyear.named_struct.a, 1... due to data type mismatch: The first parameter requires the .DATE. type, however .named_struct.a, 1.. h |
        | dayofweek  | STRUCT              | false | named_struct('a', 1)   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofweek.named_struct.a, 1... due to data type mismatch: The first parameter requires the .DATE. type, however .named_struct.a, 1.. h |
        | weekday    | STRUCT              | false | named_struct('a', 1)   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekday.named_struct.a, 1... due to data type mismatch: The first parameter requires the .DATE. type, however .named_struct.a, 1.. has |
        | weekofyear | STRUCT              | false | named_struct('a', 1)   | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekofyear.named_struct.a, 1... due to data type mismatch: The first parameter requires the .DATE. type, however .named_struct.a, 1..  |
        | day        | INT, ANSI on        | true  | 1                      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .day.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .INT..                       |
        | dayofmonth | INT, ANSI on        | true  | 1                      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofmonth.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .INT..                |
        | dayofyear  | INT, ANSI on        | true  | 1                      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofyear.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .INT..                 |
        | dayofweek  | INT, ANSI on        | true  | 1                      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofweek.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .INT..                 |
        | weekday    | INT, ANSI on        | true  | 1                      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekday.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .INT..                   |
        | weekofyear | INT, ANSI on        | true  | 1                      | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekofyear.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .INT..                |
        | day        | BIGINT, ANSI on     | true  | 1L                     | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .day.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .BIGINT..                    |
        | dayofmonth | BIGINT, ANSI on     | true  | 1L                     | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofmonth.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .BIGINT..             |
        | dayofyear  | BIGINT, ANSI on     | true  | 1L                     | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofyear.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .BIGINT..              |
        | dayofweek  | BIGINT, ANSI on     | true  | 1L                     | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .dayofweek.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .BIGINT..              |
        | weekday    | BIGINT, ANSI on     | true  | 1L                     | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekday.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .BIGINT..                |
        | weekofyear | BIGINT, ANSI on     | true  | 1L                     | DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE. Cannot resolve .weekofyear.1.. due to data type mismatch: The first parameter requires the .DATE. type, however .1. has the type .BIGINT..             |
        | day        | TIME, flag default  | false | TIME'10:11:12'         | DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION. Cannot resolve .TIME '10:11:12'. due to data type mismatch: cannot cast .TIME.6.. to .DATE..                                                         |
        | dayofmonth | TIME, flag default  | false | TIME'10:11:12'         | DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION. Cannot resolve .TIME '10:11:12'. due to data type mismatch: cannot cast .TIME.6.. to .DATE..                                                         |
        | dayofyear  | TIME, flag default  | false | TIME'10:11:12'         | DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION. Cannot resolve .TIME '10:11:12'. due to data type mismatch: cannot cast .TIME.6.. to .DATE..                                                         |
        | dayofweek  | TIME, flag default  | false | TIME'10:11:12'         | DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION. Cannot resolve .TIME '10:11:12'. due to data type mismatch: cannot cast .TIME.6.. to .DATE..                                                         |
        | weekday    | TIME, flag default  | false | TIME'10:11:12'         | DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION. Cannot resolve .TIME '10:11:12'. due to data type mismatch: cannot cast .TIME.6.. to .DATE..                                                         |
        | weekofyear | TIME, flag default  | false | TIME'10:11:12'         | DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION. Cannot resolve .TIME '10:11:12'. due to data type mismatch: cannot cast .TIME.6.. to .DATE..                                                         |

  Rule: Composition (type coercion before the cast)

    Scenario Outline: <function>: Composition: <case>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT <function>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | case                               | zone                | input                                                                               | result |
        | day        | TIMESTAMP control, LA              | America/Los_Angeles | TIMESTAMP'2025-01-01 03:00:00+00:00'                                                | 31     |
        | dayofmonth | TIMESTAMP control, LA              | America/Los_Angeles | TIMESTAMP'2025-01-01 03:00:00+00:00'                                                | 31     |
        | dayofyear  | TIMESTAMP control, LA              | America/Los_Angeles | TIMESTAMP'2025-01-01 03:00:00+00:00'                                                | 366    |
        | dayofweek  | TIMESTAMP control, LA              | America/Los_Angeles | TIMESTAMP'2025-01-01 03:00:00+00:00'                                                | 3      |
        | weekday    | TIMESTAMP control, LA              | America/Los_Angeles | TIMESTAMP'2025-01-01 03:00:00+00:00'                                                | 1      |
        | day        | function result as the argument    | UTC                 | date_add(DATE'2024-12-31', 1)                                                       | 1      |
        | dayofmonth | function result as the argument    | UTC                 | date_add(DATE'2024-12-31', 1)                                                       | 1      |
        | dayofyear  | function result as the argument    | UTC                 | date_add(DATE'2024-12-31', 1)                                                       | 1      |
        | dayofweek  | function result as the argument    | UTC                 | date_add(DATE'2024-12-31', 1)                                                       | 4      |
        | weekday    | function result as the argument    | UTC                 | date_add(DATE'2024-12-31', 1)                                                       | 2      |
        | day        | nested: function of an extractor   | UTC                 | make_date(year(DATE'2024-03-17'), 1, 1)                                             | 1      |
        | dayofmonth | nested: function of an extractor   | UTC                 | make_date(year(DATE'2024-03-17'), 1, 1)                                             | 1      |
        | dayofyear  | nested: function of an extractor   | UTC                 | make_date(year(DATE'2024-03-17'), 1, 1)                                             | 1      |
        | dayofweek  | nested: function of an extractor   | UTC                 | make_date(year(DATE'2024-03-17'), 1, 1)                                             | 2      |
        | weekday    | nested: function of an extractor   | UTC                 | make_date(year(DATE'2024-03-17'), 1, 1)                                             | 0      |
        | weekofyear | nested: function of an extractor   | UTC                 | make_date(year(DATE'2024-03-17'), 1, 1)                                             | 1      |
        | day        | nullif(DATE, DATE)                 | UTC                 | nullif(DATE'2024-03-17', DATE'2024-03-17')                                          | NULL   |
        | dayofmonth | nullif(DATE, DATE)                 | UTC                 | nullif(DATE'2024-03-17', DATE'2024-03-17')                                          | NULL   |
        | dayofyear  | nullif(DATE, DATE)                 | UTC                 | nullif(DATE'2024-03-17', DATE'2024-03-17')                                          | NULL   |
        | dayofweek  | nullif(DATE, DATE)                 | UTC                 | nullif(DATE'2024-03-17', DATE'2024-03-17')                                          | NULL   |
        | weekday    | nullif(DATE, DATE)                 | UTC                 | nullif(DATE'2024-03-17', DATE'2024-03-17')                                          | NULL   |
        | weekofyear | nullif(DATE, DATE)                 | UTC                 | nullif(DATE'2024-03-17', DATE'2024-03-17')                                          | NULL   |
        | day        | CASE DATE/TIMESTAMP picks DATE, LA | America/Los_Angeles | CASE WHEN true THEN DATE'2024-03-17' ELSE TIMESTAMP'2025-01-01 03:00:00+00:00' END  | 17     |
        | dayofmonth | CASE DATE/TIMESTAMP picks DATE, LA | America/Los_Angeles | CASE WHEN true THEN DATE'2024-03-17' ELSE TIMESTAMP'2025-01-01 03:00:00+00:00' END  | 17     |
        | dayofyear  | CASE DATE/TIMESTAMP picks DATE, LA | America/Los_Angeles | CASE WHEN true THEN DATE'2024-03-17' ELSE TIMESTAMP'2025-01-01 03:00:00+00:00' END  | 77     |
        | dayofweek  | CASE DATE/TIMESTAMP picks DATE, LA | America/Los_Angeles | CASE WHEN true THEN DATE'2024-03-17' ELSE TIMESTAMP'2025-01-01 03:00:00+00:00' END  | 1      |
        | weekofyear | CASE DATE/TIMESTAMP picks DATE, LA | America/Los_Angeles | CASE WHEN true THEN DATE'2024-03-17' ELSE TIMESTAMP'2025-01-01 03:00:00+00:00' END  | 11     |
        | day        | CASE picks the TIMESTAMP, LA       | America/Los_Angeles | CASE WHEN false THEN DATE'2024-03-17' ELSE TIMESTAMP'2025-01-01 03:00:00+00:00' END | 31     |
        | dayofmonth | CASE picks the TIMESTAMP, LA       | America/Los_Angeles | CASE WHEN false THEN DATE'2024-03-17' ELSE TIMESTAMP'2025-01-01 03:00:00+00:00' END | 31     |
        | dayofyear  | CASE picks the TIMESTAMP, LA       | America/Los_Angeles | CASE WHEN false THEN DATE'2024-03-17' ELSE TIMESTAMP'2025-01-01 03:00:00+00:00' END | 366    |
        | dayofweek  | CASE picks the TIMESTAMP, LA       | America/Los_Angeles | CASE WHEN false THEN DATE'2024-03-17' ELSE TIMESTAMP'2025-01-01 03:00:00+00:00' END | 3      |
        | weekday    | CASE picks the TIMESTAMP, LA       | America/Los_Angeles | CASE WHEN false THEN DATE'2024-03-17' ELSE TIMESTAMP'2025-01-01 03:00:00+00:00' END | 1      |
        | day        | IF picks the TIMESTAMP, LA         | America/Los_Angeles | IF(false, DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')                   | 31     |
        | dayofmonth | IF picks the TIMESTAMP, LA         | America/Los_Angeles | IF(false, DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')                   | 31     |
        | dayofyear  | IF picks the TIMESTAMP, LA         | America/Los_Angeles | IF(false, DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')                   | 366    |
        | dayofweek  | IF picks the TIMESTAMP, LA         | America/Los_Angeles | IF(false, DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')                   | 3      |
        | weekday    | IF picks the TIMESTAMP, LA         | America/Los_Angeles | IF(false, DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')                   | 1      |
        | weekofyear | CASE picks the TIMESTAMP, LA       | America/Los_Angeles | CASE WHEN false THEN DATE'2024-03-17' ELSE TIMESTAMP'2025-01-01 03:00:00+00:00' END | 1      |
        | weekofyear | IF picks the TIMESTAMP, LA         | America/Los_Angeles | IF(false, DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')                   | 1      |
        | weekofyear | coalesce TIMESTAMP, DATE, LA       | America/Los_Angeles | coalesce(TIMESTAMP'2025-01-01 03:00:00+00:00', DATE'2024-03-17')                    | 1      |
        | weekofyear | greatest DATE, TIMESTAMP, LA       | America/Los_Angeles | greatest(DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')                    | 1      |
        | weekofyear | array element, LA                  | America/Los_Angeles | array(DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')[1]                    | 1      |
        | weekofyear | TIMESTAMP control, LA              | America/Los_Angeles | TIMESTAMP'2025-01-01 03:00:00+00:00'                                                | 1      |
        | weekofyear | function result as the argument    | UTC                 | date_add(DATE'2024-12-31', 1)                                                       | 1      |

    # TODO(known gap): coalesce, greatest and array of a DATE and a TIMESTAMP are typed by the first branch before the cast, so the TIMESTAMP is read in the wrong zone.
    #   The weekday row of the CASE that picks the DATE is the Sunday bug of `weekday`.
    @sail-bug
    Scenario Outline: <function>: Composition: <case> (known gap)
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT <function>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | case                               | zone                | input                                                                               | result |
        | weekday    | CASE DATE/TIMESTAMP picks DATE, LA | America/Los_Angeles | CASE WHEN true THEN DATE'2024-03-17' ELSE TIMESTAMP'2025-01-01 03:00:00+00:00' END  | 6      |
        | day        | coalesce TIMESTAMP, DATE, LA       | America/Los_Angeles | coalesce(TIMESTAMP'2025-01-01 03:00:00+00:00', DATE'2024-03-17')                    | 31     |
        | dayofmonth | coalesce TIMESTAMP, DATE, LA       | America/Los_Angeles | coalesce(TIMESTAMP'2025-01-01 03:00:00+00:00', DATE'2024-03-17')                    | 31     |
        | dayofyear  | coalesce TIMESTAMP, DATE, LA       | America/Los_Angeles | coalesce(TIMESTAMP'2025-01-01 03:00:00+00:00', DATE'2024-03-17')                    | 366    |
        | dayofweek  | coalesce TIMESTAMP, DATE, LA       | America/Los_Angeles | coalesce(TIMESTAMP'2025-01-01 03:00:00+00:00', DATE'2024-03-17')                    | 3      |
        | weekday    | coalesce TIMESTAMP, DATE, LA       | America/Los_Angeles | coalesce(TIMESTAMP'2025-01-01 03:00:00+00:00', DATE'2024-03-17')                    | 1      |
        | day        | greatest DATE, TIMESTAMP, LA       | America/Los_Angeles | greatest(DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')                    | 31     |
        | dayofmonth | greatest DATE, TIMESTAMP, LA       | America/Los_Angeles | greatest(DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')                    | 31     |
        | dayofyear  | greatest DATE, TIMESTAMP, LA       | America/Los_Angeles | greatest(DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')                    | 366    |
        | dayofweek  | greatest DATE, TIMESTAMP, LA       | America/Los_Angeles | greatest(DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')                    | 3      |
        | weekday    | greatest DATE, TIMESTAMP, LA       | America/Los_Angeles | greatest(DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')                    | 1      |
        | day        | array element, LA                  | America/Los_Angeles | array(DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')[1]                    | 31     |
        | dayofmonth | array element, LA                  | America/Los_Angeles | array(DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')[1]                    | 31     |
        | dayofyear  | array element, LA                  | America/Los_Angeles | array(DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')[1]                    | 366    |
        | dayofweek  | array element, LA                  | America/Los_Angeles | array(DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')[1]                    | 3      |
        | weekday    | array element, LA                  | America/Los_Angeles | array(DATE'2024-03-17', TIMESTAMP'2025-01-01 03:00:00+00:00')[1]                    | 1      |

    Scenario Outline: <function>: UNION: <case>
      Given config spark.sql.session.timeZone = <zone>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <function>(d) AS result FROM (<left> UNION ALL <right>) ORDER BY 1
        """
      Then query result ordered
        | result  |
        | <first> |
        | <second> |

      Examples:
        | function   | case                       | zone                | ansi  | left                                          | right                                       | first | second |
        | day        | UNION DATE+TIMESTAMP, UTC  | UTC                 | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 1     | 17     |
        | dayofmonth | UNION DATE+TIMESTAMP, UTC  | UTC                 | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 1     | 17     |
        | dayofyear  | UNION DATE+TIMESTAMP, UTC  | UTC                 | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 1     | 77     |
        | dayofweek  | UNION DATE+TIMESTAMP, UTC  | UTC                 | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 1     | 4      |
        | weekofyear | UNION DATE+NTZ             | America/Los_Angeles | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP_NTZ'2025-01-01 03:00:00'   | 1     | 11     |
        | weekofyear | UNION STRING+DATE          | UTC                 | false | SELECT '2024-03-17' d                         | SELECT DATE'2025-01-01'                     | 1     | 11     |
        | weekofyear | UNION STRING+DATE, ANSI on | UTC                 | true  | SELECT '2024-03-17' d                         | SELECT DATE'2025-01-01'                     | 1     | 11     |
        | weekofyear | UNION STRING+TIMESTAMP, LA | America/Los_Angeles | false | SELECT '2024-03-17' d                         | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 1     | 11     |
        | weekofyear | UNION DATE+TIMESTAMP, LA   | America/Los_Angeles | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 1     | 11     |
        | weekofyear | UNION DATE+TIMESTAMP, UTC  | UTC                 | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 1     | 11     |
        | weekofyear | UNION TIMESTAMP+DATE, LA   | America/Los_Angeles | false | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' d | SELECT DATE'2024-03-17'                     | 1     | 11     |
        | day        | UNION DATE+NTZ             | America/Los_Angeles | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP_NTZ'2025-01-01 03:00:00'   | 1     | 17     |
        | dayofmonth | UNION DATE+NTZ             | America/Los_Angeles | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP_NTZ'2025-01-01 03:00:00'   | 1     | 17     |
        | dayofyear  | UNION DATE+NTZ             | America/Los_Angeles | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP_NTZ'2025-01-01 03:00:00'   | 1     | 77     |
        | dayofweek  | UNION DATE+NTZ             | America/Los_Angeles | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP_NTZ'2025-01-01 03:00:00'   | 1     | 4      |
        | day        | UNION STRING+DATE          | UTC                 | false | SELECT '2024-03-17' d                         | SELECT DATE'2025-01-01'                     | 1     | 17     |
        | dayofmonth | UNION STRING+DATE          | UTC                 | false | SELECT '2024-03-17' d                         | SELECT DATE'2025-01-01'                     | 1     | 17     |
        | dayofyear  | UNION STRING+DATE          | UTC                 | false | SELECT '2024-03-17' d                         | SELECT DATE'2025-01-01'                     | 1     | 77     |
        | dayofweek  | UNION STRING+DATE          | UTC                 | false | SELECT '2024-03-17' d                         | SELECT DATE'2025-01-01'                     | 1     | 4      |
        | day        | UNION STRING+DATE, ANSI on | UTC                 | true  | SELECT '2024-03-17' d                         | SELECT DATE'2025-01-01'                     | 1     | 17     |
        | dayofmonth | UNION STRING+DATE, ANSI on | UTC                 | true  | SELECT '2024-03-17' d                         | SELECT DATE'2025-01-01'                     | 1     | 17     |
        | dayofyear  | UNION STRING+DATE, ANSI on | UTC                 | true  | SELECT '2024-03-17' d                         | SELECT DATE'2025-01-01'                     | 1     | 77     |
        | dayofweek  | UNION STRING+DATE, ANSI on | UTC                 | true  | SELECT '2024-03-17' d                         | SELECT DATE'2025-01-01'                     | 1     | 4      |
        | day        | UNION STRING+TIMESTAMP, LA | America/Los_Angeles | false | SELECT '2024-03-17' d                         | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 17    | 31     |
        | dayofmonth | UNION STRING+TIMESTAMP, LA | America/Los_Angeles | false | SELECT '2024-03-17' d                         | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 17    | 31     |
        | dayofyear  | UNION STRING+TIMESTAMP, LA | America/Los_Angeles | false | SELECT '2024-03-17' d                         | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 77    | 366    |
        | dayofweek  | UNION STRING+TIMESTAMP, LA | America/Los_Angeles | false | SELECT '2024-03-17' d                         | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 1     | 3      |

    # TODO(known gap): a UNION of a DATE and a TIMESTAMP (or a STRING) is typed by the first branch before the cast (same cause as the Composition group).
    #   Every row fails on `main` too (checked).
    @sail-bug
    Scenario Outline: <function>: UNION: <case> (known gap)
      Given config spark.sql.session.timeZone = <zone>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <function>(d) AS result FROM (<left> UNION ALL <right>) ORDER BY 1
        """
      Then query result ordered
        | result  |
        | <first> |
        | <second> |

      Examples:
        | function   | case                       | zone                | ansi  | left                                          | right                                       | first | second |
        | day        | UNION DATE+TIMESTAMP, LA   | America/Los_Angeles | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 17    | 31     |
        | dayofmonth | UNION DATE+TIMESTAMP, LA   | America/Los_Angeles | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 17    | 31     |
        | dayofyear  | UNION DATE+TIMESTAMP, LA   | America/Los_Angeles | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 77    | 366    |
        | dayofweek  | UNION DATE+TIMESTAMP, LA   | America/Los_Angeles | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 1     | 3      |
        | weekday    | UNION DATE+TIMESTAMP, LA   | America/Los_Angeles | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 1     | 6      |
        | weekday    | UNION DATE+TIMESTAMP, UTC  | UTC                 | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 2     | 6      |
        | day        | UNION TIMESTAMP+DATE, LA   | America/Los_Angeles | false | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' d | SELECT DATE'2024-03-17'                     | 17    | 31     |
        | dayofmonth | UNION TIMESTAMP+DATE, LA   | America/Los_Angeles | false | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' d | SELECT DATE'2024-03-17'                     | 17    | 31     |
        | dayofyear  | UNION TIMESTAMP+DATE, LA   | America/Los_Angeles | false | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' d | SELECT DATE'2024-03-17'                     | 77    | 366    |
        | dayofweek  | UNION TIMESTAMP+DATE, LA   | America/Los_Angeles | false | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' d | SELECT DATE'2024-03-17'                     | 1     | 3      |
        | weekday    | UNION TIMESTAMP+DATE, LA   | America/Los_Angeles | false | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' d | SELECT DATE'2024-03-17'                     | 1     | 6      |
        | weekday    | UNION DATE+NTZ             | America/Los_Angeles | false | SELECT DATE'2024-03-17' d                     | SELECT TIMESTAMP_NTZ'2025-01-01 03:00:00'   | 2     | 6      |
        | weekday    | UNION STRING+DATE          | UTC                 | false | SELECT '2024-03-17' d                         | SELECT DATE'2025-01-01'                     | 2     | 6      |
        | weekday    | UNION STRING+DATE, ANSI on | UTC                 | true  | SELECT '2024-03-17' d                         | SELECT DATE'2025-01-01'                     | 2     | 6      |
        | weekday    | UNION STRING+TIMESTAMP, LA | America/Los_Angeles | false | SELECT '2024-03-17' d                         | SELECT TIMESTAMP'2025-01-01 03:00:00+00:00' | 1     | 6      |

  Rule: Output schema

    Scenario Outline: <function>: Schema: <case>
      When query
        """
        SELECT <function>(<input>) AS result <from>
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = <nullable>)
        """

      Examples:
        | function   | case                      | input                                | from                                                         | nullable |
        | day        | non-null DATE column      | d                                    | FROM (SELECT DATE'2024-03-17' d)                             | false    |
        | dayofmonth | non-null DATE column      | d                                    | FROM (SELECT DATE'2024-03-17' d)                             | false    |
        | dayofyear  | non-null DATE column      | d                                    | FROM (SELECT DATE'2024-03-17' d)                             | false    |
        | dayofweek  | non-null DATE column      | d                                    | FROM (SELECT DATE'2024-03-17' d)                             | false    |
        | day        | nullable DATE column      | d                                    | FROM VALUES (DATE'2024-03-17'), (CAST(NULL AS DATE)) AS t(d) | true     |
        | dayofmonth | nullable DATE column      | d                                    | FROM VALUES (DATE'2024-03-17'), (CAST(NULL AS DATE)) AS t(d) | true     |
        | dayofyear  | nullable DATE column      | d                                    | FROM VALUES (DATE'2024-03-17'), (CAST(NULL AS DATE)) AS t(d) | true     |
        | dayofweek  | nullable DATE column      | d                                    | FROM VALUES (DATE'2024-03-17'), (CAST(NULL AS DATE)) AS t(d) | true     |
        | weekofyear | nullable DATE column      | d                                    | FROM VALUES (DATE'2024-03-17'), (CAST(NULL AS DATE)) AS t(d) | true     |
        | day        | non-null TIMESTAMP column | t                                    | FROM (SELECT TIMESTAMP'2024-03-17 10:00:00' t)               | false    |
        | dayofmonth | non-null TIMESTAMP column | t                                    | FROM (SELECT TIMESTAMP'2024-03-17 10:00:00' t)               | false    |
        | dayofyear  | non-null TIMESTAMP column | t                                    | FROM (SELECT TIMESTAMP'2024-03-17 10:00:00' t)               | false    |
        | dayofweek  | non-null TIMESTAMP column | t                                    | FROM (SELECT TIMESTAMP'2024-03-17 10:00:00' t)               | false    |
        | weekofyear | non-null NTZ column       | t                                    | FROM (SELECT TIMESTAMP_NTZ'2024-03-17 10:00:00' t)           | true     |
        | weekofyear | non-null STRING column    | s                                    | FROM (SELECT '2024-03-17' s)                                 | true     |
        | day        | literal DATE              | DATE'2024-03-17'                     |                                                              | false    |
        | dayofmonth | literal DATE              | DATE'2024-03-17'                     |                                                              | false    |
        | dayofyear  | literal DATE              | DATE'2024-03-17'                     |                                                              | false    |
        | dayofweek  | literal DATE              | DATE'2024-03-17'                     |                                                              | false    |
        | weekofyear | literal STRING            | '2024-03-17'                         |                                                              | true     |
        | day        | literal TIMESTAMP         | TIMESTAMP'2024-03-17 10:00:00+00:00' |                                                              | false    |
        | dayofmonth | literal TIMESTAMP         | TIMESTAMP'2024-03-17 10:00:00+00:00' |                                                              | false    |
        | dayofyear  | literal TIMESTAMP         | TIMESTAMP'2024-03-17 10:00:00+00:00' |                                                              | false    |
        | dayofweek  | literal TIMESTAMP         | TIMESTAMP'2024-03-17 10:00:00+00:00' |                                                              | false    |
        | weekofyear | literal NTZ               | TIMESTAMP_NTZ'2024-03-17 10:00:00'   |                                                              | true     |
        | weekday    | non-null DATE column      | d                                    | FROM (SELECT DATE'2024-03-17' d)                             | false    |
        | weekday    | nullable DATE column      | d                                    | FROM VALUES (DATE'2024-03-17'), (CAST(NULL AS DATE)) AS t(d) | true     |
        | weekday    | non-null TIMESTAMP column | t                                    | FROM (SELECT TIMESTAMP'2024-03-17 10:00:00' t)               | false    |
        | weekday    | literal DATE              | DATE'2024-03-17'                     |                                                              | false    |
        | weekday    | literal TIMESTAMP         | TIMESTAMP'2024-03-17 10:00:00+00:00' |                                                              | false    |

    # TODO(known gap): a STRING or TIMESTAMP_NTZ argument is non-nullable in Sail and nullable in Spark (`Cast.scala:433,440`); weekofyear is nullable where Spark is not.
    #   Every row fails on `main` too (checked).
    @sail-bug
    Scenario Outline: <function>: Schema: <case> (known gap)
      When query
        """
        SELECT <function>(<input>) AS result <from>
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = <nullable>)
        """

      Examples:
        | function   | case                      | input                                | from                                               | nullable |
        | weekofyear | non-null DATE column      | d                                    | FROM (SELECT DATE'2024-03-17' d)                   | false    |
        | weekofyear | non-null TIMESTAMP column | t                                    | FROM (SELECT TIMESTAMP'2024-03-17 10:00:00' t)     | false    |
        | day        | non-null NTZ column       | t                                    | FROM (SELECT TIMESTAMP_NTZ'2024-03-17 10:00:00' t) | true     |
        | dayofmonth | non-null NTZ column       | t                                    | FROM (SELECT TIMESTAMP_NTZ'2024-03-17 10:00:00' t) | true     |
        | dayofyear  | non-null NTZ column       | t                                    | FROM (SELECT TIMESTAMP_NTZ'2024-03-17 10:00:00' t) | true     |
        | dayofweek  | non-null NTZ column       | t                                    | FROM (SELECT TIMESTAMP_NTZ'2024-03-17 10:00:00' t) | true     |
        | weekday    | non-null NTZ column       | t                                    | FROM (SELECT TIMESTAMP_NTZ'2024-03-17 10:00:00' t) | true     |
        | day        | non-null STRING column    | s                                    | FROM (SELECT '2024-03-17' s)                       | true     |
        | dayofmonth | non-null STRING column    | s                                    | FROM (SELECT '2024-03-17' s)                       | true     |
        | dayofyear  | non-null STRING column    | s                                    | FROM (SELECT '2024-03-17' s)                       | true     |
        | dayofweek  | non-null STRING column    | s                                    | FROM (SELECT '2024-03-17' s)                       | true     |
        | weekday    | non-null STRING column    | s                                    | FROM (SELECT '2024-03-17' s)                       | true     |
        | weekofyear | literal DATE              | DATE'2024-03-17'                     |                                                    | false    |
        | day        | literal STRING            | '2024-03-17'                         |                                                    | true     |
        | dayofmonth | literal STRING            | '2024-03-17'                         |                                                    | true     |
        | dayofyear  | literal STRING            | '2024-03-17'                         |                                                    | true     |
        | dayofweek  | literal STRING            | '2024-03-17'                         |                                                    | true     |
        | weekday    | literal STRING            | '2024-03-17'                         |                                                    | true     |
        | weekofyear | literal TIMESTAMP         | TIMESTAMP'2024-03-17 10:00:00+00:00' |                                                    | false    |
        | day        | literal NTZ               | TIMESTAMP_NTZ'2024-03-17 10:00:00'   |                                                    | true     |
        | dayofmonth | literal NTZ               | TIMESTAMP_NTZ'2024-03-17 10:00:00'   |                                                    | true     |
        | dayofyear  | literal NTZ               | TIMESTAMP_NTZ'2024-03-17 10:00:00'   |                                                    | true     |
        | dayofweek  | literal NTZ               | TIMESTAMP_NTZ'2024-03-17 10:00:00'   |                                                    | true     |
        | weekday    | literal NTZ               | TIMESTAMP_NTZ'2024-03-17 10:00:00'   |                                                    | true     |

  Rule: Call syntax

    Scenario Outline: <function>: Route: <case>
      Given config spark.sql.session.timeZone = <zone>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <route> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | case                                         | zone                | ansi  | route                                                   | result |
        | day        | R01 extract(DAY FROM DATE)                   | UTC                 | false | extract(DAY FROM DATE'2024-03-17')                      | 17     |
        | day        | R02 date_part('DAY', DATE)                   | UTC                 | false | date_part('DAY', DATE'2024-03-17')                      | 17     |
        | day        | R01 extract(DAY FROM TIMESTAMP LA crossing)  | America/Los_Angeles | false | extract(DAY FROM TIMESTAMP'2024-01-01 03:00:00+00:00')  | 31     |
        | day        | R02 date_part('DAY', TIMESTAMP LA crossing)  | America/Los_Angeles | false | date_part('DAY', TIMESTAMP'2024-01-01 03:00:00+00:00')  | 31     |
        | day        | R01 extract(DAY FROM NULL)                   | UTC                 | false | extract(DAY FROM CAST(NULL AS DATE))                    | NULL   |
        | day        | R02 date_part('DAY', NULL)                   | UTC                 | false | date_part('DAY', CAST(NULL AS DATE))                    | NULL   |
        | day        | R03 extract lowercase field                  | UTC                 | false | extract(day FROM DATE'2024-03-17')                      | 17     |
        | day        | R04 datepart alias                           | UTC                 | false | datepart('DAY', DATE'2024-03-17')                       | 17     |
        | dayofmonth | R01 extract(DAY FROM DATE)                   | UTC                 | false | extract(DAY FROM DATE'2024-03-17')                      | 17     |
        | dayofmonth | R02 date_part('DAY', DATE)                   | UTC                 | false | date_part('DAY', DATE'2024-03-17')                      | 17     |
        | dayofmonth | R01 extract(DAY FROM TIMESTAMP LA crossing)  | America/Los_Angeles | false | extract(DAY FROM TIMESTAMP'2024-01-01 03:00:00+00:00')  | 31     |
        | dayofmonth | R02 date_part('DAY', TIMESTAMP LA crossing)  | America/Los_Angeles | false | date_part('DAY', TIMESTAMP'2024-01-01 03:00:00+00:00')  | 31     |
        | dayofmonth | R01 extract(DAY FROM NULL)                   | UTC                 | false | extract(DAY FROM CAST(NULL AS DATE))                    | NULL   |
        | dayofmonth | R02 date_part('DAY', NULL)                   | UTC                 | false | date_part('DAY', CAST(NULL AS DATE))                    | NULL   |
        | dayofmonth | R03 extract lowercase field                  | UTC                 | false | extract(day FROM DATE'2024-03-17')                      | 17     |
        | dayofmonth | R04 datepart alias                           | UTC                 | false | datepart('DAY', DATE'2024-03-17')                       | 17     |
        | dayofyear  | R01 extract(DOY FROM DATE)                   | UTC                 | false | extract(DOY FROM DATE'2024-03-17')                      | 77     |
        | dayofyear  | R02 date_part('DOY', DATE)                   | UTC                 | false | date_part('DOY', DATE'2024-03-17')                      | 77     |
        | dayofyear  | R01 extract(DOY FROM TIMESTAMP LA crossing)  | America/Los_Angeles | false | extract(DOY FROM TIMESTAMP'2024-01-01 03:00:00+00:00')  | 365    |
        | dayofyear  | R02 date_part('DOY', TIMESTAMP LA crossing)  | America/Los_Angeles | false | date_part('DOY', TIMESTAMP'2024-01-01 03:00:00+00:00')  | 365    |
        | dayofyear  | R01 extract(DOY FROM NULL)                   | UTC                 | false | extract(DOY FROM CAST(NULL AS DATE))                    | NULL   |
        | dayofyear  | R02 date_part('DOY', NULL)                   | UTC                 | false | date_part('DOY', CAST(NULL AS DATE))                    | NULL   |
        | dayofyear  | R03 extract lowercase field                  | UTC                 | false | extract(doy FROM DATE'2024-03-17')                      | 77     |
        | dayofyear  | R04 datepart alias                           | UTC                 | false | datepart('DOY', DATE'2024-03-17')                       | 77     |
        | dayofweek  | R01 extract(DOW FROM NULL)                   | UTC                 | false | extract(DOW FROM CAST(NULL AS DATE))                    | NULL   |
        | dayofweek  | R02 date_part('DOW', NULL)                   | UTC                 | false | date_part('DOW', CAST(NULL AS DATE))                    | NULL   |
        | weekofyear | R01 extract(WEEK FROM DATE)                  | UTC                 | false | extract(WEEK FROM DATE'2024-03-17')                     | 11     |
        | weekofyear | R02 date_part('WEEK', DATE)                  | UTC                 | false | date_part('WEEK', DATE'2024-03-17')                     | 11     |
        | weekofyear | R01 extract(WEEK FROM TIMESTAMP LA crossing) | America/Los_Angeles | false | extract(WEEK FROM TIMESTAMP'2024-01-01 03:00:00+00:00') | 52     |
        | weekofyear | R02 date_part('WEEK', TIMESTAMP LA crossing) | America/Los_Angeles | false | date_part('WEEK', TIMESTAMP'2024-01-01 03:00:00+00:00') | 52     |
        | weekofyear | R01 extract(WEEK FROM NULL)                  | UTC                 | false | extract(WEEK FROM CAST(NULL AS DATE))                   | NULL   |
        | weekofyear | R02 date_part('WEEK', NULL)                  | UTC                 | false | date_part('WEEK', CAST(NULL AS DATE))                   | NULL   |
        | weekofyear | R03 extract lowercase field                  | UTC                 | false | extract(week FROM DATE'2024-03-17')                     | 11     |
        | weekofyear | R04 datepart alias                           | UTC                 | false | datepart('WEEK', DATE'2024-03-17')                      | 11     |
        | day        | R01 extract(DAY FROM NTZ)                    | America/Los_Angeles | false | extract(DAY FROM TIMESTAMP_NTZ'2024-01-01 03:00:00')    | 1      |
        | day        | R02 date_part('DAY', NTZ)                    | America/Los_Angeles | false | date_part('DAY', TIMESTAMP_NTZ'2024-01-01 03:00:00')    | 1      |
        | day        | R01 extract(DAY FROM STRING)                 | UTC                 | false | extract(DAY FROM '2024-03-17')                          | 17     |
        | day        | R02 date_part('DAY', STRING)                 | UTC                 | false | date_part('DAY', '2024-03-17')                          | 17     |
        | dayofmonth | R01 extract(DAY FROM NTZ)                    | America/Los_Angeles | false | extract(DAY FROM TIMESTAMP_NTZ'2024-01-01 03:00:00')    | 1      |
        | dayofmonth | R02 date_part('DAY', NTZ)                    | America/Los_Angeles | false | date_part('DAY', TIMESTAMP_NTZ'2024-01-01 03:00:00')    | 1      |
        | dayofmonth | R01 extract(DAY FROM STRING)                 | UTC                 | false | extract(DAY FROM '2024-03-17')                          | 17     |
        | dayofmonth | R02 date_part('DAY', STRING)                 | UTC                 | false | date_part('DAY', '2024-03-17')                          | 17     |
        | dayofyear  | R01 extract(DOY FROM NTZ)                    | America/Los_Angeles | false | extract(DOY FROM TIMESTAMP_NTZ'2024-01-01 03:00:00')    | 1      |
        | dayofyear  | R02 date_part('DOY', NTZ)                    | America/Los_Angeles | false | date_part('DOY', TIMESTAMP_NTZ'2024-01-01 03:00:00')    | 1      |
        | dayofyear  | R01 extract(DOY FROM STRING)                 | UTC                 | false | extract(DOY FROM '2024-03-17')                          | 77     |
        | dayofyear  | R02 date_part('DOY', STRING)                 | UTC                 | false | date_part('DOY', '2024-03-17')                          | 77     |
        | weekofyear | R01 extract(WEEK FROM NTZ)                   | America/Los_Angeles | false | extract(WEEK FROM TIMESTAMP_NTZ'2024-01-01 03:00:00')   | 1      |
        | weekofyear | R02 date_part('WEEK', NTZ)                   | America/Los_Angeles | false | date_part('WEEK', TIMESTAMP_NTZ'2024-01-01 03:00:00')   | 1      |
        | weekofyear | R01 extract(WEEK FROM STRING)                | UTC                 | false | extract(WEEK FROM '2024-03-17')                         | 11     |
        | weekofyear | R02 date_part('WEEK', STRING)                | UTC                 | false | date_part('WEEK', '2024-03-17')                         | 11     |

    # TODO(known gap): `extract` and `date_part` go through DataFusion's `date_part`: `DOW` is 0 to 6 in Sail and 1 to 7 in Spark (Sunday is 1), and `DOW_ISO`, `DAYOFWEEK`, `DAYOFWEEK_ISO` and `YEAROFWEEK` are not supported.
    #   Every row fails on `main` too (checked).
    @sail-bug
    Scenario Outline: <function>: Route: <case> (known gap)
      Given config spark.sql.session.timeZone = <zone>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <route> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | case                                            | zone                | ansi  | route                                                      | result |
        | day        | R01 extract(DAY FROM STRING bad, ANSI off)      | UTC                 | false | extract(DAY FROM 'bad')                                    | NULL   |
        | day        | R02 date_part('DAY', STRING bad, ANSI off)      | UTC                 | false | date_part('DAY', 'bad')                                    | NULL   |
        | day        | R01 extract(DAY FROM epoch)                     | UTC                 | false | extract(DAY FROM 'epoch')                                  | 1      |
        | day        | R02 date_part('DAY', epoch)                     | UTC                 | false | date_part('DAY', 'epoch')                                  | 1      |
        | dayofmonth | R01 extract(DAY FROM STRING bad, ANSI off)      | UTC                 | false | extract(DAY FROM 'bad')                                    | NULL   |
        | dayofmonth | R02 date_part('DAY', STRING bad, ANSI off)      | UTC                 | false | date_part('DAY', 'bad')                                    | NULL   |
        | dayofmonth | R01 extract(DAY FROM epoch)                     | UTC                 | false | extract(DAY FROM 'epoch')                                  | 1      |
        | dayofmonth | R02 date_part('DAY', epoch)                     | UTC                 | false | date_part('DAY', 'epoch')                                  | 1      |
        | dayofyear  | R01 extract(DOY FROM STRING bad, ANSI off)      | UTC                 | false | extract(DOY FROM 'bad')                                    | NULL   |
        | dayofyear  | R02 date_part('DOY', STRING bad, ANSI off)      | UTC                 | false | date_part('DOY', 'bad')                                    | NULL   |
        | dayofyear  | R01 extract(DOY FROM epoch)                     | UTC                 | false | extract(DOY FROM 'epoch')                                  | 1      |
        | dayofyear  | R02 date_part('DOY', epoch)                     | UTC                 | false | date_part('DOY', 'epoch')                                  | 1      |
        | dayofweek  | R01 extract(DOW FROM DATE)                      | UTC                 | false | extract(DOW FROM DATE'2024-03-17')                         | 1      |
        | dayofweek  | R02 date_part('DOW', DATE)                      | UTC                 | false | date_part('DOW', DATE'2024-03-17')                         | 1      |
        | dayofweek  | R01 extract(DOW FROM TIMESTAMP LA crossing)     | America/Los_Angeles | false | extract(DOW FROM TIMESTAMP'2024-01-01 03:00:00+00:00')     | 1      |
        | dayofweek  | R02 date_part('DOW', TIMESTAMP LA crossing)     | America/Los_Angeles | false | date_part('DOW', TIMESTAMP'2024-01-01 03:00:00+00:00')     | 1      |
        | dayofweek  | R01 extract(DOW FROM NTZ)                       | America/Los_Angeles | false | extract(DOW FROM TIMESTAMP_NTZ'2024-01-01 03:00:00')       | 2      |
        | dayofweek  | R02 date_part('DOW', NTZ)                       | America/Los_Angeles | false | date_part('DOW', TIMESTAMP_NTZ'2024-01-01 03:00:00')       | 2      |
        | dayofweek  | R01 extract(DOW FROM STRING)                    | UTC                 | false | extract(DOW FROM '2024-03-17')                             | 1      |
        | dayofweek  | R02 date_part('DOW', STRING)                    | UTC                 | false | date_part('DOW', '2024-03-17')                             | 1      |
        | dayofweek  | R01 extract(DOW FROM STRING bad, ANSI off)      | UTC                 | false | extract(DOW FROM 'bad')                                    | NULL   |
        | dayofweek  | R02 date_part('DOW', STRING bad, ANSI off)      | UTC                 | false | date_part('DOW', 'bad')                                    | NULL   |
        | dayofweek  | R01 extract(DOW FROM epoch)                     | UTC                 | false | extract(DOW FROM 'epoch')                                  | 5      |
        | dayofweek  | R02 date_part('DOW', epoch)                     | UTC                 | false | date_part('DOW', 'epoch')                                  | 5      |
        | dayofweek  | R03 extract lowercase field                     | UTC                 | false | extract(dow FROM DATE'2024-03-17')                         | 1      |
        | dayofweek  | R04 datepart alias                              | UTC                 | false | datepart('DOW', DATE'2024-03-17')                          | 1      |
        | weekday    | R01 extract(DOW_ISO FROM DATE)                  | UTC                 | false | extract(DOW_ISO FROM DATE'2024-03-17')                     | 7      |
        | weekday    | R02 date_part('DOW_ISO', DATE)                  | UTC                 | false | date_part('DOW_ISO', DATE'2024-03-17')                     | 7      |
        | weekday    | R01 extract(DOW_ISO FROM TIMESTAMP LA crossing) | America/Los_Angeles | false | extract(DOW_ISO FROM TIMESTAMP'2024-01-01 03:00:00+00:00') | 7      |
        | weekday    | R02 date_part('DOW_ISO', TIMESTAMP LA crossing) | America/Los_Angeles | false | date_part('DOW_ISO', TIMESTAMP'2024-01-01 03:00:00+00:00') | 7      |
        | weekday    | R01 extract(DOW_ISO FROM NTZ)                   | America/Los_Angeles | false | extract(DOW_ISO FROM TIMESTAMP_NTZ'2024-01-01 03:00:00')   | 1      |
        | weekday    | R02 date_part('DOW_ISO', NTZ)                   | America/Los_Angeles | false | date_part('DOW_ISO', TIMESTAMP_NTZ'2024-01-01 03:00:00')   | 1      |
        | weekday    | R01 extract(DOW_ISO FROM STRING)                | UTC                 | false | extract(DOW_ISO FROM '2024-03-17')                         | 7      |
        | weekday    | R02 date_part('DOW_ISO', STRING)                | UTC                 | false | date_part('DOW_ISO', '2024-03-17')                         | 7      |
        | weekday    | R01 extract(DOW_ISO FROM STRING bad, ANSI off)  | UTC                 | false | extract(DOW_ISO FROM 'bad')                                | NULL   |
        | weekday    | R02 date_part('DOW_ISO', STRING bad, ANSI off)  | UTC                 | false | date_part('DOW_ISO', 'bad')                                | NULL   |
        | weekday    | R01 extract(DOW_ISO FROM epoch)                 | UTC                 | false | extract(DOW_ISO FROM 'epoch')                              | 4      |
        | weekday    | R02 date_part('DOW_ISO', epoch)                 | UTC                 | false | date_part('DOW_ISO', 'epoch')                              | 4      |
        | weekday    | R01 extract(DOW_ISO FROM NULL)                  | UTC                 | false | extract(DOW_ISO FROM CAST(NULL AS DATE))                   | NULL   |
        | weekday    | R02 date_part('DOW_ISO', NULL)                  | UTC                 | false | date_part('DOW_ISO', CAST(NULL AS DATE))                   | NULL   |
        | weekday    | R03 extract lowercase field                     | UTC                 | false | extract(dow_iso FROM DATE'2024-03-17')                     | 7      |
        | weekday    | R04 datepart alias                              | UTC                 | false | datepart('DOW_ISO', DATE'2024-03-17')                      | 7      |
        | weekofyear | R01 extract(WEEK FROM STRING bad, ANSI off)     | UTC                 | false | extract(WEEK FROM 'bad')                                   | NULL   |
        | weekofyear | R02 date_part('WEEK', STRING bad, ANSI off)     | UTC                 | false | date_part('WEEK', 'bad')                                   | NULL   |
        | weekofyear | R01 extract(WEEK FROM epoch)                    | UTC                 | false | extract(WEEK FROM 'epoch')                                 | 1      |
        | weekofyear | R02 date_part('WEEK', epoch)                    | UTC                 | false | date_part('WEEK', 'epoch')                                 | 1      |

  Rule: Lazy evaluation and lambdas

    # A malformed string only matters on the row and the branch that is evaluated (`GetDateField` is `NullIntolerant`; codegen skips the
    # unexecuted branch). The values are the answers of the Spark JVM 4.2.

    # TODO(known gap): Spark resolves the branch that is never taken lazily; Sail evaluates the argument of the function first and raises on a malformed string.
    @sail-bug
    Scenario Outline: <function>: an unexecuted IF branch is not evaluated with ANSI on
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(IF(false, <function>('bad'), 1) AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | result |
        | day        | 1      |
        | dayofmonth | 1      |
        | dayofyear  | 1      |
        | dayofweek  | 1      |
        | weekday    | 1      |
        | weekofyear | 1      |

    # TODO(known gap): Spark resolves the branch that is never taken lazily; Sail evaluates the argument of the function first and raises on a malformed string.
    @sail-bug
    Scenario Outline: <function>: an unexecuted CASE branch is not evaluated with ANSI on
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(CASE WHEN false THEN <function>('bad') ELSE 1 END AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | result |
        | day        | 1      |
        | dayofmonth | 1      |
        | dayofyear  | 1      |
        | dayofweek  | 1      |
        | weekday    | 1      |
        | weekofyear | 1      |

    # TODO(known gap): Spark resolves the branch that is never taken lazily; Sail evaluates the argument of the function first and raises on a malformed string.
    @sail-bug
    Scenario Outline: <function>: an unevaluated coalesce argument is not evaluated with ANSI on
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(coalesce(1, <function>('bad')) AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | result |
        | day        | 1      |
        | dayofmonth | 1      |
        | dayofyear  | 1      |
        | dayofweek  | 1      |
        | weekday    | 1      |
        | weekofyear | 1      |

    # TODO(known gap): Spark resolves the branch that is never taken lazily; Sail evaluates the argument of the function first and raises on a malformed string.
    @sail-bug
    Scenario Outline: <function>: the right side of a false AND is not evaluated with ANSI on
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(false AND <function>('bad') = 1 AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | result |
        | day        | false  |
        | dayofmonth | false  |
        | dayofyear  | false  |
        | dayofweek  | false  |
        | weekday    | false  |
        | weekofyear | false  |

    # TODO(known gap): a malformed string raises inside the lambda instead of giving NULL for that element.
    @sail-bug
    Scenario Outline: <function>: inside a lambda a malformed string is NULL for its element with ANSI off
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT transform(array('2024-03-17', 'bad'), x -> <function>(x)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function   | result     |
        | day        | [17, NULL] |
        | dayofmonth | [17, NULL] |
        | dayofyear  | [77, NULL] |
        | dayofweek  | [1, NULL]  |
        | weekday    | [6, NULL]  |
        | weekofyear | [11, NULL] |

    # TODO(known gap): the error class of a malformed string inside a lambda is `CAST_INVALID_INPUT` in Spark.
    @sail-bug
    Scenario Outline: <function>: inside a lambda a malformed string raises with ANSI on
      Given config spark.sql.session.timeZone = UTC
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT transform(array('2024-03-17', 'bad'), x -> <function>(x)) AS result
        """
      Then query error .*CAST_INVALID_INPUT.*

      Examples:
        | function   |
        | day        |
        | dayofmonth |
        | dayofyear  |
        | dayofweek  |
        | weekday    |
        | weekofyear |
