Feature: INTERVAL DAY TO SECOND literal parsing and operations

  Rule: Basic literals

    Scenario Outline: Literal: <case>
      When query
        """
        SELECT INTERVAL <lit> DAY TO SECOND AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                   | lit           | result                               |
        | negative interval      | '-3 04:05:06' | INTERVAL '-3 04:05:06' DAY TO SECOND |
        | negative zero interval | '-0 00:00:00' | INTERVAL '0 00:00:00' DAY TO SECOND  |

    Scenario: negative hours in interval is invalid
      When query
        """
        SELECT INTERVAL '3 -04:00:00' DAY TO SECOND AS result
        """
      Then query error (?i)invalid.*interval

    Scenario: Zero multi-unit intervals preserve their syntactic family
      When query
        """
        SELECT
          typeof(INTERVAL 0 YEAR 0 MONTH) AS year_month_type,
          typeof(INTERVAL 0 DAY 0 SECOND) AS day_time_type
        """
      Then query result
        | year_month_type        | day_time_type          |
        | interval year to month | interval day to second |

  Rule: Overflow and large values

    # Sail intentionally normalizes interval fields beyond Spark's accepted ranges.
    @sail-only
    Scenario: overflow hours into days
      When query
        """
        SELECT INTERVAL '0 25:00:00' DAY TO SECOND AS result
        """
      Then query result
        | result                               |
        | INTERVAL '1 01:00:00' DAY TO SECOND |

  Rule: Cast operations

    Scenario Outline: Cast: <case>
      When query
        """
        SELECT CAST(<expr> AS INTERVAL DAY TO SECOND) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                 | expr                                                | result                              |
        | roundtrip cast to string and back    | CAST(INTERVAL '2 10:20:30' DAY TO SECOND AS STRING) | INTERVAL '2 10:20:30' DAY TO SECOND |
        | cast HOUR TO SECOND to DAY TO SECOND | INTERVAL '12:30:45' HOUR TO SECOND                  | INTERVAL '0 12:30:45' DAY TO SECOND |

  Rule: Arithmetic and comparison

    Scenario Outline: Arithmetic: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | expr                                                                      | result                              |
        | addition of intervals | INTERVAL '0 23:00:00' DAY TO SECOND + INTERVAL '0 02:00:00' DAY TO SECOND | INTERVAL '1 01:00:00' DAY TO SECOND |

    Scenario Outline: string and day-time interval addition with <operand order>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result
        FROM VALUES
          ('2026-01-01 00:00:00'),
          ('2026-03-15 12:30:00'),
          ('not-a-timestamp'),
          (CAST(NULL AS STRING))
        AS t(ts_str)
        """
      Then query result
        | result              |
        | 2026-01-01 00:00:05 |
        | 2026-03-15 12:30:05 |
        | NULL                |
        | NULL                |
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

      Examples:
        | operand order  | expr                                  |
        | string first   | ts_str + INTERVAL 1 SECOND * 5         |
        | interval first | INTERVAL 1 SECOND * 5 + ts_str         |

    Scenario: string and day-time interval addition under ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT ts_str + INTERVAL 1 SECOND * 5 AS result
        FROM VALUES
          ('2026-01-01 00:00:00'),
          (CAST(NULL AS STRING))
        AS t(ts_str)
        """
      Then query result
        | result              |
        | 2026-01-01 00:00:05 |
        | NULL                |
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: invalid string and day-time interval addition errors under ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT ts_str + INTERVAL 1 SECOND * 5 AS result
        FROM VALUES ('not-a-timestamp') AS t(ts_str)
        """
      Then query error (?i)(timestamp|CAST_INVALID_INPUT|found n at 0:1)

    Scenario Outline: string and whole-day interval addition keeps local time across DST with <operand order>
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT <expr> AS result
        FROM VALUES
          ('2019-03-09 12:00:00'),
          ('2019-11-02 12:00:00')
        AS t(ts_str)
        """
      Then query result
        | result              |
        | 2019-03-10 12:00:00 |
        | 2019-11-03 12:00:00 |

      Examples:
        | operand order  | expr                     |
        | string first   | ts_str + INTERVAL 1 DAY  |
        | interval first | INTERVAL 1 DAY + ts_str  |

    Scenario Outline: leap-second string and interval addition returns NULL in legacy mode with <operand order>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | operand order  | expr                                                |
        | string first   | '2026-06-15 23:59:60' + INTERVAL 1 SECOND           |
        | interval first | INTERVAL 1 SECOND + '2026-06-15 23:59:60'           |

    Scenario Outline: leap-second string and interval addition errors in ANSI mode with <operand order>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result
        """
      Then query error (?i)(timestamp|CAST_INVALID_INPUT|leap seconds)

      Examples:
        | operand order  | expr                                                |
        | string first   | '2026-06-15 23:59:60' + INTERVAL 1 SECOND           |
        | interval first | INTERVAL 1 SECOND + '2026-06-15 23:59:60'           |

    @sail-only
    Scenario: equality with normalized form
      When query
        """
        SELECT INTERVAL '1 24:00:00' DAY TO SECOND = INTERVAL '2 00:00:00' DAY TO SECOND AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: comparison in WHERE clause
      When query
        """
        SELECT * FROM (VALUES (1)) t(a)
        WHERE INTERVAL '2 03:04:05' DAY TO SECOND > INTERVAL '1 23:59:59' DAY TO SECOND
        """
      Then query result
        | a |
        | 1 |

  Rule: Subquery and projection

    Scenario: VALUES widens interval qualifiers across rows
      When query
        """
        SELECT day_time, year_month
        FROM VALUES
          (INTERVAL 1 DAY, INTERVAL 1 YEAR),
          (INTERVAL 1 HOUR, INTERVAL 1 MONTH),
          (NULL, NULL)
        AS t(day_time, year_month)
        """
      Then query schema
        """
        root
         |-- day_time: interval day to hour (nullable = true)
         |-- year_month: interval year to month (nullable = true)
        """

    Scenario: interval in subquery
      When query
        """
        SELECT x FROM (SELECT INTERVAL '3 10:00:00' DAY TO SECOND AS x) t
        """
      Then query result
        | x                                   |
        | INTERVAL '3 10:00:00' DAY TO SECOND |

  Rule: Date arithmetic preserves interval qualifiers

    # Equal durations can have different qualifiers and produce different result types.
    Scenario Outline: Date arithmetic preserves independent struct interval qualifiers with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(day_result) AS day_type, typeof(hour_result) AS hour_type
        FROM (
          SELECT DATE '2024-01-02' + s.d AS day_result,
                 DATE '2024-01-02' + s.h AS hour_result
          FROM (SELECT named_struct('d', INTERVAL '1' DAY, 'h', INTERVAL '24' HOUR) AS s)
        ) AS q
        """
      Then query result
        | day_result | hour_result         | day_type | hour_type |
        | 2024-01-03 | 2024-01-03 00:00:00 | date     | timestamp |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Reversed addition and subtraction preserve struct interval qualifiers with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(reversed_day) AS reversed_day_type, typeof(reversed_hour) AS reversed_hour_type,
               typeof(sub_day) AS sub_day_type, typeof(sub_hour) AS sub_hour_type
        FROM (
          SELECT s.d + DATE '2024-01-02' AS reversed_day,
                 s.h + DATE '2024-01-02' AS reversed_hour,
                 DATE '2024-01-02' - s.d AS sub_day,
                 DATE '2024-01-02' - s.h AS sub_hour
          FROM (SELECT named_struct('d', INTERVAL '1' DAY, 'h', INTERVAL '24' HOUR) AS s)
        ) AS q
        """
      Then query result
        | reversed_day | reversed_hour       | sub_day    | sub_hour            | reversed_day_type | reversed_hour_type | sub_day_type | sub_hour_type |
        | 2024-01-03   | 2024-01-03 00:00:00 | 2024-01-01 | 2024-01-01 00:00:00 | date              | timestamp          | date         | timestamp     |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Nullable struct interval fields retain date arithmetic types with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(day_result) AS day_type, typeof(hour_result) AS hour_type
        FROM (
          SELECT id, DATE '2024-01-02' + s.d AS day_result,
                     DATE '2024-01-02' + s.h AS hour_result
          FROM (
            SELECT id, named_struct('d', d, 'h', h) AS s
            FROM VALUES (0, INTERVAL '1' DAY, INTERVAL '24' HOUR),
                        (1, CAST(NULL AS INTERVAL DAY), CAST(NULL AS INTERVAL HOUR)) AS t(id, d, h)
          )
        ) AS q
        ORDER BY id
        """
      Then query result ordered
        | id | day_result | hour_result         | day_type | hour_type |
        | 0  | 2024-01-03 | 2024-01-03 00:00:00 | date     | timestamp |
        | 1  | NULL       | NULL                | date     | timestamp |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Projected absolute intervals retain date arithmetic types with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(day_result) AS day_type, typeof(hour_result) AS hour_type
        FROM (
          SELECT DATE '2024-01-02' + d AS day_result, DATE '2024-01-02' + h AS hour_result
          FROM (SELECT abs(INTERVAL '-1' DAY) AS d, abs(INTERVAL '-24' HOUR) AS h)
        ) AS q
        """
      Then query result
        | day_result | hour_result         | day_type | hour_type |
        | 2024-01-03 | 2024-01-03 00:00:00 | date     | timestamp |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Coalesced interval qualifiers determine date arithmetic types with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(narrow_result) AS narrow_type, typeof(wide_result) AS wide_type
        FROM (
          SELECT DATE '2024-01-02' + coalesce(CAST(NULL AS INTERVAL DAY), INTERVAL '1' DAY) AS narrow_result,
                 DATE '2024-01-02' + coalesce(INTERVAL '1' DAY, make_dt_interval(0)) AS wide_result
        ) AS q
        """
      Then query result
        | narrow_result | wide_result         | narrow_type | wide_type |
        | 2024-01-03    | 2024-01-03 00:00:00 | date        | timestamp |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Aggregate and scaled intervals preserve their result qualifiers with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(min_result) AS min_type, typeof(sum_result) AS sum_type,
               typeof(avg_result) AS avg_type, typeof(product_result) AS product_type,
               typeof(quotient_result) AS quotient_type
        FROM (
          SELECT DATE '2024-01-02' + min(iv) AS min_result,
                 DATE '2024-01-02' + sum(iv) AS sum_result,
                 DATE '2024-01-02' + avg(iv) AS avg_result,
                 DATE '2024-01-02' + (INTERVAL '1' DAY * 2) AS product_result,
                 DATE '2024-01-02' + (INTERVAL '2' DAY / 2) AS quotient_result
          FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY) AS t(iv)
        ) AS q
        """
      Then query result
        | min_result | sum_result | avg_result          | product_result      | quotient_result     | min_type | sum_type | avg_type  | product_type | quotient_type |
        | 2024-01-03 | 2024-01-05 | 2024-01-03 12:00:00 | 2024-01-04 00:00:00 | 2024-01-03 00:00:00 | date     | date     | timestamp | timestamp    | timestamp     |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Window intervals retain date arithmetic types with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(min_result) AS min_type, typeof(lag_result) AS lag_type
        FROM (
          SELECT id, DATE '2024-01-02' + min(iv) OVER () AS min_result,
                     DATE '2024-01-02' + lag(abs(iv)) OVER (ORDER BY id) AS lag_result
          FROM VALUES (0, INTERVAL '1' DAY), (1, INTERVAL '2' DAY) AS t(id, iv)
        ) AS q
        ORDER BY id
        """
      Then query result ordered
        | id | min_result | lag_result | min_type | lag_type |
        | 0  | 2024-01-03 | NULL       | date     | date     |
        | 1  | 2024-01-03 | 2024-01-03 | date     | date     |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Array interval elements retain date arithmetic types with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(item_result) AS item_type, typeof(append_result) AS append_type,
               typeof(missing_result) AS missing_type
        FROM (
          SELECT DATE '2024-01-02' + a[0] AS item_result,
                 DATE '2024-01-02' + array_append(a, INTERVAL '2' DAY)[0] AS append_result,
                 DATE '2024-01-02' + get(a, 9) AS missing_result
          FROM (SELECT array(abs(INTERVAL '-1' DAY)) AS a)
        ) AS q
        """
      Then query result
        | item_result | append_result | missing_result | item_type | append_type | missing_type |
        | 2024-01-03  | 2024-01-03    | NULL           | date      | date        | date         |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Map interval values retain date arithmetic types with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(result) AS result_type
        FROM (
          SELECT DATE '2024-01-02' + m['iv'] AS result
          FROM (SELECT map('iv', INTERVAL '1' DAY) AS m)
        ) AS q
        """
      Then query result
        | result     | result_type |
        | 2024-01-03 | date        |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Exploded interval elements retain date arithmetic types with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(result) AS result_type
        FROM (
          SELECT DATE '2024-01-02' + iv AS result
          FROM (SELECT explode(array(INTERVAL '1' DAY)) AS iv)
        ) AS q
        """
      Then query result
        | result     | result_type |
        | 2024-01-03 | date        |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Nested interval transforms retain date arithmetic types with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(result) AS result_type
        FROM (
          SELECT DATE '2024-01-02' +
            transform(array(INTERVAL '1' DAY), x -> transform(array(x), y -> abs(y)))[0][0] AS result
        ) AS q
        """
      Then query result
        | result     | result_type |
        | 2024-01-03 | date        |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Collected interval elements retain date arithmetic types with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(result) AS result_type
        FROM (
          SELECT DATE '2024-01-02' + get(collect_list(abs(iv)), 0) AS result
          FROM VALUES (INTERVAL '-1' DAY) AS t(iv)
        ) AS q
        """
      Then query result
        | result     | result_type |
        | 2024-01-03 | date        |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Nested common interval types widen date arithmetic with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(array_result) AS array_type, typeof(struct_result) AS struct_type
        FROM (
          SELECT DATE '2024-01-02' +
                   (CASE WHEN true THEN array(INTERVAL '1' DAY) ELSE array(INTERVAL '1' HOUR) END)[0] AS array_result,
                 DATE '2024-01-02' +
                   coalesce(named_struct('iv', INTERVAL '1' DAY), named_struct('iv', INTERVAL '1' HOUR)).iv AS struct_result
        ) AS q
        """
      Then query result
        | array_result        | struct_result       | array_type | struct_type |
        | 2024-01-03 00:00:00 | 2024-01-03 00:00:00 | timestamp  | timestamp   |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Nested interval casts determine date arithmetic types with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(widened) AS widened_type, typeof(narrowed) AS narrowed_type
        FROM (
          SELECT DATE '2024-01-02' +
                   CAST(array(abs(INTERVAL '-1' DAY)) AS ARRAY<INTERVAL DAY TO SECOND>)[0] AS widened,
                 DATE '2024-01-02' +
                   CAST(array(make_dt_interval(1)) AS ARRAY<INTERVAL DAY>)[0] AS narrowed
        ) AS q
        """
      Then query result
        | widened             | narrowed   | widened_type | narrowed_type |
        | 2024-01-03 00:00:00 | 2024-01-03 | timestamp    | date          |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Union interval widening leaves reused source qualifiers intact with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        WITH source AS (
          SELECT named_struct('d', INTERVAL '1' DAY, 'h', INTERVAL '24' HOUR) AS s
        ), widened AS (
          SELECT s FROM source
          UNION ALL
          SELECT named_struct('d', INTERVAL '24' HOUR, 'h', INTERVAL '1' DAY) AS s
        )
        SELECT q.*, typeof(union_day) AS union_day_type, typeof(source_day) AS source_day_type,
               typeof(union_hour) AS union_hour_type, typeof(source_hour) AS source_hour_type
        FROM (
          SELECT DATE '2024-01-02' + w.s.d AS union_day,
                 DATE '2024-01-02' + src.s.d AS source_day,
                 DATE '2024-01-02' + w.s.h AS union_hour,
                 DATE '2024-01-02' + src.s.h AS source_hour
          FROM widened w CROSS JOIN source src
        ) AS q
        """
      Then query result
        | union_day           | source_day | union_hour          | source_hour         | union_day_type | source_day_type | union_hour_type | source_hour_type |
        | 2024-01-03 00:00:00 | 2024-01-03 | 2024-01-03 00:00:00 | 2024-01-03 00:00:00 | timestamp      | date            | timestamp       | timestamp        |
        | 2024-01-03 00:00:00 | 2024-01-03 | 2024-01-03 00:00:00 | 2024-01-03 00:00:00 | timestamp      | date            | timestamp       | timestamp        |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Untyped and typed NULL intervals determine common qualifiers with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(untyped_null) AS untyped_type, typeof(typed_hour_null) AS typed_type
        FROM (
          SELECT DATE '2024-01-02' + coalesce(NULL, INTERVAL '1' DAY) AS untyped_null,
                 DATE '2024-01-02' + coalesce(CAST(NULL AS INTERVAL HOUR), INTERVAL '1' DAY) AS typed_hour_null
        ) AS q
        """
      Then query result
        | untyped_null | typed_hour_null     | untyped_type | typed_type |
        | 2024-01-03   | 2024-01-03 00:00:00 | date         | timestamp  |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Lambda date arithmetic retains projected interval qualifiers with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(day_result) AS day_type, typeof(hour_result) AS hour_type
        FROM (
          SELECT transform(days, x -> DATE '2024-01-02' + x)[0] AS day_result,
                 transform(hours, x -> DATE '2024-01-02' + x)[0] AS hour_result
          FROM (SELECT array(INTERVAL '1' DAY) AS days, array(INTERVAL '24' HOUR) AS hours)
        ) AS q
        """
      Then query result
        | day_result | hour_result         | day_type | hour_type |
        | 2024-01-03 | 2024-01-03 00:00:00 | date     | timestamp |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Nested lambda shadowing restores outer interval qualifiers with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT result.outer_before AS outer_before, result.inner_hour AS inner_hour,
               result.outer_after AS outer_after, typeof(result.outer_before) AS before_type,
               typeof(result.inner_hour) AS inner_type, typeof(result.outer_after) AS after_type
        FROM (
          SELECT transform(items, x -> named_struct(
            'outer_before', DATE '2024-01-02' + x.d,
            'inner_hour', transform(array(x.h), x -> DATE '2024-01-02' + x)[0],
            'outer_after', DATE '2024-01-02' + x.d))[0] AS result
          FROM (SELECT array(named_struct('d', INTERVAL '1' DAY, 'h', INTERVAL '24' HOUR)) AS items)
        )
        """
      Then query result
        | outer_before | inner_hour          | outer_after | before_type | inner_type | after_type |
        | 2024-01-03   | 2024-01-03 00:00:00 | 2024-01-03  | date        | timestamp  | date       |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Nested lambdas retain captured interval qualifiers with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT result.outer_day AS outer_day, result.inner_hour AS inner_hour,
               typeof(result.outer_day) AS outer_type, typeof(result.inner_hour) AS inner_type
        FROM (
          SELECT transform(array(INTERVAL '1' DAY), d ->
            transform(array(INTERVAL '24' HOUR), h -> named_struct(
              'outer_day', DATE '2024-01-02' + d,
              'inner_hour', DATE '2024-01-02' + h)))[0][0] AS result
        )
        """
      Then query result
        | outer_day  | inner_hour          | outer_type | inner_type |
        | 2024-01-03 | 2024-01-03 00:00:00 | date       | timestamp  |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Grouped struct fields and aggregate outputs retain interval qualifiers with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        WITH source AS (
          SELECT named_struct('d', INTERVAL '1' DAY, 'h', INTERVAL '24' HOUR) AS s
        ), grouped AS (
          SELECT s, max(s.d) AS d_max, min(s.h) AS h_min FROM source GROUP BY s
        )
        SELECT q.*, typeof(grouped_day) AS grouped_day_type, typeof(grouped_hour) AS grouped_hour_type,
               typeof(aggregate_day) AS aggregate_day_type, typeof(aggregate_hour) AS aggregate_hour_type
        FROM (
          SELECT DATE '2024-01-02' + s.d AS grouped_day,
                 DATE '2024-01-02' + s.h AS grouped_hour,
                 DATE '2024-01-02' + d_max AS aggregate_day,
                 DATE '2024-01-02' + h_min AS aggregate_hour
          FROM grouped
        ) AS q
        """
      Then query result
        | grouped_day | grouped_hour        | aggregate_day | aggregate_hour      | grouped_day_type | grouped_hour_type | aggregate_day_type | aggregate_hour_type |
        | 2024-01-03  | 2024-01-03 00:00:00 | 2024-01-03    | 2024-01-03 00:00:00 | date             | timestamp         | date               | timestamp           |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Projected nullable CASE intervals retain day qualifiers with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(result) AS result_type
        FROM (
          SELECT id, DATE '2024-01-02' + iv AS result
          FROM (SELECT id, CASE WHEN id = 0 THEN INTERVAL '1' DAY ELSE NULL END AS iv FROM range(2))
        ) AS q
        ORDER BY id
        """
      Then query result ordered
        | id | result     | result_type |
        | 0  | 2024-01-03 | date        |
        | 1  | NULL       | date        |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Set operations preserve widened interval qualifiers for <operation> with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT result, typeof(result) AS result_type
        FROM (
          SELECT DATE '2024-01-02' + iv AS result
          FROM (
            SELECT abs(INTERVAL '-1' DAY) AS iv
            <operation>
            SELECT INTERVAL '<hours>' HOUR AS iv
          )
        )
        """
      Then query result
        | result              | result_type |
        | 2024-01-03 00:00:00 | timestamp   |

      Examples:
        | operation     | hours | ansi  |
        | INTERSECT     | 24    | true  |
        | INTERSECT     | 24    | false |
        | INTERSECT ALL | 24    | true  |
        | INTERSECT ALL | 24    | false |
        | EXCEPT        | 1     | true  |
        | EXCEPT        | 1     | false |
        | EXCEPT ALL    | 1     | true  |
        | EXCEPT ALL    | 1     | false |

    Scenario Outline: Rollup preserves grouping and aggregate interval qualifiers with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        WITH results AS (
          SELECT DATE '2024-01-02' + d AS day_result,
                 DATE '2024-01-02' + h AS hour_result,
                 DATE '2024-01-02' + md AS min_day
          FROM (
            SELECT d, h, min(d) AS md
            FROM (SELECT INTERVAL '1' DAY AS d, INTERVAL '24' HOUR AS h)
            GROUP BY ROLLUP(d, h)
          )
        )
        SELECT *, typeof(day_result) AS day_type, typeof(hour_result) AS hour_type,
               typeof(min_day) AS min_type
        FROM results
        ORDER BY day_result NULLS FIRST, hour_result NULLS FIRST
        """
      Then query result ordered
        | day_result | hour_result         | min_day    | day_type | hour_type | min_type |
        | NULL       | NULL                | 2024-01-03 | date     | timestamp | date     |
        | 2024-01-03 | NULL                | 2024-01-03 | date     | timestamp | date     |
        | 2024-01-03 | 2024-01-03 00:00:00 | 2024-01-03 | date     | timestamp | date     |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Empty <function> preserves nested interval qualifiers with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        WITH collected AS (
          SELECT <function>(named_struct('d', abs(INTERVAL '-1' DAY), 'h', INTERVAL '24' HOUR)) AS items
          FROM range(0)
        )
        SELECT q.*, typeof(day_result) AS day_type, typeof(hour_result) AS hour_type
        FROM (
          SELECT size(items) AS item_count,
                 DATE '2024-01-02' + get(items, 0).d AS day_result,
                 DATE '2024-01-02' + get(items, 0).h AS hour_result
          FROM collected
        ) AS q
        """
      Then query result
        | item_count | day_result | hour_result | day_type | hour_type |
        | 0          | NULL       | NULL        | date     | timestamp |

      Examples:
        | function     | ansi  |
        | collect_list | true  |
        | collect_list | false |
        | collect_set  | true  |
        | collect_set  | false |

    Scenario Outline: Coalesced struct preserves <case> interval qualifiers beside mixed timestamp types with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT result, typeof(result) AS result_type
        FROM (
          SELECT DATE '2024-01-02' + s.iv AS result
          FROM (
            SELECT coalesce(
              struct(<left_interval> AS iv, TIMESTAMP_NTZ '2024-01-01 00:00:00' AS ts),
              struct(<right_interval> AS iv, TIMESTAMP '2024-01-01 00:00:00' AS ts)) AS s
          )
        )
        """
      Then query result
        | result   | result_type |
        | <result> | <type>      |

      Examples:
        | case         | left_interval      | right_interval     | result              | type      | ansi  |
        | DAY          | INTERVAL '1' DAY   | INTERVAL '2' DAY   | 2024-01-03          | date      | true  |
        | DAY          | INTERVAL '1' DAY   | INTERVAL '2' DAY   | 2024-01-03          | date      | false |
        | HOUR         | INTERVAL '24' HOUR | INTERVAL '48' HOUR | 2024-01-03 00:00:00 | timestamp | true  |
        | HOUR         | INTERVAL '24' HOUR | INTERVAL '48' HOUR | 2024-01-03 00:00:00 | timestamp | false |
        | DAY and HOUR | INTERVAL '1' DAY   | INTERVAL '24' HOUR | 2024-01-03 00:00:00 | timestamp | true  |
        | DAY and HOUR | INTERVAL '1' DAY   | INTERVAL '24' HOUR | 2024-01-03 00:00:00 | timestamp | false |

    Scenario Outline: Coalesced named_struct preserves <case> interval qualifiers beside mixed timestamp types with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT result, typeof(result) AS result_type
        FROM (
          SELECT DATE '2024-01-02' + s.iv AS result
          FROM (
            SELECT coalesce(
              named_struct('iv', <left_interval>, 'ts', TIMESTAMP_NTZ '2024-01-01 00:00:00'),
              named_struct('iv', <right_interval>, 'ts', TIMESTAMP '2024-01-01 00:00:00')) AS s
          )
        )
        """
      Then query result
        | result   | result_type |
        | <result> | <type>      |

      Examples:
        | case         | left_interval      | right_interval     | result              | type      | ansi  |
        | DAY          | INTERVAL '1' DAY   | INTERVAL '2' DAY   | 2024-01-03          | date      | true  |
        | DAY          | INTERVAL '1' DAY   | INTERVAL '2' DAY   | 2024-01-03          | date      | false |
        | HOUR         | INTERVAL '24' HOUR | INTERVAL '48' HOUR | 2024-01-03 00:00:00 | timestamp | true  |
        | HOUR         | INTERVAL '24' HOUR | INTERVAL '48' HOUR | 2024-01-03 00:00:00 | timestamp | false |
        | DAY and HOUR | INTERVAL '1' DAY   | INTERVAL '24' HOUR | 2024-01-03 00:00:00 | timestamp | true  |
        | DAY and HOUR | INTERVAL '1' DAY   | INTERVAL '24' HOUR | 2024-01-03 00:00:00 | timestamp | false |

    Scenario Outline: <operation> preserves array interval qualifiers with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(day_result) AS day_type, typeof(hour_result) AS hour_type
        FROM (
          SELECT DATE '2024-01-02' + <day_array>[0] AS day_result,
                 DATE '2024-01-02' + <hour_array>[0] AS hour_result
        ) AS q
        """
      Then query result
        | day_result | hour_result         | day_type | hour_type |
        | 2024-01-03 | 2024-01-03 00:00:00 | date     | timestamp |

      Examples:
        | operation    | day_array                                                              | hour_array                                                                  | ansi  |
        | reverse      | reverse(array(INTERVAL '1' DAY))                                        | reverse(array(INTERVAL '24' HOUR))                                          | true  |
        | reverse      | reverse(array(INTERVAL '1' DAY))                                        | reverse(array(INTERVAL '24' HOUR))                                          | false |
        | shuffle      | shuffle(array(INTERVAL '1' DAY))                                        | shuffle(array(INTERVAL '24' HOUR))                                          | true  |
        | shuffle      | shuffle(array(INTERVAL '1' DAY))                                        | shuffle(array(INTERVAL '24' HOUR))                                          | false |
        | array_remove | array_remove(array(INTERVAL '1' DAY, INTERVAL '2' DAY), INTERVAL '2' DAY) | array_remove(array(INTERVAL '24' HOUR, INTERVAL '48' HOUR), INTERVAL '48' HOUR) | true  |
        | array_remove | array_remove(array(INTERVAL '1' DAY, INTERVAL '2' DAY), INTERVAL '2' DAY) | array_remove(array(INTERVAL '24' HOUR, INTERVAL '48' HOUR), INTERVAL '48' HOUR) | false |

    Scenario Outline: Arrays_zip preserves independent interval field qualifiers with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT q.*, typeof(day_result) AS day_type, typeof(hour_result) AS hour_type
        FROM (
          SELECT DATE '2024-01-02' + items[0].`0` AS day_result,
                 DATE '2024-01-02' + items[0].`1` AS hour_result
          FROM (SELECT arrays_zip(array(INTERVAL '1' DAY), array(INTERVAL '24' HOUR)) AS items)
        ) AS q
        """
      Then query result
        | day_result | hour_result         | day_type | hour_type |
        | 2024-01-03 | 2024-01-03 00:00:00 | date     | timestamp |

      Examples:
        | ansi  |
        | true  |
        | false |
