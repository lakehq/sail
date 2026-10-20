Feature: to_timestamp (strict variant)
  Strict to_timestamp that throws on invalid input,
  contrasting with try_to_timestamp which returns NULL.

  Rule: Valid input parses

    Scenario Outline: Valid input: <case>
      When query
        """
        SELECT to_timestamp(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                           | args                                         | result              |
        | ISO timestamp                  | '2024-01-15 10:30:45'                        | 2024-01-15 10:30:45 |
        | Date-only parses with midnight | '2024-01-15'                                 | 2024-01-15 00:00:00 |
        | With format                    | '2024-01-15 10:30:45', 'yyyy-MM-dd HH:mm:ss' | 2024-01-15 10:30:45 |
        | Cast from date                 | DATE '2024-01-15'                            | 2024-01-15 00:00:00 |
        | Cast from timestamp            | TIMESTAMP '2024-01-15 10:30:45'              | 2024-01-15 10:30:45 |

  Rule: Invalid input honors ANSI mode
    # to_timestamp errors on invalid input under ANSI and returns NULL otherwise.

    Scenario Outline: ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                                 | args                       |
        | Garbage string under ANSI on errors  | 'not-a-timestamp'          |
        | Format mismatch under ANSI on errors | '2024-01-15', 'dd/MM/yyyy' |

    Scenario Outline: ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                        | args                       |
        | Garbage string under ANSI off returns NULL  | 'not-a-timestamp'          |
        | Format mismatch under ANSI off returns NULL | '2024-01-15', 'dd/MM/yyyy' |

  Rule: NULL input propagates

    Scenario Outline: NULL propagation: <case>
      When query
        """
        SELECT to_timestamp(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                     | args                        |
        | NULL input returns NULL  | CAST(NULL AS STRING)        |
        | NULL format returns NULL | '2024-01-15 10:30:45', NULL |

  Rule: Timezone handling — LTZ applies offset, NTZ ignores it
    # Validated against Spark JVM with session tz America/New_York.

    Scenario Outline: Session time zone: <case>
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                   | fn               | input                       | result              |
        | LTZ applies trailing Z (UTC) and renders in session tz | to_timestamp     | '2024-01-15 10:30:45Z'      | 2024-01-15 05:30:45 |
        | LTZ applies explicit offset                            | to_timestamp     | '2024-06-15 10:30:45-08:00' | 2024-06-15 14:30:45 |
        | NTZ ignores trailing Z (keeps wall clock)              | to_timestamp_ntz | '2024-01-15 10:30:45Z'      | 2024-01-15 10:30:45 |

    Scenario: NTZ ignores explicit offset
      When query
        """
        SELECT to_timestamp_ntz('2024-06-15 10:30:45-08:00') AS result
        """
      Then query result
        | result              |
        | 2024-06-15 10:30:45 |

  Rule: Fractional seconds, separators, boundaries

    Scenario Outline: Fractions and boundaries: <case>
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                        | fn               | input                           | result                     |
        | T separator parses                          | to_timestamp     | '2024-01-15T10:30:45'           | 2024-01-15 10:30:45        |
        | Fractional seconds truncate to microseconds | to_timestamp     | '2024-01-15 10:30:45.123456789' | 2024-01-15 10:30:45.123456 |
        | Single-digit fractional second              | to_timestamp     | '2024-01-15 10:30:45.1'         | 2024-01-15 10:30:45.1      |
        | Leap day                                    | to_timestamp_ntz | '2024-02-29 12:00:00'           | 2024-02-29 12:00:00        |
        | Upper boundary                              | to_timestamp_ntz | '9999-12-31 23:59:59'           | 9999-12-31 23:59:59        |

  Rule: Per-row format (column-expression format)

    Scenario: Different format per row all parse
      When query
        """
        SELECT to_timestamp(d, f) AS result FROM VALUES
          ('2024-01-15 10:30:00', 'yyyy-MM-dd HH:mm:ss'),
          ('15/01/2024 10:30:00', 'dd/MM/yyyy HH:mm:ss') AS t(d, f)
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:00 |
        | 2024-01-15 10:30:00 |

  Rule: Numeric input is the number of SECONDS since the epoch
    # Spark casts a numeric argument to TIMESTAMP with the *seconds* semantics of
    # `Cast(NumericType -> TimestampType)`: the integral part is whole seconds and the
    # fractional part is a fraction OF A SECOND. Verified on Spark JVM 4.2.0.
    # These cases discriminate seconds-vs-microseconds: `to_timestamp(1)` is
    # 00:00:01 under the real rule and 00:00:00.000001 under a micros reading, so a
    # value like 0 (identical under both) would prove nothing and is kept separate below.

    Scenario Outline: Numeric seconds: <case>
      When query
        """
        SELECT to_timestamp(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                          | input | result              |
        | zero is the epoch             | 0     | 1970-01-01 00:00:00 |
        | negative zero is the epoch    | -0.0  | 1970-01-01 00:00:00 |

    Scenario Outline: Integral and fractional numeric seconds: <case>
      When query
        """
        SELECT to_timestamp(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | input                      | result                     |
        | one is one second, not one micro    | 1                          | 1970-01-01 00:00:01        |
        | minus one is one second before      | -1                         | 1969-12-31 23:59:59        |
        | fraction is a fraction of a second  | 1.5                        | 1970-01-01 00:00:01.5      |
        | negative fraction                   | -1.5                       | 1969-12-31 23:59:58.5      |
        | sub-second only                     | 0.5                        | 1970-01-01 00:00:00.5      |
        | negative sub-second only            | -0.5                       | 1969-12-31 23:59:59.5      |
        | INT_MAX seconds                     | 2147483647                 | 2038-01-19 03:14:07        |
        | INT_MAX plus one                    | 2147483648                 | 2038-01-19 03:14:08        |
        | INT_MIN minus one                   | -2147483649                | 1901-12-13 20:45:51        |
        | last second of year 9999            | 253402300799               | 9999-12-31 23:59:59        |
        | first second past year 9999         | 253402300800               | +10000-01-01 00:00:00      |
        | first second of year 1              | -62135596800               | 0001-01-01 00:00:00        |
        | one second before year 1            | -62135596801               | 0000-12-31 23:59:59        |
        | decimal keeps the fractional second | CAST(1.5 AS DECIMAL(38,18)) | 1970-01-01 00:00:01.5     |

  Rule: Numeric overflow and non-finite values
    # BIGINT micros saturate to the timestamp range; DOUBLE values that overflow BIGINT
    # raise CAST_OVERFLOW under ANSI and saturate under ANSI off; NaN and Infinity are
    # CAST_INVALID_INPUT under ANSI and NULL otherwise. Verified on Spark JVM 4.2.0.

    Scenario Outline: Saturating BIGINT bound: <case>
      When query
        """
        SELECT to_timestamp(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case         | input                | result                        |
        | LONG_MAX     | 9223372036854775807  | +294247-01-10 04:00:54.775807 |
        | LONG_MIN     | -9223372036854775808 | -290308-12-21 19:59:05.224192 |

    Scenario Outline: DOUBLE overflowing BIGINT raises under ANSI: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp(<input>) AS result
        """
      Then query error cannot be cast to "BIGINT" due to an overflow

      Examples:
        | case              | input |
        | 1e18 seconds      | 1e18  |
        | minus 1e18        | -1e18 |

    Scenario Outline: DOUBLE overflowing BIGINT saturates without ANSI: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case         | input | result                        |
        | 1e18 seconds | 1e18  | +294247-01-10 04:00:54.775807 |
        | minus 1e18   | -1e18 | -290308-12-21 19:59:05.224192 |

    Scenario Outline: NaN and Infinity raise under ANSI: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp(<input>) AS result
        """
      Then query error cannot be cast to "TIMESTAMP" because it is malformed

      Examples:
        | case           | input                        |
        | NaN            | CAST('NaN' AS DOUBLE)        |
        | positive inf   | CAST('Infinity' AS DOUBLE)   |
        | negative inf   | CAST('-Infinity' AS DOUBLE)  |

    Scenario Outline: NaN and Infinity are NULL without ANSI: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case           | input                        |
        | NaN            | CAST('NaN' AS DOUBLE)        |
        | positive inf   | CAST('Infinity' AS DOUBLE)   |
        | negative inf   | CAST('-Infinity' AS DOUBLE)  |

  Rule: A NULL value short-circuits before the format is validated
    # Spark builds the formatter lazily inside the non-null path, so a NULL input returns
    # NULL WITHOUT ever validating the pattern. An eager validator diverges here: it raises
    # on a pattern that Spark never looks at. The pair below is what discriminates —
    # a valid input with the same bad pattern DOES raise, so asserting only that case
    # would not tell an eager validator from a lazy one.

    Scenario: a valid value with an unrecognized pattern raises
      When query
        """
        SELECT to_timestamp('2016-12-31', 'qqq') AS result
        """
      Then query error Unrecognized datetime pattern

    Scenario: a NULL value with an unrecognized pattern is NULL, not an error
      When query
        """
        SELECT to_timestamp(CAST(NULL AS STRING), 'qqq') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Non-string input types follow the cast rules
    # BOOLEAN and BINARY are cast to STRING first and then parsed, so they are NULL
    # (or a CAST error under ANSI) rather than an unsupported-type failure.
    # TIME is cast to TIMESTAMP by adding today's date, so it is not asserted here
    # (the value is not stable); see the TIME rule in to_time.feature.

    Scenario Outline: Non-string input without ANSI: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case    | input         |
        | boolean | true          |
        | binary  | X'48656C6C6F' |


  Rule: DST gaps and overlaps resolve the way java.time does
    # These currently AGREE with Spark; they are here as regression cover, because the rule
    # is easy to break and impossible to see from a UTC-only test. The harness runs in UTC,
    # which has no DST, so the session zone must be set explicitly or a hardcoded-UTC
    # implementation passes every scenario.
    #
    # ZonedDateTime.of resolves a GAP by shifting FORWARD by the gap length, and an
    # OVERLAP to the EARLIER offset. Both are what discriminate: a "reject ambiguous"
    # implementation fails the second, and a "use the later offset" one returns the same
    # wall clock but a different instant, which only shows up once converted.
    # Measured on Spark JVM 4.2.0.

    Scenario Outline: Session time zone, gap and overlap: <case>
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                     | fn               | input                   | result              |
        | spring-forward: 02:30 does not exist     | to_timestamp     | '2024-03-10 02:30:00'   | 2024-03-10 03:30:00 |
        | fall-back: 01:30 happens twice           | to_timestamp     | '2024-11-03 01:30:00'   | 2024-11-03 01:30:00 |
        | try_ variant shifts the gap the same way | try_to_timestamp | '2024-03-10 02:30:00'   | 2024-03-10 03:30:00 |
        | ltz variant shifts the gap the same way  | to_timestamp_ltz | '2024-03-10 02:30:00'   | 2024-03-10 03:30:00 |
        | NTZ has no zone, so the gap is kept      | to_timestamp_ntz | '2024-03-10 02:30:00'   | 2024-03-10 02:30:00 |
        | NTZ keeps the overlapping wall clock too | to_timestamp_ntz | '2024-11-03 01:30:00'   | 2024-11-03 01:30:00 |
        | to_date drops the time, gap and all      | to_date          | '2024-03-10 02:30:00'   | 2024-03-10          |
        | to_date on the overlapping hour          | to_date          | '2024-11-03 01:30:00'   | 2024-11-03          |

  Rule: Year and offset boundaries of the string parser
    # Regression cover for values that currently AGREE with Spark. Each row is a place a
    # narrower or wider parser lands somewhere else: the four-digit zero pad below 1000,
    # the leading `+` above 9999, proleptic negative years, and — the ones most easily
    # missed — a trailing zone designator or numeric offset, which are PARSED and applied,
    # not ignored. Session zone is the harness default, UTC.

    Scenario Outline: Boundary string: <case>
      When query
        """
        SELECT to_timestamp(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                             | input                                  | result                     |
        | first representable year         | '0001-01-01'                           | 0001-01-01 00:00:00        |
        | last four-digit year             | '9999-12-31 23:59:59.999999'           | 9999-12-31 23:59:59.999999 |
        | five-digit year gets a plus      | '10000-01-01'                          | +10000-01-01 00:00:00      |
        | year zero is proleptic, not 1 BC | '0000-12-31'                           | 0000-12-31 00:00:00        |
        | negative year                    | '-0001-01-01'                          | -0001-01-01 00:00:00       |
        | leap day                         | '2024-02-29'                           | 2024-02-29 00:00:00        |
        | Z designator means UTC           | '2024-01-15 12:00:00Z'                 | 2024-01-15 12:00:00        |
        | numeric offset is applied        | '2024-01-15 12:00:00+05:30'            | 2024-01-15 06:30:00        |
        | region zone is applied           | '2024-01-15 12:00:00 America/New_York' | 2024-01-15 17:00:00        |
        | pre-epoch keeps its fraction     | '1969-12-31 23:59:59.5'                | 1969-12-31 23:59:59.5      |
        | full microsecond precision       | '1969-12-31 23:59:59.999999'           | 1969-12-31 23:59:59.999999 |

    Scenario: a non-leap 29 February is rejected, not rolled over to 1 March
      # The discriminating half of the leap-day pair above: a lenient parser rolls
      # 2023-02-29 forward to 2023-03-01 instead of failing.
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp('2023-02-29') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Malformed offsets and numeric segments follow the configured error policy

    Scenario Outline: Safe parsing rejects malformed input with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT try_to_timestamp('2024-01-01 00:00:00+€1') AS invalid_offset,
               try_to_timestamp('99999999999999999999-01-01') AS invalid_year
        """
      Then query result
        | invalid_offset | invalid_year |
        | NULL           | NULL         |
      When query
        """
        SELECT try_to_timestamp(s) AS result FROM VALUES
          ('2024-01-01 00:00:00+€1'),
          ('2024-01-01 00:00:00+1€11'),
          ('99999999999999999999-01-01'),
          ('2024-01-01 99999999999999999999:00:00'),
          ('2024-01-01 00:00:00.123456789012345678901234567890'),
          (NULL) AS t(s)
        """
      Then query result
        | result                     |
        | NULL                       |
        | NULL                       |
        | NULL                       |
        | NULL                       |
        | 2024-01-01 00:00:00.123456 |
        | NULL                       |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario: Invalid timestamp casts return NULL without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp(s) AS parsed, CAST(s AS TIMESTAMP) AS casted FROM VALUES
          ('2024-01-01 00:00:00+€1'),
          ('99999999999999999999-01-01') AS t(s)
        """
      Then query result
        | parsed | casted |
        | NULL   | NULL   |
        | NULL   | NULL   |

    Scenario Outline: Strict parsing rejects <case> with a timestamp error
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('<input>') AS result
        """
      Then query error (?i)(invalid (timestamp|time zone)|CAST_INVALID_INPUT)

      Examples:
        | case           | input                         |
        | Unicode offset | 2024-01-01 00:00:00+€1       |
        | oversized year | 99999999999999999999-01-01    |

  Rule: Lowercase mm in a pattern means MINUTES, not months
    # Regression cover for a currently-agreeing case that is pure trap: 'yyyy-mm-dd'
    # against '2016-12-31' parses 12 as MINUTES and leaves the month at its default of 1,
    # giving 2016-01-31 00:12:00 rather than 2016-12-31. Paired with the correct 'MM'
    # spelling so the pair discriminates a case-sensitive pattern engine from a sloppy one.
    #
    # The three timeParserPolicy values are asserted together because they all produce the
    # SAME answer here — which is itself the claim worth pinning, since the policy does
    # change other patterns.

    Scenario Outline: Pattern case sensitivity under timeParserPolicy=<policy>
      Given config spark.sql.legacy.timeParserPolicy = <policy>
      When query
        """
        SELECT to_timestamp('2016-12-31', 'yyyy-MM-dd') AS upper,
               to_timestamp('2016-12-31', 'yyyy-mm-dd') AS lower
        """
      Then query result
        | upper               | lower               |
        | 2016-12-31 00:00:00 | 2016-01-31 00:12:00 |

      Examples:
        | policy    |
        | LEGACY    |
        | CORRECTED |
        | EXCEPTION |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null string literal yields a timestamp
      When query
        """
        SELECT to_timestamp('2024-01-15 10:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: a non-null string column yields a timestamp
      When query
        """
        SELECT to_timestamp(date_format(CAST(id AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss')) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT to_timestamp(c) AS result FROM VALUES ('2024-01-15 10:00:00'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

  Rule: Temporal timestamp inputs retain ignored format dependencies

    # GetTimestamp ignores the scalar format value for DATE/TIMESTAMP inputs,
    # but analysis must still extract its aggregates before that value is ignored.
    # Both arguments retain required dependencies and validation. A live aggregate
    # error remains an error even for NULL input; ignoring its eventual scalar
    # value must not erase that computation or change the number of rows.
    # Sail evaluates scalar format expressions and subqueries ordinarily, including
    # their errors. Typed temporal values need no pattern parser, but that does not
    # suppress argument computation. Spark can skip that computation; the paired
    # expectations below document the deliberate difference.
    Scenario Outline: Temporal format aggregate retains one row with ANSI <ansi>: <function>, range(<rows>)
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(<function>(DATE '2024-01-01', max('yyyy')) AS STRING) AS result
        FROM range(<rows>)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function         | rows | ansi  | result              |
        | to_timestamp     | 0    | true  | 2024-01-01 00:00:00 |
        | to_timestamp     | 3    | true  | 2024-01-01 00:00:00 |
        | to_timestamp     | 0    | false | 2024-01-01 00:00:00 |
        | to_timestamp     | 3    | false | 2024-01-01 00:00:00 |
        | to_timestamp_ltz | 0    | true  | 2024-01-01 00:00:00 |
        | to_timestamp_ltz | 3    | true  | 2024-01-01 00:00:00 |
        | to_timestamp_ltz | 0    | false | 2024-01-01 00:00:00 |
        | to_timestamp_ltz | 3    | false | 2024-01-01 00:00:00 |
        | to_timestamp_ntz | 0    | true  | 2024-01-01 00:00:00 |
        | to_timestamp_ntz | 3    | true  | 2024-01-01 00:00:00 |
        | to_timestamp_ntz | 0    | false | 2024-01-01 00:00:00 |
        | to_timestamp_ntz | 3    | false | 2024-01-01 00:00:00 |
        | try_to_timestamp | 0    | true  | 2024-01-01 00:00:00 |
        | try_to_timestamp | 3    | true  | 2024-01-01 00:00:00 |
        | try_to_timestamp | 0    | false | 2024-01-01 00:00:00 |
        | try_to_timestamp | 3    | false | 2024-01-01 00:00:00 |
        | try_to_date      | 0    | true  | 2024-01-01          |
        | try_to_date      | 3    | true  | 2024-01-01          |
        | try_to_date      | 0    | false | 2024-01-01          |
        | try_to_date      | 3    | false | 2024-01-01          |

    Scenario Outline: Temporal format aggregate preserves NULL input cardinality with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(CAST(NULL AS DATE), max('yyyy')) AS result FROM range(<rows>)
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | rows | ansi  |
        | 0    | true  |
        | 0    | false |
        | 3    | true  |
        | 3    | false |

    Scenario Outline: Temporal format preserves a live aggregate error with ANSI <ansi>: <value>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(<value>, max(CAST(raise_error(concat('format-aggregate-', id)) AS STRING)))
        FROM range(2)
        """
      Then query error format-aggregate-[01]

      Examples:
        | value              | ansi  |
        | DATE '2024-01-01'  | true  |
        | DATE '2024-01-01'  | false |
        | CAST(NULL AS DATE) | true  |
        | CAST(NULL AS DATE) | false |

    Scenario Outline: Temporal format retains grouping validation with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(max(DATE '2024-01-01'), CAST(id AS STRING)) FROM range(3)
        """
      Then query error (?i)(group|field|column)

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Temporal format retains grouped expression identity with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(to_timestamp(max(DATE '2024-01-01'), CAST(id % 2 AS STRING)) AS STRING) AS result
        FROM range(4) GROUP BY CAST(id % 2 AS STRING)
        """
      Then query result
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |

      Examples:
        | ansi  |
        | true  |
        | false |

    # Typed DATE conversion still returns a timestamp when the format is NULL,
    # so this predicate must retain the unmatched outer-join row.
    Scenario Outline: Temporal conversion preserves an unmatched outer join row with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT t.id FROM range(2) t LEFT JOIN (SELECT 0 AS id, 'yyyy' AS f) s ON t.id=s.id WHERE to_timestamp(DATE '2024-01-01', s.f) = TIMESTAMP '2024-01-01' ORDER BY t.id
        """
      Then query result
        | id |
        | 0  |
        | 1  |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Temporal format evaluates scalar expressions around a live aggregate with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(to_timestamp(DATE '2024-01-01',
          concat(max(CAST(id AS STRING)), CAST(raise_error('unused-format-value') AS STRING))) AS STRING) AS result
        FROM range(2)
        """
      Then query result for Spark
        | result              |
        | 2024-01-01 00:00:00 |
      And query error for Sail unused-format-value

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Temporal format preserves a dead aggregate branch with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(to_timestamp(max(DATE '2024-01-01'),
          CASE WHEN true THEN 'yyyy'
               ELSE concat(CAST(CAST(concat('b', 'ad') AS INT) AS STRING),
                           max(CAST(raise_error(concat('dead-format-', id)) AS STRING))) END) AS STRING) AS result
        FROM range(2)
        """
      Then query result
        | result              |
        | 2024-01-01 00:00:00 |

      Examples:
        | ansi  |
        | false |

    # TODO: Fix eager constant CAST evaluation in dead aggregate CASE branches
    # in the separate CASE work.
    @sail-bug
    Scenario: Temporal format preserves a dead aggregate branch with ANSI enabled
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(to_timestamp(max(DATE '2024-01-01'),
          CASE WHEN true THEN 'yyyy'
               ELSE concat(CAST(CAST(concat('b', 'ad') AS INT) AS STRING),
                           max(CAST(raise_error(concat('dead-format-', id)) AS STRING))) END) AS STRING) AS result
        FROM range(2)
        """
      Then query result
        | result              |
        | 2024-01-01 00:00:00 |

    Scenario Outline: Temporal format preserves a constant coalesce condition with ANSI <ansi>: <condition>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(to_timestamp(max(DATE '2024-01-01'),
          CASE WHEN <condition> THEN 'yyyy'
               ELSE max(CAST(raise_error(concat('dead-format-', id)) AS STRING)) END) AS STRING) AS result
        FROM range(2)
        """
      Then query result
        | result              |
        | 2024-01-01 00:00:00 |

      Examples:
        | condition                              | ansi  |
        | coalesce(true, false)                  | true  |
        | coalesce(true, false)                  | false |
        | coalesce(CAST(NULL AS BOOLEAN), true) | true  |
        | coalesce(CAST(NULL AS BOOLEAN), true) | false |

    # CheckAnalysis validates every expression, including lambda bodies, before
    # optimizer branch pruning. Ignoring a scalar format value cannot hide an
    # unsupported ARRAY-to-INT cast in an unreachable branch in either ANSI mode.
    Scenario Outline: Temporal format validates an invalid <case> cast in a dead branch with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', CASE WHEN false THEN CAST(<cast> AS STRING) ELSE 'yyyy' END) AS result
        """
      Then query error (?i)cast.*(array|list).*int

      Examples:
        | case   | cast                                                   | ansi  |
        | direct | CAST(array(1) AS INT)                                   | true  |
        | direct | CAST(array(1) AS INT)                                   | false |

    # TODO: Validate native cast types before dead CASE pruning; this existing
    # general validation gap is outside the datetime scope.
    @sail-bug
    Scenario Outline: Temporal format validates an invalid lambda cast in a dead branch with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', CASE WHEN false THEN CAST(<cast> AS STRING) ELSE 'yyyy' END) AS result
        """
      Then query error (?i)cast.*(array|list).*int

      Examples:
        | cast                                                  | ansi  |
        | transform(array(array(1)), x -> CAST(x AS INT))         | true  |
        | transform(array(array(1)), x -> CAST(x AS INT))         | false |

    # TRY_CAST tolerates malformed values only for supported source/target types.
    # CheckAnalysis still rejects ARRAY-to-INT inside a dead CASE branch whose
    # eventual scalar format value is unused.
    # TODO: Validate native TRY_CAST types before dead CASE pruning; this existing
    # general validation gap is outside the datetime scope.
    @sail-bug
    Scenario Outline: Temporal format validates an invalid TRY_CAST in a dead branch with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', CASE WHEN false THEN CAST(try_cast(array(id) AS INT) AS STRING) ELSE 'yyyy' END) AS result FROM range(2)
        """
      Then query error (?i)cast.*(array|list).*int

      Examples:
        | ansi  |
        | true  |
        | false |

    # Spark removes a constant-true filter and inlines no-FROM scalar subqueries,
    # leaving their errors inside the unused format expression. Sail executes
    # these retained scalar projections even when the format is unused.
    Scenario Outline: Temporal format follows ordinary scalar subquery execution with ANSI <ansi>: <format>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(to_timestamp(DATE '2024-01-01', <format>) AS STRING) AS result FROM range(2) t
        """
      Then query result for Spark
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |
      And query <sail_expectation>
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |

      Examples:
        | format                                                                    | ansi  | sail_expectation             |
        | (SELECT CAST(raise_error('unused-format') AS STRING))                      | true  | error for Sail unused-format |
        | (SELECT CAST(raise_error('unused-format') AS STRING))                      | false | error for Sail unused-format |
        | (SELECT CAST(raise_error('unused-format') AS STRING) WHERE true)           | true  | error for Sail unused-format |
        | (SELECT CAST(raise_error('unused-format') AS STRING) WHERE true)           | false | error for Sail unused-format |
        | (SELECT v FROM (SELECT CAST(raise_error('unused-format') AS STRING) AS v) s) | true  | error for Sail unused-format |
        | (SELECT v FROM (SELECT CAST(raise_error('unused-format') AS STRING) AS v) s) | false | error for Sail unused-format |

    # TODO: Support outer references in non-aggregate scalar projections.
    @sail-bug
    Scenario Outline: Temporal format follows ordinary correlated scalar subquery execution with ANSI <ansi>: <format>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(to_timestamp(DATE '2024-01-01', <format>) AS STRING) AS result FROM range(2) t
        """
      Then query result for Spark
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |
      And query <sail_expectation>
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |

      Examples:
        | format                                                                    | ansi  | sail_expectation             |
        | (SELECT CAST(raise_error(concat('unused-format-', t.id)) AS STRING))        | true  | error for Sail unused-format-[01] |
        | (SELECT CAST(raise_error(concat('unused-format-', t.id)) AS STRING))        | false | error for Sail unused-format-[01] |

    # WHERE 1=1 retains the subquery projection. Spark folds the invalid CAST in
    # ANSI mode; Sail also raises when ordinary subquery execution reaches it.
    Scenario Outline: Temporal format executes a scalar subquery projection with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(to_timestamp(DATE '2024-01-01', (SELECT CAST('bad' AS INT) WHERE 1=1)) AS STRING) AS result FROM range(2)
        """
      Then query <expectation>
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |

      Examples:
        | ansi  | expectation          |
        | true  | error (?i)cast.*bad   |
        | false | result               |

    # Spark executes a scalar subquery over a real range before its result can be
    # ignored by ToTimestamp. Preserve its required error even with one known row.
    Scenario Outline: Temporal format preserves a required scalar subquery error with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', (SELECT CAST(raise_error('required-filter') AS STRING) FROM range(1) WHERE id=0)) AS result FROM range(2)
        """
      Then query error required-filter

      Examples:
        | ansi  |
        | true  |
        | false |

    # Spark's ComputeCurrentTime uses the query's current time in subqueries.
    # Folding this filter with a default 1970 timestamp would incorrectly choose
    # TRUE and erase its required error. Use the real query context.
    Scenario Outline: Temporal format retains a time-dependent subquery filter error with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', (SELECT 'yyyy' WHERE CASE WHEN current_timestamp() < TIMESTAMP '2000-01-01 00:00:00' THEN true ELSE CAST(raise_error('required-filter') AS BOOLEAN) END)) AS result FROM range(2)
        """
      Then query error required-filter

      Examples:
        | ansi  |
        | true  |
        | false |

    # The DATE conversion ignores 'QQQ' and makes the selected timezone constant.
    # Spark initializes that zone before checking runtime NULL timestamps. Sail
    # deliberately checks NULL first, independently of whether the zone is constant;
    # no timezone conversion or zone-string validation is needed for these rows.
    Scenario Outline: Temporal format preserves NULL-first timezone validation with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT from_utc_timestamp(CASE WHEN id<0 THEN TIMESTAMP '2024-01-01 00:00:00' ELSE CAST(NULL AS TIMESTAMP) END, CASE WHEN to_timestamp(DATE '2024-01-01','QQQ')=TIMESTAMP '2024-01-01 00:00:00' THEN 'bad-zone' ELSE 'UTC' END) AS result FROM range(2)
        """
      Then query result unless Spark raises bad-zone
        | result |
        | NULL   |
        | NULL   |

      Examples:
        | ansi  |
        | true  |
        | false |

    # Spark skips the scalar CAST for these constant DATE aliases. Sail computes
    # the format ordinarily, so ANSI rejects the cast before temporal conversion.
    Scenario Outline: Temporal format evaluates scalar casts through <case> with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(to_timestamp(d, CAST('bad' AS INT)) AS STRING) AS result FROM <input>
        """
      Then query result for Spark
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |
      And query <sail_expectation>
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |

      Examples:
        | case       | input                                                                                                          | ansi  | sail_expectation            |
        | projection | (SELECT DATE '2024-01-01' AS d FROM range(2)) q                                                                  | true  | error for Sail (?i)cast.*bad |
        | projection | (SELECT DATE '2024-01-01' AS d FROM range(2)) q                                                                  | false | result for Sail             |
        | filter     | (SELECT DATE '2024-01-01' AS d, id FROM range(2)) q WHERE id >= 0                                                 | true  | error for Sail (?i)cast.*bad |
        | filter     | (SELECT DATE '2024-01-01' AS d, id FROM range(2)) q WHERE id >= 0                                                 | false | result for Sail             |
        | union      | (SELECT DATE '2024-01-01' AS d UNION ALL SELECT DATE '2024-01-01' AS d) q                                        | true  | error for Sail (?i)cast.*bad |
        | union      | (SELECT DATE '2024-01-01' AS d UNION ALL SELECT DATE '2024-01-01' AS d) q                                        | false | result for Sail             |
        | generator  | (SELECT d, explode(array(1, 2)) AS x FROM (SELECT DATE '2024-01-01' AS d) s) q                                    | true  | error for Sail (?i)cast.*bad |
        | generator  | (SELECT d, explode(array(1, 2)) AS x FROM (SELECT DATE '2024-01-01' AS d) s) q                                    | false | result for Sail             |

    # Spark's ToTimestamp.eval/doGenCode does not read the format for DATE/TIMESTAMP
    # input. ConstantFolding can still evaluate CAST('bad' AS INT) when the DATE is
    # generated; a literal DATE alias skips the same cast in the preceding cases.
    # Sail computes the format ordinarily in both shapes. Both engines reject
    # this generated-date case under ANSI and preserve both dates without ANSI.
    Scenario Outline: Temporal conversion evaluates a scalar format for generated dates with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(to_timestamp(d, CAST('bad' AS INT)) AS STRING) AS result
        FROM (SELECT explode(array(DATE '2024-01-01', DATE '2024-01-02')) AS d) q
        """
      Then query <expectation>
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-02 00:00:00 |

      Examples:
        | ansi  | expectation          |
        | true  | error (?i)cast.*bad   |
        | false | result               |

    # The projected k is the literal 1, so its filter retains the selected CAST.
    # Ordinary execution must still honor ANSI cast errors through these aliases.
    Scenario Outline: Temporal format executes a cast behind an aliased true filter with ANSI <ansi>: <case>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', <format>) AS result FROM range(2)
        """
      Then query <expectation>
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |

      Examples:
        | case             | format                                                                    | ansi  | expectation          |
        | projected key    | (SELECT CAST('bad' AS INT) FROM (SELECT 1 AS k) s WHERE k = 1)              | true  | error (?i)cast.*bad   |
        | projected key    | (SELECT CAST('bad' AS INT) FROM (SELECT 1 AS k) s WHERE k = 1)              | false | result               |
        | projected format | (SELECT v FROM (SELECT CAST('bad' AS INT) AS v, 1 AS k) s WHERE k=1)        | true  | error (?i)cast.*bad   |
        | projected format | (SELECT v FROM (SELECT CAST('bad' AS INT) AS v, 1 AS k) s WHERE k=1)        | false | result               |

    # Spark inlines both no-FROM projections and leaves the nested CAST unused.
    # Sail instead retains ordinary subquery execution: ANSI rejects the bad cast,
    # while ANSI off produces NULL for the ignored format value.
    Scenario Outline: Temporal format executes nested scalar subquery casts with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', (SELECT (SELECT CAST('bad' AS INT)))) AS result FROM range(2)
        """
      Then query result for Spark
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |
      And query <sail_expectation>
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |

      Examples:
        | ansi  | sail_expectation              |
        | true  | error for Sail (?i)cast.*bad   |
        | false | result for Sail               |

    # Spark removes LIMIT 1 from its known one-row relation and then inlines the
    # projection. Sail retains ordinary subquery execution and its projected error.
    Scenario Outline: Temporal format executes a one-row limit projection with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', (SELECT CAST(raise_error('unused-format') AS STRING) LIMIT 1)) AS result FROM range(2)
        """
      Then query result for Spark
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |
      And query error for Sail unused-format

      Examples:
        | ansi  |
        | true  |
        | false |

    # These predicates consume k, so its CAST is required. A non-nullable field
    # alone cannot justify replacing k=k or k IS NOT NULL with TRUE: evaluating
    # k can fail. Without ANSI, the cast yields NULL and the outer DATE still converts.
    Scenario Outline: Temporal format preserves a required aliased predicate cast with ANSI <ansi>: <predicate>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', (SELECT 'yyyy' FROM (SELECT CAST('bad' AS INT) AS k) s WHERE <predicate>)) AS result FROM range(2)
        """
      Then query <expectation>
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |

      Examples:
        | predicate     | ansi  | expectation          |
        | k = k         | true  | error (?i)cast.*bad   |
        | k = k         | false | result               |
        | k IS NOT NULL | true  | error (?i)cast.*bad   |
        | k IS NOT NULL | false | result               |

    # The inner range remains a real relation inside the outer no-FROM projection.
    # Spark executes that required subquery before ignoring its result.
    Scenario Outline: Temporal format retains a nested relational subquery error with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', (SELECT (SELECT CAST(raise_error('required-nested') AS STRING) FROM range(1)))) AS result FROM range(2)
        """
      Then query error required-nested

      Examples:
        | ansi  |
        | true  |
        | false |

    # ExtractWindowExpressions retains the window producer before ToTimestamp
    # ignores its scalar format value, so errors in live window input still raise.
    Scenario Outline: Temporal format retains a live window error with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', max(CAST(raise_error(concat('required-window-', id)) AS STRING)) OVER ()) AS result FROM range(2)
        """
      Then query error required-window-[01]

      Examples:
        | ansi  |
        | true  |
        | false |

    # False/NULL filters produce empty subqueries; a nested SELECT true retains a
    # row. Ordinary constant folding can reach the bad CAST before filter pruning.
    # Sail accepts ordinary subquery optimization and its ANSI errors.
    Scenario Outline: Temporal format follows ordinary subquery optimization with ANSI <ansi>: <case>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', <format>) AS result FROM range(2)
        """
      Then query <expectation>
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |

      Examples:
        | case             | format                                                                                   | ansi  | expectation          |
        | false literal    | (SELECT CAST('bad' AS INT) WHERE false)                                                   | true  | error (?i)cast.*bad   |
        | false literal    | (SELECT CAST('bad' AS INT) WHERE false)                                                   | false | result               |
        | false alias      | (SELECT CAST('bad' AS INT) FROM (SELECT 1 AS k) q WHERE k=2)                               | true  | error (?i)cast.*bad   |
        | false alias      | (SELECT CAST('bad' AS INT) FROM (SELECT 1 AS k) q WHERE k=2)                               | false | result               |
        | NULL alias       | (SELECT CAST('bad' AS INT) FROM (SELECT CAST(NULL AS BOOLEAN) AS k) q WHERE k)              | true  | error (?i)cast.*bad   |
        | NULL alias       | (SELECT CAST('bad' AS INT) FROM (SELECT CAST(NULL AS BOOLEAN) AS k) q WHERE k)              | false | result               |
        | nested predicate | (SELECT CAST('bad' AS INT) WHERE (SELECT true))                                           | true  | error (?i)cast.*bad   |
        | nested predicate | (SELECT CAST('bad' AS INT) WHERE (SELECT true))                                           | false | result               |

    # CheckAnalysis validates the inner projection even when its column is unused.
    # Ordinary subquery optimization must not erase an unsupported ARRAY-to-INT cast.
    Scenario Outline: Temporal format validates an unselected subquery projection with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', (SELECT 'yyyy' FROM (SELECT CAST(array(1) AS INT) AS unused) s)) AS result FROM range(2)
        """
      Then query error (?i)cast.*(array|list).*int

      Examples:
        | ansi  |
        | true  |
        | false |

    # The inner scalar subquery refers to v in its immediately enclosing projection.
    # Spark inlines it into the ignored format. Sail keeps ordinary producer
    # execution, so raise_error and ANSI cast failures must remain visible.
    # TODO: Support nested scalar projections that reference their parent query.
    @sail-bug
    Scenario Outline: Temporal format executes locally correlated scalar nesting with ANSI <ansi>: <case>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', (SELECT (SELECT <format>) FROM (SELECT 1 AS v) s)) AS result FROM range(2)
        """
      Then query result for Spark
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |
      And query <sail_expectation>
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |

      Examples:
        | case         | format                                                       | ansi  | sail_expectation              |
        | raise error  | CAST(raise_error(concat('unused-local-',v)) AS STRING)         | true  | error for Sail unused-local-1 |
        | raise error  | CAST(raise_error(concat('unused-local-',v)) AS STRING)         | false | error for Sail unused-local-1 |
        | invalid cast | CAST(concat('bad',v) AS INT)                                  | true  | error for Sail (?i)cast.*bad   |
        | invalid cast | CAST(concat('bad',v) AS INT)                                  | false | result for Sail               |

    # CheckAnalysis also visits nested subqueries in an unselected projection.
    # Removing that projection cannot hide its unsupported ARRAY-to-INT cast.
    Scenario Outline: Temporal format validates nested unselected subquery casts with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', (SELECT 'yyyy' FROM (SELECT (SELECT CAST(array(1) AS INT)) AS unused) s)) AS result FROM range(2)
        """
      Then query error (?i)cast.*(array|list).*int

      Examples:
        | ansi  |
        | true  |
        | false |

    # LIMIT 1 over a real range retains required subquery execution in both engines.
    Scenario Outline: Temporal format retains a limited relational subquery error with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', (SELECT CAST(raise_error('required-limit') AS STRING) FROM range(1) LIMIT 1)) AS result FROM range(2)
        """
      Then query error required-limit

      Examples:
        | ansi  |
        | true  |
        | false |

    # yyyy1 is a valid format, but the typed DATE does not need to parse it.
    # The inner scalar subquery references v from its immediate parent. Keeping
    # that producer must preserve the valid query and both outer rows.
    # TODO: Support nested scalar projections that reference their parent query.
    @sail-bug
    Scenario Outline: Temporal format retains valid locally correlated nesting with ANSI <ansi>
      Given config spark.sql.session.timeZone = UTC
      And config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(DATE '2024-01-01', (SELECT (SELECT concat('yyyy', CAST(v AS STRING))) FROM (SELECT 1 AS v) s)) AS result FROM range(2)
        """
      Then query result
        | result              |
        | 2024-01-01 00:00:00 |
        | 2024-01-01 00:00:00 |

      Examples:
        | ansi  |
        | true  |
        | false |

    # The same producer must work outside a datetime conversion; correlation
    # handling must preserve its value rather than erase it as an ignored format.
    # TODO: Support nested scalar projections that reference their parent query.
    @sail-bug
    Scenario Outline: Locally correlated nested format has its scalar value with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT (SELECT (SELECT concat('yyyy', CAST(v AS STRING))) FROM (SELECT 1 AS v) s) AS result FROM range(2)
        """
      Then query result
        | result |
        | yyyy1  |
        | yyyy1  |

      Examples:
        | ansi  |
        | true  |
        | false |

    # Scalar subqueries return NULL for no rows without dropping the outer row
    # or evaluating concat against a missing row.
    # TODO: Support correlated scalar projections, including empty inputs.
    @sail-bug
    Scenario Outline: Correlated format projection preserves scalar row counts with ANSI <ansi>: <case>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT (SELECT concat('yyyy', CAST(v AS STRING)) <suffix>) AS result FROM (SELECT 1 AS v) s
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case           | suffix                  | ansi  | result |
        | one row        |                         | true  | yyyy1  |
        | one row        |                         | false | yyyy1  |
        | false filter   | WHERE false             | true  | NULL   |
        | false filter   | WHERE false             | false | NULL   |
        | empty relation | FROM range(0) LIMIT 1   | true  | NULL   |
        | empty relation | FROM range(0) LIMIT 1   | false | NULL   |

    # The scalar-subquery handling used by temporal formats must keep a missing
    # row nullable through outer projections and UNION.
    # TODO: Support outer references in non-aggregate scalar projections.
    @sail-bug
    Scenario: Missing correlated scalar remains NULL through an outer projection
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT result IS NULL AS is_null FROM (SELECT (SELECT coalesce(CAST(o.id AS STRING), 'yyyy') FROM range(0) LIMIT 1) AS result FROM range(1) o) t
        """
      Then query result
        | is_null |
        | true    |

    # TODO: Support outer references in non-aggregate scalar projections.
    @sail-bug
    Scenario: Missing correlated scalar remains NULL through a union
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT result IS NULL AS is_null FROM (SELECT (SELECT coalesce(CAST(o.id AS STRING), 'yyyy') FROM range(0) LIMIT 1) AS result FROM range(1) o UNION ALL SELECT 'x' AS result) t
        """
      Then query result
        | is_null |
        | true    |
        | false   |

    Scenario Outline: Correlated format projection rejects multiple scalar rows with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT (SELECT concat('yyyy', CAST(v AS STRING)) FROM range(2)) AS result FROM (SELECT 1 AS v) s
        """
      Then query error (?i)(scalar.*(more than one|multiple|one row)|more than one row)

      Examples:
        | ansi  |
        | true  |
        | false |

    # A retained scalar subquery checks its own row count before the typed input
    # can ignore its value. NULL input does not remove this required query work.
    Scenario Outline: Temporal format retains scalar subquery cardinality with ANSI <ansi>: <value>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(<value>, (SELECT CAST(id AS STRING) FROM range(2))) AS result
        """
      Then query error (?i)(scalar.*(more than one|multiple|one row)|more than one row)

      Examples:
        | value              | ansi  |
        | DATE '2024-01-01'  | true  |
        | DATE '2024-01-01'  | false |
        | CAST(NULL AS DATE) | true  |
        | CAST(NULL AS DATE) | false |
