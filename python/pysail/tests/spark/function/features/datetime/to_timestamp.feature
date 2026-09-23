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

    @sail-bug
    Scenario Outline: Numeric seconds (Sail reads the value as microseconds): <case>
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

    @sail-bug
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

    @sail-bug
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

    @sail-bug
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

    @sail-bug
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

    @sail-bug
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

    @sail-bug
    Scenario: a valid value with an unrecognized pattern raises
      When query
        """
        SELECT to_timestamp('2016-12-31', 'qqq') AS result
        """
      Then query error Unrecognized datetime pattern

    @sail-bug
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

    @sail-bug
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
