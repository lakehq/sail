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

    Scenario: ISO timestamp
      When query
      """
      SELECT to_timestamp('2024-01-15 10:30:45') AS result
      """
      Then query result
      | result              |
      | 2024-01-15 10:30:45 |

    Scenario: Date-only parses with midnight
      When query
      """
      SELECT to_timestamp('2024-01-15') AS result
      """
      Then query result
      | result              |
      | 2024-01-15 00:00:00 |

    Scenario: With format
      When query
      """
      SELECT to_timestamp('2024-01-15 10:30:45', 'yyyy-MM-dd HH:mm:ss') AS result
      """
      Then query result
      | result              |
      | 2024-01-15 10:30:45 |

    Scenario: Cast from date
      When query
      """
      SELECT to_timestamp(DATE '2024-01-15') AS result
      """
      Then query result
      | result              |
      | 2024-01-15 00:00:00 |

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

    Scenario: NULL input returns NULL
      When query
      """
      SELECT to_timestamp(CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

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

  Rule: Argument count validation

    Scenario: to_timestamp zero arguments errors
      When query
        """
        SELECT to_timestamp() AS result
        """
      Then query error .*

    Scenario: to_timestamp three arguments errors
      When query
        """
        SELECT to_timestamp('2024-01-15', 'yyyy-MM-dd', 'extra') AS result
        """
      Then query error .*

  Rule: Schema inference — to_timestamp returns timestamp (with timezone), not timestamp_ntz

    Scenario: to_timestamp on untyped NULL returns timestamp schema
      When query
        """
        SELECT to_timestamp(NULL) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    # The 2-arg form lowers to `to_timestamp_micros(expr, format)` which always
    # returns timestamp_ntz, ignoring the session-timezone target computed by the
    # planner. Fix: cast the result of to_timestamp_micros to the correct target type.
    Scenario: to_timestamp on untyped NULL with format returns timestamp schema
      When query
        """
        SELECT to_timestamp(NULL, 'yyyy-MM-dd') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    # For string inputs, DataFusion coerces cast(string, Timestamp(LTZ)) back to
    # timestamp_ntz internally. NULL and typed inputs (DATE, TIMESTAMP_NTZ) are not
    # affected because their coercion paths differ.
    Scenario: to_timestamp on STRING returns timestamp schema
      When query
        """
        SELECT to_timestamp('2024-01-15 12:30:45') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: to_timestamp on DATE returns timestamp schema
      When query
        """
        SELECT to_timestamp(DATE '2024-01-15') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: to_timestamp on TIMESTAMP_NTZ returns timestamp schema
      When query
        """
        SELECT to_timestamp(TIMESTAMP_NTZ '2024-01-15 12:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

  Rule: String input — date and timestamp formats

    Scenario: to_timestamp parses date-only string
      When query
        """
        SELECT to_timestamp('2024-01-15') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: to_timestamp parses full timestamp string
      When query
        """
        SELECT to_timestamp('2024-01-15 12:30:45') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:30:45 |

    Scenario: to_timestamp parses timestamp with fractional seconds
      When query
        """
        SELECT to_timestamp('2024-01-15 12:30:45.123') AS result
        """
      Then query result
        | result                  |
        | 2024-01-15 12:30:45.123 |

  Rule: Format string (2-arg form)

    Scenario: to_timestamp with explicit format yyyy-MM-dd
      When query
        """
        SELECT to_timestamp('2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: to_timestamp with custom format MM/dd/yyyy HH:mm:ss
      When query
        """
        SELECT to_timestamp('01/15/2024 12:30:45', 'MM/dd/yyyy HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:30:45 |

    # Sail's `to_chrono_fmt` translation does not handle the `'T'` literal
    # quoting: `yyyy-MM-dd'T'HH:mm:ss` becomes `%Y-%m-%d'T'%H:%M:%S` which
    # chrono rejects ("input contains invalid characters"). Fix path: in
    # `to_chrono_fmt`, drop the single-quote escape syntax (chrono uses raw
    # literal characters between `%` directives, no quoting).
    Scenario: to_timestamp with ISO format including timezone marker
      When query
        """
        SELECT to_timestamp('2024-01-15T12:30:45', "yyyy-MM-dd'T'HH:mm:ss") AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:30:45 |

  Rule: Date and Timestamp inputs (passthrough/coercion)

    Scenario: to_timestamp on DATE produces midnight timestamp
      When query
        """
        SELECT to_timestamp(DATE '2024-01-15') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: to_timestamp on TIMESTAMP_NTZ preserves wall-clock value
      When query
        """
        SELECT to_timestamp(TIMESTAMP_NTZ '2024-06-15 14:30:00') AS result
        """
      Then query result
        | result              |
        | 2024-06-15 14:30:00 |

  Rule: NULL handling

    Scenario: to_timestamp on typed NULL STRING returns NULL
      When query
        """
        SELECT to_timestamp(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_timestamp on typed NULL DATE returns NULL
      When query
        """
        SELECT to_timestamp(CAST(NULL AS DATE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_timestamp on untyped NULL returns NULL value
      When query
        """
        SELECT to_timestamp(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: ANSI mode — invalid string input

    # Spark behaviour: ANSI=true raises CAST_INVALID_INPUT, ANSI=false returns NULL.
    # Sail's CAST is not ANSI-aware; the `to_timestamp` 1-arg form lowers to a
    # plain `cast(arg, Timestamp(_, _))` which always raises on parse failure,
    # ignoring `spark.sql.ansi.enabled = false`. Fix path: propagate
    # `PlanConfig.ansi_mode` into `CastOptions { safe: !ansi }` when wrapping
    # the coerced expr (same fix needed for several other UDFs).

    Scenario: to_timestamp invalid string under ANSI=true errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('not a date') AS result
        """
      Then query error .*

    Scenario: to_timestamp invalid string under ANSI=false returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp('not a date') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_timestamp empty string under ANSI=false returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp('') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Timezone semantics (potential divergence vectors)

    # to_timestamp with session timezone different from UTC — does the output
    # represent the same instant Spark JVM produces, or does Sail apply the
    # cast tz-naive?

    Scenario: to_timestamp on STRING with explicit offset preserves instant
      When query
        """
        SELECT to_timestamp('2024-01-15 12:00:00+02:00') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:00:00 |

    Scenario: to_timestamp on STRING with UTC marker preserves instant
      When query
        """
        SELECT to_timestamp('2024-01-15 12:00:00 UTC') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:00:00 |

    Scenario: to_timestamp roundtrip from TIMESTAMP preserves value
      When query
        """
        SELECT to_timestamp(TIMESTAMP '2024-06-15 14:30:45') AS result
        """
      Then query result
        | result              |
        | 2024-06-15 14:30:45 |

    # DST spring-forward in America/New_York: 02:30:00 on 2024-03-10 doesn't
    # exist (clocks jump from 02:00 to 03:00). Spark JVM rolls forward to
    # 03:30:00. Sail's CAST is timezone-naive so it returns the literal value
    # without DST adjustment.
    Scenario: to_timestamp on DST spring-forward boundary
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT to_timestamp('2024-03-10 02:30:00') AS result
        """
      Then query result
        | result              |
        | 2024-03-10 03:30:00 |

    # DST fall-back in America/New_York: 01:30:00 on 2024-11-03 is ambiguous
    # (clocks fall back at 02:00). Arrow's timezone offset computation fails
    # on ambiguous times, throwing ParseException. Spark JVM returns the
    # literal value 2024-11-03 01:30:00 (picks the first occurrence).
    Scenario: to_timestamp on DST fall-back boundary
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT to_timestamp('2024-11-03 01:30:00') AS result
        """
      Then query result
        | result              |
        | 2024-11-03 01:30:00 |

    Scenario: to_timestamp with tz offset under non-UTC session timezone
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT to_timestamp('2024-06-15 12:00:00+00:00') AS result
        """
      Then query result
        | result              |
        | 2024-06-15 08:00:00 |

  Rule: Multi-row vectorized path

    Scenario: to_timestamp on multi-row STRING column with NULL mix
      When query
        """
        SELECT to_timestamp(s) AS result FROM VALUES
          ('2024-01-15 12:00:00'),
          ('2024-06-15 18:30:45'),
          (CAST(NULL AS STRING)),
          ('1970-01-01 00:00:00')
          AS t(s)
        """
      Then query result
        | result              |
        | 2024-01-15 12:00:00 |
        | 2024-06-15 18:30:45 |
        | NULL                |
        | 1970-01-01 00:00:00 |

    Scenario: to_timestamp on multi-row DATE column with NULL mix
      When query
        """
        SELECT to_timestamp(d) AS result FROM VALUES
          (DATE '2024-01-15'),
          (DATE '2024-12-31'),
          (CAST(NULL AS DATE))
          AS t(d)
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |
        | 2024-12-31 00:00:00 |
        | NULL                |

    # Mixed valid + invalid under ANSI=false should give per-row NULL for
    # invalid inputs (not error the whole batch). Sail errors on first invalid
    # in the vectorized path due to the same ANSI-unaware CAST as scalar.
    Scenario: to_timestamp multi-row with invalid under ANSI=false yields per-row NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp(s) AS result FROM VALUES
          ('2024-01-15 12:00:00'),
          ('not a date'),
          ('2024-06-15 18:30:45')
          AS t(s)
        """
      Then query result
        | result              |
        | 2024-01-15 12:00:00 |
        | NULL                |
        | 2024-06-15 18:30:45 |

  Rule: spark.sql.timestampType controls return type

    # Sail honors spark.sql.timestampType for the default case (TIMESTAMP_LTZ),
    # but setting TIMESTAMP_NTZ has no effect — to_timestamp still returns LTZ.
    # The planner reads the config via PlanConfig, but the TIMESTAMP_NTZ path is
    # not exercised correctly when the string coercion overrides the cast target.
    Scenario: to_timestamp respects spark.sql.timestampType = TIMESTAMP_NTZ
      Given config spark.sql.timestampType = TIMESTAMP_NTZ
      When query
        """
        SELECT to_timestamp('2024-01-15 12:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = true)
        """

  Rule: Boundary dates

    Scenario: to_timestamp at year boundary 1970 epoch
      When query
        """
        SELECT to_timestamp('1970-01-01 00:00:00') AS result
        """
      Then query result
        | result              |
        | 1970-01-01 00:00:00 |

    Scenario: to_timestamp on leap day Feb 29
      When query
        """
        SELECT to_timestamp('2024-02-29 12:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-02-29 12:00:00 |

    Scenario: to_timestamp at year-end boundary
      When query
        """
        SELECT to_timestamp('9999-12-31 23:59:59') AS result
        """
      Then query result
        | result              |
        | 9999-12-31 23:59:59 |

  Rule: to_timestamp_ltz always returns timestamp (LTZ), ignoring spark.sql.timestampType

    # to_timestamp_ltz is the explicit LTZ variant: it always returns
    # timestamp (with session timezone), even when spark.sql.timestampType=TIMESTAMP_NTZ.

    Scenario: to_timestamp_ltz zero arguments errors
      When query
        """
        SELECT to_timestamp_ltz() AS result
        """
      Then query error .*

    Scenario: to_timestamp_ltz three arguments errors
      When query
        """
        SELECT to_timestamp_ltz('2024-01-15', 'yyyy-MM-dd', 'extra') AS result
        """
      Then query error .*

    Scenario: to_timestamp_ltz on untyped NULL returns timestamp schema
      When query
        """
        SELECT to_timestamp_ltz(NULL) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    # Same root cause as to_timestamp: the 2-arg form uses to_timestamp_micros
    # which ignores the LTZ target type and always returns timestamp_ntz.
    Scenario: to_timestamp_ltz on untyped NULL with format returns timestamp schema
      When query
        """
        SELECT to_timestamp_ltz(NULL, 'yyyy-MM-dd') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    # Same root cause as to_timestamp: DataFusion coerces cast(string, Timestamp(LTZ))
    # back to timestamp_ntz. DATE and TIMESTAMP_NTZ inputs are not affected.
    Scenario: to_timestamp_ltz on STRING returns timestamp schema
      When query
        """
        SELECT to_timestamp_ltz('2024-01-15 12:30:45') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: to_timestamp_ltz on DATE returns timestamp schema (not nullable)
      When query
        """
        SELECT to_timestamp_ltz(DATE '2024-01-15') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: to_timestamp_ltz on TIMESTAMP_NTZ returns timestamp schema (not nullable)
      When query
        """
        SELECT to_timestamp_ltz(TIMESTAMP_NTZ '2024-01-15 12:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    # When spark.sql.timestampType=TIMESTAMP_NTZ, the Spark Connect client may propagate
    # the config before the planner runs, causing to_timestamp_ltz to pick up NTZ instead
    # of always forcing LTZ. to_timestamp_ltz should return LTZ regardless of this config.
    Scenario: to_timestamp_ltz ignores spark.sql.timestampType=TIMESTAMP_NTZ and still returns LTZ
      Given config spark.sql.timestampType = TIMESTAMP_NTZ
      When query
        """
        SELECT to_timestamp_ltz('2024-01-15 12:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: to_timestamp_ltz parses date-only string
      When query
        """
        SELECT to_timestamp_ltz('2024-01-15') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: to_timestamp_ltz parses full timestamp string
      When query
        """
        SELECT to_timestamp_ltz('2024-01-15 12:30:45') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:30:45 |

    Scenario: to_timestamp_ltz parses timestamp with fractional seconds
      When query
        """
        SELECT to_timestamp_ltz('2024-01-15 12:30:45.123') AS result
        """
      Then query result
        | result                  |
        | 2024-01-15 12:30:45.123 |

    Scenario: to_timestamp_ltz with explicit format yyyy-MM-dd
      When query
        """
        SELECT to_timestamp_ltz('2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: to_timestamp_ltz with custom format MM/dd/yyyy HH:mm:ss
      When query
        """
        SELECT to_timestamp_ltz('01/15/2024 12:30:45', 'MM/dd/yyyy HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:30:45 |

    # Same as to_timestamp: to_chrono_fmt doesn't handle single-quote escaping,
    # so yyyy-MM-dd'T'HH:mm:ss becomes %Y-%m-%d'T'%H:%M:%S which chrono rejects.
    Scenario: to_timestamp_ltz with ISO format including T marker
      When query
        """
        SELECT to_timestamp_ltz('2024-01-15T12:30:45', "yyyy-MM-dd'T'HH:mm:ss") AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:30:45 |

    Scenario: to_timestamp_ltz on DATE produces midnight timestamp
      When query
        """
        SELECT to_timestamp_ltz(DATE '2024-01-15') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: to_timestamp_ltz on TIMESTAMP_NTZ preserves wall-clock value
      When query
        """
        SELECT to_timestamp_ltz(TIMESTAMP_NTZ '2024-06-15 14:30:00') AS result
        """
      Then query result
        | result              |
        | 2024-06-15 14:30:00 |

    Scenario: to_timestamp_ltz on TIMESTAMP preserves value
      When query
        """
        SELECT to_timestamp_ltz(TIMESTAMP '2024-06-15 14:30:45') AS result
        """
      Then query result
        | result              |
        | 2024-06-15 14:30:45 |

    Scenario: to_timestamp_ltz on typed NULL STRING returns NULL
      When query
        """
        SELECT to_timestamp_ltz(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_timestamp_ltz on typed NULL DATE returns NULL
      When query
        """
        SELECT to_timestamp_ltz(CAST(NULL AS DATE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_timestamp_ltz on untyped NULL returns NULL value
      When query
        """
        SELECT to_timestamp_ltz(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_timestamp_ltz invalid string under ANSI=true errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp_ltz('not a date') AS result
        """
      Then query error .*

    Scenario: to_timestamp_ltz invalid string under ANSI=false returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp_ltz('not a date') AS result
        """
      Then query result
        | result |
        | NULL   |

    # LTZ key behavior: string with explicit UTC offset is converted to
    # the session timezone (UTC). +02:00 is 2 hours ahead of UTC, so
    # 12:00:00+02:00 becomes 10:00:00 UTC.
    Scenario: to_timestamp_ltz with +02:00 offset converts to UTC
      When query
        """
        SELECT to_timestamp_ltz('2024-01-15 12:00:00+02:00') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:00:00 |

    Scenario: to_timestamp_ltz with +00:00 offset stays the same under UTC session tz
      When query
        """
        SELECT to_timestamp_ltz('2024-01-15 12:00:00+00:00') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:00:00 |

    Scenario: to_timestamp_ltz with UTC string marker stays the same under UTC session tz
      When query
        """
        SELECT to_timestamp_ltz('2024-01-15 12:00:00 UTC') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:00:00 |

    Scenario: to_timestamp_ltz with tz offset under non-UTC session timezone
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT to_timestamp_ltz('2024-06-15 12:00:00+00:00') AS result
        """
      Then query result
        | result              |
        | 2024-06-15 08:00:00 |

    Scenario: to_timestamp_ltz at epoch boundary
      When query
        """
        SELECT to_timestamp_ltz('1970-01-01 00:00:00') AS result
        """
      Then query result
        | result              |
        | 1970-01-01 00:00:00 |

    Scenario: to_timestamp_ltz on leap day
      When query
        """
        SELECT to_timestamp_ltz('2024-02-29 12:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-02-29 12:00:00 |

    Scenario: to_timestamp_ltz at year-end boundary
      When query
        """
        SELECT to_timestamp_ltz('9999-12-31 23:59:59') AS result
        """
      Then query result
        | result              |
        | 9999-12-31 23:59:59 |

    Scenario: to_timestamp_ltz on multi-row STRING column with NULL mix
      When query
        """
        SELECT to_timestamp_ltz(s) AS result FROM VALUES
          ('2024-01-15 12:00:00'),
          ('2024-06-15 18:30:45'),
          (CAST(NULL AS STRING)),
          ('1970-01-01 00:00:00')
          AS t(s)
        """
      Then query result
        | result              |
        | 2024-01-15 12:00:00 |
        | 2024-06-15 18:30:45 |
        | NULL                |
        | 1970-01-01 00:00:00 |

  Rule: to_timestamp_ntz always returns timestamp_ntz, ignoring spark.sql.timestampType

    # to_timestamp_ntz is the explicit NTZ variant: it always returns
    # timestamp_ntz (no timezone), even when spark.sql.timestampType=TIMESTAMP_LTZ.
    # Unlike to_timestamp_ltz, it ignores any timezone offset in the string —
    # it keeps the wall-clock time and strips the offset.

    Scenario: to_timestamp_ntz zero arguments errors
      When query
        """
        SELECT to_timestamp_ntz() AS result
        """
      Then query error .*

    Scenario: to_timestamp_ntz three arguments errors
      When query
        """
        SELECT to_timestamp_ntz('2024-01-15', 'yyyy-MM-dd', 'extra') AS result
        """
      Then query error .*

    Scenario: to_timestamp_ntz on untyped NULL returns timestamp_ntz schema
      When query
        """
        SELECT to_timestamp_ntz(NULL) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = true)
        """

    Scenario: to_timestamp_ntz on untyped NULL with format returns timestamp_ntz schema
      When query
        """
        SELECT to_timestamp_ntz(NULL, 'yyyy-MM-dd') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = true)
        """

    # For string inputs, DataFusion's cast coercion overrides the NTZ target type,
    # causing Sail to return timestamp_ltz schema instead of timestamp_ntz.
    Scenario: to_timestamp_ntz on STRING returns timestamp_ntz schema
      When query
        """
        SELECT to_timestamp_ntz('2024-01-15 12:30:45') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = true)
        """

    @sail-bug
    # cast(DATE, Timestamp(NTZ)) schema is timestamp (LTZ) in Sail instead of timestamp_ntz.
    # DATE inputs are non-nullable, but the type mismatch is the primary issue here.
    Scenario: to_timestamp_ntz on DATE returns timestamp_ntz schema (not nullable)
      When query
        """
        SELECT to_timestamp_ntz(DATE '2024-01-15') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = true)
        """

    @sail-bug
    Scenario: to_timestamp_ntz on TIMESTAMP returns timestamp_ntz schema (not nullable)
      When query
        """
        SELECT to_timestamp_ntz(TIMESTAMP '2024-01-15 12:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = false)
        """

    # When spark.sql.timestampType=TIMESTAMP_LTZ, Sail incorrectly returns timestamp (LTZ)
    # instead of timestamp_ntz. to_timestamp_ntz should always return NTZ regardless of config.
    Scenario: to_timestamp_ntz ignores spark.sql.timestampType=TIMESTAMP_LTZ and still returns NTZ
      Given config spark.sql.timestampType = TIMESTAMP_LTZ
      When query
        """
        SELECT to_timestamp_ntz('2024-01-15 12:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = true)
        """

    Scenario: to_timestamp_ntz parses date-only string
      When query
        """
        SELECT to_timestamp_ntz('2024-01-15') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: to_timestamp_ntz parses full timestamp string
      When query
        """
        SELECT to_timestamp_ntz('2024-01-15 12:30:45') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:30:45 |

    Scenario: to_timestamp_ntz parses timestamp with fractional seconds
      When query
        """
        SELECT to_timestamp_ntz('2024-01-15 12:30:45.123') AS result
        """
      Then query result
        | result                  |
        | 2024-01-15 12:30:45.123 |

    Scenario: to_timestamp_ntz with explicit format yyyy-MM-dd
      When query
        """
        SELECT to_timestamp_ntz('2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: to_timestamp_ntz with custom format MM/dd/yyyy HH:mm:ss
      When query
        """
        SELECT to_timestamp_ntz('01/15/2024 12:30:45', 'MM/dd/yyyy HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:30:45 |

    # Same as to_timestamp: to_chrono_fmt doesn't handle single-quote escaping,
    # so yyyy-MM-dd'T'HH:mm:ss becomes %Y-%m-%d'T'%H:%M:%S which chrono rejects.
    Scenario: to_timestamp_ntz with ISO format including T marker
      When query
        """
        SELECT to_timestamp_ntz('2024-01-15T12:30:45', "yyyy-MM-dd'T'HH:mm:ss") AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:30:45 |

    Scenario: to_timestamp_ntz on DATE produces midnight timestamp_ntz
      When query
        """
        SELECT to_timestamp_ntz(DATE '2024-01-15') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: to_timestamp_ntz on TIMESTAMP preserves wall-clock value
      When query
        """
        SELECT to_timestamp_ntz(TIMESTAMP '2024-06-15 14:30:45') AS result
        """
      Then query result
        | result              |
        | 2024-06-15 14:30:45 |

    Scenario: to_timestamp_ntz on typed NULL STRING returns NULL
      When query
        """
        SELECT to_timestamp_ntz(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_timestamp_ntz on typed NULL DATE returns NULL
      When query
        """
        SELECT to_timestamp_ntz(CAST(NULL AS DATE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_timestamp_ntz on untyped NULL returns NULL value
      When query
        """
        SELECT to_timestamp_ntz(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_timestamp_ntz invalid string under ANSI=true errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp_ntz('not a date') AS result
        """
      Then query error .*

    Scenario: to_timestamp_ntz invalid string under ANSI=false returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp_ntz('not a date') AS result
        """
      Then query result
        | result |
        | NULL   |

    # Spark NTZ strips the timezone offset and keeps the wall-clock time.
    # Sail converts the offset instead (same behavior as LTZ), returning 10:00:00 UTC
    # instead of preserving 12:00:00.
    Scenario: to_timestamp_ntz with +02:00 offset keeps wall-clock time (strips offset)
      When query
        """
        SELECT to_timestamp_ntz('2024-01-15 12:00:00+02:00') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:00:00 |

    Scenario: to_timestamp_ntz with +00:00 offset keeps wall-clock time
      When query
        """
        SELECT to_timestamp_ntz('2024-01-15 12:00:00+00:00') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:00:00 |

    Scenario: to_timestamp_ntz with UTC marker keeps wall-clock time
      When query
        """
        SELECT to_timestamp_ntz('2024-01-15 12:00:00 UTC') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 12:00:00 |

    # NTZ ignores session timezone — same result regardless of session tz
    Scenario: to_timestamp_ntz is unaffected by non-UTC session timezone
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT to_timestamp_ntz('2024-06-15 12:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-06-15 12:00:00 |

    Scenario: to_timestamp_ntz at epoch boundary
      When query
        """
        SELECT to_timestamp_ntz('1970-01-01 00:00:00') AS result
        """
      Then query result
        | result              |
        | 1970-01-01 00:00:00 |

    Scenario: to_timestamp_ntz on leap day
      When query
        """
        SELECT to_timestamp_ntz('2024-02-29 12:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-02-29 12:00:00 |

    Scenario: to_timestamp_ntz at year-end boundary
      When query
        """
        SELECT to_timestamp_ntz('9999-12-31 23:59:59') AS result
        """
      Then query result
        | result              |
        | 9999-12-31 23:59:59 |

    Scenario: to_timestamp_ntz on multi-row STRING column with NULL mix
      When query
        """
        SELECT to_timestamp_ntz(s) AS result FROM VALUES
          ('2024-01-15 12:00:00'),
          ('2024-06-15 18:30:45'),
          (CAST(NULL AS STRING)),
          ('1970-01-01 00:00:00')
          AS t(s)
        """
      Then query result
        | result              |
        | 2024-01-15 12:00:00 |
        | 2024-06-15 18:30:45 |
        | NULL                |
        | 1970-01-01 00:00:00 |

  Rule: Invalid input throws

    Scenario: Garbage string raises error
      When query
      """
      SELECT to_timestamp('not-a-timestamp')
      """
      Then query error CAST_INVALID_INPUT|cannot be cast|error parsing|error in SQL parser

    @sail-bug
    Scenario: Format mismatch raises error
      When query
      """
      SELECT to_timestamp('2024-01-15', 'dd/MM/yyyy')
      """
      Then query error CANNOT_PARSE_TIMESTAMP|invalid characters|could not be parsed

  Rule: A numeric value is a count of seconds since the epoch
    # Spark 4.2.0 datetimeExpressions.scala: ParseToTimestamp adds NumericType to its input types
    # when the result is TIMESTAMP and, without a format, becomes Cast(left, TimestampType).
    # Spark 4.2.0 Cast.scala castToTimestamp: integers are SECONDS.toMicros, fractional values are
    # (d * MICROS_PER_SECOND).toLong and decimals keep microsecond precision.

    @sail-bug
    Scenario Outline: to_timestamp reads <case> input as seconds with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(<value>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case            | ansi  | value                             | result                        |
        | integer         | true  | 1700000000                        | 2023-11-14 22:13:20           |
        | integer         | false | 1700000000                        | 2023-11-14 22:13:20           |
        | tinyint         | true  | CAST(1 AS TINYINT)                | 1970-01-01 00:00:01           |
        | double          | true  | 1.5D                              | 1970-01-01 00:00:01.5         |
        | double          | false | 1.5D                              | 1970-01-01 00:00:01.5         |
        | negative double | true  | -1.5D                             | 1969-12-31 23:59:58.5         |
        | float           | false | CAST(1.5 AS FLOAT)                | 1970-01-01 00:00:01.5         |
        | decimal         | true  | CAST(1.23456789 AS DECIMAL(10,8)) | 1970-01-01 00:00:01.234567    |
        | largest bigint  | false | 9223372036854775807               | +294247-01-10 04:00:54.775807 |

    @sail-bug
    Scenario: to_timestamp reads an integer column as seconds per row
      When query
        """
        SELECT i, to_timestamp(v) AS result
        FROM VALUES (1, 0), (2, 1700000000), (3, CAST(NULL AS INT)) AS x(i, v)
        ORDER BY i
        """
      Then query result ordered
        | i | result              |
        | 1 | 1970-01-01 00:00:00 |
        | 2 | 2023-11-14 22:13:20 |
        | 3 | NULL                |

    # Spark 4.2.0 Cast.scala doubleToTimestamp: NaN and infinities are NULL with ANSI off and the
    # Long conversion saturates, so 1e20 seconds becomes the largest timestamp.
    @sail-bug
    Scenario: to_timestamp of a double column maps non-finite values to NULL with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT i, to_timestamp(v) AS result
        FROM VALUES
          (1, 1.5D),
          (2, CAST('NaN' AS DOUBLE)),
          (3, -2.25D),
          (4, CAST('-Infinity' AS DOUBLE)),
          (5, 1e20D)
          AS x(i, v)
        ORDER BY i
        """
      Then query result ordered
        | i | result                        |
        | 1 | 1970-01-01 00:00:01.5         |
        | 2 | NULL                          |
        | 3 | 1969-12-31 23:59:57.75        |
        | 4 | NULL                          |
        | 5 | +294247-01-10 04:00:54.775807 |

    # Spark 4.2.0 SparkDateTimeUtils.doubleToTimestampAnsi raises CAST_INVALID_INPUT for NaN and
    # infinities and CAST_OVERFLOW when the microseconds overflow a BIGINT.
    @sail-bug
    Scenario Outline: to_timestamp of the <case> double fails with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp(<value>) AS result
        """
      Then query error <error>

      Examples:
        | case     | value                      | error              |
        | NaN      | CAST('NaN' AS DOUBLE)      | CAST_INVALID_INPUT |
        | infinity | CAST('Infinity' AS DOUBLE) | CAST_INVALID_INPUT |
        | huge     | 1e20D                      | CAST_OVERFLOW      |

    # BOOLEAN is not among ParseToTimestamp's input types, so it is implicitly cast to STRING and
    # 'true' then fails the timestamp cast.
    @sail-bug
    Scenario: to_timestamp of a boolean returns NULL with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp(true) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: to_timestamp of a boolean fails the string cast with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp(true) AS result
        """
      Then query error CAST_INVALID_INPUT

  @function(nullability)
  Rule: Numeric and formatted input nullability

    # Spark 4.2.0 Cast.scala: a DOUBLE to TIMESTAMP cast can yield NULL (NaN), an INT one cannot.
    @sail-bug
    Scenario: to_timestamp of a double literal is nullable
      When query
        """
        SELECT to_timestamp(1.5D) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: to_timestamp of an integer literal is not nullable
      When query
        """
        SELECT to_timestamp(1700000000) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    # Spark 4.2.0 datetimeExpressions.scala: ToTimestamp.nullable is
    # `if (failOnError) children.exists(_.nullable) else true`.
    @sail-bug
    Scenario: to_timestamp of a string literal with a format is not nullable with ANSI enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: to_timestamp of a string literal with a format is nullable with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp('2024-01-15', 'yyyy-MM-dd') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

  Rule: Without a format, to_timestamp follows the lenient STRING to TIMESTAMP cast
    # Spark 4.2.0 SparkDateTimeUtils.parseTimestampString: the input is trimmed, a bare time
    # ('10:30:45' or 'T10:30:45') takes today's date in the session zone, the zone suffix may be
    # any ZoneId.of form (GMT+1, short ids such as PST), and years reach +294247.

    @sail-bug
    Scenario Outline: to_timestamp accepts the lenient cast form <case> with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(<value>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                | ansi  | value                           | result                 |
        | surrounding spaces  | true  | '  2024-01-15 10:30:00  '       | 2024-01-15 10:30:00    |
        | surrounding spaces  | false | '  2024-01-15 10:30:00  '       | 2024-01-15 10:30:00    |
        | leading space date  | true  | ' 2024-01-16'                   | 2024-01-16 00:00:00    |
        | GMT offset zone     | true  | '2024-01-15 10:30:45 GMT+1'     | 2024-01-15 09:30:45    |
        | short zone id       | false | '2024-01-15 10:30:45 PST'       | 2024-01-15 18:30:45    |
        | six-digit year      | true  | '294247-01-01'                  | +294247-01-01 00:00:00 |

    @sail-bug
    Scenario Outline: to_timestamp of the bare time <value> takes the current date with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp(<value>) = timestamp(concat(CAST(current_date() AS STRING), ' 10:30:45')) AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | ansi  | value       |
        | true  | 'T10:30:45' |
        | false | '10:30:45'  |

    @sail-bug
    Scenario: to_timestamp applies the lenient cast per row with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT i, to_timestamp(v) AS result
        FROM VALUES
          (1, '  2024-01-15 10:00:00 '),
          (2, '2024-01-16 11:00:00 XYZ'),
          (3, '2024-01-17 12:00:00+02:00'),
          (4, '294247-01-01')
          AS x(i, v)
        ORDER BY i
        """
      Then query result ordered
        | i | result                 |
        | 1 | 2024-01-15 10:00:00    |
        | 2 | NULL                   |
        | 3 | 2024-01-17 10:00:00    |
        | 4 | +294247-01-01 00:00:00 |

    # Spark 4.2.0 SparkDateTimeUtils.stringToTimestampAnsi raises CAST_INVALID_INPUT for every
    # malformed or out-of-range string.
    @sail-bug
    Scenario Outline: to_timestamp rejects <case> with CAST_INVALID_INPUT under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp(<value>) AS result
        """
      Then query error CAST_INVALID_INPUT

      Examples:
        | case                 | value                     |
        | trailing garbage     | '2024-01-15 garbage'      |
        | an hour of 25        | '2024-01-15 25:00:00'     |
        | February 30          | '2024-02-30'              |
        | an empty string      | ''                        |
        | an unknown zone      | '2024-01-15 10:30:45 XYZ' |
        | a year beyond 294247 | '294248-01-01'            |

    Scenario Outline: to_timestamp returns NULL for <case> with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp(<value>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                 | value                     |
        | trailing garbage     | '2024-01-15 garbage'      |
        | an hour of 25        | '2024-01-15 25:00:00'     |
        | February 30          | '2024-02-30'              |
        | an unknown zone      | '2024-01-15 10:30:45 XYZ' |
        | a year beyond 294247 | '294248-01-01'            |

  Rule: With a format, a signed year beyond 9999 parses and mismatches are NULL per row

    @sail-bug
    Scenario Outline: to_timestamp parses a signed five-digit year with a yyyy format and ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT to_timestamp('+10000-01-01', 'yyyy-MM-dd') AS result
        """
      Then query result
        | result                |
        | +10000-01-01 00:00:00 |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario: to_timestamp with a format column returns NULL only for mismatched rows with ANSI disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT i, to_timestamp(v, f) AS result
        FROM VALUES
          (1, '2024-01-15 10:00', 'yyyy-MM-dd HH:mm'),
          (2, '15/01/2024', 'yyyy-MM-dd'),
          (3, '16/01/2024 11:30', 'dd/MM/yyyy HH:mm'),
          (4, '2024-01-17', CAST(NULL AS STRING))
          AS x(i, v, f)
        ORDER BY i
        """
      Then query result ordered
        | i | result              |
        | 1 | 2024-01-15 10:00:00 |
        | 2 | NULL                |
        | 3 | 2024-01-16 11:30:00 |
        | 4 | NULL                |

  Rule: A string without an offset is local time in a non-UTC session time zone
    # Spark 4.2.0 SparkDateTimeUtils.stringToTimestamp builds ZonedDateTime.of(localDateTime,
    # sessionZone): a time in a DST gap moves forward by the gap and a time in a DST overlap takes
    # the earlier offset. unix_seconds exposes the instant, which a zone-blind parser gets wrong.

    Scenario Outline: to_timestamp reads <case> as local time in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT to_timestamp(<value>) AS ts, unix_seconds(to_timestamp(<value>)) AS s
        """
      Then query result
        | ts   | s   |
        | <ts> | <s> |

      Examples:
        | case                      | zone                | value                 | ts                  | s          |
        | a summer time             | America/Los_Angeles | '2024-06-15 12:00:00' | 2024-06-15 12:00:00 | 1718478000 |
        | a time in the DST gap     | America/Los_Angeles | '2024-03-10 02:30:00' | 2024-03-10 03:30:00 | 1710066600 |
        | a time in the DST overlap | America/Los_Angeles | '2024-11-03 01:30:00' | 2024-11-03 01:30:00 | 1730622600 |
        | a half-hour offset time   | Asia/Kolkata        | '2024-06-15 12:00:00' | 2024-06-15 12:00:00 | 1718433000 |
        | a 45-minute offset time   | Pacific/Chatham     | '2024-06-15 12:00:00' | 2024-06-15 12:00:00 | 1718406900 |
        | a negative offset time    | Pacific/Pago_Pago   | '2024-06-15 12:00:00' | 2024-06-15 12:00:00 | 1718492400 |

    Scenario Outline: to_timestamp with a format reads <case> as local time in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT unix_seconds(to_timestamp(<value>, 'yyyy-MM-dd HH:mm')) AS s
        """
      Then query result
        | s   |
        | <s> |

      Examples:
        | case                      | zone                | value              | s          |
        | a time in the DST gap     | America/Los_Angeles | '2024-03-10 02:30' | 1710066600 |
        | a time in the DST overlap | America/Los_Angeles | '2024-11-03 01:30' | 1730622600 |
        | a 45-minute offset time   | Pacific/Chatham     | '2024-06-15 12:00' | 1718406900 |

    Scenario: to_timestamp reads a string column as local time per row in a DST zone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT i, to_timestamp(v) AS ts, unix_seconds(to_timestamp(v)) AS s
        FROM VALUES
          (1, '2024-01-15 08:00:00'),
          (2, '2024-07-15 20:00:00'),
          (3, '2024-03-10 02:30:00'),
          (4, '2024-11-03 01:30:00')
          AS x(i, v)
        ORDER BY i
        """
      Then query result ordered
        | i | ts                  | s          |
        | 1 | 2024-01-15 08:00:00 | 1705334400 |
        | 2 | 2024-07-15 20:00:00 | 1721098800 |
        | 3 | 2024-03-10 03:30:00 | 1710066600 |
        | 4 | 2024-11-03 01:30:00 | 1730622600 |

  Rule: A string with an offset or zone id is an instant shown in the session time zone

    Scenario Outline: to_timestamp converts <case> into <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT to_timestamp(<value>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case             | zone                | value                              | result              |
        | a UTC offset     | America/Los_Angeles | '2024-06-15 12:00:00+00:00'        | 2024-06-15 05:00:00 |
        | a UTC offset     | Asia/Kolkata        | '2024-06-15 12:00:00+00:00'        | 2024-06-15 17:30:00 |
        | a region zone id | Pacific/Chatham     | '2024-06-15 12:00:00 Europe/Paris' | 2024-06-15 22:45:00 |
        | a Z suffix       | Pacific/Pago_Pago   | '2024-06-15T12:00:00Z'             | 2024-06-15 01:00:00 |

    Scenario Outline: to_timestamp with an XXX format converts an offset string into <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT to_timestamp('2024-06-15 12:00:00 +05:30', 'yyyy-MM-dd HH:mm:ss XXX') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | zone                | result              |
        | America/Los_Angeles | 2024-06-14 23:30:00 |
        | Asia/Kolkata        | 2024-06-15 12:00:00 |
        | Pacific/Chatham     | 2024-06-15 19:15:00 |
        | Pacific/Pago_Pago   | 2024-06-14 19:30:00 |

  Rule: A number of seconds is an instant shown in the session time zone

    Scenario: CAST of an integer column to TIMESTAMP is shown in a 45-minute offset zone
      Given config spark.sql.session.timeZone = Pacific/Chatham
      When query
        """
        SELECT i, CAST(v AS TIMESTAMP) AS ts FROM VALUES (1, 0), (2, 1700000000) AS x(i, v) ORDER BY i
        """
      Then query result ordered
        | i | ts                  |
        | 1 | 1970-01-01 12:45:00 |
        | 2 | 2023-11-15 11:58:20 |

    @sail-bug
    Scenario Outline: to_timestamp of an integer number of seconds is shown in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT to_timestamp(1700000000) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | zone                | result              |
        | America/Los_Angeles | 2023-11-14 14:13:20 |
        | Asia/Kolkata        | 2023-11-15 03:43:20 |
        | Pacific/Chatham     | 2023-11-15 11:58:20 |
        | Pacific/Pago_Pago   | 2023-11-14 11:13:20 |

    @sail-bug
    Scenario: to_timestamp of a bare time takes the current date of a 45-minute offset zone
      Given config spark.sql.session.timeZone = Pacific/Chatham
      When query
        """
        SELECT to_timestamp('10:30:45') = timestamp(concat(CAST(current_date() AS STRING), ' 10:30:45')) AS result
        """
      Then query result
        | result |
        | true   |
