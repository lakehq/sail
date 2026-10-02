Feature: Datetime functions

  Rule: weekday

    @sail-bug
    Scenario: weekday sunday is 6
      When query
        """
        SELECT weekday(DATE '2024-03-17') AS result
        """
      Then query result
        | result |
        | 6      |

    # Spark's weekday is Monday-based (0..6); DataFusion's DOW is Sunday-based (0..6).
    # The whole week via a column also exercises the columnar kernel, which a
    # constant-folded literal never reaches.
    @sail-bug
    Scenario: weekday over a full week from a column
      When query
        """
        SELECT weekday(c) AS result FROM VALUES
          (DATE '2024-03-11'), (DATE '2024-03-12'), (DATE '2024-03-13'),
          (DATE '2024-03-14'), (DATE '2024-03-15'), (DATE '2024-03-16'),
          (DATE '2024-03-17'), (NULL) AS t(c)
        """
      Then query result
        | result |
        | 0      |
        | 1      |
        | 2      |
        | 3      |
        | 4      |
        | 5      |
        | 6      |
        | NULL   |

    Scenario: dayofweek is sunday-based
      When query
        """
        SELECT dayofweek(DATE '2024-03-17') AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: null handling

    Scenario: year of null
      When query
        """
        SELECT year(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: month of null
      When query
        """
        SELECT month(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: datediff with null
      When query
        """
        SELECT datediff(NULL, DATE '2024-03-15') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: last_day of null
      When query
        """
        SELECT last_day(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    # The NULL guard lives in the plan builder, so it must also hold when the
    # value arrives as a column rather than a literal.
    Scenario: year of a nullable column
      When query
        """
        SELECT year(c) AS result FROM VALUES (DATE '2024-03-17'), (NULL) AS t(c)
        """
      Then query result
        | result |
        | 2024   |
        | NULL   |

    @sail-bug
    Scenario: weekday of a null column
      When query
        """
        SELECT weekday(c) AS result FROM VALUES (NULL) AS t(c)
        """
      Then query result
        | result |
        | NULL   |

    Scenario: year of a typed null
      When query
        """
        SELECT year(CAST(NULL AS DATE)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: next_day null handling

    Scenario: next_day with null day of week
      When query
        """
        SELECT next_day(DATE '2024-03-15', NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: implicit string to date casting

    # Spark parses date strings leniently, independent of ANSI mode. Sail accepts
    # these in `year` (which has its own UDF) but rejects them in every other
    # extraction function, which routes through a stricter timestamp parser.
    Scenario: year accepts a non-zero-padded date string
      When query
        """
        SELECT year('2024-3-7') AS result
        """
      Then query result
        | result |
        | 2024   |

    @sail-bug
    Scenario: month accepts a non-zero-padded date string
      When query
        """
        SELECT month('2024-3-7') AS result
        """
      Then query result
        | result |
        | 3      |

    @sail-bug
    Scenario: year accepts a whitespace padded date string
      When query
        """
        SELECT year('  2024-03-17  ') AS result
        """
      Then query result
        | result |
        | 2024   |

    @sail-bug
    Scenario: month accepts a whitespace padded date string
      When query
        """
        SELECT month(' 2024-03-17') AS result
        """
      Then query result
        | result |
        | 3      |

    @sail-bug
    Scenario: year accepts a trailing whitespace date string
      When query
        """
        SELECT year('2024-03-17  ') AS result
        """
      Then query result
        | result |
        | 2024   |

    # Spark completes a partial date with January 1st.
    @sail-bug
    Scenario: year accepts a year-only date string
      When query
        """
        SELECT year('2024') AS result
        """
      Then query result
        | result |
        | 2024   |

    @sail-bug
    Scenario: month of a year-only date string defaults to january
      When query
        """
        SELECT month('2024') AS result
        """
      Then query result
        | result |
        | 1      |

    @sail-bug
    Scenario: last_day of a year-only date string
      When query
        """
        SELECT last_day('2024') AS result
        """
      Then query result
        | result     |
        | 2024-01-31 |

    # Spark completes a year-month date with the 1st of the month.
    @sail-bug
    Scenario: day of a year-month date string defaults to the first
      When query
        """
        SELECT day('2024-03') AS result
        """
      Then query result
        | result |
        | 1      |

    @sail-bug
    Scenario: weekday of a year-month date string
      When query
        """
        SELECT weekday('2024-03') AS result
        """
      Then query result
        | result |
        | 4      |

    @sail-bug
    Scenario: last_day of a year-month date string
      When query
        """
        SELECT last_day('2024-03') AS result
        """
      Then query result
        | result     |
        | 2024-03-31 |

  Rule: unparseable date strings honour ANSI mode

    # Spark's string→date cast is ANSI-aware: an unparseable string is an error
    # under ANSI=true but NULL under ANSI=false. Sail errors under both.

    Scenario: year of an unparseable string errors under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT year('not-a-date') AS result
        """
      Then query error .*

    @sail-bug
    Scenario: year of an unparseable string is null under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT year('not-a-date') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: year of an out-of-range date string is null under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT year('2024-13-45') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: datediff of an unparseable string is null under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT datediff('bad', DATE '1970-01-01') AS result
        """
      Then query result
        | result |
        | NULL   |

    # Via a column the cast must resolve row by row: one bad row yields NULL for
    # that row only. Sail fails the whole query instead.
    @sail-bug
    Scenario: year resolves an unparseable row to null under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT year(c) AS result
        FROM VALUES ('2024-03-17'), ('not-a-date'), (NULL) AS t(c)
        """
      Then query result
        | result |
        | 2024   |
        | NULL   |
        | NULL   |

    @sail-bug
    Scenario: year accepts a non-zero-padded row from a column
      When query
        """
        SELECT year(c) AS result
        FROM VALUES ('2024-03-17'), ('2024-3-7'), ('  2024-03-17  '), (NULL) AS t(c)
        """
      Then query result
        | result |
        | 2024   |
        | 2024   |
        | 2024   |
        | NULL   |

  Rule: months_between round-off argument

    # roundOff=true rounds to 8 decimal places; false returns full precision.
    Scenario: months_between rounds to 8 decimals by default
      When query
        """
        SELECT months_between(DATE '1997-02-28', DATE '1996-10-30', true) AS result
        """
      Then query result
        | result     |
        | 3.93548387 |

    Scenario: months_between with round-off disabled keeps full precision
      When query
        """
        SELECT months_between(DATE '1997-02-28', DATE '1996-10-30', false) AS result
        """
      Then query result
        | result            |
        | 3.935483870967742 |

    # Sail ignores a NULL round-off and returns the value anyway.
    @sail-bug
    Scenario: months_between propagates a null round-off
      When query
        """
        SELECT months_between(DATE '2024-03-31', DATE '2024-02-29', NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: months_between resolves the round-off per row
      When query
        """
        SELECT months_between(DATE '2024-03-31', DATE '2024-02-29', c) AS result
        FROM VALUES (true), (false), (NULL) AS t(c)
        """
      Then query result
        | result |
        | 1.0    |
        | 1.0    |
        | NULL   |

    # Spark requires a boolean round-off; Sail accepts anything and ignores it.
    @sail-bug
    Scenario: months_between rejects a non-boolean round-off
      When query
        """
        SELECT months_between(DATE '2024-03-31', DATE '2024-02-29', 1) AS result
        """
      Then query error .*

    # Under ANSI=true Spark implicitly promotes the string to a boolean, so 'yes'
    # means round-off enabled. Under ANSI=false there is no such promotion and the
    # string is rejected outright — Sail accepts it under both modes.
    Scenario: months_between promotes a boolean-like string under ANSI true
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT months_between(DATE '1997-02-28', DATE '1996-10-30', 'yes') AS result
        """
      Then query result
        | result     |
        | 3.93548387 |

    @sail-bug
    Scenario: months_between rejects a string round-off under ANSI false
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT months_between(DATE '1997-02-28', DATE '1996-10-30', 'yes') AS result
        """
      Then query error .*

  Rule: months_between over the full date range

    # Sail converts the date to microseconds and overflows; every other
    # extraction function handles these dates fine.
    @sail-bug
    Scenario: months_between at the maximum date
      When query
        """
        SELECT months_between(DATE '9999-12-31', DATE '1970-01-01') AS result
        """
      Then query result
        | result         |
        | 96359.96774194 |

    @sail-bug
    Scenario: months_between before the gregorian cutover
      When query
        """
        SELECT months_between(DATE '1582-10-04', DATE '1970-01-01') AS result
        """
      Then query result
        | result         |
        | -4646.90322581 |

    # The same extreme dates work when they arrive as a column, which pins the
    # overflow to constant folding at planning time rather than to the kernel.
    Scenario: months_between at the maximum date from a column
      When query
        """
        SELECT months_between(a, b) AS result FROM VALUES
          (DATE '2024-03-31', DATE '2024-02-29'),
          (DATE '9999-12-31', DATE '1970-01-01') AS t(a, b)
        """
      Then query result
        | result         |
        | 1.0            |
        | 96359.96774194 |

  Rule: hour, minute and second accept a timezone argument

    # Spark's Hour/Minute/Second carry an optional timeZoneId parameter, so a
    # second argument is accepted. Sail rejects it with an arity error.
    @sail-bug
    Scenario: hour accepts a second argument
      When query
        """
        SELECT hour(DATE '2024-03-17', DATE '2024-03-17') AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: argument type validation

    # Sail is more permissive than Spark here and silently returns a value.
    @sail-bug
    Scenario: month rejects an interval
      When query
        """
        SELECT month(INTERVAL 1 YEAR) AS result
        """
      Then query error .*

    @sail-bug
    Scenario: datediff rejects an integer
      When query
        """
        SELECT datediff(1, DATE '2024-03-17') AS result
        """
      Then query error .*

  Rule: return types

    # Spark's datediff returns INT; Sail returns BIGINT. The value matches, so
    # only a schema assertion catches this.
    @sail-bug
    Scenario: datediff returns an integer
      When query
        """
        SELECT datediff(DATE '2024-03-17', DATE '2024-03-15') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

  # Spark 4.2.0 datetimeExpressions.scala, SecondsToTimestamp: NUMERIC input. Integral is
  # `Math.multiplyExact(v, 1000000)`; DECIMAL is `BigDecimal.multiply(...).longValueExact()`;
  # FLOAT/DOUBLE is `(v * 1000000).toLong`, and NaN/Infinity give NULL (hence nullable).
  Rule: timestamp_seconds keeps fractional seconds

    @sail-bug
    Scenario Outline: timestamp_seconds keeps the fraction of a <case>
      When query
        """
        SELECT timestamp_seconds(<sec>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | sec                             | result                     |
        | positive decimal      | 1.5                             | 1970-01-01 00:00:01.5      |
        | negative decimal      | -1.5                            | 1969-12-31 23:59:58.5      |
        | six-digit decimal     | CAST(1.123456 AS DECIMAL(10,6)) | 1970-01-01 00:00:01.123456 |
        | positive double       | CAST(1.5 AS DOUBLE)             | 1970-01-01 00:00:01.5      |
        | negative double       | CAST(-1.5 AS DOUBLE)            | 1969-12-31 23:59:58.5      |
        | float                 | CAST(1.5 AS FLOAT)              | 1970-01-01 00:00:01.5      |
        | negative micro double | CAST(-0.000001 AS DOUBLE)       | 1969-12-31 23:59:59.999999 |

    @sail-bug
    Scenario: timestamp_seconds keeps the fraction of double rows
      When query
        """
        SELECT timestamp_seconds(c) AS result
        FROM VALUES (1, CAST(0.5 AS DOUBLE)), (2, CAST(-0.5 AS DOUBLE)), (3, CAST(1.25 AS DOUBLE)), (4, CAST(NULL AS DOUBLE)) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result                 |
        | 1970-01-01 00:00:00.5  |
        | 1969-12-31 23:59:59.5  |
        | 1970-01-01 00:00:01.25 |
        | NULL                   |

    @sail-bug
    Scenario: timestamp_seconds keeps the fraction of decimal rows
      When query
        """
        SELECT timestamp_seconds(c) AS result
        FROM VALUES (1, CAST(0.5 AS DECIMAL(10,3))), (2, CAST(-1.25 AS DECIMAL(10,3))) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result                 |
        | 1970-01-01 00:00:00.5  |
        | 1969-12-31 23:59:58.75 |

    @sail-bug
    Scenario Outline: timestamp_seconds of a non-finite double is NULL: <case>
      When query
        """
        SELECT timestamp_seconds(CAST('<value>' AS DOUBLE)) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case     | value    |
        | NaN      | NaN      |
        | Infinity | Infinity |

    @sail-bug
    Scenario: timestamp_seconds resolves a NaN row to NULL
      When query
        """
        SELECT timestamp_seconds(c) AS result
        FROM VALUES (1, CAST(0.5 AS DOUBLE)), (2, CAST('NaN' AS DOUBLE)), (3, CAST(1.25 AS DOUBLE)) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result                 |
        | 1970-01-01 00:00:00.5  |
        | NULL                   |
        | 1970-01-01 00:00:01.25 |

    # longValueExact() refuses to drop sub-microsecond digits.
    @sail-bug
    Scenario: timestamp_seconds rejects a decimal finer than a microsecond
      When query
        """
        SELECT timestamp_seconds(CAST(1.1234567 AS DECIMAL(10,7))) AS result
        """
      Then query error Rounding necessary

  @function(nullability)
  Rule: timestamp_seconds output schema

    # SecondsToTimestamp.nullable is true for FLOAT/DOUBLE, child.nullable otherwise.
    @sail-bug
    Scenario Outline: timestamp_seconds of a non-null <case> is nullable
      When query
        """
        SELECT timestamp_seconds(<sec>) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

      Examples:
        | case   | sec               |
        | double | CAST(1 AS DOUBLE) |
        | float  | CAST(1 AS FLOAT)  |

    Scenario Outline: timestamp_seconds of a non-null <case> is not nullable
      When query
        """
        SELECT timestamp_seconds(<sec>) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

      Examples:
        | case    | sec |
        | integer | 1   |

  # Math.multiplyExact is not ANSI-gated: it throws "long overflow" in both modes.
  Rule: timestamp_seconds and timestamp_millis overflow

    @sail-bug
    Scenario Outline: <function> of <case> overflows with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <function>(<value>) AS result
        """
      Then query error long overflow

      Examples:
        | function          | case                  | value                | ansi  |
        | timestamp_seconds | the BIGINT maximum    | 9223372036854775807  | true  |
        | timestamp_seconds | the BIGINT maximum    | 9223372036854775807  | false |
        | timestamp_seconds | the BIGINT minimum    | -9223372036854775808 | true  |
        | timestamp_seconds | one second past range | 9223372036855        | true  |
        | timestamp_millis  | the BIGINT maximum    | 9223372036854775807  | true  |
        | timestamp_millis  | the BIGINT maximum    | 9223372036854775807  | false |

    @sail-bug
    Scenario: timestamp_seconds overflows on a column row
      When query
        """
        SELECT timestamp_seconds(c) AS result
        FROM VALUES (1, CAST(1 AS BIGINT)), (2, CAST(9223372036854775807 AS BIGINT)) AS t(i, c)
        """
      Then query error long overflow

    @sail-bug
    Scenario Outline: <function> reaches the end of the TIMESTAMP range: <case>
      When query
        """
        SELECT <function>(<value>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function          | case                  | value                | result                        |
        | timestamp_seconds | the last whole second | 9223372036854        | +294247-01-10 04:00:54        |
        | timestamp_micros  | the BIGINT maximum    | 9223372036854775807  | +294247-01-10 04:00:54.775807 |
        | timestamp_micros  | the BIGINT minimum    | -9223372036854775808 | -290308-12-21 19:59:05.224192 |

    Scenario: timestamp_millis and timestamp_micros resolve negative rows from a column
      When query
        """
        SELECT timestamp_millis(c) AS millis, timestamp_micros(c) AS micros
        FROM VALUES (1, CAST(-1 AS BIGINT)), (2, CAST(1230219000123 AS BIGINT)), (3, NULL) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | millis                  | micros                     |
        | 1969-12-31 23:59:59.999 | 1969-12-31 23:59:59.999999 |
        | 2008-12-25 15:30:00.123 | 1970-01-15 05:43:39.000123 |
        | NULL                    | NULL                       |

  # SecondsToTimestamp takes NUMERIC; MillisToTimestamp and MicrosToTimestamp take INTEGRAL
  # (IntegralToTimestampBase). All are ExpectsInputTypes: no implicit cast.
  Rule: timestamp_seconds, timestamp_millis and timestamp_micros argument types

    @sail-bug
    Scenario Outline: epoch constructor <function> rejects a <case> argument
      When query
        """
        SELECT <function>(<value>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE

      Examples:
        | function          | case    | value             |
        | timestamp_seconds | string  | '1'               |
        | timestamp_millis  | decimal | 1.5               |
        | timestamp_millis  | double  | CAST(1 AS DOUBLE) |
        | timestamp_micros  | decimal | 1.5               |

  # Spark 4.2.0 datetimeExpressions.scala, TimestampToLongBase: Math.floorDiv(micros, scale),
  # so any instant before the epoch rounds DOWN, unlike unix_timestamp.
  Rule: unix_seconds and unix_millis floor toward negative infinity

    @sail-bug
    Scenario Outline: <function> floors <case>
      When query
        """
        SELECT <function>(TIMESTAMP '<ts>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function     | case                                | ts                         | result       |
        | unix_seconds | half a second before the epoch      | 1969-12-31 23:59:59.5      | -1           |
        | unix_seconds | one microsecond before the epoch    | 1969-12-31 23:59:59.999999 | -1           |
        | unix_seconds | half a second into year 1           | 0001-01-01 00:00:00.5      | -62135596800 |
        | unix_millis  | half a millisecond before the epoch | 1969-12-31 23:59:59.9995   | -1           |
        | unix_millis  | one microsecond before the epoch    | 1969-12-31 23:59:59.999999 | -1           |

    @sail-bug
    Scenario: unix_seconds floors pre-epoch rows from a column
      When query
        """
        SELECT unix_seconds(c) AS result
        FROM VALUES (1, TIMESTAMP '1969-12-31 23:59:59.5'), (2, TIMESTAMP '1970-01-01 00:00:01.5'), (3, TIMESTAMP '2008-12-25 07:30:00'), (4, NULL) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result     |
        | -1         |
        | 1          |
        | 1230190200 |
        | NULL       |

    @sail-bug
    Scenario: unix_millis floors pre-epoch rows from a column
      When query
        """
        SELECT unix_millis(c) AS result
        FROM VALUES (1, TIMESTAMP '1969-12-31 23:59:59.9995'), (2, TIMESTAMP '1969-12-31 23:59:59.999'), (3, TIMESTAMP '1970-01-01 00:00:00.0015') AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | -1     |
        | -1     |
        | 1      |

    Scenario Outline: <function> of <case>
      When query
        """
        SELECT <function>(TIMESTAMP '<ts>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function     | case                             | ts                         | result             |
        | unix_seconds | a whole second before the epoch  | 1969-12-31 23:59:59        | -1                 |
        | unix_micros  | one microsecond before the epoch | 1969-12-31 23:59:59.999999 | -1                 |
        | unix_micros  | the last 4-digit-year instant    | 9999-12-31 23:59:59.999999 | 253402300799999999 |

    # TimestampToLongBase is ExpectsInputTypes(TIMESTAMP): no implicit cast.
    @sail-bug
    Scenario Outline: epoch extractor <function> rejects a <case> argument
      When query
        """
        SELECT <function>(<value>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE

      Examples:
        | function     | case          | value                               |
        | unix_seconds | TIMESTAMP_NTZ | TIMESTAMP_NTZ '2024-01-01 00:00:00' |
        | unix_seconds | string        | '2024-01-01 00:00:00'               |
        | unix_millis  | date          | DATE '2024-01-01'                   |

  # A TIMESTAMP (LTZ) is an instant; Spark renders it, and casts it to DATE, STRING or
  # TIMESTAMP_NTZ, in the session time zone (Spark 4.2.0 Cast.scala / DateTimeUtils).
  Rule: timestamp_seconds, timestamp_millis and timestamp_micros render in the session time zone

    @sail-bug
    Scenario Outline: epoch constructors render in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT
          timestamp_seconds(0) AS seconds,
          timestamp_millis(1230219000123) AS millis,
          timestamp_micros(1720000000123456) AS micros
        """
      Then query result
        | seconds   | millis   | micros   |
        | <seconds> | <millis> | <micros> |

      Examples:
        | zone                | seconds             | millis                  | micros                     |
        | America/Los_Angeles | 1969-12-31 16:00:00 | 2008-12-25 07:30:00.123 | 2024-07-03 02:46:40.123456 |
        | Asia/Kolkata        | 1970-01-01 05:30:00 | 2008-12-25 21:00:00.123 | 2024-07-03 15:16:40.123456 |
        | Pacific/Chatham     | 1970-01-01 12:45:00 | 2008-12-26 05:15:00.123 | 2024-07-03 22:31:40.123456 |
        | Pacific/Pago_Pago   | 1969-12-31 13:00:00 | 2008-12-25 04:30:00.123 | 2024-07-02 22:46:40.123456 |

    @sail-bug
    Scenario Outline: timestamp_seconds renders column rows in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT timestamp_seconds(c) AS result
        FROM VALUES (1, 0), (2, 1230219000), (3, -1) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |
        | <r3>   |

      Examples:
        | zone                | r1                  | r2                  | r3                  |
        | America/Los_Angeles | 1969-12-31 16:00:00 | 2008-12-25 07:30:00 | 1969-12-31 15:59:59 |
        | Asia/Kolkata        | 1970-01-01 05:30:00 | 2008-12-25 21:00:00 | 1970-01-01 05:29:59 |
        | Pacific/Chatham     | 1970-01-01 12:45:00 | 2008-12-26 05:15:00 | 1970-01-01 12:44:59 |
        | Pacific/Pago_Pago   | 1969-12-31 13:00:00 | 2008-12-25 04:30:00 | 1969-12-31 12:59:59 |

    @sail-bug
    Scenario Outline: timestamp_seconds casts to DATE and STRING in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT CAST(timestamp_seconds(c) AS DATE) AS d, CAST(timestamp_seconds(c) AS STRING) AS s
        FROM VALUES (1, 0), (2, 1720000000), (3, 1704067200) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | d    | s    |
        | <d1> | <s1> |
        | <d2> | <s2> |
        | <d3> | <s3> |

      Examples:
        | zone                | d1         | s1                  | d2         | s2                  | d3         | s3                  |
        | America/Los_Angeles | 1969-12-31 | 1969-12-31 16:00:00 | 2024-07-03 | 2024-07-03 02:46:40 | 2023-12-31 | 2023-12-31 16:00:00 |
        | Asia/Kolkata        | 1970-01-01 | 1970-01-01 05:30:00 | 2024-07-03 | 2024-07-03 15:16:40 | 2024-01-01 | 2024-01-01 05:30:00 |
        | Pacific/Chatham     | 1970-01-01 | 1970-01-01 12:45:00 | 2024-07-03 | 2024-07-03 22:31:40 | 2024-01-01 | 2024-01-01 13:45:00 |
        | Pacific/Pago_Pago   | 1969-12-31 | 1969-12-31 13:00:00 | 2024-07-02 | 2024-07-02 22:46:40 | 2023-12-31 | 2023-12-31 13:00:00 |

    @sail-bug
    Scenario Outline: unix_date of a timestamp_seconds cast to DATE in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT unix_date(CAST(timestamp_seconds(0) AS DATE)) AS result
        """
      Then query result
        | result |
        | -1     |

      Examples:
        | zone                |
        | America/Los_Angeles |
        | Pacific/Pago_Pago   |

    @sail-bug
    Scenario: timestamp_seconds renders both instants of a Los Angeles DST overlap as 01:30
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT CAST(timestamp_seconds(c) AS STRING) AS result
        FROM VALUES (1, 1730622600), (2, 1730626200), (3, 1710063000) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result              |
        | 2024-11-03 01:30:00 |
        | 2024-11-03 01:30:00 |
        | 2024-03-10 01:30:00 |

  Rule: unix_seconds, unix_millis and unix_micros of session-zone literals

    Scenario Outline: epoch extractors read TIMESTAMP literals in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT
          unix_seconds(TIMESTAMP '2024-01-15 01:02:03') AS seconds,
          unix_millis(TIMESTAMP '2024-07-15 01:02:03.456') AS millis,
          unix_micros(TIMESTAMP '1970-01-01 00:00:00') AS micros
        """
      Then query result
        | seconds   | millis   | micros   |
        | <seconds> | <millis> | <micros> |

      Examples:
        | zone                | seconds    | millis        | micros       |
        | America/Los_Angeles | 1705309323 | 1721030523456 | 28800000000  |
        | Asia/Kolkata        | 1705260723 | 1720985523456 | -19800000000 |
        | Pacific/Chatham     | 1705231023 | 1720959423456 | -45900000000 |
        | Pacific/Pago_Pago   | 1705320123 | 1721044923456 | 39600000000  |

    Scenario Outline: unix_seconds reads TIMESTAMP rows in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT unix_seconds(c) AS result
        FROM VALUES (1, TIMESTAMP '2024-01-15 01:02:03'), (2, TIMESTAMP '2024-07-15 01:02:03'), (3, TIMESTAMP '1970-01-01 00:00:00') AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |
        | <r3>   |

      Examples:
        | zone                | r1         | r2         | r3     |
        | America/Los_Angeles | 1705309323 | 1721030523 | 28800  |
        | Asia/Kolkata        | 1705260723 | 1720985523 | -19800 |
        | Pacific/Chatham     | 1705231023 | 1720959423 | -45900 |
        | Pacific/Pago_Pago   | 1705320123 | 1721044923 | 39600  |

    Scenario: unix_seconds resolves Los Angeles DST literals
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          unix_seconds(TIMESTAMP '2024-03-10 02:30:00') AS gap,
          unix_seconds(TIMESTAMP '2024-11-03 01:30:00') AS overlap,
          TIMESTAMP '2024-03-10 02:30:00' AS gap_rendered
        """
      Then query result
        | gap        | overlap    | gap_rendered        |
        | 1710066600 | 1730622600 | 2024-03-10 03:30:00 |

  # Cast.scala: DATE/STRING/TIMESTAMP_NTZ -> TIMESTAMP interpret the wall clock in the
  # session zone; TIMESTAMP -> TIMESTAMP_NTZ keeps the session-zone wall clock.
  Rule: casts between TIMESTAMP, TIMESTAMP_NTZ, DATE and STRING use the session time zone

    Scenario Outline: casts to TIMESTAMP interpret the wall clock in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT
          unix_seconds(CAST(DATE '2024-01-15' AS TIMESTAMP)) AS from_date,
          unix_seconds(CAST('2024-07-15 01:02:03' AS TIMESTAMP)) AS from_string,
          unix_seconds(CAST(TIMESTAMP_NTZ '2024-01-15 01:02:03' AS TIMESTAMP)) AS from_ntz
        """
      Then query result
        | from_date   | from_string   | from_ntz   |
        | <from_date> | <from_string> | <from_ntz> |

      Examples:
        | zone                | from_date  | from_string | from_ntz   |
        | America/Los_Angeles | 1705305600 | 1721030523  | 1705309323 |
        | Asia/Kolkata        | 1705257000 | 1720985523  | 1705260723 |
        | Pacific/Chatham     | 1705227300 | 1720959423  | 1705231023 |
        | Pacific/Pago_Pago   | 1705316400 | 1721044923  | 1705320123 |

    Scenario Outline: DATE rows cast to TIMESTAMP in the session zone <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT unix_seconds(CAST(c AS TIMESTAMP)) AS result
        FROM VALUES (1, DATE '2024-01-15'), (2, DATE '2024-07-15') AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | zone                | r1         | r2         |
        | America/Los_Angeles | 1705305600 | 1721026800 |
        | Asia/Kolkata        | 1705257000 | 1720981800 |
        | Pacific/Chatham     | 1705227300 | 1720955700 |
        | Pacific/Pago_Pago   | 1705316400 | 1721041200 |

    Scenario: TIMESTAMP_NTZ in a Los Angeles DST gap or overlap casts to TIMESTAMP
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT
          unix_seconds(CAST(TIMESTAMP_NTZ '2024-03-10 02:30:00' AS TIMESTAMP)) AS gap,
          unix_seconds(CAST(TIMESTAMP_NTZ '2024-11-03 01:30:00' AS TIMESTAMP)) AS overlap
        """
      Then query result
        | gap        | overlap    |
        | 1710066600 | 1730622600 |

    @sail-bug
    Scenario Outline: TIMESTAMP casts to TIMESTAMP_NTZ keeping the session-zone wall clock in <zone>
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT
          CAST(CAST(TIMESTAMP '2024-01-15 01:02:03' AS TIMESTAMP_NTZ) AS STRING) AS from_literal,
          CAST(CAST(timestamp_seconds(0) AS TIMESTAMP_NTZ) AS STRING) AS from_epoch
        """
      Then query result
        | from_literal        | from_epoch   |
        | 2024-01-15 01:02:03 | <from_epoch> |

      Examples:
        | zone                | from_epoch          |
        | America/Los_Angeles | 1969-12-31 16:00:00 |
        | Asia/Kolkata        | 1970-01-01 05:30:00 |
        | Pacific/Chatham     | 1970-01-01 12:45:00 |
        | Pacific/Pago_Pago   | 1969-12-31 13:00:00 |
