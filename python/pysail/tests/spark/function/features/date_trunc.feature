@date_trunc
Feature: DATE_TRUNC truncates a timestamp to a unit
  # `date_trunc` (TruncTimestamp) and `trunc` (TruncDate) are different Spark expressions and do
  # NOT accept the same units: `trunc` only truncates to date-level units, and anything finer --
  # or unrecognized, or NULL -- is NULL, not an error. Each has its own unit table in the plan
  # builder, and they share the matching machinery, so `trunc.feature` covers the same ground for
  # the date expression and the two must be kept in step.
  # All expected values were captured on Spark JVM 4.1.1.

  Rule: date_trunc returns timestamp for every date-family input type

    Scenario: date_trunc on timestamp column returns timestamp type
      When query
      """
      WITH t(ts) AS (VALUES (TIMESTAMP '2026-02-02 00:00:00 UTC'))
      SELECT date_trunc('YEAR', ts) AS result FROM t
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """

    Scenario: date_trunc on timestamp_ntz column returns timestamp type
      When query
      """
      WITH t(ts) AS (VALUES (TIMESTAMP_NTZ '2026-02-02 00:00:00'))
      SELECT date_trunc('YEAR', ts) AS result FROM t
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """

    Scenario: date_trunc on timestamp_ntz literal returns timestamp type
      When query
      """
      SELECT date_trunc('YEAR', TIMESTAMP_NTZ '2026-02-02 00:00:00') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """

    Scenario: date_trunc on date literal returns timestamp type
      When query
      """
      SELECT date_trunc('DAY', DATE '2024-01-15') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2024-01-15 00:00:00 |

    Scenario: date_trunc on date column returns timestamp type
      When query
      """
      WITH t(d) AS (VALUES (DATE '2026-02-02'))
      SELECT date_trunc('YEAR', d) AS result FROM t
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2026-01-01 00:00:00 |

    Scenario: date_trunc on timestamp_ntz literal value
      When query
      """
      SELECT date_trunc('DAY', TIMESTAMP_NTZ '2024-01-15 12:34:56') AS result
      """
      Then query result
      | result              |
      | 2024-01-15 00:00:00 |

    Scenario: date_trunc on string date value coerces to timestamp
      When query
      """
      SELECT date_trunc('DAY', '2024-01-15') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2024-01-15 00:00:00 |

    Scenario: date_trunc on string timestamp value coerces to timestamp
      When query
      """
      SELECT date_trunc('DAY', '2024-01-15 12:34:56') AS result
      """
      Then query result
      | result              |
      | 2024-01-15 00:00:00 |

  Rule: truncation unit selects the granularity

    Scenario: date_trunc YEAR truncates to start of year
      When query
      """
      SELECT date_trunc('YEAR', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-01-01 00:00:00 |

    Scenario: date_trunc QUARTER truncates to start of quarter
      When query
      """
      SELECT date_trunc('QUARTER', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-04-01 00:00:00 |

    Scenario: date_trunc MONTH truncates to start of month
      When query
      """
      SELECT date_trunc('MONTH', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-05-01 00:00:00 |

    Scenario: date_trunc WEEK truncates to Monday
      When query
      """
      SELECT date_trunc('WEEK', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-05-13 00:00:00 |

    Scenario: date_trunc DAY truncates to start of day
      When query
      """
      SELECT date_trunc('DAY', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 00:00:00 |

    Scenario: date_trunc HOUR truncates to start of hour
      When query
      """
      SELECT date_trunc('HOUR', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 13:00:00 |

    Scenario: date_trunc MINUTE truncates to start of minute
      When query
      """
      SELECT date_trunc('MINUTE', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 13:45:00 |

    Scenario: date_trunc SECOND truncates fractional seconds
      When query
      """
      SELECT date_trunc('SECOND', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 13:45:30 |

    Scenario: date_trunc MILLISECOND keeps milliseconds
      When query
      """
      SELECT date_trunc('MILLISECOND', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result                  |
      | 2024-05-15 13:45:30.123 |

    Scenario: date_trunc MICROSECOND keeps microseconds
      When query
      """
      SELECT date_trunc('MICROSECOND', TIMESTAMP '2024-05-15 13:45:30.123456') AS result
      """
      Then query result
      | result                     |
      | 2024-05-15 13:45:30.123456 |

  Rule: unit aliases and case are accepted

    Scenario: date_trunc accepts YYYY alias for year
      When query
      """
      SELECT date_trunc('YYYY', TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result              |
      | 2024-01-01 00:00:00 |

    Scenario: date_trunc accepts MM alias for month
      When query
      """
      SELECT date_trunc('MM', TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result              |
      | 2024-05-01 00:00:00 |

    Scenario: date_trunc accepts DD alias for day
      When query
      """
      SELECT date_trunc('DD', TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 00:00:00 |

    Scenario: date_trunc unit is case insensitive
      When query
      """
      SELECT date_trunc('yEaR', TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result              |
      | 2024-01-01 00:00:00 |

  Rule: invalid or null unit returns null

    Scenario: date_trunc with an unrecognized unit returns null
      When query
      """
      SELECT date_trunc('INVALID', TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: date_trunc with an empty unit returns null
      When query
      """
      SELECT date_trunc('', TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: date_trunc with a null unit returns null
      When query
      """
      SELECT date_trunc(CAST(NULL AS STRING), TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: the unit is resolved per row when it comes from a column

    # The unit is not required to be a literal: Spark resolves it row by row, so each row is
    # truncated by its own unit and an unrecognized or NULL unit nullifies only that row.

    Scenario: date_trunc with the unit coming from a column
      When query
      """
      SELECT date_trunc(u, TIMESTAMP '2026-02-02 10:11:12') AS result FROM VALUES ('YEAR'), ('MONTH') AS t(u)
      """
      # The columnar path builds its own result type rather than reusing the one a literal unit
      # produces, and the optimizer may drop the branch that carries it, so pin it down here too.
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result ordered
      | result              |
      | 2026-01-01 00:00:00 |
      | 2026-02-01 00:00:00 |

    Scenario: an unrecognized unit in a column nullifies only its own row
      When query
      """
      SELECT date_trunc(u, TIMESTAMP '2026-02-02 10:11:12') AS result FROM VALUES ('YEAR'), ('INVALID') AS t(u)
      """
      Then query result ordered
      | result              |
      | 2026-01-01 00:00:00 |
      | NULL                |

    Scenario: a null unit in a column nullifies only its own row
      When query
      """
      SELECT date_trunc(u, TIMESTAMP '2026-02-02 10:11:12') AS result FROM VALUES ('YEAR'), (NULL) AS t(u)
      """
      Then query result ordered
      | result              |
      | 2026-01-01 00:00:00 |
      | NULL                |

    Scenario: units in a column keep their aliases and are case insensitive
      When query
      """
      SELECT date_trunc(u, TIMESTAMP '2026-02-02 10:11:12') AS result FROM VALUES ('yyyy'), ('MM'), ('Day'), ('hour') AS t(u)
      """
      Then query result ordered
      | result              |
      | 2026-01-01 00:00:00 |
      | 2026-02-01 00:00:00 |
      | 2026-02-02 00:00:00 |
      | 2026-02-02 10:00:00 |

  Rule: null value returns null

    # A NULL prints the same whatever its type, so every scenario here also pins the type down:
    # a result typed `void` or `timestamp_ntz` would otherwise pass unnoticed.

    Scenario: date_trunc on a null date returns null
      When query
      """
      SELECT date_trunc('DAY', CAST(NULL AS DATE)) AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result |
      | NULL   |

    Scenario: date_trunc on a null timestamp returns null
      When query
      """
      SELECT date_trunc('DAY', CAST(NULL AS TIMESTAMP)) AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result |
      | NULL   |

    Scenario: date_trunc on an untyped null value returns a null timestamp
      When query
      """
      SELECT date_trunc('DAY', NULL) AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result |
      | NULL   |

  Rule: a non-string unit is measured as its string form

    Scenario: an integer unit matches no granularity and returns null
      When query
      """
      SELECT date_trunc(1, TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: an unparseable string value under ANSI on errors

    # Sail raises here, but carries DataFusion's parser message ("Error parsing timestamp from
    # ...") instead of Spark's CAST_INVALID_INPUT class. That is the ANSI string-to-timestamp
    # cast surface, not this expression: every function that casts a string under ANSI reports
    # it the same way. Asserting on Spark's class keeps the gap visible instead of letting a
    # blank `.*` pass anything.

    @sail-bug
    Scenario: an unparseable string value errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT date_trunc('DAY', 'not a date') AS result
      """
      Then query error CAST_INVALID_INPUT

    @sail-bug
    Scenario: an empty string value errors under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT date_trunc('DAY', '') AS result
      """
      Then query error CAST_INVALID_INPUT

  Rule: an unparseable string value under ANSI off returns NULL

    Scenario: an unparseable string value returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT date_trunc('DAY', 'not a date') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: an empty string value returns NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT date_trunc('DAY', '') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: an unparseable string value nulls only its own row under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT date_trunc('DAY', s) AS result FROM VALUES ('2024-05-15'), ('not a date'), (NULL) AS t(s)
      """
      Then query result ordered
      | result              |
      | 2024-05-15 00:00:00 |
      | NULL                |
      | NULL                |

  Rule: date_trunc rejects non-date input types

    # The value is rejected for its TYPE, whatever the unit is: an unrecognized or NULL unit
    # does not turn the rejection into a NULL. Spark raises this while it analyzes the query, so
    # the scenarios match on Spark's own error class rather than on any error at all: a Sail
    # error of a different class is a different observable behaviour for a Spark client.

    Scenario: date_trunc rejects a time value
      When query
      """
      SELECT date_trunc('DAY', TIME '12:30:00') AS result
      """
      Then query error DATATYPE_MISMATCH

    Scenario: date_trunc rejects an integer value
      When query
      """
      SELECT date_trunc('DAY', 1) AS result
      """
      Then query error DATATYPE_MISMATCH

    Scenario: date_trunc rejects an integer value even when the unit is unrecognized
      When query
      """
      SELECT date_trunc('INVALID', 1) AS result
      """
      Then query error DATATYPE_MISMATCH

    Scenario: date_trunc rejects an integer value even when the unit is null
      When query
      """
      SELECT date_trunc(CAST(NULL AS STRING), 1) AS result
      """
      Then query error DATATYPE_MISMATCH

    Scenario: date_trunc rejects a boolean value even when the unit is unrecognized
      When query
      """
      SELECT date_trunc('INVALID', true) AS result
      """
      Then query error DATATYPE_MISMATCH

    Scenario: date_trunc rejects a time value even when the unit is null
      When query
      """
      SELECT date_trunc(NULL, TIME '12:30:00') AS result
      """
      Then query error DATATYPE_MISMATCH

  Rule: date boundaries and leap days

    # Truncating a timestamp whose microseconds do not fit in a 64-bit nanosecond count
    # overflows. The bound is not the maximum year but the granularity: a granularity that has
    # to be resolved against a time zone goes through the nanosecond conversion, so the year at
    # which it breaks depends on which units take that path, not on the type's range.

    Scenario: date_trunc YEAR at the maximum supported year
      When query
      """
      SELECT date_trunc('YEAR', DATE '9999-12-31') AS result
      """
      Then query result
      | result              |
      | 9999-01-01 00:00:00 |

    Scenario: date_trunc DAY beyond the nanosecond range
      When query
      """
      SELECT date_trunc('DAY', TIMESTAMP '2300-01-01 05:30:00 UTC') AS result
      """
      Then query result
      | result              |
      | 2300-01-01 00:00:00 |

    Scenario: date_trunc HOUR beyond the nanosecond range
      When query
      """
      SELECT date_trunc('HOUR', TIMESTAMP '2300-01-01 05:30:00 UTC') AS result
      """
      Then query result
      | result              |
      | 2300-01-01 05:00:00 |

    Scenario: date_trunc SECOND beyond the nanosecond range
      When query
      """
      SELECT date_trunc('SECOND', TIMESTAMP '2300-01-01 05:30:00 UTC') AS result
      """
      Then query result
      | result              |
      | 2300-01-01 05:30:00 |

    Scenario: date_trunc DAY at the maximum supported year
      When query
      """
      SELECT date_trunc('DAY', TIMESTAMP '9999-12-31 23:59:59 UTC') AS result
      """
      Then query result
      | result              |
      | 9999-12-31 00:00:00 |

    Scenario: date_trunc DAY on a leap day
      When query
      """
      SELECT date_trunc('DAY', DATE '2024-02-29') AS result
      """
      Then query result
      | result              |
      | 2024-02-29 00:00:00 |

  Rule: timezone-aware truncation on timestamps

    Scenario: date_trunc YEAR on timestamp values
      When query
      """
      SELECT date_trunc('YEAR', TIMESTAMP '2026-02-02 00:00:00 UTC') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2026-01-01 00:00:00 |

    Scenario: date_trunc MONTH on timestamp values
      When query
      """
      SELECT date_trunc('MONTH', TIMESTAMP '2026-03-15 10:30:00 UTC') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2026-03-01 00:00:00 |

    Scenario: date_trunc DAY on timestamp with America/Los_Angeles timezone
      When query
      """
      SELECT date_trunc('DAY', TIMESTAMP '2026-03-15 02:30:00 America/Los_Angeles') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2026-03-15 00:00:00 |

    Scenario: date_trunc HOUR on timestamp with America/New_York timezone
      When query
      """
      SELECT date_trunc('HOUR', TIMESTAMP '2026-03-15 14:45:30 America/New_York') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = true)
      """
      Then query result
      | result              |
      | 2026-03-15 18:00:00 |

  Rule: truncated date compares equal to a timestamp literal

    Scenario: date_trunc on date equals timestamp literal
      When query
      """
      SELECT count(*) AS result FROM (SELECT DATE '2019-08-04' AS d)
      WHERE date_trunc('year', d) = TIMESTAMP '2019-01-01 00:00:00'
      """
      Then query result
      | result |
      | 1      |

  Rule: the unit is resolved once per row, even when it is not foldable

    # A unit built with `rand` is volatile: matching it once per candidate unit draws a new value
    # for every comparison, so a row can match nothing and turn NULL. Spark resolves the unit once
    # per row and never yields NULL here. A unit from `VALUES` is folded by Spark and hides this.

    Scenario: a volatile unit never nullifies a row
      When query
      """
      SELECT count(*) AS nulls FROM (
        SELECT date_trunc(IF(rand(7) < 0.5, 'YEAR', 'MONTH'), TIMESTAMP '2024-05-15 13:45:30') AS r
        FROM range(10000)
      ) WHERE r IS NULL
      """
      Then query result
      | nulls |
      | 0     |

    # The scenario above cannot see a unit that is evaluated more than once: both draws name a
    # granularity, so any number of evaluations still truncates. Draw a NULL against a valid unit
    # instead and the count itself becomes the evidence -- one evaluation gives ~5000, and every
    # extra one pushes it up (two give ~7500, since the row turns NULL if EITHER draw does).

    # The count is asserted as a RANGE, not a value: `rand` is seeded, but nothing says the two
    # engines draw the same sequence from that seed. The range is far tighter than the gap it has
    # to separate -- one evaluation lands near 5000, two near 7500.

    Scenario: a volatile unit is drawn exactly once per row
      When query
      """
      SELECT count(*) BETWEEN 4700 AND 5300 AS drawn_once FROM (
        SELECT date_trunc(IF(rand(7) < 0.5, CAST(NULL AS STRING), 'YEAR'),
                          TIMESTAMP '2024-05-15 13:45:30') AS r
        FROM range(10000)
      ) WHERE r IS NULL
      """
      Then query result
      | drawn_once |
      | true       |

  Rule: the value is converted even when the unit matches nothing

    # Spark evaluates both arguments before it validates the unit, so an unconvertible value
    # raises under ANSI even on a row whose unit matches no granularity. Resolving the unit first
    # and skipping the conversion for unmatched rows would silently return NULL instead. Both
    # arguments are non-foldable so that Spark cannot fold the rows away.

    # Two scenarios, because one assertion cannot carry both claims. The tagged one states the
    # target -- Spark's error CLASS -- and xfails until Sail's string cast reports it. But an
    # xfail is satisfied by ANY failure, including Sail simply returning NULL, so on its own it
    # would not notice this rule breaking. The untagged one below matches what each engine
    # actually prints, so it holds on BOTH and fails the moment either stops raising.
    @sail-bug
    Scenario: an unconvertible value reports Spark's error class when its unit matches nothing
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT id, date_trunc(IF(id = 0, 'INVALID', 'DAY'), IF(id = 0, 'not a date', '2024-01-02')) AS result
      FROM range(2) ORDER BY id
      """
      Then query error CAST_INVALID_INPUT

    Scenario: an unconvertible value errors under ANSI on even when its unit matches nothing
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT id, date_trunc(IF(id = 0, 'INVALID', 'DAY'), IF(id = 0, 'not a date', '2024-01-02')) AS result
      FROM range(2) ORDER BY id
      """
      Then query error (CAST_INVALID_INPUT|error in SQL parser: found .*expected '-')

    Scenario: an unconvertible value is NULL under ANSI off even when its unit matches nothing
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT id, date_trunc(IF(id = 0, 'INVALID', 'DAY'), IF(id = 0, 'not a date', '2024-01-02')) AS result
      FROM range(2) ORDER BY id
      """
      Then query result ordered
      | id | result              |
      | 0  | NULL                |
      | 1  | 2024-01-02 00:00:00 |

    # A LITERAL unit that names no granularity is the other half of the rule: Spark's codegen
    # short-circuits to NULL without evaluating the value at all, so the same unconvertible value
    # does NOT raise here. Converting the value unconditionally would turn these into errors.

    Scenario: a literal unit that matches nothing skips the value entirely under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT id, date_trunc('INVALID', IF(id = 0, 'not a date', '2024-01-02')) AS result
      FROM range(2) ORDER BY id
      """
      Then query result ordered
      | id | result |
      | 0  | NULL   |
      | 1  | NULL   |

    Scenario: a literal NULL unit skips the value entirely under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT id, date_trunc(CAST(NULL AS STRING), IF(id = 0, 'not a date', '2024-01-02')) AS result
      FROM range(2) ORDER BY id
      """
      Then query result ordered
      | id | result |
      | 0  | NULL   |
      | 1  | NULL   |

    @sail-bug
    Scenario: a literal unit that names a granularity reports Spark's error class
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT id, date_trunc('DAY', IF(id = 0, 'not a date', '2024-01-02')) AS result
      FROM range(2) ORDER BY id
      """
      Then query error CAST_INVALID_INPUT

    Scenario: a literal unit that names a granularity still converts the value under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT id, date_trunc('DAY', IF(id = 0, 'not a date', '2024-01-02')) AS result
      FROM range(2) ORDER BY id
      """
      Then query error (CAST_INVALID_INPUT|error in SQL parser: found .*expected '-')

    # A NULL unit is NOT the same case as a unit that matches nothing, and which one skips the
    # value follows the argument ORDER of the Spark expression: `TruncTimestamp` is
    # `(format, timestamp)`, so a NULL format never reaches the timestamp and no ANSI error can
    # come out of it. `trunc` is `(date, format)` and does the opposite -- see trunc.feature.

    Scenario: a NULL unit in a column never converts the value
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT id, date_trunc(IF(id = 0, CAST(NULL AS STRING), 'DAY'),
                            IF(id = 0, 'not a date', '2024-01-02')) AS result
      FROM range(2) ORDER BY id
      """
      Then query result ordered
      | id | result              |
      | 0  | NULL                |
      | 1  | 2024-01-02 00:00:00 |

  Rule: the string "null" is an ordinary unit, not the absence of one

    # Telling "the unit was NULL" apart from "the unit matched nothing" needs a value that is not
    # NULL, since NULL is already what an unmatched unit yields. A unit is upper-cased before it
    # is matched, so a user unit can never come out lower case, and the marker is unreachable from
    # data -- but that is a property to demonstrate, not to assume: the string "null" has to
    # behave like any other unit that names no granularity, INCLUDING converting the value.

    Scenario: the string null is just a unit that matches nothing
      When query
      """
      SELECT id, date_trunc(IF(id >= 0, 'null', 'x'), TIMESTAMP '2024-05-15 13:45:30 UTC') AS result
      FROM range(2) ORDER BY id
      """
      Then query result ordered
      | id | result |
      | 0  | NULL   |
      | 1  | NULL   |

    Scenario: the string null converts the value, unlike an actual NULL unit
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT id, date_trunc(IF(id >= 0, 'null', 'x'), IF(id = 0, 'not a date', '2024-01-02')) AS result
      FROM range(2) ORDER BY id
      """
      Then query error (CAST_INVALID_INPUT|error in SQL parser: found .*expected '-')

    Scenario: an actual NULL unit skips the value where the string null does not
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT id, date_trunc(IF(id >= 0, CAST(NULL AS STRING), 'x'),
                            IF(id = 0, 'not a date', '2024-01-02')) AS result
      FROM range(2) ORDER BY id
      """
      Then query result ordered
      | id | result |
      | 0  | NULL   |
      | 1  | NULL   |

  Rule: a unit of a type that cannot become a string matches nothing

    # Spark's coercion to string never fails -- `Cast(binary, string)` is `UTF8String.fromBytes`,
    # which does not validate UTF-8 -- so a unit Arrow refuses to convert has to become a unit
    # that matches nothing, not an error.

    Scenario: a binary unit is NULL, not an error
      When query
      """
      SELECT date_trunc(X'FF', TIMESTAMP '2024-01-01 00:00:00 UTC') AS result
      """
      Then query result
      | result |
      | NULL   |

    # But a unit is measured as its STRING form, and a binary one has a perfectly good string
    # form when its bytes spell a unit. Treating any non-string literal as "matches nothing"
    # would answer NULL here, so a literal only short-circuits when it is already a string.

    Scenario: a binary unit whose bytes spell a granularity truncates
      When query
      """
      SELECT date_trunc(X'59454152', TIMESTAMP '2024-05-15 13:45:30 UTC') AS result
      """
      Then query result
      | result              |
      | 2024-01-01 00:00:00 |

    # Sail turns a binary unit it cannot decode into a NULL unit, which takes the short circuit,
    # where Spark turns it into a string that matches nothing and therefore converts the value.
    # Only observable with a NON-foldable unit, an unconvertible value and ANSI on.
    @sail-bug
    Scenario: an undecodable binary unit converts the value under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT id, date_trunc(IF(id >= 0, X'FF', X'00'), IF(id = 0, 'not a date', '2024-01-02')) AS result
      FROM range(2) ORDER BY id
      """
      Then query error (CAST_INVALID_INPUT|error in SQL parser: found .*expected '-')

  Rule: a unit from a scalar subquery is resolved before the query runs

    # `ScalarSubquery.foldable` is false in Spark, but a subquery over no relation is resolved
    # into a literal before codegen, so it takes the same short circuit a literal does: no ANSI
    # error comes out of the value even though the unit matches nothing.

    Scenario: a subquery unit that matches nothing skips the value
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT date_trunc((SELECT 'BOGUS'), 'not a date') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: a subquery unit still names its granularity
      When query
      """
      SELECT date_trunc((SELECT 'DAY'), TIMESTAMP '2024-05-15 13:45:30 UTC') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 00:00:00 |

  Rule: a zoned timestamp truncates across a DST transition

    # Every other DST scenario feeds a TIMESTAMP_NTZ or a DATE, which reach the localization the
    # plan builder inserts. A zoned timestamp skips that path entirely and is truncated by the
    # kernel alone, so it needs its own case -- and the instant, not just the wall clock, because
    # the day it belongs to is the one that lasts 25 hours.

    Scenario: truncating a zoned timestamp to the day inside the fall-back day
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
      """
      SELECT date_trunc('DAY', TIMESTAMP '2024-11-03 10:30:00 UTC') AS result
      """
      Then query result
      | result              |
      | 2024-11-03 00:00:00 |

    Scenario: the truncated zoned timestamp lands on the right instant
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
      """
      SELECT unix_timestamp(date_trunc('DAY', TIMESTAMP '2024-11-03 10:30:00 UTC')) AS result
      """
      Then query result
      | result     |
      | 1730617200 |

  Rule: a unit Spark folds but the plan builder cannot

    # Spark's `foldable` covers any expression with no input, so `concat('BOG','US')` is resolved
    # at compile time and takes the short circuit. Sail cannot name the granularity for one of
    # these at plan-build time -- the optimizer that would evaluate it runs later -- so the unit
    # is still matched per row. What it does share with a literal is the part that shows: a unit
    # with no column reference and no volatility does not convert the value, so no ANSI error
    # comes out of a row it fails to match.

    Scenario: a unit folded from an expression short-circuits like a literal
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT date_trunc(concat('BOG','US'), 'not a date') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: a unit folded from an expression still names its granularity
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT date_trunc(concat('DA','Y'), TIMESTAMP '2024-05-15 13:45:30') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 00:00:00 |

  Rule: truncation is resolved in the session time zone

    # Every other scenario runs under the harness default of UTC, where a session time zone that
    # was ignored, or hardcoded to UTC, would pass unnoticed. These pin the plumbing down by
    # truncating across a DST transition, where the local time zone changes the answer.

    Scenario: a timestamp_ntz in the spring-forward gap moves to the next valid time
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
      """
      SELECT date_trunc('SECOND', TIMESTAMP_NTZ '2024-03-10 02:30:00') AS result
      """
      Then query result
      | result              |
      | 2024-03-10 03:30:00 |

    Scenario: a timestamp_ntz in the fall-back overlap takes the earlier time
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
      """
      SELECT date_trunc('SECOND', TIMESTAMP_NTZ '2024-11-03 01:30:00') AS result
      """
      Then query result
      | result              |
      | 2024-11-03 01:30:00 |

    # The rendered wall clock CANNOT tell the two resolutions apart: 01:30 during the overlap
    # prints as 01:30 whether it is resolved to the earlier offset or the later one, so the
    # scenario above would pass just as well if the earlier one were dropped. The instant does
    # tell them apart -- an hour lies between them.

    Scenario: the fall-back overlap resolves to the earlier of the two instants
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
      """
      SELECT unix_timestamp(date_trunc('SECOND', TIMESTAMP_NTZ '2024-11-03 01:30:00')) AS result
      """
      Then query result
      | result     |
      | 1730622600 |

    Scenario: truncating to the hour inside the spring-forward gap
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
      """
      SELECT date_trunc('HOUR', TIMESTAMP_NTZ '2024-03-10 02:30:00') AS result
      """
      Then query result
      | result              |
      | 2024-03-10 03:00:00 |

    Scenario: truncating to the hour inside the fall-back overlap
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
      """
      SELECT date_trunc('HOUR', TIMESTAMP_NTZ '2024-11-03 01:30:00') AS result
      """
      Then query result
      | result              |
      | 2024-11-03 01:00:00 |

    Scenario: truncating to the hour inside the overlap keeps the earlier instant
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
      """
      SELECT unix_timestamp(date_trunc('HOUR', TIMESTAMP_NTZ '2024-11-03 01:30:00')) AS result
      """
      Then query result
      | result     |
      | 1730620800 |

    Scenario: truncating a timestamp_ntz to the day across a DST transition
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
      """
      SELECT date_trunc('DAY', TIMESTAMP_NTZ '2024-03-10 02:30:00') AS result
      """
      Then query result
      | result              |
      | 2024-03-10 00:00:00 |

    Scenario: truncating a date under a non-UTC session time zone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
      """
      SELECT date_trunc('DAY', DATE '2024-03-10') AS result
      """
      Then query result
      | result              |
      | 2024-03-10 00:00:00 |

    Scenario: truncating a timestamp_ntz from a column under a non-UTC session time zone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
      """
      SELECT date_trunc('SECOND', ts) AS result FROM VALUES (TIMESTAMP_NTZ '2024-03-10 02:30:00') AS t(ts)
      """
      Then query result
      | result              |
      | 2024-03-10 03:30:00 |

    Scenario: a literal unit under a non-UTC session time zone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
      """
      SELECT date_trunc('DAY', TIMESTAMP '2024-03-15 08:30:00 UTC') AS result
      """
      Then query result
      | result              |
      | 2024-03-15 00:00:00 |

  Rule: the unit is matched with Unicode case folding

    # Spark upper-cases the unit with `toUpperCase(Locale.ROOT)`, which folds non-ASCII letters:
    # the dotless i folds to I and the long s folds to S. Matching only ASCII case would return
    # NULL for these instead of truncating.

    Scenario: a unit spelled with the dotless i matches MINUTE
      When query
      """
      SELECT date_trunc('mınute', TIMESTAMP '2024-05-15 13:45:30.123456 UTC') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 13:45:00 |

    Scenario: a unit spelled with the long s matches SECOND
      When query
      """
      SELECT date_trunc('ſecond', TIMESTAMP '2024-05-15 13:45:30.123456 UTC') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 13:45:30 |

    Scenario: a unit spelled with the dotless i matches MICROSECOND
      When query
      """
      SELECT date_trunc('mıcrosecond', TIMESTAMP '2024-05-15 13:45:30.123456 UTC') AS result
      """
      Then query result
      | result                     |
      | 2024-05-15 13:45:30.123456 |

    # The scenarios above resolve the unit while the plan is built, because it is a literal. A
    # unit in a column is resolved per row instead. Both go through the same matcher, and this
    # case is what keeps it that way.

    Scenario: a non-ASCII unit in a column folds the same way
      When query
      """
      SELECT date_trunc(u, TIMESTAMP '2024-05-15 13:45:30.123456 UTC') AS result
      FROM VALUES ('mınute'), ('ſecond') AS t(u)
      """
      Then query result ordered
      | result              |
      | 2024-05-15 13:45:00 |
      | 2024-05-15 13:45:30 |

  Rule: a complex argument is rejected when the query is analyzed

    # Spark rejects an argument for its TYPE while it analyzes the query. Coercing a complex type
    # to its string form instead would match no granularity and quietly return NULL.

    Scenario: date_trunc rejects an array unit
      When query
      """
      SELECT date_trunc(array('YEAR'), TIMESTAMP '2024-01-01 00:00:00 UTC') AS result
      """
      Then query error DATATYPE_MISMATCH

    Scenario: date_trunc rejects a map unit
      When query
      """
      SELECT date_trunc(map('a', 'YEAR'), TIMESTAMP '2024-01-01 00:00:00 UTC') AS result
      """
      Then query error DATATYPE_MISMATCH

    Scenario: date_trunc rejects a struct unit
      When query
      """
      SELECT date_trunc(named_struct('a', 'YEAR'), TIMESTAMP '2024-01-01 00:00:00 UTC') AS result
      """
      Then query error DATATYPE_MISMATCH

    Scenario: date_trunc rejects an array value
      When query
      """
      SELECT date_trunc('YEAR', array(1)) AS result
      """
      Then query error DATATYPE_MISMATCH

  Rule: a string value parses with Spark's grammar

    # Spark's own parser accepts a single-digit month or day and a date with no month or day at
    # all. Arrow's parser demands a fixed-position `YYYY-MM-DD` of at least 10 bytes and rejects
    # all three -- and Sail's `CAST(… AS TIMESTAMP)` already uses Spark's parser, so leaving this
    # on Arrow's made date_trunc disagree with CAST on the very same string.

    Scenario: a value with a single-digit month
      When query
      """
      SELECT date_trunc('MM', '2009-2-12') AS result
      """
      Then query result
      | result              |
      | 2009-02-01 00:00:00 |

    Scenario: a value with no month or day
      When query
      """
      SELECT date_trunc('YEAR', '2024') AS result
      """
      Then query result
      | result              |
      | 2024-01-01 00:00:00 |

    Scenario: a value with no day
      When query
      """
      SELECT date_trunc('MM', '2024-3') AS result
      """
      Then query result
      | result              |
      | 2024-03-01 00:00:00 |

  Rule: a date localizes across a transition that happens at midnight

    # America/Los_Angeles moves its clock at 02:00, so local midnight always exists there and the
    # scenarios above cannot see this. In these zones the clock moves AT midnight, so the day the
    # transition falls on has no 00:00 at all and Spark reports the first instant that does exist.

    Scenario: a date whose local midnight does not exist
      Given config spark.sql.session.timeZone = Africa/Cairo
      When query
      """
      SELECT date_trunc('DAY', DATE '2024-04-26') AS result
      """
      Then query result
      | result              |
      | 2024-04-26 01:00:00 |

    Scenario: a date from a column whose local midnight does not exist
      Given config spark.sql.session.timeZone = Africa/Cairo
      When query
      """
      SELECT date_trunc('DAY', d) AS result FROM VALUES (DATE '2024-04-26') AS t(d)
      """
      Then query result
      | result              |
      | 2024-04-26 01:00:00 |

    Scenario: a date whose local midnight does not exist in another zone
      Given config spark.sql.session.timeZone = America/Havana
      When query
      """
      SELECT date_trunc('DAY', DATE '2024-03-10') AS result
      """
      Then query result
      | result              |
      | 2024-03-10 01:00:00 |

    Scenario: a timestamp_ntz inside a midnight gap
      Given config spark.sql.session.timeZone = Africa/Cairo
      When query
      """
      SELECT date_trunc('DAY', TIMESTAMP_NTZ '2024-04-26 00:30:00') AS result
      """
      Then query result
      | result              |
      | 2024-04-26 01:00:00 |

  Rule: the value is converted once, even when it is volatile

    # The branch an unmatched unit takes has to convert the value -- and convert it ONCE. Reaching
    # for `nullif(value, value)` would evaluate it twice, and DataFusion does not hoist a volatile
    # subtree into a shared expression, so the two draws would differ and the row would keep a
    # value where Spark yields NULL. This is the mirror of the volatile-unit scenario.

    Scenario: a volatile value with a unit that matches nothing is always NULL
      When query
      """
      SELECT count(*) AS non_nulls FROM (
        SELECT date_trunc(u, IF(rand(7) < 0.5, TIMESTAMP '2020-01-01 00:00:00',
                                               TIMESTAMP '2021-01-01 00:00:00')) AS r
        FROM (SELECT 'BOGUS' AS u FROM range(10000))
      ) WHERE r IS NOT NULL
      """
      Then query result
      | non_nulls |
      | 0         |

  Rule: the plan resolves the unit

    # The plan is Sail's, so it cannot be compared against Spark. These snapshots exist to make the
    # shape of the plan reviewable: how the unit is matched, and whether a literal unit still
    # collapses to a single call once the units are enumerated in the plan.
    @sail-only
    Scenario: the plan for a literal unit
      When query
        """
        EXPLAIN SELECT date_trunc('YEAR', ts) AS result FROM VALUES (TIMESTAMP '2024-05-15 13:45:30') AS t(ts)
        """
      Then query plan matches snapshot

    @sail-only
    Scenario: the plan for a unit coming from a column
      When query
        """
        EXPLAIN SELECT date_trunc(c, ts) AS result FROM VALUES (TIMESTAMP '2024-05-15 13:45:30', 'YEAR') AS t(ts, c)
        """
      Then query plan matches snapshot

  Rule: date_trunc — the argument may come from a column

    @column_args
    Scenario: date_trunc with the argument as a literal
      When query
        """
        SELECT date_trunc('MM', '2015-03-05T09:32:05.359') AS result
        """
      Then query result ordered
        | result              |
        | 2015-03-01 00:00:00 |

    @column_args
    Scenario: date_trunc takes argument 1 from a column holding two different values
      When query
        """
        SELECT date_trunc(c, '2015-03-05T09:32:05.359') AS result FROM VALUES (1, 'YEAR'), (2, 'MM') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result              |
        | 2015-01-01 00:00:00 |
        | 2015-03-01 00:00:00 |

    @column_args
    Scenario: date_trunc takes argument 1 from a column
      When query
        """
        SELECT date_trunc(c, '2015-03-05T09:32:05.359') AS result FROM VALUES (1, 'YEAR'), (2, 'YEAR') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result              |
        | 2015-01-01 00:00:00 |
        | 2015-01-01 00:00:00 |

  Rule: a unit from a subquery Spark does not fold

    # Whether the short circuit applies depends on Spark's OPTIMIZER, not on the expression:
    # `ScalarSubquery.foldable` is always false, and what decides is whether ConstantFolding
    # replaced the subquery with a literal before codegen. Measured on JVM 4.1.1, it folds a
    # subquery whose plan ends in a one-row relation -- `(SELECT 'BOGUS')`, a view over a
    # constant, `FROM (SELECT 1)` -- and does NOT fold one over `VALUES`, `range`, or a table.
    #
    # Sail decides while the plan is built, before any of that, so it treats every subquery as
    # foldable and skips the value. Detectable, since the boundary is structural, but it encodes
    # an optimizer quirk rather than a semantic rule -- `FROM (VALUES ('x'))` not folding while
    # `FROM (SELECT 1)` does answers to no principle -- so it is left for its own change.
    #
    # Only ANSI on diverges, and only with a value that cannot be converted: the pair below is
    # what shows that, and everything else about a non-folded subquery already agrees.

    @sail-bug
    Scenario: a non-folded subquery unit converts the value under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT date_trunc((SELECT 'BOGUS' FROM range(1) LIMIT 1), 'not a date') AS result
      """
      Then query error (CAST_INVALID_INPUT|error in SQL parser: found .*expected '-')

    Scenario: a non-folded subquery unit is NULL under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT date_trunc((SELECT 'BOGUS' FROM range(1) LIMIT 1), 'not a date') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: a non-folded subquery unit that names a granularity truncates under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
      """
      SELECT date_trunc((SELECT 'DAY' FROM range(1) LIMIT 1), TIMESTAMP '2024-05-15 13:45:30 UTC') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 00:00:00 |

    Scenario: a non-folded subquery unit that names a granularity truncates under ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
      """
      SELECT date_trunc((SELECT 'DAY' FROM range(1) LIMIT 1), TIMESTAMP '2024-05-15 13:45:30 UTC') AS result
      """
      Then query result
      | result              |
      | 2024-05-15 00:00:00 |
