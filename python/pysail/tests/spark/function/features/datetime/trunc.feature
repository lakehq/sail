Feature: trunc with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: trunc — the argument may come from a column

    @function(columnargs)
    Scenario: trunc with the argument as a literal
      When query
        """
        SELECT trunc('2009-02-12', 'MM') AS result
        """
      Then query result ordered
        | result     |
        | 2009-02-01 |

    # Sail rejects the column: Sail errors: Granularity of `date_trunc` must be non-null scalar Utf8
    @function(columnargs) @sail-bug
    Scenario Outline: Trunc: <case>
      When query
        """
        SELECT trunc(<date>, c) AS result FROM VALUES (1, <v1>), (2, <v2>) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | <r1>   |
        | <r2>   |

      Examples:
        | case                                                              | date         | v1     | v2     | r1         | r2         |
        | trunc takes argument 2 from a column holding two different values | '2009-02-12' | 'MM'   | 'week' | 2009-02-01 | 2009-02-09 |
        | trunc takes argument 2 from a column                              | '2019-08-04' | 'week' | 'week' | 2019-07-29 | 2019-07-29 |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null date literal yields a date
      When query
        """
        SELECT trunc(DATE '2024-01-15', 'YEAR') AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

    @sail-bug
    Scenario: a non-null date column yields a date
      When query
        """
        SELECT trunc(CAST(CAST(id AS TIMESTAMP) AS DATE), 'YEAR') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

    Scenario: a nullable date column stays nullable
      When query
        """
        SELECT trunc(c, 'YEAR') AS result FROM VALUES (DATE '2024-01-15'), (CAST(NULL AS DATE)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

  Rule: trunc truncates to a date-level unit

    Scenario: trunc to the week
      When query
        """
        SELECT trunc(DATE '2019-08-04', 'week') AS result
        """
      Then query result
        | result     |
        | 2019-07-29 |

    Scenario: trunc to the quarter
      When query
        """
        SELECT trunc(DATE '2019-08-04', 'quarter') AS result
        """
      Then query result
        | result     |
        | 2019-07-01 |

    Scenario: trunc matches the unit case-insensitively
      When query
        """
        SELECT trunc(DATE '2019-08-04', 'MoN') AS result
        """
      Then query result
        | result     |
        | 2019-08-01 |

    Scenario: trunc accepts the yy alias
      When query
        """
        SELECT trunc(DATE '2019-08-04', 'yy') AS result
        """
      Then query result
        | result     |
        | 2019-01-01 |

    Scenario: trunc takes the date from a column
      When query
        """
        SELECT trunc(d, 'month') AS result FROM VALUES (1, DATE '2019-08-04'), (2, DATE '2020-02-29') AS t(i, d) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 2019-08-01 |
        | 2020-02-01 |

  Rule: trunc returns a date

    # `trunc` returns DATE, not the TIMESTAMP that `date_trunc` returns. Nothing else pins the
    # return type down, so a change to the shared unit table cannot silently widen it.

    @sail-bug
    Scenario: trunc on a date literal returns date type
      When query
        """
        SELECT trunc(DATE '2024-05-15', 'MM') AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """
      Then query result
        | result     |
        | 2024-05-01 |

    @sail-bug
    Scenario: trunc with the unit in a column returns date type
      When query
        """
        SELECT trunc(DATE '2024-05-15', c) AS result FROM VALUES ('MM') AS t(c)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """
      Then query result
        | result     |
        | 2024-05-01 |

  Rule: a unit finer than a day is NULL, not a truncation

    @sail-bug
    Scenario: trunc to the day is NULL
      When query
        """
        SELECT trunc(DATE '2019-08-04', 'DAY') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: trunc to the hour is NULL
      When query
        """
        SELECT trunc(DATE '2019-08-04', 'HOUR') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: an unrecognized or NULL unit is NULL, not an error

    @sail-bug
    Scenario: trunc with an unrecognized unit is NULL
      When query
        """
        SELECT trunc(DATE '2019-08-04', 'bogus') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: trunc with a NULL unit is NULL
      When query
        """
        SELECT trunc(DATE '2019-08-04', NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: the unit may come from a column

    @sail-bug
    Scenario: trunc resolves the unit of each row
      When query
        """
        SELECT trunc(DATE '2019-08-04', c) AS result FROM VALUES (1, 'year'), (2, 'month'), (3, 'DAY'), (4, NULL) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 2019-01-01 |
        | 2019-08-01 |
        | NULL       |
        | NULL       |

  Rule: the unit is resolved once per row, even when it is not foldable

    # A unit built with `rand` is volatile: matching it once per candidate unit draws a new value
    # for every comparison, so a row can match nothing and turn NULL. Spark resolves the unit once
    # per row and never yields NULL here. A unit from `VALUES` is folded by Spark and hides this.

    Scenario: a volatile unit never nullifies a row
      When query
        """
        SELECT count(*) AS nulls FROM (
          SELECT trunc(DATE '2024-05-15', IF(rand(7) < 0.5, 'YEAR', 'MONTH')) AS r FROM range(10000)
        ) WHERE r IS NULL
        """
      Then query result
        | nulls |
        | 0     |

    # The scenario above cannot see a unit evaluated more than once: both draws name a
    # granularity. Draw a NULL against a valid unit and the count becomes the evidence -- one
    # evaluation lands near 5000, two near 7500. Asserted as a range because nothing says the two
    # engines draw the same sequence from the same seed.

    @sail-bug
    Scenario: a volatile unit is drawn exactly once per row
      When query
        """
        SELECT count(*) BETWEEN 4700 AND 5300 AS drawn_once FROM (
          SELECT trunc(DATE '2024-05-15', IF(rand(7) < 0.5, CAST(NULL AS STRING), 'YEAR')) AS r
          FROM range(10000)
        ) WHERE r IS NULL
        """
      Then query result
        | drawn_once |
        | true       |

  Rule: the value is converted even when the unit matches nothing

    # Spark evaluates both arguments before it validates the unit, so an unconvertible value
    # raises under ANSI even on a row whose unit matches no granularity. Resolving the unit first
    # and skipping the conversion for unmatched rows would silently return NULL instead.

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
        SELECT id, trunc(IF(id = 0, 'not a date', '2024-01-02'), IF(id = 0, 'INVALID', 'YEAR')) AS result
        FROM range(2) ORDER BY id
        """
      Then query error CAST_INVALID_INPUT

    @sail-bug
    Scenario: an unconvertible value errors under ANSI on even when its unit matches nothing
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, trunc(IF(id = 0, 'not a date', '2024-01-02'), IF(id = 0, 'INVALID', 'YEAR')) AS result
        FROM range(2) ORDER BY id
        """
      Then query error (CAST_INVALID_INPUT|error in SQL parser: found .*expected '-')

    # `trunc` is `(date, format)`, so the date is the LEFT child and Spark evaluates it before it
    # looks at the format -- a NULL format does NOT save an unconvertible value here. `date_trunc`
    # has the arguments the other way round and behaves the opposite way; both are pinned down.

    @sail-bug
    Scenario: a NULL unit in a column still converts the value
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, trunc(IF(id = 0, 'not a date', '2024-01-02'), IF(id = 0, CAST(NULL AS STRING), 'YEAR')) AS result
        FROM range(2) ORDER BY id
        """
      Then query error (CAST_INVALID_INPUT|error in SQL parser: found .*expected '-')

    @sail-bug
    Scenario: an unconvertible value is NULL under ANSI off even when its unit matches nothing
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT id, trunc(IF(id = 0, 'not a date', '2024-01-02'), IF(id = 0, 'INVALID', 'YEAR')) AS result
        FROM range(2) ORDER BY id
        """
      Then query result ordered
        | id | result     |
        | 0  | NULL       |
        | 1  | 2024-01-01 |

    # A LITERAL unit that names no granularity is the other half of the rule: Spark's codegen
    # short-circuits to NULL without evaluating the value at all, so the same unconvertible value
    # does NOT raise here. For `trunc` a unit finer than a day names no level either, so it takes
    # the same short circuit.

    @sail-bug
    Scenario: a literal unit that matches nothing skips the value entirely under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, trunc(IF(id = 0, 'not a date', '2024-01-02'), 'INVALID') AS result
        FROM range(2) ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 0  | NULL   |
        | 1  | NULL   |

    @sail-bug
    Scenario: a literal unit finer than a day skips the value entirely under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT id, trunc(IF(id = 0, 'not a date', '2024-01-02'), 'DAY') AS result
        FROM range(2) ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 0  | NULL   |
        | 1  | NULL   |

  Rule: a date beyond the nanosecond range truncates without wrapping

    # Truncating through a nanosecond timestamp overflows a 64-bit count past ~2262 and, for a
    # DATE, does so with an unchecked multiply: the result WRAPS AROUND into the past instead of
    # failing. A wrong value with no error is the worst outcome, and only a far-future date
    # exposes it.

    @sail-bug
    Scenario: trunc YEAR beyond the nanosecond range
      When query
        """
        SELECT trunc(DATE '2300-01-01', 'YEAR') AS result
        """
      Then query result
        | result     |
        | 2300-01-01 |

    @sail-bug
    Scenario: trunc MONTH beyond the nanosecond range
      When query
        """
        SELECT trunc(DATE '2300-06-15', 'MONTH') AS result
        """
      Then query result
        | result     |
        | 2300-06-01 |

    @sail-bug
    Scenario: trunc YEAR at the maximum supported year
      When query
        """
        SELECT trunc(DATE '9999-12-31', 'YEAR') AS result
        """
      Then query result
        | result     |
        | 9999-01-01 |

  Rule: a string value parses with Spark's grammar

    # Spark's parser takes a single-digit month and a date with no month or day; Arrow's demands
    # a fixed-position `YYYY-MM-DD`. Sail's `CAST(… AS DATE)` already uses Spark's.

    @sail-bug
    Scenario: a value with a single-digit month
      When query
        """
        SELECT trunc('2009-2-12', 'MM') AS result
        """
      Then query result
        | result     |
        | 2009-02-01 |

    @sail-bug
    Scenario: a value with no month or day
      When query
        """
        SELECT trunc('2024', 'YEAR') AS result
        """
      Then query result
        | result     |
        | 2024-01-01 |

  Rule: the value is converted once, even when it is volatile

    @sail-bug
    Scenario: a volatile value with a unit that matches nothing is always NULL
      When query
        """
        SELECT count(*) AS non_nulls FROM (
          SELECT trunc(IF(rand(7) < 0.5, DATE '2020-01-01', DATE '2021-01-01'), u) AS r
          FROM (SELECT 'BOGUS' AS u FROM range(10000))
        ) WHERE r IS NOT NULL
        """
      Then query result
        | non_nulls |
        | 0         |

  Rule: a timestamp value truncates on its local date

    # `trunc` takes the DATE of the value, and for a zoned timestamp that is the date in the
    # SESSION zone, not in UTC. Under a negative-offset zone an instant late in the day still
    # belongs to the previous local day, and under a positive-offset one it is already the next.
    # Nothing else in this file uses a timestamp value or a non-UTC session.

    Scenario: a zoned timestamp uses the local date behind UTC
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT trunc(TIMESTAMP '2024-06-30 20:00:00', 'MONTH') AS result
        """
      Then query result
        | result     |
        | 2024-06-01 |

    Scenario: a zoned timestamp uses the local date ahead of UTC
      Given config spark.sql.session.timeZone = Asia/Tokyo
      When query
        """
        SELECT trunc(TIMESTAMP '2024-07-01 02:00:00', 'MONTH') AS result
        """
      Then query result
        | result     |
        | 2024-07-01 |

    Scenario: a zoned timestamp from a column uses the local date
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT trunc(t, 'MONTH') AS result FROM VALUES (TIMESTAMP '2024-06-30 20:00:00') AS v(t)
        """
      Then query result
        | result     |
        | 2024-06-01 |

    Scenario: a timestamp_ntz uses its own wall clock
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT trunc(TIMESTAMP_NTZ '2024-06-30 20:00:00', 'MONTH') AS result
        """
      Then query result
        | result     |
        | 2024-06-01 |

  Rule: a unit of a type that cannot become a string matches nothing

    # Spark's coercion to string never fails -- `Cast(binary, string)` does not validate UTF-8 --
    # so a unit Arrow refuses to convert has to become a unit that matches nothing, not an error.

    @sail-bug
    Scenario: a binary unit is NULL, not an error
      When query
        """
        SELECT trunc(DATE '2024-01-01', X'FF') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: a binary unit whose bytes spell a granularity truncates
      When query
        """
        SELECT trunc(DATE '2024-05-15', X'4D4D') AS result
        """
      Then query result
        | result     |
        | 2024-05-01 |

  Rule: a literal unit short-circuits even where a column would not

    # `trunc` converts its value whatever the unit is -- the date is the LEFT child -- but that
    # is the COLUMN rule. A literal unit Spark can fold short-circuits before either child is
    # generated, so these do NOT raise, unlike their columnar twins above.

    @sail-bug
    Scenario: a literal NULL unit skips an unconvertible value under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT trunc('not a date', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: a literal unit finer than a day skips an unconvertible value under ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT trunc('not a date', 'DAY') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: a value of the wrong type is rejected when the query is analyzed

    # Spark rejects the value for its TYPE. Arrow instead REINTERPRETS an integer as a day count,
    # so without the gate `trunc(1, 'YEAR')` answers 1970-01-01 -- an invented value, with no
    # error, which is worse than the wrong error class.

    @sail-bug
    Scenario: trunc rejects an integer value
      When query
        """
        SELECT trunc(1, 'YEAR') AS result
        """
      Then query error DATATYPE_MISMATCH

    @sail-bug
    Scenario: trunc rejects a boolean value
      When query
        """
        SELECT trunc(true, 'YEAR') AS result
        """
      Then query error DATATYPE_MISMATCH

    @sail-bug
    Scenario: trunc rejects an array value
      When query
        """
        SELECT trunc(array(1), 'YEAR') AS result
        """
      Then query error DATATYPE_MISMATCH

  Rule: a complex unit is rejected when the query is analyzed

    # Spark rejects the unit for its TYPE while it analyzes the query. Coercing it to its string
    # form instead would match no granularity and quietly return NULL.

    @sail-bug
    Scenario: trunc rejects an array unit
      When query
        """
        SELECT trunc(DATE '2024-01-01', array('YEAR')) AS result
        """
      Then query error DATATYPE_MISMATCH

  Rule: a date beyond Sail's supported range

    # Spark's DATE reaches year 5881580 and `trunc` truncates one correctly; Sail's range stops
    # much earlier and rejects the value before `trunc` ever sees it. The gap is not in `trunc`
    # itself -- every way of building such a date is rejected (`DATE` literal, `make_date`,
    # `date_add`, `CAST(str AS DATE)`, `to_date`, date + int) -- so it belongs to the DATE surface.
    #
    # It is pinned here because it guards something specific: truncating past year ~294,247
    # overflows the microsecond conversion, and today that is unreachable ONLY because the range
    # stops first. Widen the range and `trunc` would wrap silently on a date Spark handles, which
    # is the same failure this PR fixed at year 2262.

    @sail-bug
    Scenario: trunc on a date past the supported range
      When query
        """
        SELECT trunc(CAST('294248-01-01' AS DATE), 'YEAR') AS result
        """
      Then query result
        | result        |
        | +294248-01-01 |

    @sail-bug
    Scenario: trunc on a date past the supported range built with to_date
      When query
        """
        SELECT trunc(to_date('294248-01-01'), 'YEAR') AS result
        """
      Then query result
        | result        |
        | +294248-01-01 |

  Rule: a date before the nanosecond range truncates like any other

    # The mirror of "a date beyond the nanosecond range": Spark 4.2.0 DateTimeUtils.truncDate works
    # on days since the epoch and handles any DATE, but Sail routes the value through a 64-bit
    # nanosecond timestamp, which cannot hold anything before 1677-09-21 00:12:43.

    Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario: trunc just inside the nanosecond range
      When query
        """
        SELECT trunc(DATE '1678-06-15', 'YEAR') AS year, trunc(DATE '1900-02-15', 'MONTH') AS month
        """
      Then query result
        | year       | month      |
        | 1678-01-01 | 1900-02-01 |

    @sail-bug
    Scenario Outline: trunc before the nanosecond range: <case>
      When query
        """
        SELECT trunc(<value>, '<unit>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                         | value                               | unit    | result     |
        | minimum date to the year     | DATE '0001-01-01'                   | YEAR    | 0001-01-01 |
        | minimum date to the month    | DATE '0001-01-01'                   | MM      | 0001-01-01 |
        | minimum date to the week     | DATE '0001-01-01'                   | WEEK    | 0001-01-01 |
        | first quarter of year 1      | DATE '0001-03-31'                   | QUARTER | 0001-01-01 |
        | week crossing the bound      | DATE '1677-09-22'                   | WEEK    | 1677-09-20 |
        | month on the bound day       | DATE '1677-09-21'                   | MM      | 1677-09-01 |
        | timestamp in year 1500       | TIMESTAMP '1500-06-15 10:00:00'     | YEAR    | 1500-01-01 |

    @sail-bug
    Scenario: trunc of distinct dates on both sides of the nanosecond range from a column
      When query
        """
        SELECT id, trunc(d, 'YEAR') AS result
        FROM VALUES (1, DATE '0001-06-15'), (2, DATE '1500-06-15'), (3, DATE '2024-06-15') AS t(id, d)
        ORDER BY id
        """
      Then query result ordered
        | id | result     |
        | 1  | 0001-01-01 |
        | 2  | 1500-01-01 |
        | 3  | 2024-01-01 |
