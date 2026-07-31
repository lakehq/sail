@approx_percentile
Feature: approx_percentile / percentile_approx aggregate function

  Rule: Single percentile preserves the input type

    Scenario: percentile_approx median over integers returns an integer
      When query
        """
        SELECT
          percentile_approx(v, 0.5, 1000000) AS median,
          typeof(percentile_approx(v, 0.5, 1000000)) AS type
        FROM VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10) AS t(v)
        """
      Then query result
        | median | type |
        | 5      | int  |

    Scenario: percentile_approx median over doubles returns a double
      When query
        """
        SELECT
          percentile_approx(v, 0.5, 1000000) AS median,
          typeof(percentile_approx(v, 0.5, 1000000)) AS type
        FROM VALUES (CAST(0 AS DOUBLE)), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10) AS t(v)
        """
      Then query result
        | median | type   |
        | 5.0    | double |

    # Asserted on the collected value rather than the rendered one. Spark
    # narrows the double result with `Decimal(double)`, which keeps the double's
    # natural scale, so `show` prints `5.0` while `collect` yields the correctly
    # scaled `Decimal('5.00')`. An Arrow decimal carries a single scale, so Sail
    # cannot reproduce that split — it matches the collected value.
    Scenario: percentile_approx median over decimals preserves precision and scale
      When query
        """
        SELECT
          percentile_approx(v, 0.5) AS median,
          typeof(percentile_approx(v, 0.5)) AS type
        FROM VALUES (CAST(0 AS DECIMAL(10,2))), (CAST(5 AS DECIMAL(10,2))), (CAST(10 AS DECIMAL(10,2))) AS t(v)
        """
      Then query result collected
        | median | type          |
        | 5.00   | decimal(10,2) |

    Scenario: approx_percentile alias without accuracy argument
      When query
        """
        SELECT approx_percentile(v, 0.5) AS median
        FROM VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10) AS t(v)
        """
      Then query result
        | median |
        | 5      |

  Rule: Array of percentiles

    Scenario: percentile_approx with an array of percentiles over integers
      When query
        """
        SELECT
          percentile_approx(v, array(0.25, 0.5, 0.75), 1000000) AS quantiles,
          typeof(percentile_approx(v, array(0.25, 0.5, 0.75), 1000000)) AS type
        FROM VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10) AS t(v)
        """
      Then query result
        | quantiles | type       |
        | [2, 5, 8] | array<int> |

    Scenario: percentile_approx with an array of percentiles over doubles
      When query
        """
        SELECT
          percentile_approx(v, array(0.25, 0.5, 0.75), 1000000) AS quantiles,
          typeof(percentile_approx(v, array(0.25, 0.5, 0.75), 1000000)) AS type
        FROM VALUES (CAST(0 AS DOUBLE)), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10) AS t(v)
        """
      Then query result
        | quantiles       | type          |
        | [2.0, 5.0, 8.0] | array<double> |

    Scenario: approx_percentile array with boundary percentiles
      When query
        """
        SELECT approx_percentile(v, array(0.0, 0.5, 1.0)) AS quantiles
        FROM VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10) AS t(v)
        """
      Then query result
        | quantiles   |
        | [0, 5, 10]  |

  Rule: Nearest-rank selection

    # The examples from Spark's own @ExpressionDescription for this function.
    Scenario Outline: Spark documentation example: <case>
      When query
        """
        SELECT percentile_approx(col, <percentage>, 100) AS r
        FROM VALUES <rows> AS tab(col)
        """
      Then query result
        | r          |
        | <expected> |

      Examples:
        | case             | rows                       | percentage            | expected  |
        | scalar median    | (0), (6), (7), (9), (10)   | 0.5                   | 7         |
        | array quantiles  | (0), (1), (2), (10)        | array(0.5, 0.4, 0.1)  | [1, 1, 0] |

    Scenario: results follow the order the percentiles were requested in
      When query
        """
        SELECT percentile_approx(v, array(0.9, 0.1, 0.5)) AS quantiles
        FROM VALUES (0), (1), (2), (3), (4) AS t(v)
        """
      Then query result
        | quantiles |
        | [4, 0, 2] |

    Scenario: a single observation is returned for every percentile
      When query
        """
        SELECT percentile_approx(v, array(0.0, 0.5, 1.0)) AS quantiles
        FROM VALUES (42) AS t(v)
        """
      Then query result
        | quantiles    |
        | [42, 42, 42] |

  Rule: ANSI interval types — the selected value is correct

    # Equality comparisons are used instead of literal rendering so the
    # scenarios are independent of how intervals are printed. The discrete
    # nearest-rank value matches Spark for both year-month (INTERVAL MONTH)
    # and day-time (INTERVAL SECOND) intervals, including the array form.

    Scenario: percentile_approx over year-month intervals selects the right value
      When query
        """
        SELECT percentile_approx(c, 0.5) = INTERVAL '1' MONTH AS matches
        FROM VALUES (INTERVAL '0' MONTH), (INTERVAL '1' MONTH), (INTERVAL '2' MONTH), (INTERVAL '10' MONTH) AS t(c)
        """
      Then query result
        | matches |
        | true    |

    Scenario: percentile_approx array over year-month intervals selects the right values
      When query
        """
        SELECT percentile_approx(c, array(0.25, 0.5, 0.75))
                 = array(INTERVAL '0' MONTH, INTERVAL '1' MONTH, INTERVAL '2' MONTH) AS matches
        FROM VALUES (INTERVAL '0' MONTH), (INTERVAL '1' MONTH), (INTERVAL '2' MONTH), (INTERVAL '10' MONTH) AS t(c)
        """
      Then query result
        | matches |
        | true    |

    Scenario: percentile_approx over day-time intervals selects the right value
      When query
        """
        SELECT percentile_approx(c, 0.5) = INTERVAL '1' SECOND AS matches
        FROM VALUES (INTERVAL '0' SECOND), (INTERVAL '1' SECOND), (INTERVAL '2' SECOND), (INTERVAL '10' SECOND) AS t(c)
        """
      Then query result
        | matches |
        | true    |

  Rule: ANSI interval types — type label is widened (Sail-wide gap)

    # The selected value is correct (see the rule above), but Sail widens the
    # interval subrange at the type layer: `interval month` -> `interval year
    # to month` and `interval second` -> `interval day to second`. This is a
    # Sail-wide issue affecting every expression that returns an interval (even
    # `SELECT INTERVAL '1' MONTH`), not `percentile_approx` specifically, so it
    # is tagged @sail-bug rather than blocking this function.

    @sail-bug
    Scenario: percentile_approx preserves the year-month interval subrange
      When query
        """
        SELECT typeof(percentile_approx(c, 0.5)) AS type
        FROM VALUES (INTERVAL '0' MONTH), (INTERVAL '1' MONTH), (INTERVAL '2' MONTH) AS t(c)
        """
      Then query result
        | type           |
        | interval month |

    @sail-bug
    Scenario: percentile_approx preserves the day-time interval subrange
      When query
        """
        SELECT typeof(percentile_approx(c, 0.5)) AS type
        FROM VALUES (INTERVAL '0' SECOND), (INTERVAL '1' SECOND), (INTERVAL '2' SECOND) AS t(c)
        """
      Then query result
        | type            |
        | interval second |

  Rule: NULL handling

    Scenario: percentile_approx ignores NULLs
      When query
        """
        SELECT percentile_approx(v, 0.5) AS median
        FROM VALUES (CAST(NULL AS INT)), (1), (2), (3), (CAST(NULL AS INT)) AS t(v)
        """
      Then query result
        | median |
        | 2      |

    Scenario: percentile_approx with all NULLs returns NULL
      When query
        """
        SELECT percentile_approx(v, 0.5) AS median
        FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS t(v)
        """
      Then query result
        | median |
        | NULL   |

    Scenario: percentile_approx array with all NULLs returns NULL
      When query
        """
        SELECT percentile_approx(v, array(0.25, 0.5)) AS quantiles
        FROM VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)) AS t(v)
        """
      Then query result
        | quantiles |
        | NULL      |

  Rule: Group by support

    Scenario: percentile_approx with group by
      When query
        """
        SELECT grp, percentile_approx(value, 0.5) AS median
        FROM VALUES ('A', 1), ('A', 2), ('A', 3), ('B', 10), ('B', 20), ('B', 30) AS t(grp, value)
        GROUP BY grp
        ORDER BY grp
        """
      Then query result ordered
        | grp | median |
        | A   | 2      |
        | B   | 20     |

  Rule: Argument validation

    Scenario: percentile_approx with accuracy zero errors
      When query
        """
        SELECT percentile_approx(v, 0.5, 0) AS r
        FROM VALUES (0), (1), (2) AS t(v)
        """
      Then query error The accuracy must be between \(0, 2147483647\]

    Scenario: percentile_approx with accuracy above the int range errors
      When query
        """
        SELECT percentile_approx(v, 0.5, 3000000000) AS r
        FROM VALUES (0), (1), (2) AS t(v)
        """
      Then query error The accuracy must be between \(0, 2147483647\]

    Scenario: percentile_approx with percentage out of range errors
      When query
        """
        SELECT percentile_approx(v, 1.5) AS r
        FROM VALUES (0), (1), (2) AS t(v)
        """
      Then query error The percentage must be between \[0.0, 1.0\]

    Scenario: percentile_approx with too few arguments errors
      When query
        """
        SELECT percentile_approx(v) AS r
        FROM VALUES (0), (1), (2) AS t(v)
        """
      Then query error `percentile_approx`.*requires

    Scenario: percentile_approx with too many arguments errors
      When query
        """
        SELECT percentile_approx(v, 0.5, 100, 999) AS r
        FROM VALUES (0), (1), (2) AS t(v)
        """
      Then query error `percentile_approx`.*requires

  Rule: Window frames

    # The aggregate is registered as a window function, so the accumulator is
    # reused across rows: evaluating one frame must not consume the state
    # accumulated for the next one.

    Scenario: percentile_approx as a running median over an ordered window
      When query
        """
        SELECT v, percentile_approx(v, 0.5) OVER (ORDER BY v) AS m
        FROM VALUES (1), (2), (3), (4), (5) AS t(v)
        ORDER BY v
        """
      Then query result ordered
        | v | m |
        | 1 | 1 |
        | 2 | 1 |
        | 3 | 2 |
        | 4 | 2 |
        | 5 | 3 |

    Scenario: percentile_approx running median restarts per partition
      When query
        """
        SELECT g, v, percentile_approx(v, 0.5) OVER (PARTITION BY g ORDER BY v) AS m
        FROM VALUES ('a', 1), ('a', 2), ('a', 3), ('b', 10), ('b', 20) AS t(g, v)
        ORDER BY g, v
        """
      Then query result ordered
        | g | v  | m  |
        | a | 1  | 1  |
        | a | 2  | 1  |
        | a | 3  | 2  |
        | b | 10 | 10 |
        | b | 20 | 10 |

  Rule: accuracy controls the approximation

    # `accuracy` sets the sketch's relative error to `1 / accuracy`, and the
    # query short-circuits at the two extremes: percentages at or below the
    # relative error return the minimum, and those at or above `1 - relative
    # error` return the maximum. With a low accuracy that is observable on tiny
    # inputs, so these scenarios pin the approximation rather than an exact
    # nearest-rank pick.

    Scenario Outline: relative error short-circuit: <case>
      When query
        """
        SELECT percentile_approx(col, <percentage>, <accuracy>) AS r
        FROM VALUES (0), (1), (2), (10) AS tab(col)
        """
      Then query result
        | r          |
        | <expected> |

      Examples:
        | case                                              | percentage | accuracy | expected |
        | accuracy one collapses a percentile to the minimum | 0.9        | 1        | 0        |
        | a percentage inside the upper error gives the max  | 0.995      | 100      | 10       |
        | a percentage inside the lower error gives the min  | 0.005      | 100      | 0        |

    Scenario: accuracy one collapses an array of percentiles to the minimum
      When query
        """
        SELECT percentile_approx(col, array(0.1, 0.9), 1) AS r
        FROM VALUES (0), (1), (2), (10) AS tab(col)
        """
      Then query result
        | r      |
        | [0, 0] |

  Rule: Date and timestamp inputs

    Scenario: percentile_approx over dates returns a date
      When query
        """
        SELECT
          percentile_approx(d, 0.5) AS median,
          typeof(percentile_approx(d, 0.5)) AS type
        FROM VALUES (DATE '2020-01-01'), (DATE '2020-01-03'), (DATE '2020-01-05') AS t(d)
        """
      Then query result
        | median     | type |
        | 2020-01-03 | date |

    Scenario: percentile_approx array over dates
      When query
        """
        SELECT percentile_approx(d, array(0.0, 1.0)) AS quantiles
        FROM VALUES (DATE '2020-01-01'), (DATE '2020-01-03'), (DATE '2020-01-05') AS t(d)
        """
      Then query result
        | quantiles                |
        | [2020-01-01, 2020-01-05] |

    # Compared by equality rather than by rendering, so the scenario does not
    # depend on the session time zone.
    Scenario: percentile_approx over timestamps selects the right value
      When query
        """
        SELECT
          percentile_approx(ts, 0.5) = TIMESTAMP '2020-01-01 00:00:02' AS matches,
          typeof(percentile_approx(ts, 0.5)) AS type
        FROM VALUES
          (TIMESTAMP '2020-01-01 00:00:00'),
          (TIMESTAMP '2020-01-01 00:00:02'),
          (TIMESTAMP '2020-01-01 00:00:04') AS t(ts)
        """
      Then query result
        | matches | type      |
        | true    | timestamp |

    Scenario: percentile_approx over timestamps without time zone selects the right value
      When query
        """
        SELECT
          percentile_approx(ts, 0.5) = TIMESTAMP_NTZ '2020-01-01 00:00:02' AS matches,
          typeof(percentile_approx(ts, 0.5)) AS type
        FROM VALUES
          (TIMESTAMP_NTZ '2020-01-01 00:00:00'),
          (TIMESTAMP_NTZ '2020-01-01 00:00:02'),
          (TIMESTAMP_NTZ '2020-01-01 00:00:04') AS t(ts)
        """
      Then query result
        | matches | type          |
        | true    | timestamp_ntz |

  Rule: Decimal results keep the double's exact decimal digits

    # Spark narrows the double result with `Decimal(double)`, which goes through
    # the double's shortest round-trip decimal string and then rescales exactly
    # with HALF_UP. Rescaling by multiplying with 10^scale in binary floating
    # point instead leaks the double's garbage bits into the result digits.
    # Asserted on collected values, since `show` prints the unscaled form.

    Scenario: the median of two exact decimals is one of them
      When query
        """
        SELECT percentile_approx(v, 0.5, 100) AS median
        FROM VALUES (CAST(1 AS DECIMAL(38,37))), (CAST(2 AS DECIMAL(38,37))) AS t(v)
        """
      Then query result collected
        | median                                  |
        | 1.0000000000000000000000000000000000000 |

    Scenario Outline: decimal narrowing: <case>
      When query
        """
        SELECT percentile_approx(v, 0.0) AS r
        FROM VALUES (CAST(<literal> AS DECIMAL(38,<scale>))) AS t(v)
        """
      Then query result collected
        | r          |
        | <expected> |

      Examples:
        | case                                       | literal                                | scale | expected                       |
        | a wide decimal keeps the digits Spark uses | 12345678901234567890                   | 0     | 12345678901234567000           |
        | a scaled decimal keeps those digits too    | 123456789012345678.90                  | 2     | 123456789012345680.00          |
        | a power of ten survives the round trip     | 1e30                                   | 0     | 1000000000000000000000000000000 |
        | overflowing the precision yields NULL      | 99999999999999999999999999999999999999 | 0     | NULL                           |

    # A tiny decimal is where the rescale rounding earns its keep: the shortest
    # representation of the double nearest 1e-37 is 0.0000…0009999999999999999,
    # 53 fractional digits. Truncating to scale 37 would return 0; rounding
    # HALF_UP returns the value itself, as Spark does.
    Scenario Outline: decimal narrowing rounds a tiny value back to itself: <case>
      When query
        """
        SELECT percentile_approx(v, 0.0) = CAST(<literal> AS DECIMAL(38,<scale>)) AS matches
        FROM VALUES (CAST(<literal> AS DECIMAL(38,<scale>))) AS t(v)
        """
      Then query result
        | matches |
        | true    |

      Examples:
        | case   | literal                                 | scale |
        | 1e-37  | 0.0000000000000000000000000000000000001 | 37    |
        | 3e-37  | 0.0000000000000000000000000000000000003 | 37    |
        | 1e-30  | 0.000000000000000000000000000001         | 30    |

  Rule: Decimal narrowing overflows the declared precision

    # Widening to a double can round the value past what the declared type
    # holds: the largest DECIMAL(38,38) widens to exactly 1.0, whose rescale to
    # scale 38 needs 39 digits. Spark's `changePrecision` then fails and
    # `UnsafeRowWriter` writes a NULL rather than raising.
    #
    # Asserted on collected values: the narrowing happens on the way into the
    # output row, so `show` still prints the un-narrowed `1.0` where `collect`
    # yields the NULL.

    Scenario Outline: the rescaled value no longer fits: <case>
      When query
        """
        SELECT percentile_approx(v, 0.0) AS r
        FROM VALUES (CAST(<literal> AS DECIMAL(<precision>,<scale>))) AS t(v)
        """
      Then query result collected
        | r    |
        | NULL |

      Examples:
        | case                        | literal                                 | precision | scale |
        | the largest DECIMAL(38,38)  | 0.99999999999999999999999999999999999999 | 38        | 38    |
        | the largest DECIMAL(19,19)  | 0.9999999999999999999                    | 19        | 19    |
        | the largest DECIMAL(38,37)  | 9.9999999999999999999999999999999999999  | 38        | 37    |
        | the largest DECIMAL(18,6)   | 999999999999.999999                      | 18        | 6     |

    # The element type is declared non-nullable, yet every element narrows to
    # NULL — Spark declares `ArrayType(child.dataType, false)` and writes the
    # NULLs anyway. Rendered by `str()` on the collected list, hence `None`.
    Scenario: every element of an overflowing array result is NULL
      When query
        """
        SELECT percentile_approx(v, array(0.0, 1.0)) AS r
        FROM VALUES (CAST(0.99999999999999999999999999999999999999 AS DECIMAL(38,38))) AS t(v)
        """
      Then query result collected
        | r            |
        | [None, None] |

  Rule: Decimal narrowing keeps the sign

    # HALF_UP rounds away from zero, and a value that rounds down to zero must
    # not come back as a negative zero. Compared by equality so the scenarios do
    # not depend on how a small decimal is rendered.

    Scenario Outline: a negative value survives the round trip: <case>
      When query
        """
        SELECT percentile_approx(v, 0.0) = CAST(<literal> AS DECIMAL(38,37)) AS matches
        FROM VALUES (CAST(<literal> AS DECIMAL(38,37))) AS t(v)
        """
      Then query result
        | matches |
        | true    |

      Examples:
        | case              | literal                                  |
        | negative 1e-37    | -0.0000000000000000000000000000000000001 |
        | negative 3e-37    | -0.0000000000000000000000000000000000003 |
        | negative one      | -1.0                                     |

    Scenario: the median of two negative decimals keeps its sign
      When query
        """
        SELECT percentile_approx(v, 0.5, 100) AS median
        FROM VALUES (CAST(-2 AS DECIMAL(38,37))), (CAST(-1 AS DECIMAL(38,37))) AS t(v)
        """
      Then query result collected
        | median                                   |
        | -2.0000000000000000000000000000000000000 |

  Rule: Decimals widen through an exact division where one is available

    # Widening reads `unscaled / 10^scale` directly when both operands are
    # exactly representable — scale at most 22 and an unscaled value at most
    # 2^53 — and falls back to the exact decimal string otherwise. Both paths
    # must agree, so these pin values on either side of each boundary.

    Scenario: a median inside the exact-division domain
      When query
        """
        SELECT percentile_approx(v, 0.5, 100) AS median
        FROM VALUES (CAST(1.5 AS DECIMAL(18,6))), (CAST(2.5 AS DECIMAL(18,6))), (CAST(3.5 AS DECIMAL(18,6))) AS t(v)
        """
      Then query result collected
        | median   |
        | 2.500000 |

    Scenario Outline: the two widening paths agree: <case>
      When query
        """
        SELECT percentile_approx(v, 0.0) AS r
        FROM VALUES (CAST(<literal> AS DECIMAL(18,6))) AS t(v)
        """
      Then query result collected
        | r         |
        | <literal> |

      Examples:
        | case                                   | literal            |
        | a fractional value divides exactly     | 1234567.891234     |
        | its negative divides exactly too       | -1234567.891234    |
        | an unscaled value of exactly 2^53      | 9007199254.740992  |
        | one past 2^53 falls back to the string | 9007199254.740993  |

    # Scale 22 is the last power of ten that is exact as a double, so 23 leaves
    # the fast path. Compared by equality: both render in exponent notation.
    Scenario Outline: the scale boundary of the exact division: <case>
      When query
        """
        SELECT percentile_approx(v, 0.0) = CAST(<literal> AS DECIMAL(38,<scale>)) AS matches
        FROM VALUES (CAST(<literal> AS DECIMAL(38,<scale>))) AS t(v)
        """
      Then query result
        | matches |
        | true    |

      Examples:
        | case                          | literal                  | scale |
        | scale 22 stays on the fast path | 0.0000000000000000012345 | 22    |
        | scale 23 leaves it              | 0.00000000000000000000123 | 23   |

  Rule: Empty percentage array

    Scenario: an empty percentage array returns NULL
      When query
        """
        SELECT percentile_approx(v, array()) AS r
        FROM VALUES (1), (2) AS t(v)
        """
      Then query result
        | r    |
        | NULL |

  Rule: Result schema

    # `typeof` shows the type but not nullability, so the values-only scenarios
    # above leave the declared schema unasserted.

    Scenario: the scalar result is nullable
      When query
        """
        SELECT percentile_approx(v, 0.5) AS median
        FROM VALUES (1), (2), (3) AS t(v)
        """
      Then query schema
        """
        root
         |-- median: integer (nullable = true)
        """

    Scenario: the array result is nullable with non-nullable elements
      When query
        """
        SELECT percentile_approx(v, array(0.25, 0.75)) AS quantiles
        FROM VALUES (1), (2), (3) AS t(v)
        """
      Then query schema
        """
        root
         |-- quantiles: array (nullable = true)
         |    |-- element: integer (containsNull = false)
        """

    # Spark declares the element non-nullable for decimals too, and then writes
    # NULLs into it when the rescaled value overflows the declared precision --
    # `InternalRow` never checks. Arrow does check, and rejects the array
    # outright ("Non-nullable field of ListArray cannot contain nulls"), so Sail
    # has to declare the decimal element nullable to answer the query at all.
    # A forced divergence: the alternative is failing a query Spark answers.
    @sail-bug
    Scenario: the decimal array element is reported non-nullable
      When query
        """
        SELECT percentile_approx(v, array(0.25, 0.75)) AS quantiles
        FROM VALUES (CAST(1 AS DECIMAL(10,2))) AS t(v)
        """
      Then query schema
        """
        root
         |-- quantiles: array (nullable = true)
         |    |-- element: decimal(10,2) (containsNull = false)
        """

  Rule: Input-type surface gaps (Sail-wide, tracked)

    # Spark's `inputTypes` is a TypeCollection combined with ImplicitCastInputTypes,
    # so it also accepts STRING and untyped NULL by casting them to DOUBLE. Sail
    # rejects both at planning. Pre-existing; not introduced by the sketch port.

    @sail-bug
    Scenario: percentile_approx accepts a string column and returns double
      When query
        """
        SELECT
          percentile_approx(v, 0.5) AS r,
          typeof(percentile_approx(v, 0.5)) AS type
        FROM VALUES ('1'), ('2'), ('3') AS t(v)
        """
      Then query result
        | r   | type   |
        | 2.0 | double |

    @sail-bug
    Scenario: percentile_approx accepts an untyped NULL and returns double
      When query
        """
        SELECT
          percentile_approx(NULL, 0.5) AS r,
          typeof(percentile_approx(NULL, 0.5)) AS type
        """
      Then query result
        | r    | type   |
        | NULL | double |

  Rule: Argument-handling gaps (Sail-wide, tracked)

    # Pre-existing divergences on the argument surface, unchanged by the port.

    @sail-bug
    Scenario: DISTINCT deduplicates the observations
      When query
        """
        SELECT percentile_approx(DISTINCT v, 0.5) AS r
        FROM VALUES (1), (1), (1), (2), (3) AS t(v)
        """
      Then query result
        | r |
        | 2 |

    @sail-bug
    Scenario: a NULL element in the percentage array reads as zero
      When query
        """
        SELECT percentile_approx(v, array(0.5, CAST(NULL AS DOUBLE))) AS r
        FROM VALUES (1), (2) AS t(v)
        """
      Then query result
        | r      |
        | [1, 1] |

    @sail-bug
    Scenario: a non-integral accuracy is rejected
      When query
        """
        SELECT percentile_approx(v, 0.5, 100.7) AS r
        FROM VALUES (0), (1), (2) AS t(v)
        """
      Then query error The third parameter requires the "INTEGRAL" type

  Rule: Sliding window frames (Sail-wide, tracked)

    # Spark re-initializes the aggregation buffer per frame, so it needs no
    # retraction. A Greenwald-Khanna sketch cannot retract observations, and
    # DataFusion requires `retract_batch` for any non-growing frame, so these
    # fail where the growing frames above succeed.

    @sail-bug
    Scenario: percentile_approx over a centred sliding frame
      When query
        """
        SELECT v, percentile_approx(v, 0.5) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS m
        FROM VALUES (1), (2), (3), (4) AS t(v)
        ORDER BY v
        """
      Then query result ordered
        | v | m |
        | 1 | 1 |
        | 2 | 2 |
        | 3 | 3 |
        | 4 | 3 |

    @sail-bug
    Scenario: percentile_approx over a trailing sliding frame
      When query
        """
        SELECT v, percentile_approx(v, 0.5) OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS m
        FROM VALUES (1), (2), (3), (4) AS t(v)
        ORDER BY v
        """
      Then query result ordered
        | v | m |
        | 1 | 2 |
        | 2 | 3 |
        | 3 | 3 |
        | 4 | 4 |

  Rule: NaN observations

    # Spark's sketch sorts NaN last, so NaN occupies the top of the range. But
    # `compressImmut` also drops the first sample whenever
    # `first.value <= second.value` is false — and any comparison against NaN is
    # false. So a sketch whose second sample is NaN loses its minimum, and the
    # `relativeError` short-circuit then returns NaN for percentile 0.0 too.
    # Every value is CAST to DOUBLE explicitly: an untyped decimal literal in the
    # same VALUES list changes the column type and hides the case.

    Scenario Outline: NaN observations: <case>
      When query
        """
        SELECT percentile_approx(v, <percentage>) AS result
        FROM VALUES <rows> AS t(v)
        """
      Then query result
        | result     |
        | <expected> |

      Examples:
        | case                                    | rows                                                                     | percentage | expected |
        | one NaN sorts to the top, min unaffected | (CAST('NaN' AS DOUBLE)), (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE))    | 0.0        | 1.0      |
        | one NaN, median unaffected               | (CAST('NaN' AS DOUBLE)), (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE))    | 0.5        | 2.0      |
        | one NaN wins the upper percentiles       | (CAST('NaN' AS DOUBLE)), (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE))    | 0.75       | NaN      |
        | one NaN wins the maximum                 | (CAST('NaN' AS DOUBLE)), (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE))    | 1.0        | NaN      |
        | two NaNs drop the minimum from the sketch | (CAST('NaN' AS DOUBLE)), (CAST('NaN' AS DOUBLE)), (CAST(1.0 AS DOUBLE)) | 0.0        | NaN      |
        | two NaNs also win the maximum            | (CAST('NaN' AS DOUBLE)), (CAST('NaN' AS DOUBLE)), (CAST(1.0 AS DOUBLE)) | 1.0        | NaN      |
        | a single NaN observation                 | (CAST('NaN' AS DOUBLE))                                                  | 0.5        | NaN      |

    Scenario: an array of percentiles over a NaN-poisoned sketch
      When query
        """
        SELECT percentile_approx(v, array(0.0, 1.0)) AS result
        FROM VALUES (CAST('NaN' AS DOUBLE)), (CAST('NaN' AS DOUBLE)), (CAST(1.0 AS DOUBLE)) AS t(v)
        """
      Then query result
        | result     |
        | [NaN, NaN] |
