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

    Scenario: accuracy one collapses every percentile to the minimum
      When query
        """
        SELECT percentile_approx(col, 0.9, 1) AS r
        FROM VALUES (0), (1), (2), (10) AS tab(col)
        """
      Then query result
        | r |
        | 0 |

    Scenario: accuracy one collapses an array of percentiles to the minimum
      When query
        """
        SELECT percentile_approx(col, array(0.1, 0.9), 1) AS r
        FROM VALUES (0), (1), (2), (10) AS tab(col)
        """
      Then query result
        | r      |
        | [0, 0] |

    Scenario: a percentage inside the upper relative error returns the maximum
      When query
        """
        SELECT percentile_approx(col, 0.995, 100) AS r
        FROM VALUES (0), (1), (2), (10) AS tab(col)
        """
      Then query result
        | r  |
        | 10 |

    Scenario: a percentage inside the lower relative error returns the minimum
      When query
        """
        SELECT percentile_approx(col, 0.005, 100) AS r
        FROM VALUES (0), (1), (2), (10) AS tab(col)
        """
      Then query result
        | r |
        | 0 |

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
