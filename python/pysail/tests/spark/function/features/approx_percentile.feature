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

    Scenario: percentile_approx median over decimals preserves precision and scale
      When query
        """
        SELECT
          percentile_approx(v, 0.5) AS median,
          typeof(percentile_approx(v, 0.5)) AS type
        FROM VALUES (CAST(0 AS DECIMAL(10,2))), (CAST(5 AS DECIMAL(10,2))), (CAST(10 AS DECIMAL(10,2))) AS t(v)
        """
      Then query result
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
      Then query error .*

    Scenario: percentile_approx with percentage out of range errors
      When query
        """
        SELECT percentile_approx(v, 1.5) AS r
        FROM VALUES (0), (1), (2) AS t(v)
        """
      Then query error .*

    Scenario: percentile_approx with too few arguments errors
      When query
        """
        SELECT percentile_approx(v) AS r
        FROM VALUES (0), (1), (2) AS t(v)
        """
      Then query error .*

    Scenario: percentile_approx with too many arguments errors
      When query
        """
        SELECT percentile_approx(v, 0.5, 100, 999) AS r
        FROM VALUES (0), (1), (2) AS t(v)
        """
      Then query error .*
