@percentile_disc
Feature: percentile_disc() returns the discrete percentile for a numeric column

  # Regression coverage for `percentile_disc` over numeric inputs (including
  # DECIMAL) and the array-of-percentiles form. Prior to these fixes the
  # signature was `OneOf(Exact([<numeric>, Float64]))`, which rejected calls
  # of the form `percentile_disc(array(p1, p2, ...))` at type-coercion time
  # for ALL numeric inputs (not just decimals). The signature is now
  # `Signature::user_defined` with `coerce_types` mapping every numeric to
  # `Float64` (matching Spark's `double` return type) and runtime dispatch in
  # `accumulator()`.

  Rule: DECIMAL(p, s) inputs are accepted and return DOUBLE (Spark-compat)

    Scenario Outline: percentile_disc <pct> <case>
      When query
        """
        SELECT percentile_disc(<pct>) WITHIN GROUP (ORDER BY CAST(x AS <type>)) AS p
        FROM (VALUES <values>) AS t(x)
        """
      Then query result
        | p   |
        | <p> |

      Examples:
        | pct  | case                             | type           | values                                 | p    |
        | 0.5  | on DECIMAL(10,2)                 | DECIMAL(10,2)  | (1.50), (2.50), (3.50), (4.50), (5.50) | 3.5  |
        | 0.5  | on high-precision DECIMAL(38,10) | DECIMAL(38,10) | (1.0), (2.0), (3.0), (4.0), (5.0)      | 3.0  |
        | 0.0  | on DECIMAL returns minimum       | DECIMAL(10,2)  | (5), (10), (15)                        | 5.0  |
        | 1.0  | on DECIMAL returns maximum       | DECIMAL(10,2)  | (5), (10), (15)                        | 15.0 |
        | 0.25 | (first quartile) on DECIMAL      | DECIMAL(10,2)  | (10), (20), (30), (40)                 | 10.0 |
        | 0.75 | (third quartile) on DECIMAL      | DECIMAL(10,2)  | (10), (20), (30), (40)                 | 30.0 |

  Rule: NULL handling matches Spark (NULLs ignored, all-NULL/empty → NULL)

    Scenario Outline: percentile_disc <case>
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY <order>) AS p
        FROM (VALUES <values>) AS t(x)
        """
      Then query result
        | p   |
        | <p> |

      Examples:
        | case                                      | order                       | values                              | p    |
        | ignores NULL DECIMAL values               | CAST(x AS DECIMAL(10,2))    | (NULL), (1.0), (2.0), (3.0), (NULL) | 2.0  |
        | with all-NULL DECIMAL column returns NULL | CAST(NULL AS DECIMAL(10,2)) | (1), (2), (3)                       | NULL |
        | ignores NULL ORDER BY values (INT)        | x                           | (1), (CAST(NULL AS INT)), (3)       | 1.0  |
        | on all-NULL INT column returns NULL       | x                           | (CAST(NULL AS INT)), (NULL), (NULL) | NULL |

    Scenario: percentile_disc on empty input returns NULL
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (SELECT 1 AS x WHERE false) AS t
        """
      Then query result
        | p    |
        | NULL |

  Rule: ORDER BY DESC reverses the percentile direction

    # `percentile_disc`'s index `ceil(p * n) - 1` is asymmetric: the naive
    # `1 - p` inversion that works for `percentile_cont` returns the WRONG
    # value here. With DESC sorted `[4,3,2,1]`, `percentile_disc(0.25)` is
    # position 0 = the max value (4), not position 2 = 3.

    Scenario Outline: percentile_disc <pct> DESC <case>
      When query
        """
        SELECT percentile_disc(<pct>) WITHIN GROUP (ORDER BY <order> DESC) AS p
        FROM (VALUES <values>) AS t(x)
        """
      Then query result
        | p   |
        | <p> |

      Examples:
        | pct  | case                      | order                    | values                            | p   |
        | 0.5  | on DECIMAL                | CAST(x AS DECIMAL(10,2)) | (1.0), (2.0), (3.0), (4.0), (5.0) | 3.0 |
        | 0.25 | selects from the high end | x                        | (1), (2), (3), (4)                | 4.0 |
        | 0.75 | selects from the low end  | x                        | (1), (2), (3), (4)                | 2.0 |
        | 0.0  | returns the maximum       | x                        | (1), (2), (3), (4), (5)           | 5.0 |
        | 1.0  | returns the minimum       | x                        | (1), (2), (3), (4), (5)           | 1.0 |

  Rule: Single value, duplicates and even-count populations

    Scenario Outline: Population: <case>
      When query
        """
        SELECT percentile_disc(<pct>) WITHIN GROUP (ORDER BY <orderby>) AS p
        FROM (VALUES <values>) AS t(x)
        """
      Then query result
        | p        |
        | <result> |

      Examples:
        | case                                                   | pct | orderby                  | values                                | result |
        | percentile_disc on a single value                      | 0.5 | x                        | (42)                                  | 42.0   |
        | percentile_disc on all-duplicate input                 | 0.5 | x                        | (5), (5), (5), (5)                    | 5.0    |
        | percentile_disc with even row count picks lower middle | 0.5 | x                        | (1), (2), (3), (4)                    | 2.0    |
        | percentile_disc 0.5 with negative DECIMAL values       | 0.5 | CAST(x AS DECIMAL(10,2)) | (-10.0), (-5.0), (0.0), (5.0), (10.0) | 0.0    |

  Rule: STRING inputs are implicitly cast to DOUBLE (Spark-compat)

    Scenario: percentile_disc on STRING numeric values
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES ('1.5'), ('2.5'), ('3.5'), ('4.5'), ('5.5')) AS t(x)
        """
      Then query result
        | p   |
        | 3.5 |

    Scenario: percentile_disc on STRING numeric values with array
      When query
        """
        SELECT percentile_disc(array(0.0, 0.5, 1.0)) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES ('1'), ('2'), ('3'), ('4'), ('5')) AS t(x)
        """
      Then query result
        | p               |
        | [1.0, 3.0, 5.0] |

  Rule: DECIMAL inputs work under GROUP BY

    Scenario: per-group median on DECIMAL
      When query
        """
        SELECT g, percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(x AS DECIMAL(10,2))) AS p
        FROM (VALUES ('A', 1.0), ('A', 2.0), ('A', 3.0), ('B', 10.0), ('B', 20.0)) AS t(g, x)
        GROUP BY g
        ORDER BY g
        """
      Then query result ordered
        | g | p    |
        | A | 2.0  |
        | B | 10.0 |

  Rule: Array-of-percentiles form returns ARRAY<DOUBLE>

    Scenario Outline: Array of percentiles: <case>
      When query
        """
        SELECT percentile_disc(<pct>) WITHIN GROUP (ORDER BY <orderby>) AS p
        FROM (VALUES <values>) AS t(x)
        """
      Then query result
        | p        |
        | <result> |

      Examples:
        | case                                                 | pct                         | orderby                  | values                            | result             |
        | percentile_disc with array of percentiles on DECIMAL | array(0.25, 0.5, 0.75)      | CAST(x AS DECIMAL(10,2)) | (10), (20), (30), (40)            | [10.0, 20.0, 30.0] |
        | percentile_disc with array of percentiles on INT     | array(0.25, 0.5, 0.75)      | x                        | (1), (2), (3), (4), (5)           | [2.0, 3.0, 4.0]    |
        | percentile_disc with array on DOUBLE                 | array(0.0, 0.5, 1.0)        | CAST(x AS DOUBLE)        | (1.0), (2.0), (3.0), (4.0), (5.0) | [1.0, 3.0, 5.0]    |
        | Empty percentile array returns NULL                  | array()                     | x                        | (1), (2), (3), (4), (5)           | NULL               |
        | NULL element in percentile array is treated as 0.0   | array(CAST(NULL AS DOUBLE)) | x                        | (1), (2), (3), (4), (5)           | [1.0]              |
    Scenario: percentile_disc with array under GROUP BY
      When query
        """
        SELECT g, percentile_disc(array(0.25, 0.5, 0.75)) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES ('A', 10), ('A', 20), ('A', 30), ('A', 40), ('B', 100), ('B', 200), ('B', 300), ('B', 400)) AS t(g, x)
        GROUP BY g
        ORDER BY g
        """
      Then query result ordered
        | g | p                     |
        | A | [10.0, 20.0, 30.0]    |
        | B | [100.0, 200.0, 300.0] |

  Rule: Invalid arguments raise an error

    # Error scenarios pin a keyword from the expected validation message
    # rather than matching `.*`. This keeps the tests asserting that the
    # FAILURE PATH is the one we intend (out-of-range vs. type mismatch),
    # not just that "some error" happened.

    Scenario Outline: <case> is rejected as out of range
      When query
        """
        SELECT percentile_disc(<pct>) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (1), (2), (3)) AS t(x)
        """
      Then query error .*(out of range|VALUE_OUT_OF_RANGE).*

      Examples:
        | case                                | pct             |
        | Negative percentile                 | -0.1            |
        | Percentile greater than 1           | 1.1             |
        | Out-of-range value inside the array | array(0.5, 1.5) |

    Scenario: DISTINCT with WITHIN GROUP is rejected
      When query
        """
        SELECT percentile_disc(DISTINCT 0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (1), (2), (3), (2), (1)) AS t(x)
        """
      Then query error .*(DISTINCT|distinct).*

    Scenario Outline: <case> is rejected as non-numeric
      When query
        """
        SELECT percentile_disc(<pct>) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES <values>) AS t(x)
        """
      Then query error .*(numeric|UNEXPECTED_INPUT_TYPE).*

      Examples:
        | case                               | pct               | values                                   |
        | BOOLEAN ORDER BY                   | 0.5               | (true), (false)                          |
        | DATE ORDER BY                      | 0.5               | (DATE '2024-01-01'), (DATE '2024-01-02') |
        | ARRAY ORDER BY                     | 0.5               | (array(1)), (array(2))                   |
        | BOOLEAN percentile arg             | true              | (1), (2), (3)                            |
        | DATE percentile arg                | DATE '2024-01-01' | (1), (2), (3)                            |
        | Array of strings as percentile arg | array('0.5')      | (1), (2), (3)                            |

  Rule: FLOAT inputs are accepted and return DOUBLE

    Scenario: percentile_disc 0.5 on FLOAT
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(x AS FLOAT)) AS p
        FROM (VALUES (1), (2), (3), (4)) AS t(x)
        """
      Then query result
        | p   |
        | 2.0 |

  Rule: NaN and Infinity order as the extremes (DOUBLE)

    Scenario Outline: NaN and Infinity: <case>
      When query
        """
        SELECT percentile_disc(<pct>) WITHIN GROUP (ORDER BY <orderby>) AS p
        FROM (VALUES <values>) AS t(x)
        """
      Then query result
        | p        |
        | <result> |

      Examples:
        | case                              | pct | orderby | values                                            | result    |
        | NaN orders as the maximum         | 1.0 | x       | (double('1.0')), (double('2.0')), (double('NaN')) | NaN       |
        | NaN is not selected below the top | 0.5 | x       | (double('1.0')), (double('2.0')), (double('NaN')) | 2.0       |
        | positive Infinity is the maximum  | 1.0 | x       | (double('1.0')), (double('Infinity'))             | Infinity  |
        | negative Infinity is the minimum  | 0.0 | x       | (double('1.0')), (double('-Infinity'))            | -Infinity |

  Rule: FILTER restricts the aggregated population

    Scenario: percentile_disc with FILTER (WHERE)
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) FILTER (WHERE x > 1) AS p
        FROM (VALUES (1), (2), (3), (4)) AS t(x)
        """
      Then query result
        | p   |
        | 3.0 |

  Rule: percentile_disc works as a window function (OVER)

    # @sail-bug: Spark supports percentile_disc as a window function; Sail's window
    # resolver has no branch for ordered-set (WITHIN GROUP) aggregates, and
    # percentile_disc is registered as F::unknown in the window registry. Fixing it
    # needs: (1) a WITHIN-GROUP branch in resolver/expression/window.rs that routes
    # the WITHIN GROUP ORDER BY into the aggregate's order_bys, and (2) a real
    # handler in function/window.rs (replacing F::unknown). Deferred — separate work.
    @sail-bug
    Scenario: percentile_disc OVER () broadcasts to every row
      When query
        """
        SELECT x, percentile_disc(0.5) WITHIN GROUP (ORDER BY x) OVER () AS p
        FROM (VALUES (1), (2), (3), (4)) AS t(x)
        ORDER BY x
        """
      Then query result ordered
        | x | p   |
        | 1 | 2.0 |
        | 2 | 2.0 |
        | 3 | 2.0 |
        | 4 | 2.0 |

    @sail-bug
    Scenario: percentile_disc OVER (PARTITION BY)
      When query
        """
        SELECT g, percentile_disc(0.5) WITHIN GROUP (ORDER BY x) OVER (PARTITION BY g) AS p
        FROM (VALUES ('a', 1), ('a', 3), ('b', 10), ('b', 20)) AS t(g, x)
        ORDER BY g, x
        """
      Then query result ordered
        | g | p    |
        | a | 1.0  |
        | a | 1.0  |
        | b | 10.0 |
        | b | 10.0 |

  Rule: ANSI mode affects STRING coercion of the ORDER BY column

    Scenario: non-numeric STRING under ANSI off yields NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES ('abc'), ('def')) AS t(x)
        """
      Then query result
        | p    |
        | NULL |

    Scenario: non-numeric STRING under ANSI on errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES ('abc'), ('def')) AS t(x)
        """
      Then query error (?i)cast

  Rule: More invalid arguments are rejected

    Scenario: NULL percentage is rejected
      When query
        """
        SELECT percentile_disc(NULL) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (1), (2), (3)) AS t(x)
        """
      Then query error (?i)null

    Scenario: TIMESTAMP ORDER BY is rejected
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(x AS TIMESTAMP)) AS p
        FROM (VALUES (TIMESTAMP '2024-01-01 00:00:00')) AS t(x)
        """
      Then query error .*(numeric|UNEXPECTED_INPUT_TYPE|TIMESTAMP).*

    Scenario: multiple ORDER BY expressions are rejected
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x, y) AS p
        FROM (VALUES (1, 1), (2, 2)) AS t(x, y)
        """
      Then query error (?i)within_group|one value

    Scenario: calendar INTERVAL ORDER BY is rejected
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY make_interval(0, 0, 0, x)) AS p
        FROM (VALUES (1), (2), (3)) AS t(x)
        """
      Then query error .*(numeric|ORDERING|INTERVAL).*

  Rule: ANSI day-time interval inputs return INTERVAL (Spark-compat)

    # Year-month intervals raise NOT_IMPLEMENTED in Spark itself, so only day-time
    # is testable. Stored as i64 internally and reconstructed to the interval type.
    Scenario: percentile_disc 0.5 on INTERVAL DAY returns an interval
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (INTERVAL '1' DAY), (INTERVAL '3' DAY), (INTERVAL '5' DAY)) AS t(x)
        """
      Then query result
        | p                                   |
        | INTERVAL '3 00:00:00' DAY TO SECOND |

    Scenario: percentile_disc with array of percentiles on INTERVAL DAY
      When query
        """
        SELECT percentile_disc(array(0.0, 0.5, 1.0)) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (INTERVAL '2' DAY), (INTERVAL '5' DAY), (INTERVAL '11' DAY)) AS t(x)
        """
      Then query result
        | p                                                                                                                |
        | [INTERVAL '2 00:00:00' DAY TO SECOND, INTERVAL '5 00:00:00' DAY TO SECOND, INTERVAL '11 00:00:00' DAY TO SECOND] |
