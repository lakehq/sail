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

    Scenario: percentile_disc 0.5 on DECIMAL(10,2)
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(x AS DECIMAL(10,2))) AS p
        FROM (VALUES (1.50), (2.50), (3.50), (4.50), (5.50)) AS t(x)
        """
      Then query result
        | p   |
        | 3.5 |

    Scenario: percentile_disc 0.5 on high-precision DECIMAL(38,10)
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(x AS DECIMAL(38,10))) AS p
        FROM (VALUES (1.0), (2.0), (3.0), (4.0), (5.0)) AS t(x)
        """
      Then query result
        | p   |
        | 3.0 |

    Scenario: percentile_disc 0.0 on DECIMAL returns minimum
      When query
        """
        SELECT percentile_disc(0.0) WITHIN GROUP (ORDER BY CAST(x AS DECIMAL(10,2))) AS p
        FROM (VALUES (5), (10), (15)) AS t(x)
        """
      Then query result
        | p   |
        | 5.0 |

    Scenario: percentile_disc 1.0 on DECIMAL returns maximum
      When query
        """
        SELECT percentile_disc(1.0) WITHIN GROUP (ORDER BY CAST(x AS DECIMAL(10,2))) AS p
        FROM (VALUES (5), (10), (15)) AS t(x)
        """
      Then query result
        | p    |
        | 15.0 |

    Scenario: percentile_disc 0.25 (first quartile) on DECIMAL
      When query
        """
        SELECT percentile_disc(0.25) WITHIN GROUP (ORDER BY CAST(x AS DECIMAL(10,2))) AS p
        FROM (VALUES (10), (20), (30), (40)) AS t(x)
        """
      Then query result
        | p    |
        | 10.0 |

    Scenario: percentile_disc 0.75 (third quartile) on DECIMAL
      When query
        """
        SELECT percentile_disc(0.75) WITHIN GROUP (ORDER BY CAST(x AS DECIMAL(10,2))) AS p
        FROM (VALUES (10), (20), (30), (40)) AS t(x)
        """
      Then query result
        | p    |
        | 30.0 |

  Rule: NULL handling matches Spark (NULLs ignored, all-NULL/empty → NULL)

    Scenario: percentile_disc ignores NULL DECIMAL values
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(x AS DECIMAL(10,2))) AS p
        FROM (VALUES (NULL), (1.0), (2.0), (3.0), (NULL)) AS t(x)
        """
      Then query result
        | p   |
        | 2.0 |

    Scenario: percentile_disc with all-NULL DECIMAL column returns NULL
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(NULL AS DECIMAL(10,2))) AS p
        FROM (VALUES (1), (2), (3)) AS t(x)
        """
      Then query result
        | p    |
        | NULL |

    Scenario: percentile_disc ignores NULL ORDER BY values (INT)
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (1), (CAST(NULL AS INT)), (3)) AS t(x)
        """
      Then query result
        | p   |
        | 1.0 |

    Scenario: percentile_disc on all-NULL INT column returns NULL
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (CAST(NULL AS INT)), (NULL), (NULL)) AS t(x)
        """
      Then query result
        | p    |
        | NULL |

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

    Scenario: percentile_disc 0.5 with DESC on DECIMAL
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(x AS DECIMAL(10,2)) DESC) AS p
        FROM (VALUES (1.0), (2.0), (3.0), (4.0), (5.0)) AS t(x)
        """
      Then query result
        | p   |
        | 3.0 |

    Scenario: percentile_disc 0.25 DESC selects from the high end
      When query
        """
        SELECT percentile_disc(0.25) WITHIN GROUP (ORDER BY x DESC) AS p
        FROM (VALUES (1), (2), (3), (4)) AS t(x)
        """
      Then query result
        | p   |
        | 4.0 |

    Scenario: percentile_disc 0.75 DESC selects from the low end
      When query
        """
        SELECT percentile_disc(0.75) WITHIN GROUP (ORDER BY x DESC) AS p
        FROM (VALUES (1), (2), (3), (4)) AS t(x)
        """
      Then query result
        | p   |
        | 2.0 |

    Scenario: percentile_disc 0.0 DESC returns the maximum
      When query
        """
        SELECT percentile_disc(0.0) WITHIN GROUP (ORDER BY x DESC) AS p
        FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)
        """
      Then query result
        | p   |
        | 5.0 |

    Scenario: percentile_disc 1.0 DESC returns the minimum
      When query
        """
        SELECT percentile_disc(1.0) WITHIN GROUP (ORDER BY x DESC) AS p
        FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)
        """
      Then query result
        | p   |
        | 1.0 |

  Rule: Single value, duplicates and even-count populations

    Scenario: percentile_disc on a single value
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (42)) AS t(x)
        """
      Then query result
        | p    |
        | 42.0 |

    Scenario: percentile_disc on all-duplicate input
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (5), (5), (5), (5)) AS t(x)
        """
      Then query result
        | p   |
        | 5.0 |

    Scenario: percentile_disc with even row count picks lower middle
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (1), (2), (3), (4)) AS t(x)
        """
      Then query result
        | p   |
        | 2.0 |

    Scenario: percentile_disc 0.5 with negative DECIMAL values
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY CAST(x AS DECIMAL(10,2))) AS p
        FROM (VALUES (-10.0), (-5.0), (0.0), (5.0), (10.0)) AS t(x)
        """
      Then query result
        | p   |
        | 0.0 |

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

    Scenario: percentile_disc with array of percentiles on DECIMAL
      When query
        """
        SELECT percentile_disc(array(0.25, 0.5, 0.75)) WITHIN GROUP (ORDER BY CAST(x AS DECIMAL(10,2))) AS p
        FROM (VALUES (10), (20), (30), (40)) AS t(x)
        """
      Then query result
        | p                  |
        | [10.0, 20.0, 30.0] |

    Scenario: percentile_disc with array of percentiles on INT
      When query
        """
        SELECT percentile_disc(array(0.25, 0.5, 0.75)) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)
        """
      Then query result
        | p               |
        | [2.0, 3.0, 4.0] |

    Scenario: percentile_disc with array on DOUBLE
      When query
        """
        SELECT percentile_disc(array(0.0, 0.5, 1.0)) WITHIN GROUP (ORDER BY CAST(x AS DOUBLE)) AS p
        FROM (VALUES (1.0), (2.0), (3.0), (4.0), (5.0)) AS t(x)
        """
      Then query result
        | p               |
        | [1.0, 3.0, 5.0] |

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

    Scenario: Empty percentile array returns NULL
      When query
        """
        SELECT percentile_disc(array()) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)
        """
      Then query result
        | p    |
        | NULL |

    Scenario: NULL element in percentile array is treated as 0.0
      When query
        """
        SELECT percentile_disc(array(CAST(NULL AS DOUBLE))) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (1), (2), (3), (4), (5)) AS t(x)
        """
      Then query result
        | p     |
        | [1.0] |

  Rule: Invalid arguments raise an error

    Scenario: Negative percentile is rejected
      When query
        """
        SELECT percentile_disc(-0.1) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (1), (2), (3)) AS t(x)
        """
      Then query error .*

    Scenario: Percentile greater than 1 is rejected
      When query
        """
        SELECT percentile_disc(1.1) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (1), (2), (3)) AS t(x)
        """
      Then query error .*

    Scenario: Out-of-range value inside percentile array is rejected
      When query
        """
        SELECT percentile_disc(array(0.5, 1.5)) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (1), (2), (3)) AS t(x)
        """
      Then query error .*

    Scenario: DISTINCT with WITHIN GROUP is rejected
      When query
        """
        SELECT percentile_disc(DISTINCT 0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (1), (2), (3), (2), (1)) AS t(x)
        """
      Then query error .*

    Scenario: BOOLEAN ORDER BY is rejected
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (true), (false)) AS t(x)
        """
      Then query error .*

    Scenario: DATE ORDER BY is rejected
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (DATE '2024-01-01'), (DATE '2024-01-02')) AS t(x)
        """
      Then query error .*

    Scenario: ARRAY ORDER BY is rejected
      When query
        """
        SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) AS p
        FROM (VALUES (array(1)), (array(2))) AS t(x)
        """
      Then query error .*
