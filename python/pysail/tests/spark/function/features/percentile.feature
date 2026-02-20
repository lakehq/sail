Feature: percentile() aggregate function computes percentiles

  Rule: Basic percentile calculation

    Scenario: percentile 0.3 with two values
      When query
      """
      SELECT percentile(x, 0.3) AS p30 FROM (VALUES (0), (10)) AS t(x)
      """
      Then query result
      | p30 |
      | 3.0 |

    Scenario: percentile 0.5 (median) with even number of values
      When query
      """
      SELECT percentile(x, 0.5) AS median FROM (VALUES (0), (1), (2), (3)) AS t(x)
      """
      Then query result
      | median |
      | 1.5    |

    Scenario: percentile 0.5 with odd number of values
      When query
      """
      SELECT percentile(x, 0.5) AS median FROM (VALUES (0), (1), (2), (3), (4)) AS t(x)
      """
      Then query result
      | median |
      | 2.0    |

    Scenario: percentile 0.0 returns minimum
      When query
      """
      SELECT percentile(x, 0.0) AS min_val FROM (VALUES (5), (10), (15)) AS t(x)
      """
      Then query result
      | min_val |
      | 5.0     |

    Scenario: percentile 1.0 returns maximum
      When query
      """
      SELECT percentile(x, 1.0) AS max_val FROM (VALUES (5), (10), (15)) AS t(x)
      """
      Then query result
      | max_val |
      | 15.0    |

  Rule: Quartile calculations

    Scenario: percentile 0.25 (first quartile)
      When query
      """
      SELECT percentile(x, 0.25) AS q1 FROM (VALUES (0), (1), (2), (3), (4), (5), (6), (7)) AS t(x)
      """
      Then query result
      | q1   |
      | 1.75 |

    Scenario: percentile 0.75 (third quartile)
      When query
      """
      SELECT percentile(x, 0.75) AS q3 FROM (VALUES (0), (1), (2), (3), (4), (5), (6), (7)) AS t(x)
      """
      Then query result
      | q3   |
      | 5.25 |

    Scenario: multiple percentiles
      When query
      """
      SELECT
        percentile(x, 0.25) AS q1,
        percentile(x, 0.50) AS q2,
        percentile(x, 0.75) AS q3
      FROM (SELECT id AS x FROM range(100)) AS t
      """
      Then query result
      | q1    | q2   | q3    |
      | 24.75 | 49.5 | 74.25 |

  Rule: NULL handling

    Scenario: percentile ignores NULLs
      When query
      """
      SELECT percentile(x, 0.5) AS median FROM (VALUES (NULL), (1), (2), (3), (NULL)) AS t(x)
      """
      Then query result
      | median |
      | 2.0    |

    Scenario: percentile with all NULLs returns NULL
      When query
      """
      SELECT percentile(x, 0.5) AS median FROM (VALUES (CAST(NULL AS INT)), (NULL), (NULL)) AS t(x)
      """
      Then query result
      | median |
      | NULL   |

    Scenario: percentile on empty dataset returns NULL
      When query
      """
      SELECT percentile(x, 0.5) AS median FROM (SELECT 1 AS x WHERE false) AS t
      """
      Then query result
      | median |
      | NULL   |

  Rule: Different numeric types

    Scenario: percentile with negative values
      When query
      """
      SELECT percentile(x, 0.5) AS median FROM (VALUES (-10), (-5), (0), (5), (10)) AS t(x)
      """
      Then query result
      | median |
      | 0.0    |

    Scenario: percentile with single value
      When query
      """
      SELECT percentile(x, 0.5) AS median FROM (VALUES (42)) AS t(x)
      """
      Then query result
      | median |
      | 42.0   |

    Scenario: percentile with float values
      When query
      """
      SELECT percentile(x, 0.5) AS median FROM (VALUES (0.0), (1.0), (2.5), (3.5), (5.0), (6.0), (7.5), (8.5)) AS t(x)
      """
      Then query result
      | median |
      | 4.25   |

    Scenario: percentile with duplicates
      When query
      """
      SELECT percentile(x, 0.5) AS median FROM (VALUES (1), (2), (2), (2), (3)) AS t(x)
      """
      Then query result
      | median |
      | 2.0    |

  Rule: Group by support

    Scenario: percentile with group by
      When query
      """
      SELECT grp, percentile(value, 0.5) AS median
      FROM (VALUES ('A', 1), ('A', 2), ('A', 3), ('B', 10), ('B', 20), ('B', 30)) AS t(grp, value)
      GROUP BY grp
      ORDER BY grp
      """
      Then query result ordered
      | grp | median |
      | A   | 2.0    |
      | B   | 20.0   |

  Rule: Comparison with min/max

    Scenario: percentile 0 and 1 match min and max
      When query
      """
      SELECT
        min(x) AS min_value,
        percentile(x, 0.0) AS p_0,
        percentile(x, 0.5) AS p_50,
        percentile(x, 1.0) AS p_100,
        max(x) AS max_value
      FROM (VALUES (0), (1), (2), (3)) AS t(x)
      """
      Then query result
      | min_value | p_0 | p_50 | p_100 | max_value |
      | 0         | 0.0 | 1.5  | 3.0   | 3         |

  Rule: Array of percentiles

    Scenario: percentile with array of percentile values
      When query
      """
      SELECT percentile(col, array(0.25, 0.5, 0.75)) AS percentiles FROM (VALUES (0), (1), (2), (3), (4)) AS t(col)
      """
      Then query result
      | percentiles     |
      | [1.0, 2.0, 3.0] |

    Scenario: percentile with full array of percentile values
      When query
      """
      SELECT percentile(x, array(0.0, 0.25, 0.5, 0.75, 1.0)) AS percentiles FROM (VALUES (0), (10)) AS tab(x)
      """
      Then query result
      | percentiles                |
      | [0.0, 2.5, 5.0, 7.5, 10.0] |
