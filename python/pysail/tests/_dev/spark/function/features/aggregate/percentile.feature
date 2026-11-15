Feature: percentile() aggregate function computes percentiles

  Rule: Basic percentile calculation

    # The column alias varies per case, so it drives both the query and the
    # expected header and the auto-derived name stays asserted.
    Scenario Outline: percentile <p> <case>
      When query
        """
        SELECT percentile(x, <p>) AS <alias> FROM (VALUES <values>) AS t(x)
        """
      Then query result
        | <alias>  |
        | <result> |

      Examples:
        | p   | case                       | alias   | values                  | result |
        | 0.3 | with two values            | p30     | (0), (10)               | 3.0    |
        | 0.5 | with even number of values | median  | (0), (1), (2), (3)      | 1.5    |
        | 0.5 | with odd number of values  | median  | (0), (1), (2), (3), (4) | 2.0    |
        | 0.0 | returns minimum            | min_val | (5), (10), (15)         | 5.0    |
        | 1.0 | returns maximum            | max_val | (5), (10), (15)         | 15.0   |

  Rule: Quartile calculations

    Scenario Outline: percentile <p> (<case> quartile)
      When query
        """
        SELECT percentile(x, <p>) AS <alias> FROM (VALUES (0), (1), (2), (3), (4), (5), (6), (7)) AS t(x)
        """
      Then query result
        | <alias>  |
        | <result> |

      Examples:
        | p    | case  | alias | result |
        | 0.25 | first | q1    | 1.75   |
        | 0.75 | third | q3    | 5.25   |

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

    Scenario Outline: percentile <case>
      When query
        """
        SELECT percentile(x, 0.5) AS median FROM (VALUES <values>) AS t(x)
        """
      Then query result
        | median   |
        | <median> |

      Examples:
        | case                        | values                              | median |
        | ignores NULLs               | (NULL), (1), (2), (3), (NULL)       | 2.0    |
        | with all NULLs returns NULL | (CAST(NULL AS INT)), (NULL), (NULL) | NULL   |

    Scenario: percentile on empty dataset returns NULL
      When query
        """
        SELECT percentile(x, 0.5) AS median FROM (SELECT 1 AS x WHERE false) AS t
        """
      Then query result
        | median |
        | NULL   |

  Rule: Different numeric types

    Scenario Outline: percentile with <case>
      When query
        """
        SELECT percentile(x, 0.5) AS median FROM (VALUES <values>) AS t(x)
        """
      Then query result
        | median   |
        | <median> |

      Examples:
        | case            | values                                                 | median |
        | negative values | (-10), (-5), (0), (5), (10)                            | 0.0    |
        | single value    | (42)                                                   | 42.0   |
        | float values    | (0.0), (1.0), (2.5), (3.5), (5.0), (6.0), (7.5), (8.5) | 4.25   |
        | duplicates      | (1), (2), (2), (2), (3)                                | 2.0    |

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
