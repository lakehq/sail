Feature: Window functions coverage

  # Base data: VALUES (grp, x, y) for all tests
  # grp=1: (1,10,1.0), (1,20,2.0), (1,30,3.0)
  # grp=2: (2,40,4.0), (2,50,5.0)

  Rule: Ranking window functions
    Scenario: row_number
      When query
        """
        SELECT x, ROW_NUMBER() OVER (ORDER BY x) AS rn
        FROM VALUES (10), (20), (30) AS t(x)
        """
      Then query result ordered
        | x  | rn |
        | 10 | 1  |
        | 20 | 2  |
        | 30 | 3  |

    Scenario: rank with ties
      When query
        """
        SELECT x, RANK() OVER (ORDER BY x) AS rnk
        FROM VALUES (10), (10), (20), (30) AS t(x)
        """
      Then query result ordered
        | x  | rnk |
        | 10 | 1   |
        | 10 | 1   |
        | 20 | 3   |
        | 30 | 4   |

    Scenario: dense_rank with ties
      When query
        """
        SELECT x, DENSE_RANK() OVER (ORDER BY x) AS drnk
        FROM VALUES (10), (10), (20), (30) AS t(x)
        """
      Then query result ordered
        | x  | drnk |
        | 10 | 1    |
        | 10 | 1    |
        | 20 | 2    |
        | 30 | 3    |

    Scenario: ntile
      When query
        """
        SELECT x, NTILE(2) OVER (ORDER BY x) AS tile
        FROM VALUES (10), (20), (30), (40) AS t(x)
        """
      Then query result ordered
        | x  | tile |
        | 10 | 1    |
        | 20 | 1    |
        | 30 | 2    |
        | 40 | 2    |

    Scenario: percent_rank
      When query
        """
        SELECT x, CAST(PERCENT_RANK() OVER (ORDER BY x) AS DECIMAL(3,2)) AS pr
        FROM VALUES (10), (20), (30), (40) AS t(x)
        """
      Then query result ordered
        | x  | pr   |
        | 10 | 0.00 |
        | 20 | 0.33 |
        | 30 | 0.67 |
        | 40 | 1.00 |

    Scenario: cume_dist
      When query
        """
        SELECT x, CAST(CUME_DIST() OVER (ORDER BY x) AS DECIMAL(4,2)) AS cd
        FROM VALUES (10), (20), (30), (40) AS t(x)
        """
      Then query result ordered
        | x  | cd   |
        | 10 | 0.25 |
        | 20 | 0.50 |
        | 30 | 0.75 |
        | 40 | 1.00 |

  Rule: Lead and lag
    Scenario: lead
      When query
        """
        SELECT x, LEAD(x) OVER (ORDER BY x) AS next_x
        FROM VALUES (10), (20), (30) AS t(x)
        """
      Then query result ordered
        | x  | next_x |
        | 10 | 20     |
        | 20 | 30     |
        | 30 | NULL   |

    Scenario: lead with offset and default
      When query
        """
        SELECT x, LEAD(x, 2, -1) OVER (ORDER BY x) AS next2
        FROM VALUES (10), (20), (30), (40) AS t(x)
        """
      Then query result ordered
        | x  | next2 |
        | 10 | 30    |
        | 20 | 40    |
        | 30 | -1    |
        | 40 | -1    |

    Scenario: lag
      When query
        """
        SELECT x, LAG(x) OVER (ORDER BY x) AS prev_x
        FROM VALUES (10), (20), (30) AS t(x)
        """
      Then query result ordered
        | x  | prev_x |
        | 10 | NULL   |
        | 20 | 10     |
        | 30 | 20     |

    Scenario: lag with offset and default
      When query
        """
        SELECT x, LAG(x, 2, -1) OVER (ORDER BY x) AS prev2
        FROM VALUES (10), (20), (30), (40) AS t(x)
        """
      Then query result ordered
        | x  | prev2 |
        | 10 | -1    |
        | 20 | -1    |
        | 30 | 10    |
        | 40 | 20    |

  Rule: nth_value
    Scenario: nth_value
      When query
        """
        SELECT x, NTH_VALUE(x, 2) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second
        FROM VALUES (10), (20), (30) AS t(x)
        """
      Then query result ordered
        | x  | second |
        | 10 | 20     |
        | 20 | 20     |
        | 30 | 20     |

  Rule: Basic aggregate window functions (unbounded)
    Scenario: count over window
      When query
        """
        SELECT x, COUNT(*) OVER (ORDER BY x) AS cnt
        FROM VALUES (10), (20), (30) AS t(x)
        """
      Then query result ordered
        | x  | cnt |
        | 10 | 1   |
        | 20 | 2   |
        | 30 | 3   |

    Scenario: sum over window
      When query
        """
        SELECT x, SUM(x) OVER (ORDER BY x) AS running_sum
        FROM VALUES (10), (20), (30) AS t(x)
        """
      Then query result ordered
        | x  | running_sum |
        | 10 | 10          |
        | 20 | 30          |
        | 30 | 60          |

    Scenario: avg over window
      When query
        """
        SELECT x, CAST(AVG(x) OVER (ORDER BY x) AS DECIMAL(10,1)) AS running_avg
        FROM VALUES (10), (20), (30) AS t(x)
        """
      Then query result ordered
        | x  | running_avg |
        | 10 | 10.0        |
        | 20 | 15.0        |
        | 30 | 20.0        |

    Scenario: min and max over window
      When query
        """
        SELECT x,
          MIN(x) OVER (ORDER BY x) AS running_min,
          MAX(x) OVER (ORDER BY x) AS running_max
        FROM VALUES (10), (20), (30) AS t(x)
        """
      Then query result ordered
        | x  | running_min | running_max |
        | 10 | 10          | 10          |
        | 20 | 10          | 20          |
        | 30 | 10          | 30          |

  Rule: Aggregate window functions with sliding frames
    Scenario: sum with sliding frame
      When query
        """
        SELECT x, SUM(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS slide_sum
        FROM VALUES (10), (20), (30), (40) AS t(x)
        """
      Then query result ordered
        | x  | slide_sum |
        | 10 | 10        |
        | 20 | 30        |
        | 30 | 50        |
        | 40 | 70        |

    Scenario: avg with sliding frame
      When query
        """
        SELECT x, CAST(AVG(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS DECIMAL(10,1)) AS slide_avg
        FROM VALUES (10), (20), (30), (40) AS t(x)
        """
      Then query result ordered
        | x  | slide_avg |
        | 10 | 10.0      |
        | 20 | 15.0      |
        | 30 | 25.0      |
        | 40 | 35.0      |

    Scenario: count with sliding frame
      When query
        """
        SELECT x, COUNT(*) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS slide_cnt
        FROM VALUES (10), (20), (30), (40) AS t(x)
        """
      Then query result ordered
        | x  | slide_cnt |
        | 10 | 1         |
        | 20 | 2         |
        | 30 | 2         |
        | 40 | 2         |

    Scenario: min with sliding frame
      When query
        """
        SELECT x, MIN(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS slide_min
        FROM VALUES (10), (20), (30), (40) AS t(x)
        """
      Then query result ordered
        | x  | slide_min |
        | 10 | 10        |
        | 20 | 10        |
        | 30 | 20        |
        | 40 | 30        |

    Scenario: max with sliding frame
      When query
        """
        SELECT x, MAX(x) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS slide_max
        FROM VALUES (10), (20), (30), (40) AS t(x)
        """
      Then query result ordered
        | x  | slide_max |
        | 10 | 10        |
        | 20 | 20        |
        | 30 | 30        |
        | 40 | 40        |

  Rule: Statistical window functions
    Scenario: stddev over window
      When query
        """
        SELECT x, CAST(STDDEV(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL(10,2)) AS sd
        FROM VALUES (10), (20), (30), (40) AS t(x)
        """
      Then query result ordered
        | x  | sd    |
        | 10 | NULL  |
        | 20 | 7.07  |
        | 30 | 10.00 |
        | 40 | 12.91 |

    Scenario: stddev_pop over window
      When query
        """
        SELECT x, CAST(STDDEV_POP(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL(10,2)) AS sdp
        FROM VALUES (10), (20), (30), (40) AS t(x)
        """
      Then query result ordered
        | x  | sdp   |
        | 10 | 0.00  |
        | 20 | 5.00  |
        | 30 | 8.16  |
        | 40 | 11.18 |

    Scenario: variance over window
      When query
        """
        SELECT x, CAST(VARIANCE(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL(10,2)) AS v
        FROM VALUES (10), (20), (30), (40) AS t(x)
        """
      Then query result ordered
        | x  | v      |
        | 10 | NULL   |
        | 20 | 50.00  |
        | 30 | 100.00 |
        | 40 | 166.67 |

    Scenario: var_pop over window
      When query
        """
        SELECT x, CAST(VAR_POP(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL(10,2)) AS vp
        FROM VALUES (10), (20), (30), (40) AS t(x)
        """
      Then query result ordered
        | x  | vp     |
        | 10 | 0.00   |
        | 20 | 25.00  |
        | 30 | 66.67  |
        | 40 | 125.00 |

    Scenario: corr over window
      When query
        """
        SELECT x, CAST(CORR(x, y) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL(3,1)) AS c
        FROM VALUES (10, 1.0), (20, 2.0), (30, 3.0), (40, 4.0) AS t(x, y)
        """
      Then query result ordered
        | x  | c   |
        | 10 | NULL |
        | 20 | 1.0  |
        | 30 | 1.0  |
        | 40 | 1.0  |

    Scenario: covar_pop over window
      When query
        """
        SELECT x, CAST(COVAR_POP(x, y) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL(10,2)) AS cp
        FROM VALUES (10, 1.0), (20, 2.0), (30, 3.0) AS t(x, y)
        """
      Then query result ordered
        | x  | cp    |
        | 10 | 0.00  |
        | 20 | 2.50  |
        | 30 | 6.67  |

    Scenario: covar_samp over window
      When query
        """
        SELECT x, CAST(COVAR_SAMP(x, y) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL(10,2)) AS cs
        FROM VALUES (10, 1.0), (20, 2.0), (30, 3.0) AS t(x, y)
        """
      Then query result ordered
        | x  | cs    |
        | 10 | NULL  |
        | 20 | 5.00  |
        | 30 | 10.00 |

  Rule: Boolean aggregate window functions
    Scenario: bool_and and bool_or over window
      When query
        """
        SELECT x,
          BOOL_AND(flag) OVER (ORDER BY x) AS b_and,
          BOOL_OR(flag) OVER (ORDER BY x) AS b_or
        FROM VALUES (1, true), (2, false), (3, true) AS t(x, flag)
        """
      Then query result ordered
        | x | b_and | b_or |
        | 1 | true  | true |
        | 2 | false | true |
        | 3 | false | true |

  Rule: Bit aggregate window functions
    Scenario: bit_and and bit_or over window
      When query
        """
        SELECT x,
          BIT_AND(v) OVER (ORDER BY x) AS ba,
          BIT_OR(v) OVER (ORDER BY x) AS bo,
          BIT_XOR(v) OVER (ORDER BY x) AS bx
        FROM VALUES (1, 7), (2, 3), (3, 5) AS t(x, v)
        """
      Then query result ordered
        | x | ba | bo | bx |
        | 1 | 7  | 7  | 7  |
        | 2 | 3  | 7  | 4  |
        | 3 | 1  | 7  | 1  |

  Rule: Collection window functions
    Scenario: collect_list over window
      When query
        """
        SELECT x, COLLECT_LIST(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lst
        FROM VALUES (10), (20), (30) AS t(x)
        """
      Then query result ordered
        | x  | lst          |
        | 10 | [10]         |
        | 20 | [10, 20]     |
        | 30 | [10, 20, 30] |

    Scenario: collect_set over window
      When query
        """
        SELECT x, SORT_ARRAY(COLLECT_SET(v) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS st
        FROM VALUES (1, 'a'), (2, 'b'), (3, 'a') AS t(x, v)
        """
      Then query result ordered
        | x | st         |
        | 1 | [a]        |
        | 2 | [a, b]     |
        | 3 | [a, b]     |

  Rule: Partitioned window functions
    Scenario: sum with partition by
      When query
        """
        SELECT grp, x, SUM(x) OVER (PARTITION BY grp ORDER BY x) AS psum
        FROM VALUES (1,10), (1,20), (1,30), (2,40), (2,50) AS t(grp, x)
        ORDER BY grp, x
        """
      Then query result ordered
        | grp | x  | psum |
        | 1   | 10 | 10   |
        | 1   | 20 | 30   |
        | 1   | 30 | 60   |
        | 2   | 40 | 40   |
        | 2   | 50 | 90   |

    Scenario: row_number with partition by
      When query
        """
        SELECT grp, x, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY x) AS rn
        FROM VALUES (1,10), (1,20), (1,30), (2,40), (2,50) AS t(grp, x)
        ORDER BY grp, x
        """
      Then query result ordered
        | grp | x  | rn |
        | 1   | 10 | 1  |
        | 1   | 20 | 2  |
        | 1   | 30 | 3  |
        | 2   | 40 | 1  |
        | 2   | 50 | 2  |

  Rule: Skewness and kurtosis
    Scenario: skewness over window
      When query
        """
        SELECT x, CAST(SKEWNESS(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL(10,2)) AS sk
        FROM VALUES (10), (20), (30), (40), (50) AS t(x)
        """
      Then query result ordered
        | x  | sk   |
        | 10 | NULL |
        | 20 | 0.00 |
        | 30 | 0.00 |
        | 40 | 0.00 |
        | 50 | 0.00 |

    Scenario: kurtosis over window
      When query
        """
        SELECT x, CAST(KURTOSIS(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL(10,2)) AS kt
        FROM VALUES (10), (20), (30), (40), (50) AS t(x)
        """
      Then query result ordered
        | x  | kt    |
        | 10 | NULL  |
        | 20 | -2.00 |
        | 30 | -1.50 |
        | 40 | -1.36 |
        | 50 | -1.30 |

  Rule: count_if window function
    Scenario: count_if over window
      When query
        """
        SELECT x, COUNT_IF(x > 15) OVER (ORDER BY x) AS ci
        FROM VALUES (10), (20), (30) AS t(x)
        """
      Then query result ordered
        | x  | ci |
        | 10 | 0  |
        | 20 | 1  |
        | 30 | 2  |

  Rule: min_by and max_by
    Scenario: max_by over window
      When query
        """
        SELECT x, MAX_BY(label, x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS mb
        FROM VALUES (10, 'a'), (30, 'c'), (20, 'b') AS t(x, label)
        """
      Then query result ordered
        | x  | mb |
        | 10 | a  |
        | 20 | b  |
        | 30 | c  |

    Scenario: min_by over window
      When query
        """
        SELECT x, MIN_BY(label, x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS mb
        FROM VALUES (10, 'a'), (30, 'c'), (20, 'b') AS t(x, label)
        """
      Then query result ordered
        | x  | mb |
        | 10 | a  |
        | 20 | a  |
        | 30 | a  |

  Rule: try_sum and try_avg
    Scenario: try_sum over window
      When query
        """
        SELECT x, TRY_SUM(x) OVER (ORDER BY x) AS ts
        FROM VALUES (10), (20), (30) AS t(x)
        """
      Then query result ordered
        | x  | ts |
        | 10 | 10 |
        | 20 | 30 |
        | 30 | 60 |

    Scenario: try_avg over window
      When query
        """
        SELECT x, CAST(TRY_AVG(x) OVER (ORDER BY x) AS DECIMAL(10,1)) AS ta
        FROM VALUES (10), (20), (30) AS t(x)
        """
      Then query result ordered
        | x  | ta   |
        | 10 | 10.0 |
        | 20 | 15.0 |
        | 30 | 20.0 |

  Rule: Regression window functions
    Scenario: regr_count over window
      When query
        """
        SELECT x, REGR_COUNT(y, x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rc
        FROM VALUES (10, 1.0), (20, 2.0), (30, 3.0) AS t(x, y)
        """
      Then query result ordered
        | x  | rc |
        | 10 | 1  |
        | 20 | 2  |
        | 30 | 3  |

    Scenario: regr_slope over window
      When query
        """
        SELECT x, CAST(REGR_SLOPE(y, x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS DECIMAL(10,2)) AS rs
        FROM VALUES (10, 1.0), (20, 2.0), (30, 3.0) AS t(x, y)
        """
      Then query result ordered
        | x  | rs   |
        | 10 | NULL |
        | 20 | 0.10 |
        | 30 | 0.10 |

  Rule: approx_count_distinct
    Scenario: approx_count_distinct over window
      When query
        """
        SELECT x, APPROX_COUNT_DISTINCT(v) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acd
        FROM VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'c') AS t(x, v)
        """
      Then query result ordered
        | x | acd |
        | 1 | 1   |
        | 2 | 2   |
        | 3 | 2   |
        | 4 | 3   |
