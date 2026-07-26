@collect_list @collect_set
Feature: collect_list / collect_set

  collect_list gathers all values of a group into an array (order and duplicates preserved);
  collect_set gathers the distinct values. Both ignore NULLs and return an empty array `[]`
  for a group with no (non-NULL) values.

  Sail maps both onto DataFusion's array_agg (sail-plan `aggregate.rs`: `array_agg_compacted`
  for collect_list, `collect_set` for collect_set). DataFusion's array_agg returns NULL over
  zero rows, so the empty case is coalesced to a typed empty array to match Spark.

  Rule: collect_list collects values of any type, preserving order and duplicates

    Scenario Outline: collect_list over <case>
      When query
        """
        SELECT collect_list(v) AS r FROM VALUES <values> AS t(v)
        """
      Then query result
        | r   |
        | <r> |

      Examples:
        | case                                     | values                                                       | r                        |
        | integers keep input order and duplicates | (1), (2), (1)                                                | [1, 2, 1]                |
        | strings                                  | ('a'), ('b'), ('a')                                          | [a, b, a]                |
        | doubles                                  | (1.5D), (2.5D)                                               | [1.5, 2.5]               |
        | decimals keep their scale                | (CAST(1.50 AS DECIMAL(5, 2))), (CAST(2.00 AS DECIMAL(5, 2))) | [1.50, 2.00]             |
        | booleans                                 | (true), (false), (true)                                      | [true, false, true]      |
        | dates                                    | (DATE'2024-01-01'), (DATE'2024-02-02')                       | [2024-01-01, 2024-02-02] |
        | array elements                           | (array(1, 2)), (array(3))                                    | [[1, 2], [3]]            |
        | struct elements                          | (named_struct('a', 1)), (named_struct('a', 2))               | [{1}, {2}]               |

    # collect_list accepts map elements (unlike collect_set, which requires orderable elements).
    Scenario: map elements
      When query
        """
        SELECT collect_list(v) AS r FROM VALUES (map('a', 1)) AS t(v)
        """
      Then query result
        | r          |
        | [{a -> 1}] |

  Rule: NULLs are ignored

    Scenario: collect_list drops NULLs and keeps the non-NULL values
      When query
        """
        SELECT collect_list(v) AS r FROM VALUES
          (1), (CAST(NULL AS INT)), (2), (CAST(NULL AS INT))
        AS t(v)
        """
      Then query result
        | r      |
        | [1, 2] |

  Rule: An empty or all-NULL group collects to an empty array, not NULL

    Scenario Outline: <fn> over an all-NULL group
      When query
        """
        SELECT <fn>(v) AS r FROM VALUES
          (CAST(NULL AS INT)), (CAST(NULL AS INT))
        AS t(v)
        """
      Then query result
        | r  |
        | [] |

      Examples:
        | fn           |
        | collect_list |
        | collect_set  |

    Scenario: collect_list whose FILTER removes every row
      When query
        """
        SELECT collect_list(v) FILTER (WHERE v > 100) AS r FROM VALUES (1), (2) AS t(v)
        """
      Then query result
        | r  |
        | [] |

    Scenario: an all-NULL group is an empty array while populated groups keep their values
      When query
        """
        SELECT g, collect_list(v) AS r FROM VALUES
          ('a', 1),
          ('a', CAST(NULL AS INT)),
          ('b', CAST(NULL AS INT))
        AS t(g, v) GROUP BY g
        """
      Then query result
        | g | r   |
        | a | [1] |
        | b | []  |

    # The same empty-collection case as it surfaces through PIVOT: the absent (2013, dotNET)
    # cell collects to [].
    Scenario: collect_set in a pivot fills an absent combination with an empty array
      When query
        """
        SELECT * FROM (
          SELECT year, course, earnings FROM VALUES
            (2012, 'dotNET', 10),
            (2012, 'Java', 2),
            (2013, 'Java', 3)
          AS courseSales(year, course, earnings)
        ) PIVOT (
          collect_set(earnings) FOR (course) IN ('dotNET', 'Java')
        )
        """
      Then query result
        | year | dotNET | Java |
        | 2012 | [10]   | [2]  |
        | 2013 | []     | [3]  |

  Rule: collect_set returns the distinct values, ignoring NULLs

    Scenario Outline: Collect_set returns the distinct values, ignoring NULLs: <case>
      When query
        """
        SELECT sort_array(<expr>) AS r FROM VALUES <values> AS t(v)
        """
      Then query result
        | r   |
        | <r> |

      Examples:
        | case                                          | expr                     | values                        | r         |
        | collect_set removes duplicates                | collect_set(v)           | (1), (2), (1), (3)            | [1, 2, 3] |
        | collect_set drops NULLs                       | collect_set(v)           | (1), (CAST(NULL AS INT)), (1) | [1]       |
        | collect_list with DISTINCT removes duplicates | collect_list(DISTINCT v) | (1), (2), (1)                 | [1, 2]    |

  Rule: collect_set float equality diverges from Spark

    # Spark's collect_set never treats two NaNs as equal, so duplicate NaNs are all kept.
    # Sail (DataFusion's distinct array_agg) deduplicates NaN, dropping the repeat -> divergence.
    @sail-bug
    Scenario: collect_set keeps duplicate NaN values
      When query
        """
        SELECT sort_array(collect_set(v)) AS r FROM VALUES
          (CAST('NaN' AS DOUBLE)), (CAST('NaN' AS DOUBLE)), (1.0D)
        AS t(v)
        """
      Then query result
        | r               |
        | [1.0, NaN, NaN] |

    # Spark's collect_set treats -0.0 and 0.0 as equal and keeps a single 0.0. Sail keeps both
    # -> divergence.
    @sail-bug
    Scenario: collect_set deduplicates -0.0 and 0.0 to a single 0.0
      When query
        """
        SELECT sort_array(collect_set(v)) AS r FROM VALUES
          (CAST('0.0' AS DOUBLE)), (CAST('-0.0' AS DOUBLE))
        AS t(v)
        """
      Then query result
        | r     |
        | [0.0] |

  Rule: collect_set requires orderable element types

    # Spark rejects collect_set over MAP (unorderable) at analysis time. Sail accepts it and
    # returns a result -> divergence (Sail is too lenient).
    @sail-bug
    Scenario: collect_set over a map raises an analysis error
      When query
        """
        SELECT collect_set(v) AS r FROM VALUES (map('a', 1)) AS t(v)
        """
      Then query error .*

  Rule: FILTER restricts the collected rows

    Scenario: collect_list with FILTER WHERE collects only matching rows
      When query
        """
        SELECT collect_list(v) FILTER (WHERE v > 1) AS r FROM VALUES (1), (2), (3) AS t(v)
        """
      Then query result
        | r      |
        | [2, 3] |
