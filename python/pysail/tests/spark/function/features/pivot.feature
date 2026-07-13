@pivot
Feature: PIVOT rotates rows into columns

  Pivot groups by the remaining (or explicitly grouped) columns and produces one
  output column per pivot value, computing each aggregate only over the rows
  matching that value (Spark semantics: aggregate FILTER per value).

  Rule: Single aggregate with implicit grouping

    Scenario: pivot sum of earnings by course, grouped by year
      When query
        """
        SELECT * FROM (
          SELECT year, course, earnings FROM VALUES
            (2012, 'dotNET', 10000),
            (2012, 'Java', 20000),
            (2013, 'dotNET', 48000),
            (2013, 'Java', 30000)
          AS courseSales(year, course, earnings)
        ) PIVOT (
          sum(earnings) FOR (course) IN ('dotNET', 'Java')
        )
        """
      Then query result
        | year | dotNET | Java  |
        | 2012 | 10000  | 20000 |
        | 2013 | 48000  | 30000 |

    Scenario: pivot value absent from data yields NULL column
      When query
        """
        SELECT * FROM (
          SELECT year, course, earnings FROM VALUES
            (2012, 'dotNET', 10000),
            (2013, 'Java', 30000)
          AS courseSales(year, course, earnings)
        ) PIVOT (
          sum(earnings) FOR (course) IN ('dotNET', 'Java', 'Scala')
        )
        """
      Then query result
        | year | dotNET | Java  | Scala |
        | 2012 | 10000  | NULL  | NULL  |
        | 2013 | NULL   | 30000 | NULL  |

  Rule: Multiple aggregates name columns value_aggName

    Scenario: pivot with sum and avg aggregates
      When query
        """
        SELECT * FROM (
          SELECT year, course, earnings FROM VALUES
            (2012, 'dotNET', 10000),
            (2012, 'dotNET', 5000),
            (2012, 'Java', 20000),
            (2013, 'Java', 30000)
          AS courseSales(year, course, earnings)
        ) PIVOT (
          sum(earnings) AS s, avg(earnings) AS a
          FOR (course) IN ('dotNET', 'Java')
        )
        """
      Then query result
        | year | dotNET_s | dotNET_a | Java_s | Java_a  |
        | 2012 | 15000    | 7500.0   | 20000  | 20000.0 |
        | 2013 | NULL     | NULL     | 30000  | 30000.0 |

  Rule: Aliased pivot values

    Scenario: pivot value aliases rename output columns
      When query
        """
        SELECT * FROM (
          SELECT year, course, earnings FROM VALUES
            (2012, 'dotNET', 10000),
            (2012, 'Java', 20000)
          AS courseSales(year, course, earnings)
        ) PIVOT (
          sum(earnings) FOR (course) IN ('dotNET' AS net, 'Java' AS jvm)
        )
        """
      Then query result
        | year | net   | jvm   |
        | 2012 | 10000 | 20000 |

  Rule: Numeric pivot values

    Scenario: pivot on an integer column
      When query
        """
        SELECT * FROM (
          SELECT name, year, earnings FROM VALUES
            ('Alice', 2012, 10000),
            ('Alice', 2013, 20000),
            ('Bob', 2012, 30000)
          AS sales(name, year, earnings)
        ) PIVOT (
          sum(earnings) FOR (year) IN (2012, 2013)
        )
        """
      Then query result
        | name  | 2012  | 2013  |
        | Alice | 10000 | 20000 |
        | Bob   | 30000 | NULL  |

  Rule: A non-aggregate pivot expression is rejected

    Scenario: pivot expression without an aggregate function raises AnalysisException
      When query
        """
        SELECT * FROM (
          SELECT year, course, earnings FROM VALUES
            (2012, 'Java', 20000)
          AS courseSales(year, course, earnings)
        ) PIVOT (
          earnings FOR (course) IN ('Java')
        )
        """
      Then query error Aggregate expression required for pivot

  Rule: Multiple grouping columns

    Scenario: pivot keeps all non-pivot, non-aggregate columns as grouping
      When query
        """
        SELECT * FROM (
          SELECT region, year, course, earnings FROM VALUES
            ('US', 2012, 'dotNET', 10000),
            ('US', 2012, 'Java', 20000),
            ('EU', 2012, 'dotNET', 5000)
          AS sales(region, year, course, earnings)
        ) PIVOT (
          sum(earnings) FOR (course) IN ('dotNET', 'Java')
        )
        """
      Then query result
        | region | year | dotNET | Java  |
        | EU     | 2012 | 5000   | NULL  |
        | US     | 2012 | 10000  | 20000 |

  Rule: Explicit NULL pivot value

    Scenario: pivot on an explicit NULL value names the column null and matches NULL rows
      When query
        """
        SELECT * FROM (
          SELECT year, course, earnings FROM VALUES
            (2012, 'Java', 20000),
            (2012, CAST(NULL AS STRING), 5000),
            (2013, 'Java', 30000)
          AS courseSales(year, course, earnings)
        ) PIVOT (
          sum(earnings) FOR (course) IN (NULL, 'Java')
        )
        """
      Then query result
        | year | null | Java  |
        | 2012 | 5000 | 20000 |
        | 2013 | NULL | 30000 |

  Rule: Count aggregate fills absent combinations with NULL, not 0

    # An absent (group, pivot-value) combination is NULL even for count: on the PivotFirst
    # path Spark only computes the aggregate over (group, value) pairs that occur in the
    # data, so 2013/dotNET is NULL, not 0. Each cell is gated on whether any row matched the
    # pivot value, so a group with no matching rows yields NULL.
    Scenario: pivot count of rows per course (absent combo is NULL, not 0)
      When query
        """
        SELECT * FROM (
          SELECT year, course FROM VALUES
            (2012, 'dotNET'),
            (2012, 'dotNET'),
            (2012, 'Java'),
            (2013, 'Java')
          AS courseSales(year, course)
        ) PIVOT (
          count(1) FOR (course) IN ('dotNET', 'Java')
        )
        """
      Then query result
        | year | dotNET | Java |
        | 2012 | 2      | 1    |
        | 2013 | NULL   | 1    |

    # The flip side that pins the semantics: when the (group, value) pair DOES occur but the
    # counted column is all-NULL, count is 0 (count ignores NULLs) — so 2012/dotNET is 0
    # while the absent 2013/dotNET is NULL. The gate keeps the 0 (rows matched) and turns
    # only the truly-absent cell into NULL.
    Scenario: count over an existing all-NULL group is 0 while an absent group is NULL
      When query
        """
        SELECT * FROM (
          SELECT year, course, earnings FROM VALUES
            (2012, 'dotNET', CAST(NULL AS INT)),
            (2012, 'Java', 20000),
            (2013, 'Java', 30000)
          AS courseSales(year, course, earnings)
        ) PIVOT (
          count(earnings) FOR (course) IN ('dotNET', 'Java')
        )
        """
      Then query result
        | year | dotNET | Java |
        | 2012 | 0      | 1    |
        | 2013 | NULL   | 1    |

    # count(DISTINCT) is the same counting family: absent combo is NULL, not 0.
    Scenario: pivot count(DISTINCT) leaves absent combinations NULL
      When query
        """
        SELECT * FROM (
          SELECT year, course, earnings FROM VALUES
            (2012, 'dotNET', 10),
            (2012, 'dotNET', 10),
            (2012, 'Java', 2),
            (2013, 'Java', 3)
          AS courseSales(year, course, earnings)
        ) PIVOT (
          count(DISTINCT earnings) FOR (course) IN ('dotNET', 'Java')
        )
        """
      Then query result
        | year | dotNET | Java |
        | 2012 | 1      | 1    |
        | 2013 | NULL   | 1    |

    # approx_count_distinct returns 0 on empty too, so it must also be NULL on an absent
    # combo (PivotFirst path).
    Scenario: pivot approx_count_distinct leaves absent combinations NULL
      When query
        """
        SELECT * FROM (
          SELECT year, course, earnings FROM VALUES
            (2012, 'dotNET', 10),
            (2012, 'dotNET', 20),
            (2012, 'Java', 2),
            (2013, 'Java', 3)
          AS courseSales(year, course, earnings)
        ) PIVOT (
          approx_count_distinct(earnings) FOR (course) IN ('dotNET', 'Java')
        )
        """
      Then query result
        | year | dotNET | Java |
        | 2012 | 2      | 1    |
        | 2013 | NULL   | 1    |

    # Mixing count with a non-PivotFirst aggregate (a string max) forces Spark's general
    # path, where count's absent combo is 0 (not NULL). The NULL gate only applies on the
    # PivotFirst path, so the general-path count must keep 0 here (regression guard for the
    # fix): 2013/dotNET_c is 0 while the string max for the same absent cell is NULL.
    Scenario: count on the general path keeps 0 for absent combos
      When query
        """
        SELECT * FROM (
          SELECT year, course, earnings, note FROM VALUES
            (2012, 'dotNET', 10, 'p'),
            (2012, 'dotNET', 20, 'q'),
            (2012, 'Java', 2, 'r'),
            (2013, 'Java', 3, 's')
          AS courseSales(year, course, earnings, note)
        ) PIVOT (
          count(earnings) AS c, max(note) AS m
          FOR (course) IN ('dotNET', 'Java')
        )
        """
      Then query result
        | year | dotNET_c | dotNET_m | Java_c | Java_m |
        | 2012 | 2        | q        | 1      | r      |
        | 2013 | 0        | NULL     | 1      | s      |

  Rule: collect_list/collect_set fill absent combinations with an empty array

    # Spark's general (non-PivotFirst) path computes collect_list over the existing group,
    # ignoring NULLs, so an absent (group, value) combination is an empty array [], not NULL.
    # collect_list/collect_set coalesce their empty aggregate to [] (see collect_list.feature),
    # so this matches Spark.
    Scenario: pivot collect_list fills an absent combination with an empty array
      When query
        """
        SELECT * FROM (
          SELECT year, course, earnings FROM VALUES
            (2012, 'dotNET', 10),
            (2012, 'Java', 2),
            (2013, 'Java', 3)
          AS courseSales(year, course, earnings)
        ) PIVOT (
          collect_list(earnings) FOR (course) IN ('dotNET', 'Java')
        )
        """
      Then query result
        | year | dotNET | Java |
        | 2012 | [10]   | [2]  |
        | 2013 | []     | [3]  |
