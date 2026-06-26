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

  Rule: Count aggregate

    Scenario: pivot count of rows per course
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
        | 2013 | 0      | 1    |
