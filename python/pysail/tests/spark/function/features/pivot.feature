Feature: PIVOT rotates rows into columns with aggregation

  Rule: Basic pivot with explicit values

    Scenario: pivot sum with explicit values
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW courses AS
        SELECT * FROM VALUES
          ('dotNET', 2012, 10000),
          ('Java', 2012, 20000),
          ('dotNET', 2012, 5000),
          ('dotNET', 2013, 48000),
          ('Java', 2013, 30000)
        AS t(course, year, earnings)
        """
      When query
        """
        SELECT * FROM courses
        PIVOT (
          SUM(earnings)
          FOR (course) IN ('dotNET', 'Java')
        )
        ORDER BY year
        """
      Then query result ordered
        | year | dotNET | Java  |
        | 2012 | 15000  | 20000 |
        | 2013 | 48000  | 30000 |

    Scenario: pivot sum with reordered explicit values
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW courses AS
        SELECT * FROM VALUES
          ('dotNET', 2012, 10000),
          ('Java', 2012, 20000),
          ('dotNET', 2012, 5000),
          ('dotNET', 2013, 48000),
          ('Java', 2013, 30000)
        AS t(course, year, earnings)
        """
      When query
        """
        SELECT * FROM courses
        PIVOT (
          SUM(earnings)
          FOR (course) IN ('Java', 'dotNET')
        )
        ORDER BY year
        """
      Then query result ordered
        | year | Java  | dotNET |
        | 2012 | 20000 | 15000  |
        | 2013 | 30000 | 48000  |

  Rule: Pivot with aliased values

    Scenario: pivot with aliases for pivot values
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW courses AS
        SELECT * FROM VALUES
          ('dotNET', 2012, 10000),
          ('Java', 2012, 20000),
          ('dotNET', 2012, 5000),
          ('dotNET', 2013, 48000),
          ('Java', 2013, 30000)
        AS t(course, year, earnings)
        """
      When query
        """
        SELECT * FROM courses
        PIVOT (
          SUM(earnings)
          FOR (course) IN ('dotNET' AS net, 'Java' AS java)
        )
        ORDER BY year
        """
      Then query result ordered
        | year | net   | java  |
        | 2012 | 15000 | 20000 |
        | 2013 | 48000 | 30000 |

  Rule: Pivot with multiple aggregate functions

    Scenario: pivot with sum and count
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW courses AS
        SELECT * FROM VALUES
          ('dotNET', 2012, 10000),
          ('Java', 2012, 20000),
          ('dotNET', 2012, 5000),
          ('dotNET', 2013, 48000),
          ('Java', 2013, 30000)
        AS t(course, year, earnings)
        """
      When query
        """
        SELECT * FROM courses
        PIVOT (
          SUM(earnings) AS total, COUNT(earnings) AS cnt
          FOR (course) IN ('dotNET', 'Java')
        )
        ORDER BY year
        """
      Then query result ordered
        | year | dotNET_total | dotNET_cnt | Java_total | Java_cnt |
        | 2012 | 15000        | 2          | 20000      | 1        |
        | 2013 | 48000        | 1          | 30000      | 1        |

  Rule: Pivot with integer pivot column

    Scenario: pivot on integer column
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW courses AS
        SELECT * FROM VALUES
          ('dotNET', 2012, 10000),
          ('Java', 2012, 20000),
          ('dotNET', 2012, 5000),
          ('dotNET', 2013, 48000),
          ('Java', 2013, 30000)
        AS t(course, year, earnings)
        """
      When query
        """
        SELECT * FROM courses
        PIVOT (
          SUM(earnings)
          FOR (year) IN (2012, 2013)
        )
        ORDER BY course
        """
      Then query result ordered
        | course | 2012  | 2013  |
        | Java   | 20000 | 30000 |
        | dotNET | 15000 | 48000 |

  Rule: Pivot with NULL handling

    Scenario: pivot sum with null values in data
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW data AS
        SELECT * FROM VALUES
          ('A', 1, 10),
          ('A', 2, CAST(NULL AS INT)),
          ('B', 1, 30),
          ('B', 2, 40)
        AS t(grp, cat, val)
        """
      When query
        """
        SELECT * FROM data
        PIVOT (
          SUM(val)
          FOR (cat) IN (1, 2)
        )
        ORDER BY grp
        """
      Then query result ordered
        | grp | 1  | 2    |
        | A   | 10 | NULL |
        | B   | 30 | 40   |

  Rule: Pivot with no matching values produces NULLs

    Scenario: pivot value not in data yields null
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW data AS
        SELECT * FROM VALUES
          ('A', 1, 10),
          ('B', 1, 20)
        AS t(grp, cat, val)
        """
      When query
        """
        SELECT * FROM data
        PIVOT (
          SUM(val)
          FOR (cat) IN (1, 2)
        )
        ORDER BY grp
        """
      Then query result ordered
        | grp | 1  | 2    |
        | A   | 10 | NULL |
        | B   | 20 | NULL |

  Rule: Pivot with avg aggregate

    Scenario: pivot avg
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW data AS
        SELECT * FROM VALUES
          ('A', 'x', 10),
          ('A', 'x', 20),
          ('A', 'y', 30),
          ('B', 'x', 40),
          ('B', 'y', 50),
          ('B', 'y', 60)
        AS t(grp, cat, val)
        """
      When query
        """
        SELECT * FROM data
        PIVOT (
          AVG(val)
          FOR (cat) IN ('x', 'y')
        )
        ORDER BY grp
        """
      Then query result ordered
        | grp | x    | y    |
        | A   | 15.0 | 30.0 |
        | B   | 40.0 | 55.0 |

  Rule: Pivot with count aggregate

    Scenario: pivot count
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW data AS
        SELECT * FROM VALUES
          ('A', 'x', 10),
          ('A', 'x', 20),
          ('A', 'y', 30),
          ('B', 'x', 40)
        AS t(grp, cat, val)
        """
      When query
        """
        SELECT * FROM data
        PIVOT (
          COUNT(val)
          FOR (cat) IN ('x', 'y')
        )
        ORDER BY grp
        """
      Then query result ordered
        | grp | x | y |
        | A   | 2 | 1 |
        | B   | 1 | 0 |

  Rule: Pivot with NULL pivot category

    Scenario: pivot with NULL in pivot column values
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW data AS
        SELECT * FROM VALUES
          ('A', 1, 10),
          ('A', CAST(NULL AS INT), 20),
          ('B', 1, 30),
          ('B', CAST(NULL AS INT), 40)
        AS t(grp, cat, val)
        """
      When query
        """
        SELECT * FROM data
        PIVOT (
          SUM(val)
          FOR (cat) IN (1, NULL)
        )
        ORDER BY grp
        """
      Then query result ordered
        | grp | 1  | NULL |
        | A   | 10 | 20   |
        | B   | 30 | 40   |

  Rule: Pivot with count star aggregate

    Scenario: pivot count star
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW data AS
        SELECT * FROM VALUES
          ('A', 'x'),
          ('A', 'x'),
          ('A', 'y'),
          ('B', 'x')
        AS t(grp, cat)
        """
      When query
        """
        SELECT * FROM data
        PIVOT (
          COUNT(*)
          FOR (cat) IN ('x', 'y')
        )
        ORDER BY grp
        """
      Then query result ordered
        | grp | x | y |
        | A   | 2 | 1 |
        | B   | 1 | 0 |
