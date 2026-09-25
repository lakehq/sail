Feature: Set operations (INTERSECT, EXCEPT)

  Rule: INTERSECT DISTINCT

    Scenario: intersect distinct two tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (3), (4), (5), (6), (7)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 3  |
        | 4  |
        | 5  |

    Scenario: intersect distinct removes duplicates
      When query
        """
        SELECT * FROM (VALUES (1), (1), (2), (2), (3)) AS a(id)
        INTERSECT DISTINCT
        SELECT * FROM (VALUES (1), (2), (2), (4)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: intersect distinct three tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (2), (3), (4), (5), (6)) AS b(id)
        INTERSECT
        SELECT * FROM (VALUES (3), (4), (5), (6), (7)) AS c(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 3  |
        | 4  |
        | 5  |

  # Note: INTERSECT ALL tests excluded — DataFusion's LogicalPlanBuilder::intersect(is_all=true)
  # produces extra duplicates compared to Spark. Pre-existing upstream bug.
  # https://github.com/apache/datafusion/issues/12955

  Rule: EXCEPT DISTINCT

    Scenario: except distinct two tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        EXCEPT
        SELECT * FROM (VALUES (3), (4), (5), (6), (7)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: except distinct removes duplicates
      When query
        """
        SELECT * FROM (VALUES (1), (1), (2), (2), (3)) AS a(id)
        EXCEPT DISTINCT
        SELECT * FROM (VALUES (2), (4)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 3  |

    Scenario: except distinct three tables
      When query
        """
        SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS a(id)
        EXCEPT
        SELECT * FROM (VALUES (4), (5), (6)) AS b(id)
        EXCEPT
        SELECT * FROM (VALUES (1), (7)) AS c(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 2  |
        | 3  |

  Rule: EXCEPT ALL

    Scenario: except all preserves duplicates
      When query
        """
        SELECT * FROM (VALUES (1), (1), (2), (2), (3)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (2)) AS b(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |
        | 3  |

    Scenario: except all three tables
      When query
        """
        SELECT * FROM (VALUES (1), (1), (1), (2), (2), (3), (3)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (2), (3)) AS b(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (3)) AS c(id)
        ORDER BY id
        """
      Then query result ordered
        | id |
        | 1  |
        | 2  |

    Scenario: except all subtracts matching count
      When query
        """
        SELECT * FROM (VALUES (1), (1), (1)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (1)) AS b(id)
        """
      Then query result
        | id |
        | 1  |

  Rule: Wide table set operations

    Scenario: intersect distinct with multiple columns
      When query
        """
        SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS a(id, name)
        INTERSECT
        SELECT * FROM (VALUES (2, 'b'), (3, 'c'), (4, 'd')) AS b(id, name)
        ORDER BY id
        """
      Then query result ordered
        | id | name |
        | 2  | b    |
        | 3  | c    |

    Scenario: except all with multiple columns
      When query
        """
        SELECT * FROM (VALUES (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')) AS a(id, name)
        EXCEPT ALL
        SELECT * FROM (VALUES (1, 'a'), (3, 'c')) AS b(id, name)
        ORDER BY id
        """
      Then query result ordered
        | id | name |
        | 1  | a    |
        | 2  | b    |

  Rule: Null handling

    Scenario: intersect with nulls
      When query
        """
        SELECT * FROM (VALUES (1), (NULL), (3)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (NULL), (3), (4)) AS b(id)
        ORDER BY id ASC NULLS LAST
        """
      Then query result ordered
        | id   |
        | 3    |
        | NULL |

    Scenario: except all with nulls
      When query
        """
        SELECT * FROM (VALUES (1), (NULL), (NULL), (3)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (NULL), (3)) AS b(id)
        ORDER BY id ASC NULLS LAST
        """
      Then query result ordered
        | id   |
        | 1    |
        | NULL |

  Rule: Empty results

    Scenario: intersect with no common rows
      When query
        """
        SELECT * FROM (VALUES (1), (2)) AS a(id)
        INTERSECT
        SELECT * FROM (VALUES (3), (4)) AS b(id)
        """
      Then query result
        | id |

    Scenario: except all removing everything
      When query
        """
        SELECT * FROM (VALUES (1), (2)) AS a(id)
        EXCEPT ALL
        SELECT * FROM (VALUES (1), (2), (3)) AS b(id)
        """
      Then query result
        | id |

  Rule: UNION column types

    Scenario Outline: UNION retains a DOUBLE first input when combined with DECIMAL: <operator>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT typeof(v) AS result_type
        FROM (
          SELECT CAST(1.25 AS DOUBLE) AS v
          <operator>
          SELECT CAST(2.5 AS DECIMAL(2,1)) AS v
        ) AS q
        """
      Then query result
        | result_type |
        | double      |
        | double      |

      Examples:
        | operator  | ansi  |
        | UNION ALL | false |
        | UNION     | false |
        | UNION ALL | true  |
        | UNION     | true  |

    Scenario: UNION keeps interval values from inputs with different qualifiers
      When query
        """
        SELECT k, CAST(v AS INT) AS months
        FROM (
          SELECT 1 AS k, INTERVAL '14' MONTH AS v
          UNION ALL
          SELECT 2 AS k, INTERVAL '1' YEAR AS v
        ) AS q
        ORDER BY k
        """
      Then query result ordered
        | k | months |
        | 1 | 14     |
        | 2 | 12     |

    Scenario: UNION of DATE and TIMESTAMP matches DATE values in a non-UTC session time zone
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT d
        FROM VALUES (DATE '2024-06-15'), (DATE '2024-06-16') AS t(d)
        WHERE d IN (SELECT DATE '2024-06-15' UNION ALL SELECT TIMESTAMP '2000-01-01 00:00:00')
        ORDER BY d
        """
      Then query result ordered
        | d          |
        | 2024-06-15 |

    @sail-bug
    Scenario: UNION widens interval qualifiers like Spark
      When query
        """
        SELECT v FROM (SELECT INTERVAL '1' DAY AS v UNION ALL SELECT INTERVAL '1 02' DAY TO HOUR AS v)
        """
      Then query schema
        """
        root
         |-- v: interval day to hour (nullable = false)
        """

    @sail-bug
    Scenario: UNION widens DATE with TIMESTAMP to TIMESTAMP
      When query
        """
        SELECT typeof(v) AS result_type
        FROM (SELECT DATE '2024-06-15' AS v UNION ALL SELECT TIMESTAMP '2000-01-01 00:00:00' AS v)
        """
      Then query result
        | result_type |
        | timestamp   |
        | timestamp   |

  Rule: UNION nested field metadata

    Scenario Outline: UNION preserves nested interval qualifiers: <operator>
      When query
        """
        SELECT k, CAST(s.v AS INT) AS years
        FROM (
          SELECT 1 AS k, struct(INTERVAL '1' YEAR AS v) AS s
          <operator>
          SELECT 2 AS k, struct(CAST(NULL AS INTERVAL YEAR) AS v) AS s
        ) AS q
        ORDER BY k
        """
      Then query result ordered
        | k | years |
        | 1 | 1     |
        | 2 | NULL  |

      Examples:
        | operator  |
        | UNION ALL |
        | UNION     |

    @sail-bug
    Scenario: UNION widens numeric fields beside nested interval metadata
      When query
        """
        SELECT typeof(s.n) AS number_type
        FROM (
          SELECT struct(INTERVAL '1' YEAR AS v, CAST(1 AS INT) AS n) AS s
          UNION ALL
          SELECT struct(CAST(NULL AS INTERVAL YEAR) AS v, CAST(2 AS BIGINT) AS n) AS s
        ) AS q
        """
      Then query result
        | number_type |
        | bigint      |
        | bigint      |

  Rule: UNION timestamp consumers

    Scenario Outline: UNION preserves timestamp units for UTC conversions: <operator>, <kind>, ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      And config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT
          from_utc_timestamp(v, 'America/Los_Angeles') AS from_utc,
          to_utc_timestamp(v, 'America/Los_Angeles') AS to_utc
        FROM (
          SELECT v
          FROM VALUES
            (CAST('2024-06-15 12:00:00' AS <kind>)),
            (CAST(NULL AS <kind>))
            AS t(v)
          <operator>
          SELECT '2024-06-16 12:00:00' AS v
        ) AS q
        ORDER BY from_utc
        """
      Then query result ordered
        | from_utc            | to_utc              |
        | NULL                | NULL                |
        | 2024-06-15 05:00:00 | 2024-06-15 19:00:00 |
        | 2024-06-16 05:00:00 | 2024-06-16 19:00:00 |
      And query schema
        """
        root
         |-- from_utc: timestamp (nullable = true)
         |-- to_utc: timestamp (nullable = true)
        """

      Examples:
        | operator  | kind          | ansi  |
        | UNION ALL | TIMESTAMP     | true  |
        | UNION ALL | TIMESTAMP     | false |
        | UNION     | TIMESTAMP     | true  |
        | UNION     | TIMESTAMP     | false |
        | UNION ALL | TIMESTAMP_NTZ | true  |
        | UNION ALL | TIMESTAMP_NTZ | false |
        | UNION     | TIMESTAMP_NTZ | true  |
        | UNION     | TIMESTAMP_NTZ | false |

    @sail-bug
    Scenario: An ANSI TIMESTAMP and STRING UNION returns Spark timestamps
      Given config spark.sql.ansi.enabled = true
      And config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT CAST(NULL AS TIMESTAMP) AS v
        UNION ALL
        SELECT CAST(NULL AS STRING) AS v
        """
      Then query result collected
        | v    |
        | NULL |
        | NULL |
      And query schema
        """
        root
         |-- v: timestamp (nullable = true)
        """
