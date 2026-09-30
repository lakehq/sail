# CAST scenarios imported from test/bug_catalog (0886a9e7f): datetime/time_type.feature
Feature: Additional CAST coverage from time_type

  @spark-4.1
  Rule: CAST from TIME

    Scenario: time_type catalog: CAST of a TIME column to STRING drops trailing fractional zeros
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(t AS STRING) AS s FROM VALUES
          (TIME '00:00:00'), (TIME '09:05:03.05'), (TIME '23:59:59.999999'), (NULL) AS x(t)
        """
      Then query result
        | s               |
        | 00:00:00        |
        | 09:05:03.05     |
        | 23:59:59.999999 |
        | NULL            |

    Scenario: time_type catalog: CAST of a TIME literal to STRING is not nullable
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIME '12:34:56.5' AS STRING) AS r
        """
      Then query schema
        """
        root
         |-- r: string (nullable = false)
        """

    # `Cast.castToTime` truncates a TIME to the target precision (`truncateTimeToPrecision`).

    @sail-bug
    Scenario: time_type catalog: CAST of TIME to every sub-microsecond precision truncates
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT
          CAST(TIME '12:34:56.987654' AS TIME(1)) AS p1,
          CAST(TIME '12:34:56.987654' AS TIME(2)) AS p2,
          CAST(TIME '12:34:56.987654' AS TIME(4)) AS p4,
          CAST(TIME '12:34:56.987654' AS TIME(5)) AS p5
        """
      Then query schema
        """
        root
         |-- p1: time(1) (nullable = false)
         |-- p2: time(2) (nullable = false)
         |-- p4: time(4) (nullable = false)
         |-- p5: time(5) (nullable = false)
        """
      And query result
        | p1         | p2          | p4            | p5             |
        | 12:34:56.9 | 12:34:56.98 | 12:34:56.9876 | 12:34:56.98765 |

    Scenario: time_type catalog: CAST of a TIME column to TIME(0) and TIME(3) truncates
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(t AS TIME(0)) AS p0, CAST(t AS TIME(3)) AS p3 FROM VALUES
          (TIME '00:00:00.999999'), (TIME '09:05:03.5'), (TIME '23:59:59.999999') AS x(t)
        """
      Then query schema
        """
        root
         |-- p0: time(0) (nullable = false)
         |-- p3: time(3) (nullable = false)
        """
      And query result
        | p0       | p3           |
        | 00:00:00 | 00:00:00.999 |
        | 09:05:03 | 09:05:03.5   |
        | 23:59:59 | 23:59:59.999 |

    Scenario: time_type catalog: TIME precision beyond 6 is rejected
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(TIME '12:34:56' AS TIME(7)) AS r
        """
      Then query error UNSUPPORTED_TIME_PRECISION

    # TIME to integral is the whole seconds of the day; to decimal keeps the fraction.

    Scenario: time_type catalog: CAST of a TIME literal to numbers counts seconds since midnight
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT
          CAST(TIME '12:34:56.5' AS BIGINT) AS l,
          CAST(TIME '12:34:56.5' AS INT) AS i,
          CAST(TIME '23:59:59.999999' AS DECIMAL(20, 6)) AS d,
          CAST(TIME '00:00:01' AS SMALLINT) AS s
        """
      Then query result
        | l     | i     | d            | s |
        | 45296 | 45296 | 86399.999999 | 1 |

    # Integral casts count whole seconds, independently of the physical TIME precision.

    Scenario: time_type catalog: CAST of a TIME column to BIGINT counts seconds since midnight
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(t AS BIGINT) AS l FROM VALUES
          (TIME '00:00:00'), (TIME '09:05:03.5'), (TIME '23:59:59.999999') AS x(t)
        """
      Then query result
        | l     |
        | 0     |
        | 32703 |
        | 86399 |

    Scenario Outline: time_type catalog: CAST of TIME to a too narrow number overflows under ANSI: <case>
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(TIME '12:34:56' AS <type>) AS r
        """
      Then query error <error>

      Examples:
        | case          | type          | error                      |
        | TINYINT       | TINYINT       | CAST_OVERFLOW              |
        | SMALLINT      | SMALLINT      | CAST_OVERFLOW              |

    Scenario: time_type catalog: CAST of TIME to a narrow decimal overflows under ANSI
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(TIME '12:34:56' AS DECIMAL(4, 0)) AS r
        """
      Then query error NUMERIC_VALUE_OUT_OF_RANGE

    Scenario: time_type catalog: CAST of TIME to a too narrow number is NULL without ANSI
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          CAST(TIME '12:34:56' AS TINYINT) AS t,
          CAST(TIME '12:34:56' AS SMALLINT) AS s,
          CAST(TIME '12:34:56' AS DECIMAL(4, 0)) AS d
        """
      Then query result
        | t    | s    | d    |
        | NULL | NULL | NULL |

    Scenario Outline: time_type catalog: CAST between TIME and <case> is rejected at analysis
      Given config spark.sql.timeType.enabled = true
      When query
        """
        SELECT CAST(<value> AS <type>) AS r
        """
      Then query error CAST_WITHOUT_SUGGESTION

      Examples:
        | case                    | value                               | type                    |
        | TIMESTAMP target        | TIME '12:34:56'                     | TIMESTAMP               |
        | TIMESTAMP_NTZ target    | TIME '12:34:56'                     | TIMESTAMP_NTZ           |
        | DATE target             | TIME '12:34:56'                     | DATE                    |
        | DOUBLE target           | TIME '12:34:56.5'                   | DOUBLE                  |
        | BOOLEAN target          | TIME '12:34:56'                     | BOOLEAN                 |
        | INTERVAL HOUR TO SECOND | TIME '12:34:56'                     | INTERVAL HOUR TO SECOND |
        | TIMESTAMP source        | TIMESTAMP '2024-01-15 12:34:56'     | TIME                    |
        | TIMESTAMP_NTZ source    | TIMESTAMP_NTZ '2024-01-15 12:34:56' | TIME                    |
        | INT source              | 43200                               | TIME                    |

  @spark-4.1
  Rule: CAST from STRING to TIME

    Scenario: time_type catalog: CAST of STRING to TIME accepts Spark's lenient forms
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(s AS TIME) AS r FROM VALUES
          ('00:00:00'), ('9:5:3.5'), (' 23:59:59.999999 '), ('T12:34:56'), (NULL) AS x(s)
        """
      Then query schema
        """
        root
         |-- r: time(6) (nullable = true)
        """
      And query result
        | r               |
        | 00:00:00        |
        | 09:05:03.5      |
        | 23:59:59.999999 |
        | 12:34:56        |
        | NULL            |

    Scenario: time_type catalog: CAST of a STRING literal to TIME is nullable
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('12:34:56.123' AS TIME(3)) AS r
        """
      Then query schema
        """
        root
         |-- r: time(3) (nullable = true)
        """
      And query result
        | r            |
        | 12:34:56.123 |

    Scenario Outline: time_type catalog: CAST of a malformed STRING to TIME fails under ANSI: <case>
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(<value> AS TIME) AS r
        """
      Then query error CAST_INVALID_INPUT

      Examples:
        | case              | value                 |
        | garbage           | 'garbage'             |
        | the hour 24       | '24:00:00'            |
        | an hour beyond 23 | '25:00:00'            |
        | a date and time   | '2024-01-15 12:34:56' |
        | a bare number     | '12'                  |

    Scenario: time_type catalog: CAST of a malformed STRING column to TIME is NULL without ANSI
      Given config spark.sql.timeType.enabled = true
      And config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST(s AS TIME) AS r FROM VALUES ('10:00:00'), ('garbage'), ('24:00:00'), ('12') AS x(s)
        """
      Then query result
        | r        |
        | 10:00:00 |
        | NULL     |
        | NULL     |
        | NULL     |
