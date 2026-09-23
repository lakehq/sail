Feature: datediff output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to datediff yields the schema Spark declares
      When query
        """
        SELECT datediff('2009-07-31', '2009-07-30') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to datediff yields the schema Spark declares
      When query
        """
        SELECT datediff(CAST(id AS STRING), '2009-07-30') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    @sail-bug
    Scenario: a nullable column input to datediff stays nullable
      When query
        """
        SELECT datediff(c, '2009-07-30') AS result FROM VALUES ('2009-07-31'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Return type

    Scenario: the two-argument form returns INT
      When query
        """
        SELECT datediff('2009-07-31', '2009-07-30') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: the three-argument form returns BIGINT, unlike the two-argument one
      When query
        """
        SELECT datediff(DAY, DATE '2024-01-01', DATE '2024-01-10') AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: the three-argument form with a time unit returns BIGINT
      When query
        """
        SELECT datediff(HOUR, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-02 03:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

  # Spark 4.2.0 datetimeExpressions.scala: DateDiff casts both sides to DATE (a timestamp keeps
  # only its date in the session zone) and returns `end - start` as an Int day count.
  Rule: Two-argument datediff over the date range

    Scenario Outline: datediff edge: <case>
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SELECT <function>(<end>, <start>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                   | function  | end                              | start                           | result   |
        | whole literal range                    | datediff  | DATE '9999-12-31'                | DATE '0001-01-01'               | 3652058  |
        | whole literal range, negative          | datediff  | DATE '0001-01-01'                | DATE '9999-12-31'               | -3652058 |
        | past the maximum literal date          | datediff  | DATE '+10000-01-01'              | DATE '9999-12-31'               | 1        |
        | timestamps two seconds apart           | datediff  | TIMESTAMP '2024-01-02 00:00:01'  | TIMESTAMP '2024-01-01 23:59:59' | 1        |
        | timestamp and date across a leap day   | date_diff | TIMESTAMP '2024-03-01 00:00:00'  | DATE '2024-02-28'               | 2        |
        | NULL start                             | datediff  | DATE '2024-01-01'                | NULL                            | NULL     |

    Scenario: datediff reads each row's own dates
      When query
        """
        SELECT datediff(e, s) AS result
        FROM VALUES (1, DATE '2024-03-01', DATE '2024-02-28'), (2, DATE '2023-03-01', DATE '2023-02-28'),
          (3, DATE '2024-01-01', DATE '2024-12-31'), (4, CAST(NULL AS DATE), DATE '2024-01-01') AS t(i, e, s)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | 2      |
        | 1      |
        | -365   |
        | NULL   |
