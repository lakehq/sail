Feature: nvl2 output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to nvl2 yields the schema Spark declares
      When query
        """
        SELECT nvl2(NULL, 2, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """

  Rule: Condition independence and evaluation

    Scenario Outline: NVL2 keeps the condition separate from mixed timestamp result types
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT CAST(nvl2(<condition>, TIMESTAMP_NTZ '2020-01-01 00:00:00',
                        TIMESTAMP '2020-01-01 08:00:00+00:00') AS STRING) AS value,
               typeof(nvl2(<condition>, TIMESTAMP_NTZ '2020-01-01 00:00:00',
                           TIMESTAMP '2020-01-01 08:00:00+00:00')) AS result_type
        """
      Then query result
        | value               | result_type |
        | 2020-01-01 00:00:00 | timestamp   |

      Examples:
        | condition |
        | 'present' |
        | array(1)  |

    Scenario: NVL2 accepts a volatile condition with timestamp branches
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT size(collect_list(v)) AS n
        FROM (
          SELECT nvl2(NULLIF(CAST(rand(1) * 2 AS INT), 0),
                      TIMESTAMP_NTZ '2020-01-01 00:00:00',
                      TIMESTAMP '2020-01-01 08:00:00+00:00') AS v
          FROM range(1000)
        )
        """
      Then query result
        | n    |
        | 1000 |

    Scenario: NVL2 accepts a volatile scalar subquery condition
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT CAST(nvl2(
          (SELECT NULLIF(CAST(rand() * 2 AS INT), 0) FROM range(1)),
          TIMESTAMP_NTZ '2020-01-01 00:00:00',
          TIMESTAMP '2020-01-01 08:00:00+00:00'
        ) AS STRING) AS value
        """
      Then query result
        | value               |
        | 2020-01-01 00:00:00 |

    Scenario: NVL2 leaves an unused branch unevaluated
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT nvl2(CAST(id AS INT), CAST(id + 1 AS INT),
                    CAST(raise_error('unused NVL2 branch') AS INT)) AS result
        FROM range(1)
        """
      Then query result
        | result |
        | 1      |

    Scenario: NVL2 evaluates a selected branch
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT nvl2(CAST(NULL AS INT), CAST(id + 1 AS INT),
                    CAST(raise_error('selected NVL2 branch') AS INT)) AS result
        FROM range(1)
        """
      Then query error selected NVL2 branch

  Rule: Interval result qualifiers

    Scenario Outline: NVL2 preserves qualifiers from its interval result branches
      Given config spark.sql.session.timeZone = Pacific/Apia
      When query
        """
        SELECT CAST(DATE '2011-12-30' + nvl2(<condition>, <then_value>, <else_value>) AS STRING) AS value,
               typeof(DATE '2011-12-30' + nvl2(<condition>, <then_value>, <else_value>)) AS result_type
        """
      Then query result
        | value   | result_type   |
        | <value> | <result_type> |

      Examples:
        | condition             | then_value                 | else_value                 | value               | result_type |
        | 'present'             | INTERVAL '1' DAY           | INTERVAL '2' DAY           | 2011-12-31          | date        |
        | CAST(NULL AS STRING)  | INTERVAL '1' DAY           | INTERVAL '2' DAY           | 2012-01-01          | date        |
        | 'present'             | INTERVAL '1' DAY           | INTERVAL '1' HOUR          | 2012-01-01 00:00:00 | timestamp   |
        | 'present'             | INTERVAL '1' HOUR          | INTERVAL '1' DAY           | 2011-12-31 01:00:00 | timestamp   |
        | CAST(NULL AS STRING)  | INTERVAL '1' HOUR          | INTERVAL '1' DAY           | 2012-01-01 00:00:00 | timestamp   |
        | 'present'             | INTERVAL '1' DAY           | NULL                       | 2011-12-31          | date        |
        | CAST(NULL AS STRING)  | NULL                       | INTERVAL '2' DAY           | 2012-01-01          | date        |
        | CAST(NULL AS STRING)  | CAST(NULL AS INTERVAL DAY) | INTERVAL '2' DAY           | 2012-01-01          | date        |
        | CAST(NULL AS STRING)  | INTERVAL '1' DAY           | CAST(NULL AS INTERVAL DAY) | NULL                | date        |
        | INTERVAL '24' HOUR     | INTERVAL '1' DAY           | INTERVAL '2' DAY           | 2011-12-31          | date        |
