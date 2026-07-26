@timestampadd
Feature: timestampadd function

    Scenario Outline: timestampadd: <case>
      When query
        """
        SELECT timestampadd(<unit>, <n>, timestamp<ts>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case             | unit        | n  | ts                           | result                     |
        | add years        | YEAR        | 2  | '2016-03-11 09:00:07'        | 2018-03-11 09:00:07        |
        | add weeks        | WEEK        | 5  | '2016-03-11 09:00:07'        | 2016-04-15 09:00:07        |
        | subtract days    | day         | -5 | '2016-03-11 09:00:07'        | 2016-03-06 09:00:07        |
        | add microseconds | MICROSECOND | 2  | '2016-03-11 09:00:07.000001' | 2016-03-11 09:00:07.000003 |

  @spark_null
  Rule: Output schema

    Scenario: a non-null timestamp literal yields a timestamp
      When query
        """
        SELECT timestampadd(HOUR, 1, TIMESTAMP '2024-01-15 10:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: a non-null timestamp column yields a timestamp
      When query
        """
        SELECT timestampadd(HOUR, 1, CAST(id AS TIMESTAMP)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: a nullable timestamp column stays nullable
      When query
        """
        SELECT timestampadd(HOUR, 1, c) AS result FROM VALUES (TIMESTAMP '2024-01-15 10:00:00'), (CAST(NULL AS TIMESTAMP)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
