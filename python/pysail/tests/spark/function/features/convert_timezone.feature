Feature: convert_timezone function

  Rule: convert_timezone with timestamp_ntz literal

    Scenario: convert Amsterdam winter time to UTC
      When query
        """
        SELECT date_format(
          convert_timezone('Europe/Amsterdam', 'UTC', TIMESTAMP_NTZ '2023-01-01 10:00:00'),
          'yyyy-MM-dd HH'
        ) AS x
        """
      Then query result
        | x             |
        | 2023-01-01 09 |

    Scenario: convert UTC to Amsterdam winter time
      When query
        """
        SELECT convert_timezone('UTC', 'Europe/Amsterdam', TIMESTAMP_NTZ '2023-01-01 09:00:00') AS x
        """
      Then query result
        | x                   |
        | 2023-01-01 10:00:00 |

    Scenario: convert between timezones with different offsets
      When query
        """
        SELECT convert_timezone('Europe/Brussels', 'America/Los_Angeles', TIMESTAMP_NTZ '2021-12-06 00:00:00') AS x
        """
      Then query result
        | x                   |
        | 2021-12-05 15:00:00 |

  Rule: convert_timezone with to_timestamp_ntz on a LTZ column (session timezone = Europe/Amsterdam)

    Scenario: convert_timezone and to_timestamp_ntz return correct UTC hour for winter Amsterdam timestamp
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      When query
        """
        SELECT date_format(
          convert_timezone(
            'Europe/Amsterdam',
            'UTC',
            to_timestamp_ntz(TIMESTAMP '2023-01-01 10:00:00')
          ),
          'yyyy-MM-dd HH'
        ) AS x
        """
      Then query result
        | x             |
        | 2023-01-01 09 |
