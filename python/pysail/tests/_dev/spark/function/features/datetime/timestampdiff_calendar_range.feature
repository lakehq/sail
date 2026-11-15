Feature: TIMESTAMPDIFF counts calendar units for timestamps in any year

  Scenario: TIMESTAMPDIFF counts calendar units for TIMESTAMP_NTZ values far from the present
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        id,
        timestampdiff(MONTH, TIMESTAMP_NTZ '2020-01-31 12:00:00', e) AS months,
        datediff(YEAR, TIMESTAMP_NTZ '2020-01-31 12:00:00', e) AS years,
        timestampdiff(QUARTER, e, TIMESTAMP_NTZ '2020-01-31 12:00:00') AS quarters
      FROM VALUES
        (0, TIMESTAMP_NTZ '0001-01-01 00:00:00'),
        (1, TIMESTAMP_NTZ '1500-03-01 00:00:00'),
        (2, TIMESTAMP_NTZ '1677-09-21 00:00:00'),
        (3, TIMESTAMP_NTZ '2262-04-12 00:00:00'),
        (4, TIMESTAMP_NTZ '2300-01-31 11:00:00'),
        (5, TIMESTAMP_NTZ '3000-06-15 12:00:00'),
        (6, TIMESTAMP_NTZ '9999-12-31 23:59:59')
        AS t(id, e)
      ORDER BY id
      """
    Then query result ordered
      | id | months | years | quarters |
      | 0  | -24228 | -2019 | 8076     |
      | 1  | -6238  | -519  | 2079     |
      | 2  | -4108  | -342  | 1369     |
      | 3  | 2906   | 242   | -968     |
      | 4  | 3359   | 279   | -1119    |
      | 5  | 11764  | 980   | -3921    |
      | 6  | 95759  | 7979  | -31919   |

  Scenario: TIMESTAMPDIFF counts calendar units for TIMESTAMP values far from the present (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        id,
        timestampdiff(MONTH, TIMESTAMP '2020-01-31 12:00:00', e) AS months,
        datediff(YEAR, TIMESTAMP '2020-01-31 12:00:00', e) AS years,
        timestampdiff(QUARTER, e, TIMESTAMP '2020-01-31 12:00:00') AS quarters
      FROM VALUES
        (0, TIMESTAMP '0001-01-01 00:00:00'),
        (1, TIMESTAMP '1500-03-01 00:00:00'),
        (2, TIMESTAMP '1677-09-21 00:00:00'),
        (3, TIMESTAMP '2262-04-12 00:00:00'),
        (4, TIMESTAMP '2300-01-31 11:00:00'),
        (5, TIMESTAMP '3000-06-15 12:00:00'),
        (6, TIMESTAMP '9999-12-31 23:59:59')
        AS t(id, e)
      ORDER BY id
      """
    Then query result ordered
      | id | months | years | quarters |
      | 0  | -24228 | -2019 | 8076     |
      | 1  | -6238  | -519  | 2079     |
      | 2  | -4108  | -342  | 1369     |
      | 3  | 2906   | 242   | -968     |
      | 4  | 3359   | 279   | -1119    |
      | 5  | 11764  | 980   | -3921    |
      | 6  | 95759  | 7979  | -31919   |

  Scenario: TIMESTAMPDIFF counts calendar units between constant timestamps far from the present (UTC)
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        timestampdiff(MONTH, TIMESTAMP_NTZ '2020-01-31 12:00:00', TIMESTAMP_NTZ '0001-01-01 00:00:00') AS first_year,
        timestampdiff(MONTH, TIMESTAMP_NTZ '2020-01-31 12:00:00', TIMESTAMP_NTZ '3000-06-15 12:00:00') AS far_future,
        timestampdiff(YEAR, TIMESTAMP '9999-12-31 23:59:59', TIMESTAMP '2020-01-31 12:00:00') AS last_year
      """
    Then query result
      | first_year | far_future | last_year |
      | -24228     | 11764      | -7979     |

  Scenario: TIMESTAMPDIFF counts calendar units between constant timestamps far from the present (America/Los_Angeles)
    Given config spark.sql.session.timeZone = America/Los_Angeles
    When query
      """
      SELECT
        timestampdiff(MONTH, TIMESTAMP_NTZ '2020-01-31 12:00:00', TIMESTAMP_NTZ '0001-01-01 00:00:00') AS first_year,
        timestampdiff(MONTH, TIMESTAMP_NTZ '2020-01-31 12:00:00', TIMESTAMP_NTZ '3000-06-15 12:00:00') AS far_future,
        timestampdiff(YEAR, TIMESTAMP '9999-12-31 23:59:59', TIMESTAMP '2020-01-31 12:00:00') AS last_year
      """
    Then query result
      | first_year | far_future | last_year |
      | -24228     | 11764      | -7979     |
