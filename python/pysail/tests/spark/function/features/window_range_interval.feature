Feature: Window RANGE frame with interval boundaries

  Rule: RANGE frame with INTERVAL PRECEDING on timestamp ORDER BY

    Scenario: count with interval seconds preceding
      When query
        """
        SELECT
          val,
          COUNT(*) OVER (ORDER BY time RANGE BETWEEN INTERVAL '3' SECOND PRECEDING AND CURRENT ROW) AS cnt
        FROM (
          SELECT * FROM VALUES
            (TIMESTAMP '2016-05-25 13:30:00.000', 1),
            (TIMESTAMP '2016-05-25 13:30:01.000', 2),
            (TIMESTAMP '2016-05-25 13:30:03.000', 3),
            (TIMESTAMP '2016-05-25 13:30:06.000', 4),
            (TIMESTAMP '2016-05-25 13:30:10.000', 5)
          AS t(time, val)
        )
        ORDER BY val
        """
      Then query result ordered
        | val | cnt |
        | 1   | 1   |
        | 2   | 2   |
        | 3   | 3   |
        | 4   | 2   |
        | 5   | 1   |

  Rule: RANGE frame with CAST interval boundary

    Scenario: sum with cast interval boundary following
      When query
        """
        SELECT
          bid,
          SUM(bid) OVER (ORDER BY time RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CAST(0 AS INTERVAL SECOND) FOLLOWING) AS total_bid
        FROM (
          SELECT * FROM VALUES
            (TIMESTAMP '2016-05-25 13:30:00.023', CAST(720.50 AS DOUBLE)),
            (TIMESTAMP '2016-05-25 13:30:00.030', CAST(51.97 AS DOUBLE))
          AS t(time, bid)
        )
        ORDER BY bid
        """
      Then query result ordered
        | bid   | total_bid |
        | 51.97 | 772.47    |
        | 720.5 | 720.5     |

  Rule: RANGE frame with interval minutes

    Scenario: sum with interval minutes preceding
      When query
        """
        SELECT
          val,
          SUM(val) OVER (ORDER BY time RANGE BETWEEN INTERVAL '3' MINUTE PRECEDING AND CURRENT ROW) AS total
        FROM (
          SELECT * FROM VALUES
            (TIMESTAMP '2016-05-25 13:30:00.000', 10),
            (TIMESTAMP '2016-05-25 13:31:00.000', 20),
            (TIMESTAMP '2016-05-25 13:33:00.000', 30),
            (TIMESTAMP '2016-05-25 13:36:00.000', 40)
          AS t(time, val)
        )
        ORDER BY val
        """
      Then query result ordered
        | val | total |
        | 10  | 10    |
        | 20  | 30    |
        | 30  | 60    |
        | 40  | 70    |
