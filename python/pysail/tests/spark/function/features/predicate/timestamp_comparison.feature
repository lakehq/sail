Feature: Timestamp and string predicate coercion

  Rule: Timestamp comparisons use Spark coercion semantics

    Scenario: String-to-timestamp comparison uses the session time zone and microsecond precision
      Given config spark.sql.session.timeZone = Asia/Shanghai
      When query
        """
        SELECT
          TIMESTAMP '2024-05-01 12:00:00' > '2024-05-01 13:00:00' AS after,
          TIMESTAMP '2024-05-01 12:00:00' = CONCAT('2024-05-01 12:00:', '00') AS dynamic_match,
          TIMESTAMP '2024-05-01 12:00:00.123456' = '2024-05-01 12:00:00.123456789' AS precise_match
        """
      Then query result
        | after | dynamic_match | precise_match |
        | false | true          | true          |

    Scenario: Timestamp IN uses the ANSI common type
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT
          TIMESTAMP '2024-05-01 12:00:00.123456'
            IN ('2024-05-01 12:00:00.123456789') AS matched,
          TIMESTAMP '2024-05-01 12:00:00'
            IN ('2024-05-01 12:00:00', 1) AS mixed_matched
        """
      Then query result
        | matched | mixed_matched |
        | false   | true          |
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES (TIMESTAMP '2024-05-01 12:00:00.123456') AS t(event_time)
        WHERE event_time IN (SELECT '2024-05-01 12:00:00.123456789')
        """
      Then query result
        | matched |
        | 0       |
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT TIMESTAMP '2024-05-01 12:00:00.123456'
          IN ('2024-05-01 12:00:00.123456789') AS matched
        """
      Then query result
        | matched |
        | true    |
      When query
        """
        SELECT COUNT(*) AS matched
        FROM VALUES (TIMESTAMP '2024-05-01 12:00:00.123456') AS t(event_time)
        WHERE event_time IN (SELECT '2024-05-01 12:00:00.123456789')
        """
      Then query result
        | matched |
        | 1       |
