Feature: DATE_TRUNC preserves timestamp type

  Rule: date_trunc on timestamp preserves type

    Scenario: date_trunc on timestamp column preserves timestamp type
      When query
      """
      SELECT date_trunc('YEAR', TIMESTAMP '2026-02-02 00:00:00 UTC') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp (nullable = false)
      """

    Scenario: date_trunc on timestamp_ntz column preserves timestamp_ntz type
      When query
      """
      SELECT date_trunc('YEAR', TIMESTAMP_NTZ '2026-02-02 00:00:00') AS result
      """
      Then query schema
      """
      root
       |-- result: timestamp_ntz (nullable = false)
      """

    Scenario: date_trunc YEAR on timestamp values
      When query
      """
      SELECT date_trunc('YEAR', TIMESTAMP '2026-02-02 00:00:00 UTC') AS result
      """
      Then query result
      | result              |
      | 2026-01-01 00:00:00 |

    Scenario: date_trunc MONTH on timestamp values
      When query
      """
      SELECT date_trunc('MONTH', TIMESTAMP '2026-03-15 10:30:00 UTC') AS result
      """
      Then query result
      | result              |
      | 2026-03-01 00:00:00 |
