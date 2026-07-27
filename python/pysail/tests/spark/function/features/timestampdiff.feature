Feature: timestampdiff calendar units

  Rule: timestampdiff uses calendar-aware month, quarter, and year units

    Scenario: timestampdiff MONTH counts a leap February calendar month
      When query
      """
      SELECT timestampdiff(MONTH, TIMESTAMP '2024-02-01 00:00:00', TIMESTAMP '2024-03-01 00:00:00') AS result
      """
      Then query result
      | result |
      | 1      |

    Scenario: timestampdiff MONTH truncates incomplete trailing months
      When query
      """
      SELECT timestampdiff(MONTH, TIMESTAMP '2024-01-31 10:00:00', TIMESTAMP '2024-02-29 09:59:59') AS result
      """
      Then query result
      | result |
      | 0      |

    Scenario: timestampdiff MONTH includes complete trailing month at matching day and time
      When query
      """
      SELECT timestampdiff(MONTH, TIMESTAMP '2024-02-29 10:00:00', TIMESTAMP '2024-03-29 10:00:00') AS result
      """
      Then query result
      | result |
      | 1      |

    Scenario: timestampdiff QUARTER counts calendar quarters
      When query
      """
      SELECT timestampdiff(QUARTER, TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-04-01 00:00:00') AS result
      """
      Then query result
      | result |
      | 1      |

    Scenario: timestampdiff YEAR counts completed calendar years
      When query
      """
      SELECT timestampdiff(YEAR, TIMESTAMP '2020-02-29 12:00:00', TIMESTAMP '2021-02-28 11:59:59') AS result
      """
      Then query result
      | result |
      | 0      |

    Scenario: timestampdiff MONTH truncates negative intervals toward zero
      When query
      """
      SELECT timestampdiff(MONTH, TIMESTAMP '2024-03-01 00:00:00', TIMESTAMP '2024-02-01 00:00:01') AS result
      """
      Then query result
      | result |
      | 0      |

    Scenario: date_diff and datediff use the same calendar month behavior
      When query
      """
      SELECT
        date_diff(MONTH, TIMESTAMP '2024-02-01 00:00:00', TIMESTAMP '2024-03-01 00:00:00') AS date_diff_result,
        datediff(MONTH, TIMESTAMP '2024-02-01 00:00:00', TIMESTAMP '2024-03-01 00:00:00') AS datediff_result
      """
      Then query result
      | date_diff_result | datediff_result |
      | 1                | 1               |
