Feature: sequence() over DATE returns expected arrays

  Scenario: sequence(date, date) uses default step of 1 day
    When query
    """
    SELECT sequence(date '2024-01-01', date '2024-01-05') AS seq
    """
    Then query result ordered
    | seq |
    | [2024-01-01, 2024-01-02, 2024-01-03, 2024-01-04, 2024-01-05] |

  Scenario: sequence(date, date, interval days) uses the provided step
    When query
    """
    SELECT sequence(date '2024-01-01', date '2024-01-10', interval 2 days) AS seq
    """
    Then query result ordered
    | seq |
    | [2024-01-01, 2024-01-03, 2024-01-05, 2024-01-07, 2024-01-09] |
