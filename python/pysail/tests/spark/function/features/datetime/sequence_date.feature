@sequence_date
Feature: sequence() over DATE returns expected arrays

    Scenario Outline: sequence over DATE: <case>
      When query
        """
        SELECT sequence(<args>) AS seq
        """
      Then query result ordered
        | seq   |
        | <seq> |

      Examples:
        | case                                                       | args                                                  | seq                                                          |
        | sequence(date, date) uses default step of 1 day            | date '2024-01-01', date '2024-01-05'                  | [2024-01-01, 2024-01-02, 2024-01-03, 2024-01-04, 2024-01-05] |
        | sequence(date, date, interval days) uses the provided step | date '2024-01-01', date '2024-01-10', interval 2 days | [2024-01-01, 2024-01-03, 2024-01-05, 2024-01-07, 2024-01-09] |
