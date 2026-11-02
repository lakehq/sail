Feature: min and max aggregate functions

  Rule: extrema are duplicate agnostic

    Scenario: distinct does not change min or max
      When query
        """
        SELECT MIN(DISTINCT value) AS minimum, MAX(DISTINCT value) AS maximum
        FROM VALUES (2), (1), (2), (3) AS t(value)
        """
      Then query result
        | minimum | maximum |
        | 1       | 3       |
