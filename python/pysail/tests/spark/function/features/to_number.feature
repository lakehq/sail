@to_number
Feature: to_number basic smoke tests (shared format parser with to_char)

  Rule: Basic usage

    Scenario: parse integer
      When query
        """
        SELECT to_number('123', '999') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: parse with dollar sign
      When query
        """
        SELECT to_number('$1,234', '$9,999') AS result
        """
      Then query result
        | result |
        | 1234   |

    Scenario: parse decimal
      When query
        """
        SELECT to_number('1.23', '9.99') AS result
        """
      Then query result
        | result |
        | 1.23   |

    Scenario: NULL input
      When query
        """
        SELECT to_number(CAST(NULL AS STRING), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: L currency is not valid

    Scenario: L format rejected
      When query
        """
        SELECT to_number('$1,234', 'L9,999') AS result
        """
      Then query error .*
