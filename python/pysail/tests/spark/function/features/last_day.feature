@last_day
Feature: last_day comprehensive tests

  Rule: Argument count validation

    Scenario: last_day zero arguments errors
      When query
        """
        SELECT last_day() AS result
        """
      Then query error .*

    Scenario: last_day two arguments errors
      When query
        """
        SELECT last_day(DATE'2024-01-01', 'extra') AS result
        """
      Then query error .*

  Rule: NULL handling

    Scenario: last_day NULL date
      When query
        """
        SELECT last_day(CAST(NULL AS DATE)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: last_day NULL string
      When query
        """
        SELECT last_day(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Basic usage per month

    Scenario: last_day January
      When query
        """
        SELECT last_day(DATE'2024-01-15') AS result
        """
      Then query result
        | result     |
        | 2024-01-31 |

    Scenario: last_day February leap year
      When query
        """
        SELECT last_day(DATE'2024-02-01') AS result
        """
      Then query result
        | result     |
        | 2024-02-29 |

    Scenario: last_day February non-leap year
      When query
        """
        SELECT last_day(DATE'2023-02-01') AS result
        """
      Then query result
        | result     |
        | 2023-02-28 |

    Scenario: last_day March
      When query
        """
        SELECT last_day(DATE'2024-03-01') AS result
        """
      Then query result
        | result     |
        | 2024-03-31 |

    Scenario: last_day April
      When query
        """
        SELECT last_day(DATE'2024-04-15') AS result
        """
      Then query result
        | result     |
        | 2024-04-30 |

    Scenario: last_day December
      When query
        """
        SELECT last_day(DATE'2024-12-05') AS result
        """
      Then query result
        | result     |
        | 2024-12-31 |

  Rule: String input coercion

    Scenario: last_day with string input
      When query
        """
        SELECT last_day('2024-03-15') AS result
        """
      Then query result
        | result     |
        | 2024-03-31 |

    Scenario: last_day with invalid string errors
      When query
        """
        SELECT last_day('not-a-date') AS result
        """
      Then query error .*

  Rule: Boundary dates

    Scenario: last_day epoch
      When query
        """
        SELECT last_day(DATE'1970-01-01') AS result
        """
      Then query result
        | result     |
        | 1970-01-31 |

    Scenario: last_day minimum date
      When query
        """
        SELECT last_day(DATE'0001-01-01') AS result
        """
      Then query result
        | result     |
        | 0001-01-31 |

    Scenario: last_day maximum date
      When query
        """
        SELECT last_day(DATE'9999-12-01') AS result
        """
      Then query result
        | result     |
        | 9999-12-31 |

  Rule: Multi-row

    Scenario: last_day multi-row
      When query
        """
        SELECT last_day(d) AS result FROM VALUES (DATE'2024-01-15'), (CAST(NULL AS DATE)), (DATE'2024-02-29') AS t(d)
        """
      Then query result
        | result     |
        | 2024-01-31 |
        | NULL       |
        | 2024-02-29 |
