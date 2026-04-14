@make_interval
Feature: make_interval (strict variant)
  Strict make_interval that throws on overflow, contrasting with
  try_make_interval which returns NULL.

  Rule: Valid argument combinations build interval

    Scenario: All zeros yields zero interval
      When query
      """
      SELECT CAST(make_interval(0, 0, 0, 0, 0, 0, 0) AS STRING) AS result
      """
      Then query result
      | result    |
      | 0 seconds |

    Scenario: Year only
      When query
      """
      SELECT CAST(make_interval(1) AS STRING) AS result
      """
      Then query result
      | result  |
      | 1 years |

    Scenario: Full args
      When query
      """
      SELECT CAST(make_interval(1, 2, 3, 4, 5, 6, 7.5) AS STRING) AS result
      """
      Then query result
      | result                                                 |
      | 1 years 2 months 25 days 5 hours 6 minutes 7.5 seconds |

    Scenario: NULL field propagates to NULL result
      When query
      """
      SELECT make_interval(1, NULL, 3) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Overflow raises error

    Scenario: Year overflow raises error
      When query
      """
      SELECT make_interval(2147483647)
      """
      Then query error overflow|month_day_nano_interval|ARITHMETIC

  Rule: Argument count validation matches Spark

    Scenario: No args raises WRONG_NUM_ARGS
      When query
      """
      SELECT make_interval()
      """
      Then query error WRONG_NUM_ARGS
