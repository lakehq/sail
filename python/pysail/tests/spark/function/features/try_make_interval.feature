@try_make_interval
Feature: try_make_interval
  Safe variant of make_interval that returns NULL on overflow or
  argument-type errors instead of throwing.

  Rule: Valid argument combinations build interval

    Scenario: All zeros yields zero interval
      When query
      """
      SELECT CAST(try_make_interval(0, 0, 0, 0, 0, 0, 0) AS STRING) AS result
      """
      Then query result
      | result    |
      | 0 seconds |

    Scenario: Year only
      When query
      """
      SELECT CAST(try_make_interval(1) AS STRING) AS result
      """
      Then query result
      | result  |
      | 1 years |

    Scenario: Year and month
      When query
      """
      SELECT CAST(try_make_interval(1, 6) AS STRING) AS result
      """
      Then query result
      | result            |
      | 1 years 6 months |

    Scenario: Year, month, week, day
      When query
      """
      SELECT CAST(try_make_interval(1, 2, 3, 4) AS STRING) AS result
      """
      Then query result
      | result                    |
      | 1 years 2 months 25 days |

    Scenario: Full args with hours, mins, fractional secs
      When query
      """
      SELECT CAST(try_make_interval(0, 1, 0, 1, 0, 0, 100.000001) AS STRING) AS result
      """
      Then query result
      | result                                          |
      | 1 months 1 days 1 minutes 40.000001 seconds   |

    Scenario: Negative values
      When query
      """
      SELECT CAST(try_make_interval(-1, -1, -1, -1, -1, -1, -1.5) AS STRING) AS result
      """
      Then query result
      | result                                                          |
      | -1 years -1 months -8 days -1 hours -1 minutes -1.5 seconds |

    Scenario: NULL field propagates to NULL result
      When query
      """
      SELECT try_make_interval(100, NULL, 3) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Overflow returns NULL instead of error

    Scenario: Year overflow returns NULL
      When query
      """
      SELECT try_make_interval(2147483647) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Argument count validation matches Spark

    Scenario: No args raises WRONG_NUM_ARGS
      When query
      """
      SELECT try_make_interval()
      """
      Then query error WRONG_NUM_ARGS

  Rule: Multi-row batches

    Scenario: Mixed rows including overflow
      When query
      """
      SELECT CAST(try_make_interval(y) AS STRING) AS result FROM VALUES (0), (1), (CAST(NULL AS INT)), (2147483647) AS t(y)
      """
      Then query result
      | result    |
      | 0 seconds |
      | 1 years   |
      | NULL      |
      | NULL      |

