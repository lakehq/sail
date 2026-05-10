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

    Scenario: Full args with all-ones
      When query
      """
      SELECT CAST(make_interval(1, 1, 1, 1, 1, 1, 1.0) AS STRING) AS result
      """
      Then query result
      | result                                              |
      | 1 years 1 months 8 days 1 hours 1 minutes 1 seconds |

  Rule: Per-row NULL propagation across all fields

    Scenario: Each field NULL in turn yields NULL row
      When query
      """
      SELECT CAST(make_interval(y, m, w, d, h, mi, sec) AS STRING) AS result FROM VALUES
        (CAST(NULL AS INT), 2, 3, 4, 5, 6, CAST(7.5 AS DOUBLE)),
        (1, CAST(NULL AS INT), 3, 4, 5, 6, CAST(7.5 AS DOUBLE)),
        (1, 2, CAST(NULL AS INT), 4, 5, 6, CAST(7.5 AS DOUBLE)),
        (1, 2, 3, CAST(NULL AS INT), 5, 6, CAST(7.5 AS DOUBLE)),
        (1, 2, 3, 4, CAST(NULL AS INT), 6, CAST(7.5 AS DOUBLE)),
        (1, 2, 3, 4, 5, CAST(NULL AS INT), CAST(7.5 AS DOUBLE)),
        (1, 2, 3, 4, 5, 6, CAST(NULL AS DOUBLE)) AS t(y, m, w, d, h, mi, sec)
      """
      Then query result
      | result |
      | NULL   |
      | NULL   |
      | NULL   |
      | NULL   |
      | NULL   |
      | NULL   |
      | NULL   |

  Rule: Non-finite seconds yield NULL

    Scenario: NaN seconds returns NULL
      When query
      """
      SELECT CAST(make_interval(0, 0, 0, 0, 0, 0, CAST('NaN' AS DOUBLE)) AS STRING) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Infinity seconds returns NULL
      When query
      """
      SELECT CAST(make_interval(0, 0, 0, 0, 0, 0, CAST('Infinity' AS DOUBLE)) AS STRING) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Overflow raises error

    @spark-4
    Scenario: Year overflow raises error
      When query
      """
      SELECT make_interval(2147483647)
      """
      Then query error overflow|month_day_nano_interval|ARITHMETIC|calendar_interval|Unsupported

  Rule: Argument count validation matches Spark

    @spark-4
    Scenario: No args raises WRONG_NUM_ARGS
      When query
      """
      SELECT make_interval()
      """
      Then query error WRONG_NUM_ARGS|UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION|month_day_nano_interval|calendar_interval|Unsupported
