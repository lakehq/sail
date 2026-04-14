@try_to_time @spark-4
Feature: try_to_time
  Safe variant of to_time that returns NULL on parse failure
  instead of throwing an exception.

  NOTE: TIME type is a preview feature in Spark 4.1.1; the JVM raises
  `[UNSUPPORTED_TIME_TYPE] The data type TIME is not supported`. Sail
  implements TIME fully, so several scenarios are tagged @sail-only
  pending Spark stabilization.

  Rule: Single-argument form parses with default formats

    Scenario: HH:MM:SS basic
      When query
      """
      SELECT try_to_time('10:30:45') AS result
      """
      Then query result
      | result   |
      | 10:30:45 |

    Scenario: HH:MM only
      When query
      """
      SELECT try_to_time('10:30') AS result
      """
      Then query result
      | result   |
      | 10:30:00 |

    Scenario: Microseconds precision
      When query
      """
      SELECT try_to_time('10:30:45.123456') AS result
      """
      Then query result
      | result          |
      | 10:30:45.123456 |

    Scenario: Midnight
      When query
      """
      SELECT try_to_time('00:00:00') AS result
      """
      Then query result
      | result   |
      | 00:00:00 |

    Scenario: Last second of day
      When query
      """
      SELECT try_to_time('23:59:59') AS result
      """
      Then query result
      | result   |
      | 23:59:59 |

    Scenario: Last microsecond of day
      When query
      """
      SELECT try_to_time('23:59:59.999999') AS result
      """
      Then query result
      | result          |
      | 23:59:59.999999 |

    Scenario: Garbage string returns NULL
      When query
      """
      SELECT try_to_time('not-a-time') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Empty string returns NULL
      When query
      """
      SELECT try_to_time('') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Out-of-range hour returns NULL
      When query
      """
      SELECT try_to_time('25:00:00') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Out-of-range minute returns NULL
      When query
      """
      SELECT try_to_time('10:60:00') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-only
    # Sail uses chrono lenient parsing → 60s rolls to next minute.
    # Spark JVM 4.1.1: UNSUPPORTED_TIME_TYPE (TIME not yet supported).
    Scenario: Lenient overflow on seconds (Sail rolls over)
      When query
      """
      SELECT try_to_time('10:30:60') AS result
      """
      Then query result
      | result   |
      | 10:31:00 |

    Scenario: Negative time returns NULL
      When query
      """
      SELECT try_to_time('-01:00:00') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Date-only string returns NULL
      When query
      """
      SELECT try_to_time('2024-01-15') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: NULL input
      When query
      """
      SELECT try_to_time(CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Two-argument form parses with format string

    Scenario: HH-MM-SS custom format
      When query
      """
      SELECT try_to_time('10-30-45', 'HH-mm-ss') AS result
      """
      Then query result
      | result   |
      | 10:30:45 |

    Scenario: HH:MM only with format
      When query
      """
      SELECT try_to_time('10:30', 'HH:mm') AS result
      """
      Then query result
      | result   |
      | 10:30:00 |

    Scenario: Format mismatch returns NULL
      When query
      """
      SELECT try_to_time('10:30:45', 'HH-mm-ss') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: NULL value with format returns NULL
      When query
      """
      SELECT try_to_time(CAST(NULL AS STRING), 'HH:mm:ss') AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Multi-row arrays handle per-row failures

    Scenario: Mixed valid and invalid in batch
      When query
      """
      SELECT try_to_time(t) AS result FROM VALUES ('10:30:45'), ('garbage'), ('00:00:00'), (NULL) AS x(t)
      """
      Then query result
      | result   |
      | 10:30:45 |
      | NULL     |
      | 00:00:00 |
      | NULL     |
