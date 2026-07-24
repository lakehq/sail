@try_to_time @spark-4.1
Feature: try_to_time
  Safe variant of to_time that returns NULL on parse failure
  instead of throwing an exception.

  NOTE: TIME type is a preview feature in Spark 4.x; the JVM raises
  `[UNSUPPORTED_TIME_TYPE] The data type TIME is not supported` for
  any TIME usage. The scenarios below reflect Sail's implementation.
  Once Spark stabilises TIME support, remove @sail-only and update
  expected values to match Spark JVM output. — owners to decide.

  Rule: Single-argument form parses with default formats

    @sail-only
    Scenario: HH:MM:SS basic
      When query
      """
      SELECT try_to_time('10:30:45') AS result
      """
      Then query result
      | result   |
      | 10:30:45 |

    @sail-only
    Scenario: HH:MM only
      When query
      """
      SELECT try_to_time('10:30') AS result
      """
      Then query result
      | result   |
      | 10:30:00 |

    @sail-only
    Scenario: Microseconds precision
      When query
      """
      SELECT try_to_time('10:30:45.123456') AS result
      """
      Then query result
      | result          |
      | 10:30:45.123456 |

    @sail-only
    Scenario: Midnight
      When query
      """
      SELECT try_to_time('00:00:00') AS result
      """
      Then query result
      | result   |
      | 00:00:00 |

    @sail-only
    Scenario: Last second of day
      When query
      """
      SELECT try_to_time('23:59:59') AS result
      """
      Then query result
      | result   |
      | 23:59:59 |

    @sail-only
    Scenario: Last microsecond of day
      When query
      """
      SELECT try_to_time('23:59:59.999999') AS result
      """
      Then query result
      | result          |
      | 23:59:59.999999 |

    @sail-only
    Scenario: Garbage string returns NULL
      When query
      """
      SELECT try_to_time('not-a-time') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-only
    Scenario: Empty string returns NULL
      When query
      """
      SELECT try_to_time('') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-only
    Scenario: Out-of-range hour returns NULL
      When query
      """
      SELECT try_to_time('25:00:00') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-only
    Scenario: Out-of-range minute returns NULL
      When query
      """
      SELECT try_to_time('10:60:00') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-only
    Scenario: Negative time returns NULL
      When query
      """
      SELECT try_to_time('-01:00:00') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-only
    Scenario: Date-only string returns NULL
      When query
      """
      SELECT try_to_time('2024-01-15') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-only
    Scenario: NULL input
      When query
      """
      SELECT try_to_time(CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Two-argument form parses with format string

    @sail-only
    Scenario: HH-MM-SS custom format
      When query
      """
      SELECT try_to_time('10-30-45', 'HH-mm-ss') AS result
      """
      Then query result
      | result   |
      | 10:30:45 |

    @sail-only
    Scenario: HH:MM only with format
      When query
      """
      SELECT try_to_time('10:30', 'HH:mm') AS result
      """
      Then query result
      | result   |
      | 10:30:00 |

    @sail-only
    Scenario: Format mismatch returns NULL
      When query
      """
      SELECT try_to_time('10:30:45', 'HH-mm-ss') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-only
    Scenario: NULL value with format returns NULL
      When query
      """
      SELECT try_to_time(CAST(NULL AS STRING), 'HH:mm:ss') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-only
    Scenario: NULL format returns NULL
      When query
      """
      SELECT try_to_time('10:30:45', NULL) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Non-finite floating-point string literals return NULL

    @sail-only
    Scenario: NaN string returns NULL
      When query
      """
      SELECT try_to_time('NaN') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-only
    Scenario: Infinity string returns NULL
      When query
      """
      SELECT try_to_time('Infinity') AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Per-row format (column-expression format)

    @sail-only
    Scenario: Different format per row all parse
      When query
      """
      SELECT try_to_time(t, f) AS result FROM VALUES
        ('10:30:45', 'HH:mm:ss'),
        ('10-30-45', 'HH-mm-ss') AS x(t, f)
      """
      Then query result
      | result   |
      | 10:30:45 |
      | 10:30:45 |

    @sail-only
    Scenario: Per-row format with NULL format propagates to NULL
      When query
      """
      SELECT try_to_time(t, f) AS result FROM VALUES
        ('10:30:45', 'HH:mm:ss'),
        ('10:30:45', CAST(NULL AS STRING)) AS x(t, f)
      """
      Then query result
      | result   |
      | 10:30:45 |
      | NULL     |

  Rule: Multi-row arrays handle per-row failures

    @sail-only
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
