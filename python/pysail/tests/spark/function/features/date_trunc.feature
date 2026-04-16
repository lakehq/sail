Feature: DATE_TRUNC and TRUNC functions

  Rule: DATE_TRUNC

    Scenario: date_trunc day on timestamp in normal range
      When query
      """
      SELECT DATE_TRUNC('DAY', TIMESTAMP '2026-01-01 12:30:00') AS result
      """
      Then query result
      | result              |
      | 2026-01-01 00:00:00 |

    Scenario: date_trunc week on timestamp in normal range
      When query
      """
      SELECT DATE_TRUNC('WEEK', TIMESTAMP '2026-01-07 12:00:00') AS result
      """
      Then query result
      | result              |
      | 2026-01-05 00:00:00 |

    Scenario: date_trunc month on timestamp in normal range
      When query
      """
      SELECT DATE_TRUNC('MONTH', TIMESTAMP '2026-03-15 10:00:00') AS result
      """
      Then query result
      | result              |
      | 2026-03-01 00:00:00 |

    Scenario: date_trunc year on timestamp in normal range
      When query
      """
      SELECT DATE_TRUNC('YEAR', TIMESTAMP '2026-06-15 10:00:00') AS result
      """
      Then query result
      | result              |
      | 2026-01-01 00:00:00 |

    Scenario: date_trunc quarter on timestamp in normal range
      When query
      """
      SELECT DATE_TRUNC('QUARTER', TIMESTAMP '2026-05-15 10:00:00') AS result
      """
      Then query result
      | result              |
      | 2026-04-01 00:00:00 |

    Scenario: date_trunc hour on timestamp
      When query
      """
      SELECT DATE_TRUNC('HOUR', TIMESTAMP '2026-01-01 12:30:45') AS result
      """
      Then query result
      | result              |
      | 2026-01-01 12:00:00 |

    Scenario: date_trunc minute on timestamp
      When query
      """
      SELECT DATE_TRUNC('MINUTE', TIMESTAMP '2026-01-01 12:30:45') AS result
      """
      Then query result
      | result              |
      | 2026-01-01 12:30:00 |

    Scenario: date_trunc second on timestamp
      When query
      """
      SELECT DATE_TRUNC('SECOND', TIMESTAMP '2026-01-01 12:30:45.123456') AS result
      """
      Then query result
      | result              |
      | 2026-01-01 12:30:45 |

    Scenario: date_trunc week on date before 1677 (out of nanosecond range)
      When query
      """
      SELECT DATE_TRUNC('WEEK', DATE '1500-03-15') AS result
      """
      Then query result
      | result              |
      | 1500-03-12 00:00:00 |

    Scenario: date_trunc month on date before 1677 (out of nanosecond range)
      When query
      """
      SELECT DATE_TRUNC('MONTH', DATE '1500-01-15') AS result
      """
      Then query result
      | result              |
      | 1500-01-01 00:00:00 |

    Scenario: date_trunc year on date before 1677 (out of nanosecond range)
      When query
      """
      SELECT DATE_TRUNC('YEAR', DATE '1500-06-15') AS result
      """
      Then query result
      | result              |
      | 1500-01-01 00:00:00 |

    Scenario: date_trunc quarter on date before 1677 (out of nanosecond range)
      When query
      """
      SELECT DATE_TRUNC('QUARTER', DATE '1500-05-15') AS result
      """
      Then query result
      | result              |
      | 1500-04-01 00:00:00 |

    Scenario: date_trunc week on date after 2262 (out of nanosecond range)
      When query
      """
      SELECT DATE_TRUNC('WEEK', DATE '2300-07-15') AS result
      """
      Then query result
      | result              |
      | 2300-07-09 00:00:00 |

    Scenario: date_trunc NULL format returns NULL
      When query
      """
      SELECT DATE_TRUNC(NULL, DATE '2026-01-01') AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: TRUNC

    Scenario: trunc day on date in normal range
      When query
      """
      SELECT TRUNC(DATE '2026-01-15', 'DAY') AS result
      """
      Then query result
      | result     |
      | 2026-01-15 |

    Scenario: trunc week on date in normal range
      When query
      """
      SELECT TRUNC(DATE '2026-01-07', 'WEEK') AS result
      """
      Then query result
      | result     |
      | 2026-01-05 |

    Scenario: trunc month on date in normal range
      When query
      """
      SELECT TRUNC(DATE '2026-03-15', 'MONTH') AS result
      """
      Then query result
      | result     |
      | 2026-03-01 |

    Scenario: trunc year on date in normal range
      When query
      """
      SELECT TRUNC(DATE '2026-06-15', 'YEAR') AS result
      """
      Then query result
      | result     |
      | 2026-01-01 |

    Scenario: trunc quarter on date in normal range
      When query
      """
      SELECT TRUNC(DATE '2026-05-15', 'QUARTER') AS result
      """
      Then query result
      | result     |
      | 2026-04-01 |

    Scenario: trunc mm alias for month
      When query
      """
      SELECT TRUNC(DATE '2026-03-15', 'MM') AS result
      """
      Then query result
      | result     |
      | 2026-03-01 |

    Scenario: trunc yyyy alias for year
      When query
      """
      SELECT TRUNC(DATE '2026-06-15', 'YYYY') AS result
      """
      Then query result
      | result     |
      | 2026-01-01 |

    Scenario: trunc week on date before 1677 (out of nanosecond range)
      When query
      """
      SELECT TRUNC(DATE '1500-03-15', 'WEEK') AS result
      """
      Then query result
      | result     |
      | 1500-03-12 |

    Scenario: trunc month on date before 1677 (out of nanosecond range)
      When query
      """
      SELECT TRUNC(DATE '1500-01-15', 'MONTH') AS result
      """
      Then query result
      | result     |
      | 1500-01-01 |

    Scenario: trunc year on date before 1677 (out of nanosecond range)
      When query
      """
      SELECT TRUNC(DATE '1500-06-15', 'YEAR') AS result
      """
      Then query result
      | result     |
      | 1500-01-01 |

    Scenario: trunc week on date after 2262 (out of nanosecond range)
      When query
      """
      SELECT TRUNC(DATE '2300-07-15', 'WEEK') AS result
      """
      Then query result
      | result     |
      | 2300-07-09 |

    Scenario: trunc NULL format returns NULL
      When query
      """
      SELECT TRUNC(DATE '2026-01-01', NULL) AS result
      """
      Then query result
      | result |
      | NULL   |
