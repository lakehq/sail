@try_to_timestamp
Feature: try_to_timestamp
  Safe variant of to_timestamp that returns NULL on parse failure.

  Rule: Single-argument form parses with default formats

    Scenario: ISO timestamp parses
      When query
        """
        SELECT try_to_timestamp('2024-01-15 10:30:45') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:45 |

    Scenario: Date-only parses with midnight time
      When query
        """
        SELECT try_to_timestamp('2024-01-15') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: Microseconds preserved
      When query
        """
        SELECT try_to_timestamp('2024-01-15 10:30:45.123456') AS result
        """
      Then query result
        | result                     |
        | 2024-01-15 10:30:45.123456 |

    Scenario: Cast from date
      When query
        """
        SELECT try_to_timestamp(DATE '2024-01-15') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: Garbage returns NULL
      When query
        """
        SELECT try_to_timestamp('not-a-timestamp') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Empty string returns NULL
      When query
        """
        SELECT try_to_timestamp('') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Invalid month returns NULL
      When query
        """
        SELECT try_to_timestamp('2024-13-15 10:30:45') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL input
      When query
        """
        SELECT try_to_timestamp(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Two-argument form parses with format string

    Scenario: Spark format yyyy-MM-dd HH:mm:ss
      When query
        """
        SELECT try_to_timestamp('2024-01-15 10:30:45', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:45 |

    Scenario: Custom format dd/MM/yyyy
      When query
        """
        SELECT try_to_timestamp('15/01/2024 10:30:45', 'dd/MM/yyyy HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:45 |

    Scenario: Format mismatch returns NULL
      When query
        """
        SELECT try_to_timestamp('2024-01-15', 'dd/MM/yyyy') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL value with format returns NULL
      When query
        """
        SELECT try_to_timestamp(CAST(NULL AS STRING), 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL format returns NULL
      When query
        """
        SELECT try_to_timestamp('2024-01-15 10:30:45', NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Non-finite floating-point string literals return NULL

    Scenario: NaN string returns NULL
      When query
        """
        SELECT try_to_timestamp('NaN') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Infinity string returns NULL
      When query
        """
        SELECT try_to_timestamp('Infinity') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Negative Infinity string returns NULL
      When query
        """
        SELECT try_to_timestamp('-Infinity') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Per-row format (column-expression format)

    Scenario: Different format per row all parse
      When query
        """
        SELECT try_to_timestamp(d, f) AS result FROM VALUES
          ('2024-01-15 10:30:00', 'yyyy-MM-dd HH:mm:ss'),
          ('15/01/2024 10:30:00', 'dd/MM/yyyy HH:mm:ss') AS t(d, f)
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:00 |
        | 2024-01-15 10:30:00 |

    Scenario: Per-row format with NULL format propagates to NULL
      When query
        """
        SELECT try_to_timestamp(d, f) AS result FROM VALUES
          ('2024-01-15 10:30:00', 'yyyy-MM-dd HH:mm:ss'),
          ('2024-01-16 11:00:00', CAST(NULL AS STRING)) AS t(d, f)
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:00 |
        | NULL                |

  Rule: Multi-row arrays handle per-row failures

    Scenario: Mixed valid and invalid in batch
      When query
        """
        SELECT try_to_timestamp(t) AS result FROM VALUES
          ('2024-01-15 10:30:45'),
          ('garbage'),
          ('2024-01-15'),
          (NULL) AS x(t)
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:45 |
        | NULL                |
        | 2024-01-15 00:00:00 |
        | NULL                |
