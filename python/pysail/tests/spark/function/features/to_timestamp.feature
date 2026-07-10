@to_timestamp
Feature: to_timestamp (strict variant)
  Strict to_timestamp that throws on invalid input,
  contrasting with try_to_timestamp which returns NULL.

  Rule: Valid input parses

    Scenario: ISO timestamp
      When query
        """
        SELECT to_timestamp('2024-01-15 10:30:45') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:45 |

    Scenario: Date-only parses with midnight
      When query
        """
        SELECT to_timestamp('2024-01-15') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: With format
      When query
        """
        SELECT to_timestamp('2024-01-15 10:30:45', 'yyyy-MM-dd HH:mm:ss') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:45 |

    Scenario: Cast from date
      When query
        """
        SELECT to_timestamp(DATE '2024-01-15') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 00:00:00 |

    Scenario: Cast from timestamp
      When query
        """
        SELECT to_timestamp(TIMESTAMP '2024-01-15 10:30:45') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:45 |

  Rule: Invalid input honors ANSI mode
    # to_timestamp errors on invalid input under ANSI and returns NULL otherwise.

    Scenario: Garbage string under ANSI on errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('not-a-timestamp') AS result
        """
      Then query error .*

    Scenario: Garbage string under ANSI off returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp('not-a-timestamp') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Format mismatch under ANSI on errors
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp('2024-01-15', 'dd/MM/yyyy') AS result
        """
      Then query error .*

    Scenario: Format mismatch under ANSI off returns NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp('2024-01-15', 'dd/MM/yyyy') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL input propagates

    Scenario: NULL input returns NULL
      When query
        """
        SELECT to_timestamp(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL format returns NULL
      When query
        """
        SELECT to_timestamp('2024-01-15 10:30:45', NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Timezone handling — LTZ applies offset, NTZ ignores it
    # Validated against Spark JVM with session tz America/New_York.

    Scenario: LTZ applies trailing Z (UTC) and renders in session tz
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT to_timestamp('2024-01-15 10:30:45Z') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 05:30:45 |

    Scenario: LTZ applies explicit offset
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT to_timestamp('2024-06-15 10:30:45-08:00') AS result
        """
      Then query result
        | result              |
        | 2024-06-15 14:30:45 |

    Scenario: NTZ ignores trailing Z (keeps wall clock)
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT to_timestamp_ntz('2024-01-15 10:30:45Z') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:45 |

    Scenario: NTZ ignores explicit offset
      When query
        """
        SELECT to_timestamp_ntz('2024-06-15 10:30:45-08:00') AS result
        """
      Then query result
        | result              |
        | 2024-06-15 10:30:45 |

  Rule: Fractional seconds, separators, boundaries

    Scenario: T separator parses
      When query
        """
        SELECT to_timestamp('2024-01-15T10:30:45') AS result
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:45 |

    Scenario: Fractional seconds truncate to microseconds
      When query
        """
        SELECT to_timestamp('2024-01-15 10:30:45.123456789') AS result
        """
      Then query result
        | result                     |
        | 2024-01-15 10:30:45.123456 |

    Scenario: Single-digit fractional second
      When query
        """
        SELECT to_timestamp('2024-01-15 10:30:45.1') AS result
        """
      Then query result
        | result                |
        | 2024-01-15 10:30:45.1 |

    Scenario: Leap day
      When query
        """
        SELECT to_timestamp_ntz('2024-02-29 12:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-02-29 12:00:00 |

    Scenario: Upper boundary
      When query
        """
        SELECT to_timestamp_ntz('9999-12-31 23:59:59') AS result
        """
      Then query result
        | result              |
        | 9999-12-31 23:59:59 |

  Rule: Per-row format (column-expression format)

    Scenario: Different format per row all parse
      When query
        """
        SELECT to_timestamp(d, f) AS result FROM VALUES
          ('2024-01-15 10:30:00', 'yyyy-MM-dd HH:mm:ss'),
          ('15/01/2024 10:30:00', 'dd/MM/yyyy HH:mm:ss') AS t(d, f)
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:00 |
        | 2024-01-15 10:30:00 |
