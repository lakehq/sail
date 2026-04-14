@try_to_date
Feature: try_to_date
  Safe variant of to_date that returns NULL on parse failure
  instead of throwing an exception. Strict to_date throws.

  Rule: Single-argument form parses with default formats

    Scenario: ISO date string
      When query
      """
      SELECT try_to_date('2024-01-15') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    Scenario: Beginning of epoch
      When query
      """
      SELECT try_to_date('1970-01-01') AS result
      """
      Then query result
      | result     |
      | 1970-01-01 |

    Scenario: Leap year valid date
      When query
      """
      SELECT try_to_date('2024-02-29') AS result
      """
      Then query result
      | result     |
      | 2024-02-29 |

    Scenario: Non-leap year invalid date returns NULL
      When query
      """
      SELECT try_to_date('2023-02-29') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Garbage string returns NULL
      When query
      """
      SELECT try_to_date('not-a-date') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Empty string returns NULL
      When query
      """
      SELECT try_to_date('') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Trailing garbage parses prefix (matches Spark lenient behavior)
      When query
      """
      SELECT try_to_date('2024-01-15 garbage') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    Scenario: Out-of-range month returns NULL
      When query
      """
      SELECT try_to_date('2024-13-01') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Out-of-range day returns NULL
      When query
      """
      SELECT try_to_date('2024-01-32') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Year boundary low
      When query
      """
      SELECT try_to_date('0001-01-01') AS result
      """
      Then query result
      | result     |
      | 0001-01-01 |

    Scenario: Year boundary high
      When query
      """
      SELECT try_to_date('9999-12-31') AS result
      """
      Then query result
      | result     |
      | 9999-12-31 |

    Scenario: NULL input
      When query
      """
      SELECT try_to_date(CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Cast from timestamp preserves date
      When query
      """
      SELECT try_to_date(TIMESTAMP '2024-01-15 10:30:00') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    Scenario: Cast from date is identity
      When query
      """
      SELECT try_to_date(DATE '2024-01-15') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

  Rule: Two-argument form parses with format string

    Scenario: Spark yyyy-MM-dd format
      When query
      """
      SELECT try_to_date('2024-01-15', 'yyyy-MM-dd') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    Scenario: US-style MM/dd/yyyy format
      When query
      """
      SELECT try_to_date('01/15/2024', 'MM/dd/yyyy') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    Scenario: European dd-MM-yyyy format
      When query
      """
      SELECT try_to_date('15-01-2024', 'dd-MM-yyyy') AS result
      """
      Then query result
      | result     |
      | 2024-01-15 |

    Scenario: Format mismatch returns NULL
      When query
      """
      SELECT try_to_date('2024-01-15', 'MM/dd/yyyy') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: Garbage with format returns NULL
      When query
      """
      SELECT try_to_date('garbage', 'yyyy-MM-dd') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: NULL value with format returns NULL
      When query
      """
      SELECT try_to_date(CAST(NULL AS STRING), 'yyyy-MM-dd') AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Per-row format (column-expression format)

    Scenario: Different format per row all parse
      When query
      """
      SELECT try_to_date(d, f) AS result FROM VALUES
        ('2024-01-15', 'yyyy-MM-dd'),
        ('15/01/2024', 'dd/MM/yyyy'),
        ('Jan 15 2024', 'MMM dd yyyy') AS t(d, f)
      """
      Then query result
      | result     |
      | 2024-01-15 |
      | 2024-01-15 |
      | 2024-01-15 |

    Scenario: Per-row format with one invalid row returns NULL only there
      When query
      """
      SELECT try_to_date(d, f) AS result FROM VALUES
        ('2024-01-15', 'yyyy-MM-dd'),
        ('garbage', 'yyyy-MM-dd'),
        ('15/01/2024', 'dd/MM/yyyy') AS t(d, f)
      """
      Then query result
      | result     |
      | 2024-01-15 |
      | NULL       |
      | 2024-01-15 |

    Scenario: NULL format propagates to NULL result for that row
      When query
      """
      SELECT try_to_date(d, f) AS result FROM VALUES
        ('2024-01-15', 'yyyy-MM-dd'),
        ('2024-01-16', CAST(NULL AS STRING)) AS t(d, f)
      """
      Then query result
      | result     |
      | 2024-01-15 |
      | NULL       |

  Rule: Multi-row arrays handle per-row failures

    Scenario: Mixed valid and invalid in batch
      When query
      """
      SELECT try_to_date(d) AS result FROM VALUES ('2024-01-15'), ('garbage'), ('2024-02-29'), (NULL) AS t(d)
      """
      Then query result
      | result     |
      | 2024-01-15 |
      | NULL       |
      | 2024-02-29 |
      | NULL       |
