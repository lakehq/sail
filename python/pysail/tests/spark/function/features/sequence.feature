@sequence
Feature: sequence comprehensive tests

  Rule: Argument count validation

    Scenario: sequence zero arguments errors
      When query
        """
        SELECT sequence() AS result
        """
      Then query error .*

    Scenario: sequence one argument errors
      When query
        """
        SELECT sequence(1) AS result
        """
      Then query error .*

  Rule: NULL combinatorial

    Scenario: sequence NULL start
      When query
        """
        SELECT sequence(CAST(NULL AS INT), 5) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: sequence NULL stop
      When query
        """
        SELECT sequence(1, CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: sequence NULL step
      When query
        """
        SELECT sequence(1, 5, CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: sequence all NULL
      When query
        """
        SELECT sequence(CAST(NULL AS INT), CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Basic integer sequences

    Scenario: sequence ascending
      When query
        """
        SELECT sequence(1, 5) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

    Scenario: sequence descending
      When query
        """
        SELECT sequence(5, 1) AS result
        """
      Then query result
        | result          |
        | [5, 4, 3, 2, 1] |

    Scenario: sequence equal start and stop
      When query
        """
        SELECT sequence(3, 3) AS result
        """
      Then query result
        | result |
        | [3]    |

    Scenario: sequence zero to zero
      When query
        """
        SELECT sequence(0, 0) AS result
        """
      Then query result
        | result |
        | [0]    |

  Rule: With explicit step

    Scenario: sequence with positive step
      When query
        """
        SELECT sequence(1, 10, 2) AS result
        """
      Then query result
        | result          |
        | [1, 3, 5, 7, 9] |

    Scenario: sequence with negative step
      When query
        """
        SELECT sequence(10, 1, -2) AS result
        """
      Then query result
        | result           |
        | [10, 8, 6, 4, 2] |

    Scenario: sequence step of one
      When query
        """
        SELECT sequence(1, 5, 1) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

    Scenario: sequence step larger than range
      When query
        """
        SELECT sequence(1, 5, 10) AS result
        """
      Then query result
        | result |
        | [1]    |

    Scenario: sequence negative step larger than range
      When query
        """
        SELECT sequence(5, 1, -10) AS result
        """
      Then query result
        | result |
        | [5]    |

  Rule: Negative ranges

    Scenario: sequence negative ascending
      When query
        """
        SELECT sequence(-5, -1) AS result
        """
      Then query result
        | result               |
        | [-5, -4, -3, -2, -1] |

    Scenario: sequence negative descending
      When query
        """
        SELECT sequence(-1, -5) AS result
        """
      Then query result
        | result               |
        | [-1, -2, -3, -4, -5] |

  Rule: Type coercion

    Scenario: sequence BIGINT
      When query
        """
        SELECT sequence(CAST(1 AS BIGINT), CAST(5 AS BIGINT)) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

    Scenario: sequence BIGINT descending
      When query
        """
        SELECT sequence(CAST(5 AS BIGINT), CAST(1 AS BIGINT)) AS result
        """
      Then query result
        | result          |
        | [5, 4, 3, 2, 1] |

    Scenario: sequence TINYINT
      When query
        """
        SELECT sequence(CAST(1 AS TINYINT), CAST(5 AS TINYINT)) AS result
        """
      Then query result
        | result          |
        | [1, 2, 3, 4, 5] |

  Rule: Timestamp sequences

    Scenario: sequence timestamp with day interval
      When query
        """
        SELECT sequence(TIMESTAMP'2024-01-01', TIMESTAMP'2024-01-05', INTERVAL 1 DAY) AS result
        """
      Then query result
        | result                                                                                            |
        | [2024-01-01 00:00:00, 2024-01-02 00:00:00, 2024-01-03 00:00:00, 2024-01-04 00:00:00, 2024-01-05 00:00:00] |

  Rule: Multi-row

    Scenario: sequence multi-row
      When query
        """
        SELECT sequence(a, b) AS result FROM VALUES (1, 3), (5, 5), (3, 1) AS t(a, b)
        """
      Then query result
        | result    |
        | [1, 2, 3] |
        | [5]       |
        | [3, 2, 1] |

  Rule: Error conditions

    Scenario: sequence step zero errors
      When query
        """
        SELECT sequence(1, 5, 0) AS result
        """
      Then query error .*

    Scenario: sequence step wrong direction errors
      When query
        """
        SELECT sequence(1, 5, -1) AS result
        """
      Then query error .*

    Scenario: sequence string input errors
      When query
        """
        SELECT sequence('a', 'z') AS result
        """
      Then query error .*
