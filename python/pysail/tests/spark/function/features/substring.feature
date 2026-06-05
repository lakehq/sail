@substring
Feature: substring() and substr() extract substrings

  Rule: Basic usage with positive positions (1-based)

    Scenario: substring with pos=1 returns full length from start
      When query
        """
        SELECT substring('Spark SQL', 1, 4) AS result
        """
      Then query result
        | result |
        | Spar   |

    Scenario: substring with pos=5 returns from that position
      When query
        """
        SELECT substring('Spark SQL', 5, 1) AS result
        """
      Then query result
        | result |
        | k      |

    Scenario: substring without length returns tail
      When query
        """
        SELECT substring('Spark SQL', 7) AS result
        """
      Then query result
        | result |
        | SQL    |

    Scenario: substr is an alias for substring
      When query
        """
        SELECT substr('Spark SQL', 1, 5) AS result
        """
      Then query result
        | result |
        | Spark  |

  Rule: Position zero is treated as position one (Spark semantics)

    Scenario: substring with pos=0 returns same as pos=1
      When query
        """
        SELECT substring('Spark SQL', 0, 5) AS result
        """
      Then query result
        | result |
        | Spark  |

    Scenario: substring with pos=0 returns full requested length
      When query
        """
        SELECT substring('abcdefghijklmno', 0, 15) AS result
        """
      Then query result
        | result          |
        | abcdefghijklmno |

    Scenario: substr with pos=0 returns tail same as pos=1
      When query
        """
        SELECT substr('Spark SQL', 0) AS result
        """
      Then query result
        | result    |
        | Spark SQL |

  Rule: Negative positions count from the end of the string

    Scenario: substring with pos=-3 starts 3 chars from the end
      When query
        """
        SELECT substring('Spark SQL', -3, 3) AS result
        """
      Then query result
        | result |
        | SQL    |

    Scenario: substring with pos=-1 starts at last character
      When query
        """
        SELECT substring('Spark SQL', -1, 1) AS result
        """
      Then query result
        | result |
        | L      |

    Scenario: substr with negative pos returns tail from that position
      When query
        """
        SELECT substr('Spark SQL', -3) AS result
        """
      Then query result
        | result |
        | SQL    |

  Rule: Edge cases

    Scenario: substring length exceeds remaining string
      When query
        """
        SELECT substring('Spark', 3, 100) AS result
        """
      Then query result
        | result |
        | ark    |

    Scenario: substring with zero length returns empty string
      When query
        """
        SELECT substring('Spark SQL', 1, 0) AS result
        """
      Then query result
        | result |
        |        |

    Scenario: substring on null returns null
      When query
        """
        SELECT substring(CAST(NULL AS STRING), 1, 3) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: substring on column values with pos=0
      When query
        """
        SELECT substring(id, 0, 15) AS result
        FROM VALUES ('abcdefghijklmno') AS t(id)
        """
      Then query result
        | result          |
        | abcdefghijklmno |
