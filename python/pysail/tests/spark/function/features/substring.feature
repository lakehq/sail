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

    Scenario: substring with pos beyond start of string (3-arg) returns empty
      # Spark adjusts the length when pos is so negative it overshoots the start.
      # effective_start = char_length + pos + 1 = 5 + (-100) + 1 = -94 < 1
      # adjusted_len = max(5 - (1 - (-94)), 0) = max(-90, 0) = 0 → empty string
      # Sail incorrectly clamps pos to 1 without adjusting len → returns "Spark".
      When query
        """
        SELECT substring('Spark', -100, 5) AS result
        """
      Then query result
        | result |
        |        |

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

  Rule: NULL propagation — any NULL argument returns NULL

    Scenario: null string argument (typed) returns null
      When query
        """
        SELECT substring(CAST(NULL AS STRING), 1, 3) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: null position argument (typed INT) returns null in 3-arg form
      When query
        """
        SELECT substring('hello', CAST(NULL AS INT), 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: null position argument (untyped NULL) returns null in 3-arg form
      When query
        """
        SELECT substring('hello', NULL, 2) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: null length argument (typed INT) returns null
      When query
        """
        SELECT substring('hello', 1, CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: null position argument in 2-arg form returns null
      When query
        """
        SELECT substring('hello', CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: all arguments null returns null
      When query
        """
        SELECT substring(NULL, NULL, NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Position argument accepts numeric types beyond INT

    Scenario: position as TINYINT is accepted
      When query
        """
        SELECT substring('hello', CAST(2 AS TINYINT), 2) AS result
        """
      Then query result
        | result |
        | el     |

    Scenario: position as SMALLINT is accepted
      When query
        """
        SELECT substring('hello', CAST(2 AS SMALLINT), 2) AS result
        """
      Then query result
        | result |
        | el     |

    Scenario: position as BIGINT is accepted
      When query
        """
        SELECT substring('hello', CAST(2 AS BIGINT), 2) AS result
        """
      Then query result
        | result |
        | el     |

    Scenario: position as FLOAT is accepted and truncated to int
      When query
        """
        SELECT substring('hello', CAST(2.0 AS FLOAT), 2) AS result
        """
      Then query result
        | result |
        | el     |

    Scenario: position as DOUBLE is accepted and truncated to int
      When query
        """
        SELECT substring('hello', 2.0, 2) AS result
        """
      Then query result
        | result |
        | el     |

    Scenario: position as STRING is accepted and cast to int
      When query
        """
        SELECT substring('hello', '2', 2) AS result
        """
      Then query result
        | result |
        | el     |

    @sail-bug
    Scenario: position as BIGINT overflowing INT raises CAST_OVERFLOW
      # Spark requires pos to fit in INT. BIGINT that overflows INT is an error.
      # Sail bug: silently returns empty string instead of raising CAST_OVERFLOW.
      When query
        """
        SELECT substring('hello', CAST(9999999999 AS BIGINT), 1) AS result
        """
      Then query error .*CAST_OVERFLOW.*

  Rule: Length argument edge cases

    Scenario: negative length returns empty string
      When query
        """
        SELECT substring('hello', 1, -1) AS result
        """
      Then query result
        | result |
        |        |

    Scenario: length larger than remaining string returns available suffix
      When query
        """
        SELECT substring('hello', 3, 2147483647) AS result
        """
      Then query result
        | result |
        | llo    |

  Rule: Position boundary values

    Scenario: position equal to INT_MAX returns empty string
      When query
        """
        SELECT substring('hello', 2147483647, 1) AS result
        """
      Then query result
        | result |
        |        |

    Scenario: position equal to INT_MIN returns empty string
      When query
        """
        SELECT substring('hello', -2147483648, 1) AS result
        """
      Then query result
        | result |
        |        |

    Scenario: very negative position without length returns full string
      # pos so negative it overshoots start: 2-arg form returns full string
      When query
        """
        SELECT substring('hello', -9999) AS result
        """
      Then query result
        | result |
        | hello  |

  Rule: BINARY input is sliced by byte offset, result type is BINARY

    Scenario: binary substring with pos and len returns binary slice
      When query
        """
        SELECT substring(X'48656C6C6F', 1, 3) AS result
        """
      Then query result
        | result     |
        | [48 65 6C] |

    Scenario: binary substring without len returns binary suffix
      When query
        """
        SELECT substring(X'48656C6C6F', 2) AS result
        """
      Then query result
        | result          |
        | [65 6C 6C 6F]   |

    Scenario: binary substring with negative pos returns binary suffix from end
      When query
        """
        SELECT substring(X'48656C6C6F', -2, 2) AS result
        """
      Then query result
        | result  |
        | [6C 6F] |

    Scenario: binary substring with pos=0 is treated as pos=1
      When query
        """
        SELECT substring(X'48656C6C6F', 0, 3) AS result
        """
      Then query result
        | result     |
        | [48 65 6C] |

  Rule: Unicode and multibyte characters are counted by codepoint

    Scenario: two-byte Latin characters counted by codepoint not byte
      When query
        """
        SELECT substring('café', 3, 2) AS result
        """
      Then query result
        | result |
        | fé     |
