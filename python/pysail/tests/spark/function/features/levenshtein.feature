@levenshtein
Feature: levenshtein() returns edit distance between two strings

  Rule: Basic usage

    Scenario: basic distance
      When query
        """
        SELECT levenshtein('kitten', 'sitting') AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: identical strings
      When query
        """
        SELECT levenshtein('hello', 'hello') AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: empty string vs non-empty
      When query
        """
        SELECT levenshtein('', 'abc') AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: both empty strings
      When query
        """
        SELECT levenshtein('', '') AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: single character difference
      When query
        """
        SELECT levenshtein('abc', 'adc') AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: Threshold (3-argument form)

    Scenario: distance within threshold
      When query
        """
        SELECT levenshtein('kitten', 'sitting', 4) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: distance exceeds threshold
      When query
        """
        SELECT levenshtein('kitten', 'sitting', 2) AS result
        """
      Then query result
        | result |
        | -1     |

    Scenario: distance equals threshold (boundary)
      When query
        """
        SELECT levenshtein('kitten', 'sitting', 3) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: threshold zero with different strings
      When query
        """
        SELECT levenshtein('abc', 'def', 0) AS result
        """
      Then query result
        | result |
        | -1     |

    Scenario: threshold zero with identical strings
      When query
        """
        SELECT levenshtein('abc', 'abc', 0) AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: Null handling

    Scenario: first argument null
      When query
        """
        SELECT levenshtein(CAST(NULL AS STRING), 'hello') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: second argument null
      When query
        """
        SELECT levenshtein('hello', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: both arguments null
      When query
        """
        SELECT levenshtein(CAST(NULL AS STRING), CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: null threshold
      When query
        """
        SELECT levenshtein('kitten', 'sitting', CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Unicode and special characters

    Scenario: unicode strings
      When query
        """
        SELECT levenshtein('café', 'cafe') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: strings with spaces
      When query
        """
        SELECT levenshtein('hello world', 'hello world!') AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: Column expressions

    Scenario: levenshtein on columns from inline table
      When query
        """
        SELECT levenshtein(s1, s2) AS result
        FROM VALUES ('abc', 'abc'), ('abc', 'def'), ('kitten', 'sitting') AS t(s1, s2)
        """
      Then query result
        | result |
        | 0      |
        | 3      |
        | 3      |

    Scenario: threshold on columns from inline table
      When query
        """
        SELECT levenshtein(s1, s2, 2) AS result
        FROM VALUES ('abc', 'abc'), ('abc', 'def'), ('kitten', 'sitting') AS t(s1, s2)
        """
      Then query result
        | result |
        | 0      |
        | -1     |
        | -1     |

  Rule: Per-row threshold (threshold varies per row)

    Scenario: different threshold per row
      When query
        """
        SELECT levenshtein(s1, s2, t) AS result
        FROM VALUES ('abc', 'def', 2), ('abc', 'def', 5), ('abc', 'def', 3) AS t(s1, s2, t)
        ORDER BY t
        """
      Then query result ordered
        | result |
        | -1     |
        | 3      |
        | 3      |

    Scenario: null threshold per row treated as zero
      When query
        """
        SELECT levenshtein(s1, s2, t) AS result
        FROM VALUES ('kitten', 'sitting', CAST(NULL AS INT)), ('abc', 'def', 1) AS t(s1, s2, t)
        ORDER BY t NULLS FIRST
        """
      Then query result ordered
        | result |
        | -1     |
        | -1     |

    Scenario: null threshold per row with identical strings returns zero
      When query
        """
        SELECT levenshtein(s1, s2, t) AS result
        FROM VALUES ('abc', 'abc', CAST(NULL AS INT)) AS t(s1, s2, t)
        """
      Then query result
        | result |
        | 0      |

    Scenario: null strings with threshold in columns
      When query
        """
        SELECT levenshtein(s1, s2, t) AS result
        FROM VALUES
          (CAST(NULL AS STRING), 'hello', 5),
          ('hello', CAST(NULL AS STRING), 5),
          (CAST(NULL AS STRING), CAST(NULL AS STRING), 5),
          ('abc', 'abc', CAST(NULL AS INT)),
          ('abc', 'def', CAST(NULL AS INT))
        AS t(s1, s2, t)
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |
        | NULL   |
        | 0      |
        | -1     |

  Rule: Threshold edge cases

    Scenario: negative threshold returns minus one
      When query
        """
        SELECT levenshtein('kitten', 'sitting', -1) AS result
        """
      Then query result
        | result |
        | -1     |

    Scenario: negative threshold with identical strings returns minus one
      When query
        """
        SELECT levenshtein('abc', 'abc', -1) AS result
        """
      Then query result
        | result |
        | -1     |

    Scenario: very large threshold returns actual distance
      When query
        """
        SELECT levenshtein('abc', 'def', 1000) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: threshold equals one at boundary
      When query
        """
        SELECT levenshtein('abc', 'adc', 1) AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: Case sensitivity and long strings

    Scenario: case sensitive comparison
      When query
        """
        SELECT levenshtein('ABC', 'abc') AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: long strings
      When query
        """
        SELECT levenshtein(REPEAT('a', 100), REPEAT('b', 100)) AS result
        """
      Then query result
        | result |
        | 100    |

