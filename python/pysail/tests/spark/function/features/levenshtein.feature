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
