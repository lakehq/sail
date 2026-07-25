@regexp_extract
Feature: regexp_extract() extracts regex capture groups from strings

  Rule: Basic extraction

    Scenario: regexp_extract with group index 1 (default)
      When query
        """
        SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 1) AS result
        """
      Then query result
        | result |
        | 100    |

    Scenario: regexp_extract with group index 2
      When query
        """
        SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 2) AS result
        """
      Then query result
        | result |
        | 200    |

    Scenario: regexp_extract with group index 0 returns entire match
      When query
        """
        SELECT regexp_extract('hello 123 world', '(\\d+)', 0) AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: regexp_extract defaults to group index 1
      When query
        """
        SELECT regexp_extract('abc-def', '([a-z]+)-([a-z]+)') AS result
        """
      Then query result
        | result |
        | abc    |

  Rule: Multiple groups

    Scenario: regexp_extract on date-like string group 1
      When query
        """
        SELECT regexp_extract('2024-01-15', '(\\d+)-(\\d+)-(\\d+)', 1) AS result
        """
      Then query result
        | result |
        | 2024   |

    Scenario: regexp_extract on date-like string group 2
      When query
        """
        SELECT regexp_extract('2024-01-15', '(\\d+)-(\\d+)-(\\d+)', 2) AS result
        """
      Then query result
        | result |
        | 01     |

    Scenario: regexp_extract on date-like string group 3
      When query
        """
        SELECT regexp_extract('2024-01-15', '(\\d+)-(\\d+)-(\\d+)', 3) AS result
        """
      Then query result
        | result |
        | 15     |

    Scenario: regexp_extract returns empty string for unmatched optional group
      When query
        """
        SELECT regexp_extract('aaaac', '(a+)(b)?(c)', 2) AS result
        """
      Then query result
        | result |
        |        |

    Scenario: regexp_extract preserves later groups after unmatched optional group
      When query
        """
        SELECT regexp_extract('aaaac', '(a+)(b)?(c)', 3) AS result
        """
      Then query result
        | result |
        | c      |

  Rule: No match and edge cases

    Scenario: regexp_extract returns empty string when no match
      When query
        """
        SELECT regexp_extract('hello', '(\\d+)', 1) AS result
        """
      Then query result
        | result |
        |        |

    Scenario: regexp_extract with anchored pattern at beginning
      When query
        """
        SELECT regexp_extract('123abc', '^(\\d+)', 1) AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: regexp_extract with anchored pattern at end
      When query
        """
        SELECT regexp_extract('abc123', '(\\d+)$', 1) AS result
        """
      Then query result
        | result |
        | 123    |

  @spark_null
  Rule: Output schema

    @sail-bug
    Scenario: a non-null string literal yields a non-nullable string
      When query
        """
        SELECT regexp_extract('abc123', '[0-9]+', 0) AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    @sail-bug
    Scenario: a non-null string column yields a non-nullable string
      When query
        """
        SELECT regexp_extract(CAST(id AS STRING), '[0-9]', 0) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT regexp_extract(c, '[0-9]', 0) AS result FROM VALUES ('a1'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """
