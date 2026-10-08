Feature: regexp_instr returns the 1-based start position of a regex match

  # Spark's regexp_instr(str, regexp[, idx]) returns the 1-based position of the
  # start of the first match, or 0 if there is no match. The optional `idx`
  # ("matched group id") argument is accepted but does NOT change the returned
  # position: Spark always reports the start of the whole match (unlike
  # PostgreSQL/Oracle and DataFusion, whose 3rd argument is the search start).
  # All expected values below were verified against the Spark JVM.

  Rule: Basic position

    Scenario: regexp_instr returns the start of the first match
      When query
        """
        SELECT regexp_instr('1a 2b 14m', '\\d+(a|b|m)') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: regexp_instr reports the position of the first of several matches
      When query
        """
        SELECT regexp_instr('zzz 9q', '\\d+(q)') AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: regexp_instr returns 0 when there is no match
      When query
        """
        SELECT regexp_instr('abc', '\\d+') AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: The idx argument does not affect the returned position

    # idx is the "matched group id" but Spark always returns the start of the
    # whole match, even when the requested group starts elsewhere or does not
    # exist. These cases are exactly where the naive mapping to DataFusion's
    # regexp_instr (3rd arg = search start) diverges.

    Scenario: idx 1 yields the whole-match start
      When query
        """
        SELECT regexp_instr('1a 2b 14m', '\\d+(a|b|m)', 1) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: idx 2 yields the whole-match start, not the group-2 position
      When query
        """
        SELECT regexp_instr('1a 2b 14m', '\\d+(a|b|m)', 2) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: idx is ignored even when the group starts at a different position
      When query
        """
        SELECT regexp_instr('xx1a', '(\\d+)(a)', 2) AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: idx is ignored when the match is not at the start of the string
      When query
        """
        SELECT regexp_instr('zzz 9q', '\\d+(q)', 2) AS result
        """
      Then query result
        | result |
        | 5      |

    Scenario: out-of-range idx does not error and returns the whole-match start
      When query
        """
        SELECT regexp_instr('1a', '(\\d)(a)', 5) AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: Pattern from a column

    Scenario: regexp_instr with the pattern taken from a column
      When query
        """
        SELECT regexp_instr(str, regexp) AS result
        FROM VALUES ('1a 2b 14m', '\\d+(a|b|m)') AS t(str, regexp)
        """
      Then query result
        | result |
        | 1      |
