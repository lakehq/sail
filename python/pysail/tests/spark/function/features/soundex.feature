@soundex
Feature: soundex() returns the Soundex code of a string

  Rule: Basic usage

    Scenario: standard word
      When query
        """
        SELECT soundex('Robert') AS result
        """
      Then query result
        | result |
        | R163   |

    Scenario: similar sounding names produce same code
      When query
        """
        SELECT soundex('Robert') = soundex('Rupert') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: different sounding names produce different codes
      When query
        """
        SELECT soundex('Robert') = soundex('Smith') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: single character
      When query
        """
        SELECT soundex('A') AS result
        """
      Then query result
        | result |
        | A000   |

    Scenario: all same code letters
      When query
        """
        SELECT soundex('BFPV') AS result
        """
      Then query result
        | result |
        | B000   |

  Rule: Edge cases

    Scenario: empty string
      When query
        """
        SELECT soundex('') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: numeric string returns input unchanged
      When query
        """
        SELECT soundex('123') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: non-alpha first character returns input unchanged
      When query
        """
        SELECT soundex('123abc') AS result
        """
      Then query result
        | result |
        | 123abc |

    Scenario: space first character returns input unchanged
      When query
        """
        SELECT soundex(' abc') AS result
        """
      Then query result
        | result |
        |  abc   |

    Scenario: null input returns null
      When query
        """
        SELECT soundex(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Non-alpha characters after first letter act as separators

    Scenario: digit separates same-code letters
      When query
        """
        SELECT soundex('B1F') AS result
        """
      Then query result
        | result |
        | B100   |

    Scenario: space separates same-code letters
      When query
        """
        SELECT soundex('B F') AS result
        """
      Then query result
        | result |
        | B100   |

    Scenario: letters with embedded digits
      When query
        """
        SELECT soundex('a1bc') AS result
        """
      Then query result
        | result |
        | A120   |

  Rule: H and W handling (ignored separators)

    Scenario: H and W do not separate identical codes
      When query
        """
        SELECT soundex('Ashcraft') AS result
        """
      Then query result
        | result |
        | A261   |

    Scenario: vowel separates identical codes
      When query
        """
        SELECT soundex('Tymczak') AS result
        """
      Then query result
        | result |
        | T522   |

  Rule: Column expressions

    Scenario: soundex on column values
      When query
        """
        SELECT soundex(name) AS result
        FROM VALUES ('Robert'), ('Rupert'), ('Smith') AS t(name)
        """
      Then query result
        | result |
        | R163   |
        | R163   |
        | S530   |

    Scenario: soundex with null in column
      When query
        """
        SELECT soundex(name) AS result
        FROM VALUES ('Hello'), (CAST(NULL AS STRING)), ('World') AS t(name)
        """
      Then query result
        | result |
        | H400   |
        | NULL   |
        | W643   |
