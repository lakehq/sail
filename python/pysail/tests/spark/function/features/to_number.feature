@to_number
Feature: to_number (shared format parser with to_char)

  Rule: Basic usage

    Scenario: parse integer
      When query
        """
        SELECT to_number('123', '999') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: parse with dollar sign
      When query
        """
        SELECT to_number('$1,234', '$9,999') AS result
        """
      Then query result
        | result |
        | 1234   |

    Scenario: parse decimal
      When query
        """
        SELECT to_number('1.23', '9.99') AS result
        """
      Then query result
        | result |
        | 1.23   |

    Scenario: integer with extra format slots
      When query
        """
        SELECT to_number('12', '999') AS result
        """
      Then query result
        | result |
        | 12     |

    Scenario: zero-padded input
      When query
        """
        SELECT to_number('007', '009') AS result
        """
      Then query result
        | result |
        | 7      |

    Scenario: trailing zero preserved in decimal
      When query
        """
        SELECT to_number('1.50', '9.99') AS result
        """
      Then query result
        | result |
        | 1.50   |

    Scenario: full combo thousands plus decimals
      When query
        """
        SELECT to_number('1,234.56', '9,999.99') AS result
        """
      Then query result
        | result  |
        | 1234.56 |

    Scenario: no integer part
      When query
        """
        SELECT to_number('.5', '.9') AS result
        """
      Then query result
        | result |
        | 0.5    |

  Rule: Sign handling - S

    Scenario: S prefix with negative
      When query
        """
        SELECT to_number('-123', 'S999') AS result
        """
      Then query result
        | result |
        | -123   |

    Scenario: S prefix with positive plus sign
      When query
        """
        SELECT to_number('+123', 'S999') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: S prefix with positive leading blank
      When query
        """
        SELECT to_number(' 123', 'S999') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: trailing S negative
      When query
        """
        SELECT to_number(' 5-', '9S') AS result
        """
      Then query result
        | result |
        | -5     |

    Scenario: trailing S positive
      When query
        """
        SELECT to_number(' 5+', '9S') AS result
        """
      Then query result
        | result |
        | 5      |

  Rule: Sign handling - MI

    Scenario: MI suffix with negative
      When query
        """
        SELECT to_number('123-', '999MI') AS result
        """
      Then query result
        | result |
        | -123   |

    Scenario: MI suffix with positive
      When query
        """
        SELECT to_number('123', '999MI') AS result
        """
      Then query result
        | result |
        | 123    |

  Rule: Sign handling - PR

    Scenario: PR brackets with negative
      When query
        """
        SELECT to_number('<123>', '999PR') AS result
        """
      Then query result
        | result |
        | -123   |

    Scenario: PR with positive padded spaces
      When query
        """
        SELECT to_number(' 123 ', '999PR') AS result
        """
      Then query result
        | result |
        | 123    |

  Rule: Currency

    Scenario: dollar prefix
      When query
        """
        SELECT to_number('$123', '$999') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: dollar prefix with thousands
      When query
        """
        SELECT to_number('$1,234', '$9,999') AS result
        """
      Then query result
        | result |
        | 1234   |

  Rule: Grouping and decimal markers (case-insensitive)

    Scenario: uppercase G grouping
      When query
        """
        SELECT to_number('123', '9G999') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: uppercase D decimal
      When query
        """
        SELECT to_number('1.5', '9D9') AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: lowercase g grouping
      When query
        """
        SELECT to_number('1,234', '9g999') AS result
        """
      Then query result
        | result |
        | 1234   |

    Scenario: lowercase d decimal
      When query
        """
        SELECT to_number('1.5', '9d9') AS result
        """
      Then query result
        | result |
        | 1.5    |

  Rule: NULL combinatorial

    Scenario: NULL string input
      When query
        """
        SELECT to_number(CAST(NULL AS STRING), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: NULL format
      When query
        """
        SELECT to_number('123', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: both NULL
      When query
        """
        SELECT to_number(CAST(NULL AS STRING), CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Multi-row

    Scenario: multi-row with NULL
      When query
        """
        SELECT to_number(v, '999') AS result FROM VALUES
          ('1'),
          ('50'),
          ('100'),
          (CAST(NULL AS STRING)) AS t(v)
        """
      Then query result ordered
        | result |
        | 1      |
        | 50     |
        | 100    |
        | NULL   |

  Rule: Large magnitudes

    Scenario: 20-digit integer
      When query
        """
        SELECT to_number('99999999999999999999', '99999999999999999999') AS result
        """
      Then query result
        | result                 |
        | 99999999999999999999   |

  Rule: Whitespace handling

    Scenario: surrounding spaces in input
      When query
        """
        SELECT to_number(' 123 ', '999') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: leading space only
      When query
        """
        SELECT to_number('  1', '999') AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: Input errors (valid format, wrong input)

    Scenario: input larger than format
      When query
        """
        SELECT to_number('1234', '999') AS result
        """
      Then query error .*

    Scenario: negative input without sign spec
      When query
        """
        SELECT to_number('-123', '999') AS result
        """
      Then query error .*

    Scenario: non-numeric input
      When query
        """
        SELECT to_number('abc', '999') AS result
        """
      Then query error .*

    Scenario: decimal in integer format
      When query
        """
        SELECT to_number('12.3', '999') AS result
        """
      Then query error .*

    Scenario: empty input string
      When query
        """
        SELECT to_number('', '999') AS result
        """
      Then query error .*

    Scenario: dollar suffix not allowed
      When query
        """
        SELECT to_number('123$', '999$') AS result
        """
      Then query error .*

  Rule: Format errors

    Scenario: empty format
      When query
        """
        SELECT to_number('123', '') AS result
        """
      Then query error .*

    Scenario: double S in format
      When query
        """
        SELECT to_number('123', 'SS999') AS result
        """
      Then query error .*

    Scenario: comma at start
      When query
        """
        SELECT to_number('123', ',999') AS result
        """
      Then query error .*

    Scenario: comma at end
      When query
        """
        SELECT to_number('123', '999,') AS result
        """
      Then query error .*

    Scenario: dot only
      When query
        """
        SELECT to_number('1.5', '.') AS result
        """
      Then query error .*

    Scenario: L format rejected
      When query
        """
        SELECT to_number('$1,234', 'L9,999') AS result
        """
      Then query error .*

  Rule: All-null short-circuit must NOT bypass format validation

    # Lock the invariant: format errors fire BEFORE the all-null short-circuit.
    # Moving the `values.null_count() == values.len()` check above the
    # RegexSpec parsing would silence these errors (silent bug).

    Scenario: to_number all-null column with invalid double-S format still errors
      When query
        """
        SELECT to_number(v, 'SS999') AS result FROM VALUES
          (CAST(NULL AS STRING)),
          (CAST(NULL AS STRING))
          AS t(v)
        """
      Then query error .*

    Scenario: to_number all-null column with invalid comma-start format still errors
      When query
        """
        SELECT to_number(v, ',999') AS result FROM VALUES
          (CAST(NULL AS STRING))
          AS t(v)
        """
      Then query error .*

    Scenario: to_number all-null column with invalid dot-only format still errors
      When query
        """
        SELECT to_number(v, '.') AS result FROM VALUES
          (CAST(NULL AS STRING))
          AS t(v)
        """
      Then query error .*

    Scenario: to_number all-null column with VALID format returns all NULL
      When query
        """
        SELECT to_number(v, '999') AS result FROM VALUES
          (CAST(NULL AS STRING)),
          (CAST(NULL AS STRING))
          AS t(v)
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |

  Rule: Format must be a constant literal

    Scenario: non-literal format column reference errors at planning time
      When query
        """
        SELECT to_number('123', fmt) AS result FROM VALUES ('$999') AS t(fmt)
        """
      Then query error .*
