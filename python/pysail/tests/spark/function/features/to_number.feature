Feature: to_number comprehensive tests

  Rule: Argument count validation

    Scenario: to_number zero arguments errors
      When query
        """
        SELECT to_number() AS result
        """
      Then query error .*

    Scenario: to_number one argument errors
      When query
        """
        SELECT to_number('123') AS result
        """
      Then query error .*

    Scenario: to_number three arguments errors
      When query
        """
        SELECT to_number('123', '999', 'extra') AS result
        """
      Then query error .*

  Rule: NULL combinatorial

    Scenario: to_number NULL value
      When query
        """
        SELECT to_number(CAST(NULL AS STRING), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_number NULL format returns NULL
      When query
        """
        SELECT to_number('123', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_number both NULL returns NULL
      When query
        """
        SELECT to_number(CAST(NULL AS STRING), CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Basic parsing

    Scenario: to_number basic integer
      When query
        """
        SELECT to_number('123', '999') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: to_number zero-padded
      When query
        """
        SELECT to_number('00042', '00000') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: to_number with comma separator
      When query
        """
        SELECT to_number('12,345', '99,999') AS result
        """
      Then query result
        | result |
        | 12345  |

    Scenario: to_number with decimal
      When query
        """
        SELECT to_number('1.23', '9.99') AS result
        """
      Then query result
        | result |
        | 1.23   |

    Scenario: to_number with dollar sign
      When query
        """
        SELECT to_number('$1,234', '$9,999') AS result
        """
      Then query result
        | result |
        | 1234   |

    Scenario: to_number zero
      When query
        """
        SELECT to_number('0', '9') AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: to_number leading spaces
      When query
        """
        SELECT to_number('  42', '999') AS result
        """
      Then query result
        | result |
        | 42     |

  Rule: G and D separators

    Scenario: to_number G separator
      When query
        """
        SELECT to_number('12,345', '99G999') AS result
        """
      Then query result
        | result |
        | 12345  |

    Scenario: to_number D separator
      When query
        """
        SELECT to_number('123.45', '999D99') AS result
        """
      Then query result
        | result |
        | 123.45 |

  Rule: Sign handling

    Scenario: to_number S prefix negative
      When query
        """
        SELECT to_number('-123', 'S999') AS result
        """
      Then query result
        | result |
        | -123   |

    Scenario: to_number S prefix positive
      When query
        """
        SELECT to_number('+123', 'S999') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: to_number S suffix negative
      When query
        """
        SELECT to_number('123-', '999S') AS result
        """
      Then query result
        | result |
        | -123   |

    Scenario: to_number S suffix positive
      When query
        """
        SELECT to_number('123+', '999S') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: to_number MI prefix negative
      When query
        """
        SELECT to_number('-123', 'MI999') AS result
        """
      Then query result
        | result |
        | -123   |

    Scenario: to_number MI prefix space
      When query
        """
        SELECT to_number(' 123', 'MI999') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: to_number PR negative
      When query
        """
        SELECT to_number('<123>', '999PR') AS result
        """
      Then query result
        | result |
        | -123   |

    Scenario: to_number PR positive
      When query
        """
        SELECT to_number(' 123 ', '999PR') AS result
        """
      Then query result
        | result |
        | 123    |

  Rule: L format rejected

    Scenario: to_number L format rejected
      When query
        """
        SELECT to_number('$1,234', 'L9,999') AS result
        """
      Then query error .*

  Rule: Multi-row

    Scenario: to_number multi-row
      When query
        """
        SELECT to_number(v, '999') AS result FROM VALUES ('123'), ('  0'), (' 42') AS t(v)
        """
      Then query result
        | result |
        | 123    |
        | 0      |
        | 42     |

  Rule: Error conditions

    Scenario: to_number mismatched input errors
      When query
        """
        SELECT to_number('abc', '999') AS result
        """
      Then query error .*

    Scenario: to_number all spaces errors
      When query
        """
        SELECT to_number('   ', '999') AS result
        """
      Then query error .*

    Scenario: to_number empty value errors
      When query
        """
        SELECT to_number('', '999') AS result
        """
      Then query error .*

    Scenario: to_number empty format errors
      When query
        """
        SELECT to_number('123', '') AS result
        """
      Then query error .*


  Rule: Basic usage

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
  Rule: Currency

    Scenario: dollar prefix
      When query
        """
        SELECT to_number('$123', '$999') AS result
        """
      Then query result
        | result |
        | 123    |
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

    Scenario: decimal in integer format
      When query
        """
        SELECT to_number('12.3', '999') AS result
        """
      Then query error .*

    Scenario: dollar suffix not allowed
      When query
        """
        SELECT to_number('123$', '999$') AS result
        """
      Then query error .*
  Rule: Format errors

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
  Rule: All-null short-circuit must NOT bypass format validation

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
