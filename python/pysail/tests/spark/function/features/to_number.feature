@to_number
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

