@try_to_number
Feature: try_to_number comprehensive tests (safe version of to_number)

  Rule: Argument count validation

    Scenario: try_to_number zero arguments errors
      When query
        """
        SELECT try_to_number() AS result
        """
      Then query error .*

    Scenario: try_to_number one argument errors
      When query
        """
        SELECT try_to_number('123') AS result
        """
      Then query error .*

    Scenario: try_to_number three arguments errors
      When query
        """
        SELECT try_to_number('123', '999', 'extra') AS result
        """
      Then query error .*

  Rule: NULL combinatorial

    Scenario: try_to_number NULL value
      When query
        """
        SELECT try_to_number(CAST(NULL AS STRING), '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_to_number NULL format
      When query
        """
        SELECT try_to_number('123', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_to_number both NULL
      When query
        """
        SELECT try_to_number(CAST(NULL AS STRING), CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Basic parsing (same as to_number)

    Scenario: try_to_number basic integer
      When query
        """
        SELECT try_to_number('123', '999') AS result
        """
      Then query result
        | result |
        | 123    |

    Scenario: try_to_number decimal
      When query
        """
        SELECT try_to_number('1.23', '9.99') AS result
        """
      Then query result
        | result |
        | 1.23   |

    Scenario: try_to_number with dollar
      When query
        """
        SELECT try_to_number('$1,234', '$9,999') AS result
        """
      Then query result
        | result |
        | 1234   |

    Scenario: try_to_number S negative
      When query
        """
        SELECT try_to_number('-123', 'S999') AS result
        """
      Then query result
        | result |
        | -123   |

    Scenario: try_to_number G separator
      When query
        """
        SELECT try_to_number('12,345', '99G999') AS result
        """
      Then query result
        | result |
        | 12345  |

    Scenario: try_to_number D separator
      When query
        """
        SELECT try_to_number('123.45', '999D99') AS result
        """
      Then query result
        | result |
        | 123.45 |

  Rule: Safe behavior (errors become NULL)

    Scenario: try_to_number invalid string returns NULL
      When query
        """
        SELECT try_to_number('abc', '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_to_number format mismatch returns NULL
      When query
        """
        SELECT try_to_number('$123', '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_to_number all spaces returns NULL
      When query
        """
        SELECT try_to_number('   ', '999') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_to_number empty value returns NULL
      When query
        """
        SELECT try_to_number('', '999') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Multi-row with mixed valid and invalid

    Scenario: try_to_number multi-row with invalid
      When query
        """
        SELECT try_to_number(v, '999') AS result FROM VALUES ('123'), ('abc'), (' 42'), (NULL), (''), ('  0') AS t(v)
        """
      Then query result
        | result |
        | 123    |
        | NULL   |
        | 42     |
        | NULL   |
        | NULL   |
        | 0      |

  Rule: Error conditions (type errors still error)

    Scenario: try_to_number empty format errors
      When query
        """
        SELECT try_to_number('123', '') AS result
        """
      Then query error .*
