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

  Rule: Result values (migrated from test_try_to_number.txt doctests)

    Scenario: try_to_number doctest #1 (result)
      When query
        """
        SELECT try_to_number('<-$12,345.67>', 'S$999,099.99PR') AS result, typeof(try_to_number('<-$12,345.67>', 'S$999,099.99PR')) AS type
        """
      Then query result
        | result | type |
        | 12345.67 | decimal(8,2) |

    Scenario: try_to_number doctest #2 (result)
      When query
        """
        SELECT try_to_number('<$1,212,345.67>', 'S$0,000,000.99PR') AS result, typeof(try_to_number('<$1,212,345.67>', 'S$0,000,000.99PR')) AS type
        """
      Then query result
        | result | type |
        | -1212345.67 | decimal(9,2) |

    Scenario: try_to_number doctest #3 (result)
      When query
        """
        SELECT try_to_number('$345', 'S$999,099.99') AS result, typeof(try_to_number('$345', 'S$999,099.99')) AS type
        """
      Then query result
        | result | type |
        | 345.00 | decimal(8,2) |

    Scenario: try_to_number doctest #4 (result)
      When query
        """
        SELECT try_to_number('$045', 'S$999,099.99') AS result, typeof(try_to_number('$045', 'S$999,099.99')) AS type
        """
      Then query result
        | result | type |
        | 45.00 | decimal(8,2) |

    Scenario: try_to_number doctest #5 (result)
      When query
        """
        SELECT try_to_number('<1234>', '999999PR') AS result, typeof(try_to_number('<1234>', '999999PR')) AS type
        """
      Then query result
        | result | type |
        | -1234 | decimal(6,0) |

    Scenario: try_to_number doctest #6 (result)
      When query
        """
        SELECT try_to_number('12,454.8-', '99,999.9S') AS result, typeof(try_to_number('12,454.8-', '99,999.9S')) AS type
        """
      Then query result
        | result | type |
        | -12454.8 | decimal(6,1) |

    Scenario: try_to_number doctest #7 (result)
      When query
        """
        SELECT try_to_number('<-$123,456.32>', 'S$999,999.999999PR') AS result, typeof(try_to_number('<-$123,456.32>', 'S$999,999.999999PR')) AS type
        """
      Then query result
        | result | type |
        | 123456.320000 | decimal(12,6) |

    Scenario: try_to_number doctest #8 (result)
      When query
        """
        SELECT try_to_number('$123,456.32', 'MI$999,999.99S') AS result, typeof(try_to_number('$123,456.32', 'MI$999,999.99S')) AS type
        """
      Then query result
        | result | type |
        | 123456.32 | decimal(8,2) |

    Scenario: try_to_number doctest #9 (result)
      When query
        """
        SELECT try_to_number('$123,456.32-', 'MI$999,999.99S') AS result, typeof(try_to_number('$123,456.32-', 'MI$999,999.99S')) AS type
        """
      Then query result
        | result | type |
        | -123456.32 | decimal(8,2) |

    Scenario: try_to_number doctest #10 (result)
      When query
        """
        SELECT try_to_number('-$123,456.32-', 'MI$999,999.99S') AS result, typeof(try_to_number('-$123,456.32-', 'MI$999,999.99S')) AS type
        """
      Then query result
        | result | type |
        | 123456.32 | decimal(8,2) |

    Scenario: try_to_number doctest #11 (result)
      When query
        """
        SELECT try_to_number('-$123,456.32+', 'MI$999,999.99S') AS result, typeof(try_to_number('-$123,456.32+', 'MI$999,999.99S')) AS type
        """
      Then query result
        | result | type |
        | -123456.32 | decimal(8,2) |

    Scenario: try_to_number doctest #12 (result)
      When query
        """
        SELECT try_to_number('-$123,456.32', 'MI$999,999.99S') AS result, typeof(try_to_number('-$123,456.32', 'MI$999,999.99S')) AS type
        """
      Then query result
        | result | type |
        | -123456.32 | decimal(8,2) |

    Scenario: try_to_number doctest #13 (result)
      When query
        """
        SELECT try_to_number('045', 'S$999,099.99') AS result, typeof(try_to_number('045', 'S$999,099.99')) AS type
        """
      Then query result
        | result | type |
        | NULL | decimal(8,2) |

    Scenario: try_to_number doctest #14 (result)
      When query
        """
        SELECT try_to_number('1234', '999999PR') AS result, typeof(try_to_number('1234', '999999PR')) AS type
        """
      Then query result
        | result | type |
        | 1234 | decimal(6,0) |

    Scenario: try_to_number doctest #15 (result)
      When query
        """
        SELECT try_to_number('<1234>', '999999PR') AS result, typeof(try_to_number('>1234>', '999999PR')) AS type
        """
      Then query result
        | result | type |
        | -1234 | decimal(6,0) |

    Scenario: try_to_number doctest #16 (result)
      When query
        """
        SELECT try_to_number('1234>', '999999PR') AS result, typeof(try_to_number('1234>', '999999PR')) AS type
        """
      Then query result
        | result | type |
        | NULL | decimal(6,0) |

    Scenario: try_to_number doctest #17 (result)
      When query
        """
        SELECT try_to_number('12454.8-', '99,999.9S') AS result, typeof(try_to_number('12454.8-', '99,999.9S')) AS type
        """
      Then query result
        | result | type |
        | NULL | decimal(6,1) |

    Scenario: try_to_number doctest #18 (result)
      When query
        """
        SELECT try_to_number('<$1212345.67>', 'S$0,000,000.99PR') AS result, typeof(try_to_number('<$1,212,345.67>', 'S$0,000,000.99PR')) AS type
        """
      Then query result
        | result | type |
        | NULL | decimal(9,2) |

    Scenario: try_to_number doctest #19 (result)
      When query
        """
        SELECT try_to_number('-$123,456.32', 'S$999,999.999999PR') AS result, typeof(try_to_number('-$123,456.32', 'S$999,999.999999PR')) AS type
        """
      Then query result
        | result | type |
        | -123456.320000 | decimal(12,6) |

    Scenario: try_to_number doctest #20 (result)
      When query
        """
        SELECT try_to_number('-123,456.32-', 'MI$999,999.99S') AS result, typeof(try_to_number('-123,456.32-', 'MI$999,999.99S')) AS type
        """
      Then query result
        | result | type |
        | NULL | decimal(8,2) |

    Scenario: try_to_number doctest #21 (result)
      When query
        """
        SELECT try_to_number('+<$123,456.32+', 'MI$999,999.99S') AS result, typeof(try_to_number('+<$123,456.32+', 'MI$999,999.99S')) AS type
        """
      Then query result
        | result | type |
        | NULL | decimal(8,2) |

    Scenario: try_to_number doctest #22 (result)
      When query
        """
        SELECT try_to_number('-123,456.32', 'MI$999,999.99S') AS result, typeof(try_to_number('-123,456.32', 'MI$999,999.99S')) AS type
        """
      Then query result
        | result | type |
        | NULL | decimal(8,2) |

