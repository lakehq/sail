Feature: concat_ws function

  Rule: concat_ws with scalar arguments

    Scenario: concat_ws with multiple string arguments
      When query
        """
        SELECT concat_ws(',', 'a', 'b', 'c') AS result
        """
      Then query result
        | result  |
        | a,b,c   |

    Scenario: concat_ws with null arguments
      When query
        """
        SELECT concat_ws(',', 'a', NULL, 'c') AS result
        """
      Then query result
        | result  |
        | a,c     |

    Scenario: concat_ws with single argument
      When query
        """
        SELECT concat_ws(',', 'a') AS result
        """
      Then query result
        | result  |
        | a       |

    Scenario: concat_ws with no arguments after separator
      When query
        """
        SELECT concat_ws(',') AS result
        """
      Then query result
        | result  |
        |         |

    Scenario: concat_ws with null separator returns null
      When query
        """
        SELECT concat_ws(NULL, 'a', 'b', 'c') AS result
        """
      Then query result
        | result  |
        | NULL    |

    Scenario: concat_ws coerces integer arguments to string
      When query
        """
        SELECT concat_ws('-', 'a', 1, 2) AS result
        """
      Then query result
        | result  |
        | a-1-2   |

    Scenario: concat_ws coerces double arguments to string
      When query
        """
        SELECT concat_ws('-', 'a', 1.5) AS result
        """
      Then query result
        | result  |
        | a-1.5   |

    Scenario: concat_ws coerces boolean arguments to string
      When query
        """
        SELECT concat_ws('-', 'a', true, false) AS result
        """
      Then query result
        | result         |
        | a-true-false   |

  Rule: concat_ws with array arguments

    Scenario: concat_ws with array argument
      When query
        """
        SELECT concat_ws(',', array('a', 'b', 'c')) AS result
        """
      Then query result
        | result  |
        | a,b,c   |

    Scenario: concat_ws with array containing nulls
      When query
        """
        SELECT concat_ws(',', array('a', NULL, 'c')) AS result
        """
      Then query result
        | result  |
        | a,c     |

    Scenario: concat_ws with multiple arrays
      When query
        """
        SELECT concat_ws(',', array('a', 'b'), array('c', 'd')) AS result
        """
      Then query result
        | result    |
        | a,b,c,d   |

    Scenario: concat_ws with mixed scalar and array arguments
      When query
        """
        SELECT concat_ws(',', 'x', array('a', 'b'), 'y') AS result
        """
      Then query result
        | result    |
        | x,a,b,y   |

  Rule: concat_ws over multiple rows (column inputs)

    Scenario: concat_ws null separator over a column returns NULL per row
      When query
        """
        SELECT concat_ws(NULL, v) AS result
        FROM VALUES ('a'), ('b'), ('c') AS t(v)
        ORDER BY v
        """
      Then query result ordered
        | result |
        | NULL   |
        | NULL   |
        | NULL   |

    Scenario: concat_ws skips per-row NULL over a column
      When query
        """
        SELECT concat_ws(',', v, 'X') AS result
        FROM VALUES ('a'), (CAST(NULL AS STRING)), ('c') AS t(v)
        ORDER BY v
        """
      Then query result ordered
        | result |
        | X      |
        | a,X    |
        | c,X    |

    Scenario: concat_ws with a per-row separator column including NULL
      When query
        """
        SELECT concat_ws(sep, a, b) AS result FROM VALUES
          (0, ',', 'a', 'b'),
          (1, CAST(NULL AS STRING), 'a', 'b'),
          (2, '|', 'x', 'y')
        AS t(id, sep, a, b) ORDER BY id
        """
      Then query result ordered
        | result |
        | a,b    |
        | NULL   |
        | x\|y   |

    Scenario: concat_ws with all-scalar arguments broadcasts over rows
      When query
        """
        SELECT concat_ws(',', 'a', 'b') AS result
        FROM VALUES (1), (2), (3) AS t(x)
        """
      Then query result
        | result |
        | a,b    |
        | a,b    |
        | a,b    |

  Rule: concat_ws argument coercion and validation

    Scenario: concat_ws skips a whole-NULL array argument
      When query
        """
        SELECT concat_ws(',', CAST(NULL AS ARRAY<STRING>), 'z') AS result
        """
      Then query result
        | result |
        | z      |

    Scenario: concat_ws coerces binary to string
      When query
        """
        SELECT concat_ws(',', X'4869') AS result
        """
      Then query result
        | result |
        | Hi     |

    Scenario: concat_ws rejects struct arguments
      When query
        """
        SELECT concat_ws(',', named_struct('a', 1)) AS result
        """
      Then query error .*

    Scenario: concat_ws with zero arguments errors
      When query
        """
        SELECT concat_ws() AS result
        """
      Then query error .*

    Scenario: concat_ws does not skip empty-string arguments (only NULLs)
      When query
        """
        SELECT concat_ws(',', '', 'a', '', 'b') AS result
        """
      Then query result
        | result |
        | ,a,,b  |

    Scenario: concat_ws with all-NULL arguments returns empty string
      When query
        """
        SELECT concat_ws(',', CAST(NULL AS STRING), CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        |        |

    Scenario: concat_ws with empty separator concatenates with nothing between
      When query
        """
        SELECT concat_ws('', 'a', 'b', 'c') AS result
        """
      Then query result
        | result |
        | abc    |

    Scenario: concat_ws with an all-NULL array returns empty string
      When query
        """
        SELECT concat_ws(',', array(CAST(NULL AS STRING), CAST(NULL AS STRING))) AS result
        """
      Then query result
        | result |
        |        |

    Scenario: concat_ws coerces a numeric separator to string
      When query
        """
        SELECT concat_ws(1, 'a', 'b') AS result
        """
      Then query result
        | result |
        | a1b    |

    Scenario: concat_ws nested inside concat_ws
      When query
        """
        SELECT concat_ws('|', concat_ws(',', 'a', 'b'), concat_ws(',', 'c', 'd')) AS result
        """
      Then query result
        | result   |
        | a,b\|c,d |

    Scenario: concat_ws with an empty array returns empty string
      When query
        """
        SELECT concat_ws(',', array()) AS result
        """
      Then query result
        | result |
        |        |
