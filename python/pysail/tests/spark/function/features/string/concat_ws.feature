Feature: concat_ws function

  Rule: concat_ws with scalar arguments

    Scenario Outline: Scalar arguments: <case>
      When query
        """
        SELECT concat_ws(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                          | args                  | result       |
        | concat_ws with multiple string arguments      | ',', 'a', 'b', 'c'    | a,b,c        |
        | concat_ws with null arguments                 | ',', 'a', NULL, 'c'   | a,c          |
        | concat_ws with single argument                | ',', 'a'              | a            |
        | concat_ws with no arguments after separator   | ','                   |              |
        | concat_ws with null separator returns null    | NULL, 'a', 'b', 'c'   | NULL         |
        | concat_ws coerces integer arguments to string | '-', 'a', 1, 2        | a-1-2        |
        | concat_ws coerces double arguments to string  | '-', 'a', 1.5         | a-1.5        |
        | concat_ws coerces boolean arguments to string | '-', 'a', true, false | a-true-false |

  Rule: concat_ws with array arguments

    Scenario Outline: Array arguments: <case>
      When query
        """
        SELECT concat_ws(',', <args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                            | args                             | result  |
        | concat_ws with array argument                   | array('a', 'b', 'c')             | a,b,c   |
        | concat_ws with array containing nulls           | array('a', NULL, 'c')            | a,c     |
        | concat_ws with multiple arrays                  | array('a', 'b'), array('c', 'd') | a,b,c,d |
        | concat_ws with mixed scalar and array arguments | 'x', array('a', 'b'), 'y'        | x,a,b,y |

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
        ORDER BY v NULLS FIRST
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

    Scenario Outline: Coercion: <case>
      When query
        """
        SELECT concat_ws(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                             | args                                                   | result |
        | concat_ws skips a whole-NULL array argument                      | ',', CAST(NULL AS ARRAY<STRING>), 'z'                  | z      |
        | concat_ws coerces binary to string                               | ',', X'4869'                                           | Hi     |
        | concat_ws does not skip empty-string arguments (only NULLs)      | ',', '', 'a', '', 'b'                                  | ,a,,b  |
        | concat_ws with all-NULL arguments returns empty string           | ',', CAST(NULL AS STRING), CAST(NULL AS STRING)        |        |
        | concat_ws with empty separator concatenates with nothing between | '', 'a', 'b', 'c'                                      | abc    |
        | concat_ws with an all-NULL array returns empty string            | ',', array(CAST(NULL AS STRING), CAST(NULL AS STRING)) |        |
        | concat_ws coerces a numeric separator to string                  | 1, 'a', 'b'                                            | a1b    |
        | concat_ws with an empty array returns empty string               | ',', array()                                           |        |

    Scenario Outline: Validation: <case>
      When query
        """
        SELECT concat_ws(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                                 | args                      |
        | concat_ws rejects struct arguments   | ',', named_struct('a', 1) |
        | concat_ws with zero arguments errors |                           |

    Scenario: concat_ws nested inside concat_ws
      When query
        """
        SELECT concat_ws('|', concat_ws(',', 'a', 'b'), concat_ws(',', 'c', 'd')) AS result
        """
      Then query result
        | result   |
        | a,b\|c,d |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null separator yields a non-nullable string
      When query
        """
        SELECT concat_ws(',', 'a', 'b') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a non-null separator column yields a non-nullable string
      When query
        """
        SELECT concat_ws(CAST(id AS STRING), 'a', 'b') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable separator column stays nullable
      When query
        """
        SELECT concat_ws(c, 'a', 'b') AS result FROM VALUES (','), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Result values (migrated from test_concat_ws.txt doctests)

    Scenario: concat_ws doctest #1 (result)
      When query
        """
        SELECT concat_ws('-', string_col, int_col, date_col) AS r FROM VALUES ('a', 1, DATE '2024-01-15') AS t(string_col, int_col, date_col)
        """
      Then query result
        | r              |
        | a-1-2024-01-15 |

    Scenario: concat_ws doctest #2 (result)
      When query
        """
        SELECT concat_ws('-', string_col, int_col) AS r FROM VALUES ('a', CAST(NULL AS INT)) AS t(string_col, int_col)
        """
      Then query result
        | r |
        | a |
