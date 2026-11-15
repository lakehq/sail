Feature: base64 functions encode and decode binary strings

  Rule: Null propagation

    Scenario Outline: Null propagation: <case>
      When query
        """
        SELECT base64(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                            | input                |
        | base64 returns null for a null string           | CAST(NULL AS STRING) |
        | base64 returns null for an untyped null literal | NULL                 |

    Scenario: base64 preserves nulls in column values
      When query
        """
        SELECT base64(value) AS result
        FROM VALUES ('ab'), (CAST(NULL AS STRING)) AS data(value)
        ORDER BY value IS NULL, value
        """
      Then query result
        | result |
        | YWI=   |
        | NULL   |

  Rule: Null-tolerant decoding

    Scenario Outline: Null-tolerant decoding: <case>
      When query
        """
        SELECT unbase64(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                   | input                | result |
        | unbase64 returns null for a null string                | CAST(NULL AS STRING) | NULL   |
        | unbase64 returns null for an untyped null literal      | NULL                 | NULL   |
        | unbase64 ignores whitespace and decodes unpadded input | '   ab   '           | [69]   |
        | unbase64 ignores non-base64 characters                 | '%'                  | []     |

  Rule: Empty and multi-value handling

    Scenario Outline: Empty and multi-byte: <case>
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                              | fn       | input  | result     |
        | base64 encodes an empty string to an empty string | base64   | ''     |            |
        | unbase64 decodes an empty string to empty bytes   | unbase64 | ''     | []         |
        | unbase64 decodes a multi-byte value               | unbase64 | 'Zm9v' | [66 6F 6F] |

    Scenario: base64 preserves nulls and empty strings across a column
      When query
        """
        SELECT base64(value) AS result
        FROM VALUES ('foo'), (''), (CAST(NULL AS STRING)), ('bar') AS data(value)
        ORDER BY value IS NULL, value
        """
      Then query result
        | result |
        |        |
        | YmFy   |
        | Zm9v   |
        | NULL   |

    Scenario: base64 encodes a binary column preserving nulls and empty
      When query
        """
        SELECT base64(value) AS result
        FROM VALUES (CAST('hi' AS BINARY)), (CAST('' AS BINARY)), (CAST(NULL AS BINARY)) AS data(value)
        ORDER BY value IS NULL, value
        """
      Then query result
        | result |
        |        |
        | aGk=   |
        | NULL   |

    Scenario: unbase64 reverses base64 for a column round-trip
      When query
        """
        SELECT unbase64(base64(value)) AS result
        FROM VALUES ('foo'), (''), ('bar') AS data(value)
        ORDER BY value
        """
      Then query result
        | result     |
        | []         |
        | [62 61 72] |
        | [66 6F 6F] |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null binary literal yields a non-nullable string
      When query
        """
        SELECT base64(X'48656C6C6F') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """

    Scenario: a nullable binary column stays nullable
      When query
        """
        SELECT base64(c) AS result FROM VALUES (X'48'), (CAST(NULL AS BINARY)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    @sail-bug
    Scenario: a non-null binary column yields a non-nullable string
      When query
        """
        SELECT base64(CAST(CAST(id AS STRING) AS BINARY)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = false)
        """
