Feature: base64 functions encode and decode binary strings

  Rule: Null propagation

    Scenario: base64 returns null for a null string
      When query
      """
      SELECT base64(CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: base64 returns null for an untyped null literal
      When query
      """
      SELECT base64(NULL) AS result
      """
      Then query result
      | result |
      | NULL   |

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

    Scenario: unbase64 returns null for a null string
      When query
      """
      SELECT unbase64(CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: unbase64 returns null for an untyped null literal
      When query
      """
      SELECT unbase64(NULL) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: unbase64 ignores whitespace and decodes unpadded input
      When query
      """
      SELECT unbase64('   ab   ') AS result
      """
      Then query result
      | result |
      | [69]   |

    Scenario: unbase64 ignores non-base64 characters
      When query
      """
      SELECT unbase64('%') AS result
      """
      Then query result
      | result |
      | []     |

  Rule: Empty and multi-value handling

    Scenario: base64 encodes an empty string to an empty string
      When query
      """
      SELECT base64('') AS result
      """
      Then query result
      | result |
      |        |

    Scenario: unbase64 decodes an empty string to empty bytes
      When query
      """
      SELECT unbase64('') AS result
      """
      Then query result
      | result |
      | []     |

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

    Scenario: unbase64 decodes a multi-byte value
      When query
      """
      SELECT unbase64('Zm9v') AS result
      """
      Then query result
      | result     |
      | [66 6F 6F] |

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
