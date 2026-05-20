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
