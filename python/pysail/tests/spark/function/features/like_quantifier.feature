Feature: LIKE and ILIKE with ALL/ANY/SOME quantifiers

  Rule: LIKE ALL matches every pattern

    Scenario: like all is true when all patterns match
      When query
      """
      SELECT 'Dan' LIKE ALL ('%an%', '%an') AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: like all is false when some pattern does not match
      When query
      """
      SELECT 'Evan_W' LIKE ALL ('%an%', '%an') AS result
      """
      Then query result
      | result |
      | false  |

    Scenario: like all with a single pattern
      When query
      """
      SELECT 'abc' LIKE ALL ('a%') AS result
      """
      Then query result
      | result |
      | true   |

  Rule: LIKE ANY and LIKE SOME match at least one pattern

    Scenario: like any is true when one pattern matches
      When query
      """
      SELECT 'Dan' LIKE ANY ('%an%', '%xy') AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: like any is false when no pattern matches
      When query
      """
      SELECT 'John' LIKE ANY ('%an%', '%an') AS result
      """
      Then query result
      | result |
      | false  |

    Scenario: like some is equivalent to like any
      When query
      """
      SELECT 'Dan' LIKE SOME ('%an%', '%xy') AS result
      """
      Then query result
      | result |
      | true   |

  Rule: NOT LIKE ALL means the value matches none of the patterns

    Scenario: not like all is true when the value matches no pattern
      When query
      """
      SELECT 'John' NOT LIKE ALL ('%an%', '%an') AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: not like all is false when the value matches at least one pattern
      When query
      """
      SELECT 'Evan_W' NOT LIKE ALL ('%an%', '%an') AS result
      """
      Then query result
      | result |
      | false  |

  Rule: NOT LIKE ANY and NOT LIKE SOME mean at least one pattern is not matched

    Scenario: not like any is true when at least one pattern is not matched
      When query
      """
      SELECT 'Evan_W' NOT LIKE ANY ('%an%', '%an') AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: not like any is false when every pattern matches
      When query
      """
      SELECT 'Dan' NOT LIKE ANY ('%an%', '%an') AS result
      """
      Then query result
      | result |
      | false  |

    Scenario: not like some is true when at least one pattern is not matched
      When query
      """
      SELECT 'Evan_W' NOT LIKE SOME ('%an%', '%an') AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: not like some is false when every pattern matches
      When query
      """
      SELECT 'Dan' NOT LIKE SOME ('%an%', '%an') AS result
      """
      Then query result
      | result |
      | false  |

  Rule: ILIKE quantifiers are case-insensitive

    Scenario: ilike any matches ignoring case
      When query
      """
      SELECT 'JOHN' ILIKE ANY ('%ohn') AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: like any is case-sensitive
      When query
      """
      SELECT 'JOHN' LIKE ANY ('%ohn') AS result
      """
      Then query result
      | result |
      | false  |

    Scenario: ilike all matches every pattern ignoring case
      When query
      """
      SELECT 'Spark' ILIKE ALL ('%ARK', 'SP%') AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: not ilike all matches none ignoring case
      When query
      """
      SELECT 'JOHN' NOT ILIKE ALL ('%xyz%', '%abc%') AS result
      """
      Then query result
      | result |
      | true   |

  Rule: NULL patterns follow three-valued logic

    Scenario: like all with a matching pattern and a null pattern returns null
      When query
      """
      SELECT 'abc' LIKE ALL ('a%', NULL) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: like any short-circuits to true past a null pattern
      When query
      """
      SELECT 'abc' LIKE ANY ('a%', NULL) AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: like any with no match and a null pattern returns null
      When query
      """
      SELECT 'xyz' LIKE ANY ('a%', NULL) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: ESCAPE applies to every pattern in the quantifier

    Scenario: like all with an escaped wildcard
      When query
      """
      SELECT 'a%b' LIKE ALL ('a/%b', 'a%') ESCAPE '/' AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: like any with an escaped wildcard
      When query
      """
      SELECT 'a%b' LIKE ANY ('a/%b', 'zzz') ESCAPE '/' AS result
      """
      Then query result
      | result |
      | true   |
