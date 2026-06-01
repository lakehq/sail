Feature: hash() returns murmur3 hash

  Rule: Basic usage

    Scenario: hash integer
      When query
        """
        SELECT hash(42) AS result
        """
      Then query result
        | result   |
        | 29417773 |

    Scenario: hash string
      When query
        """
        SELECT hash('hello') AS result
        """
      Then query result
        | result      |
        | -1008564952 |

    Scenario: hash multiple args
      When query
        """
        SELECT hash(1, 'a', 2) AS result
        """
      Then query result
        | result     |
        | -355304976 |

    Scenario: hash expands wildcard inputs
      When query
        """
        SELECT c1, c2, hash(*)
        FROM VALUES ('ABC', 'DEF') AS t(c1, c2)
        """
      Then query result
        | c1  | c2  | hash(c1, c2) |
        | ABC | DEF | 599895104    |

  Rule: Null handling

    Scenario: hash null input
      When query
        """
        SELECT hash(CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | 42     |
