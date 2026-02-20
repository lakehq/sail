Feature: xxhash64() returns 64-bit xxHash

  Rule: Basic usage

    Scenario: xxhash64 integer
      When query
        """
        SELECT xxhash64(42) AS result
        """
      Then query result
        | result              |
        | -387659249110444264 |

    Scenario: xxhash64 string
      When query
        """
        SELECT xxhash64('hello') AS result
        """
      Then query result
        | result               |
        | -4367754540140381902 |

    Scenario: xxhash64 multiple args
      When query
        """
        SELECT xxhash64(1, 'a', 2) AS result
        """
      Then query result
        | result              |
        | 4450643625805672383 |

  Rule: Null handling

    Scenario: xxhash64 null input
      When query
        """
        SELECT xxhash64(CAST(NULL AS INT)) AS result
        """
      Then query result
        | result |
        | 42     |
