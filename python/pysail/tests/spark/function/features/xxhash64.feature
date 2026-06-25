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

    Scenario: xxhash64 null string input also returns the seed
      When query
        """
        SELECT xxhash64(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | 42     |

  Rule: Type coverage

    # The hash must agree with Spark for every input type, since the encoding fed
    # to xxHash is type-specific. All values verified against the Spark JVM.

    Scenario: xxhash64 bigint
      When query
        """
        SELECT xxhash64(CAST(1 AS BIGINT)) AS result
        """
      Then query result
        | result               |
        | -7001672635703045582 |

    Scenario: xxhash64 int and boolean true hash identically
      When query
        """
        SELECT xxhash64(CAST(1 AS INT)) AS i, xxhash64(true) AS b
        """
      Then query result
        | i                    | b                    |
        | -6698625589789238999 | -6698625589789238999 |

    Scenario: xxhash64 double
      When query
        """
        SELECT xxhash64(CAST(1.5 AS DOUBLE)) AS result
        """
      Then query result
        | result              |
        | 7738255526519901366 |

    Scenario: xxhash64 float
      When query
        """
        SELECT xxhash64(CAST(1.5 AS FLOAT)) AS result
        """
      Then query result
        | result              |
        | 6163473420726370430 |

    Scenario: xxhash64 decimal
      When query
        """
        SELECT xxhash64(CAST(1.50 AS DECIMAL(10,2))) AS result
        """
      Then query result
        | result               |
        | -6873856301616164681 |

    Scenario: xxhash64 date
      When query
        """
        SELECT xxhash64(DATE '2024-01-15') AS result
        """
      Then query result
        | result              |
        | 2166432641145730595 |

    # NOTE: TIMESTAMP (LTZ) is intentionally NOT asserted with a golden value —
    # its hash depends on the session timezone (micros-since-epoch), so the value
    # is not portable across environments. The migration still hashes it; only the
    # golden value would be flaky.

    Scenario: xxhash64 binary
      When query
        """
        SELECT xxhash64(X'48656C6C6F') AS result
        """
      Then query result
        | result              |
        | 6777584228807376986 |

    Scenario: xxhash64 array
      When query
        """
        SELECT xxhash64(array(1, 2, 3)) AS result
        """
      Then query result
        | result              |
        | 8592097078962733837 |

    Scenario: xxhash64 struct
      When query
        """
        SELECT xxhash64(named_struct('a', 1, 'b', 'x')) AS result
        """
      Then query result
        | result              |
        | 8510603489595372987 |
