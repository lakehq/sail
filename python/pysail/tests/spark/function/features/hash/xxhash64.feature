Feature: xxhash64() returns 64-bit xxHash

  Rule: Basic usage

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT xxhash64(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                   | args      | result               |
        | xxhash64 integer       | 42        | -387659249110444264  |
        | xxhash64 string        | 'hello'   | -4367754540140381902 |
        | xxhash64 multiple args | 1, 'a', 2 | 4450643625805672383  |

  Rule: Null handling

    Scenario Outline: Null: <case>
      When query
        """
        SELECT xxhash64(<args>) AS result
        """
      Then query result
        | result |
        | 42     |

      Examples:
        | case                                             | args                 |
        | xxhash64 null input                              | CAST(NULL AS INT)    |
        | xxhash64 null string input also returns the seed | CAST(NULL AS STRING) |

  Rule: Type coverage

    # The hash must agree with Spark for every input type, since the encoding fed
    # to xxHash is type-specific. All values verified against the Spark JVM.

    # NOTE: TIMESTAMP (LTZ) is intentionally NOT asserted with a golden value —
    # its hash depends on the session timezone (micros-since-epoch), so the value
    # is not portable across environments. The migration still hashes it; only the
    # golden value would be flaky.
    Scenario Outline: Type: <case>
      When query
        """
        SELECT xxhash64(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case             | arg                            | result               |
        | xxhash64 bigint  | CAST(1 AS BIGINT)              | -7001672635703045582 |
        | xxhash64 double  | CAST(1.5 AS DOUBLE)            | 7738255526519901366  |
        | xxhash64 float   | CAST(1.5 AS FLOAT)             | 6163473420726370430  |
        | xxhash64 decimal | CAST(1.50 AS DECIMAL(10,2))    | -6873856301616164681 |
        | xxhash64 date    | DATE '2024-01-15'              | 2166432641145730595  |
        | xxhash64 binary  | X'48656C6C6F'                  | 6777584228807376986  |
        | xxhash64 array   | array(1, 2, 3)                 | 8592097078962733837  |
        | xxhash64 struct  | named_struct('a', 1, 'b', 'x') | 8510603489595372987  |

    Scenario: xxhash64 int and boolean true hash identically
      When query
        """
        SELECT xxhash64(CAST(1 AS INT)) AS i, xxhash64(true) AS b
        """
      Then query result
        | i                    | b                    |
        | -6698625589789238999 | -6698625589789238999 |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal yields a non-nullable bigint
      When query
        """
        SELECT xxhash64('a') AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a non-null column yields a non-nullable bigint
      When query
        """
        SELECT xxhash64(id) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

    Scenario: a nullable column: xxhash64 is still non-nullable
      When query
        """
        SELECT xxhash64(c) AS result FROM VALUES ('a'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = false)
        """

  Rule: Interval values

    # Same rule as `hash` (`hash.scala:556-557`): a day-time interval hashes its microseconds as a
    # long. Values measured on the Spark 4.2 JVM.
    Scenario Outline: xxhash64 of an interval: <case>
      When query
        """
        SELECT xxhash64(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case            | input                               | result              |
        | DAY             | INTERVAL '2' DAY                    | 4929742394593830899 |
        | DAY TO SECOND   | INTERVAL '1 02:03:04' DAY TO SECOND | 4873107316074688783 |
        | date difference | DATE'2024-01-15' - DATE'2024-01-01' | 2791293123331190712 |

    Scenario: xxhash64 of a NULL interval is the seed
      When query
        """
        SELECT xxhash64(CAST(NULL AS INTERVAL DAY)) AS result
        """
      Then query result
        | result |
        | 42     |

    # A year-month interval has to be hashed as its months, and Sail cannot read them out yet: the
    # cast `CAST(INTERVAL '2' YEAR AS INT)` is unimplemented, which `main` fails on too.
    @sail-bug
    Scenario: xxhash64 of a year-month interval
      When query
        """
        SELECT xxhash64(INTERVAL '2' YEAR) AS result
        """
      Then query result
        | result              |
        | 7788558767429488329 |
