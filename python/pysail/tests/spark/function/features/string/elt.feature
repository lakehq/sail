Feature: elt output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to elt yields the schema Spark declares
      When query
        """
        SELECT elt(1, 'scala', 'java') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to elt yields the schema Spark declares
      When query
        """
        SELECT elt(CAST(id AS INT), 'scala', 'java') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to elt stays nullable
      When query
        """
        SELECT elt(c, 'scala', 'java') AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Result values (migrated from test_elt.txt doctests)

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT elt(<args>)
        """
      Then query result
        | <name>   |
        | <result> |

      Examples:
        | case                    | args                                             | name                                      | result |
        | elt doctest #2 (result) | 1, 10, 20                                        | elt(1, 10, 20)                            | 10     |
        | elt doctest #3 (result) | 1, 'scala', 'java'                               | elt(1, scala, java)                       | scala  |
        | elt doctest #5 (result) | 6, 'scala', 'java', 'c', 'c++', 'python', 'rust' | elt(6, scala, java, c, c++, python, rust) | rust   |

    Scenario: elt doctest #4 (result) — out-of-range index with ANSI off
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT elt(11, 10, 20)
        """
      Then query result
        | elt(11, 10, 20) |
        | NULL            |

    # `elt` honours ANSI: an out-of-range index raises instead of returning NULL.
    # Sail returns NULL in both modes.
    @sail-bug
    Scenario: elt doctest #4 (result) — out-of-range index with ANSI on
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT elt(11, 10, 20)
        """
      Then query error \[INVALID_ARRAY_INDEX\] The index 11 is out of bounds. The array has 2 elements.

  Rule: Output schema (migrated from test_elt.txt printSchema doctests)

    Scenario: elt doctest #1 (schema)
      When query
        """
        SELECT elt(1, 10, 20)
        """
      Then query schema
        """
        root
         |-- elt(1, 10, 20): string (nullable = true)
        """

  Rule: Basic usage

    Scenario: select first element
      When query
        """
        SELECT elt(1, 'hello', 'world') AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: select second element
      When query
        """
        SELECT elt(2, 'hello', 'world') AS result
        """
      Then query result
        | result |
        | world  |

    Scenario: select from multiple arguments
      When query
        """
        SELECT elt(3, 'a', 'b', 'c', 'd') AS result
        """
      Then query result
        | result |
        | c      |

  Rule: Null handling

    Scenario: null index returns null
      When query
        """
        SELECT elt(CAST(NULL AS INT), 'hello', 'world') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: null value at selected index returns null
      When query
        """
        SELECT elt(1, CAST(NULL AS STRING), 'world') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: null value at non-selected index returns non-null
      When query
        """
        SELECT elt(2, CAST(NULL AS STRING), 'world') AS result
        """
      Then query result
        | result |
        | world  |

  Rule: Out-of-range index returns null (non-ANSI mode)

    Scenario: index zero returns null in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT elt(0, 'hello', 'world') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: negative index returns null in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT elt(-1, 'hello', 'world') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: index beyond argument count returns null in non-ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT elt(5, 'a', 'b', 'c') AS result
        """
      Then query result
        | result |
        | NULL   |

  # ANSI is pinned rather than inherited from the session default: these three are the pair of
  # the non-ANSI Rule above, and a scenario that relies on the default would quietly measure
  # the other mode if anything before it left the flag set.
  Rule: Out-of-range index raises error under ANSI

    @sail-bug
    Scenario: index zero raises error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT elt(0, 'hello', 'world') AS result
        """
      Then query error (?i)invalid.*index|out.*bound

    @sail-bug
    Scenario: negative index raises error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT elt(-1, 'hello', 'world') AS result
        """
      Then query error (?i)invalid.*index|out.*bound

    @sail-bug
    Scenario: index beyond count raises error in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT elt(5, 'a', 'b', 'c') AS result
        """
      Then query error (?i)invalid.*index|out.*bound

  Rule: Column expressions

    Scenario: elt on column values
      When query
        """
        SELECT elt(idx, v1, v2) AS result
        FROM VALUES (1, 'a', 'x'), (2, 'b', 'y'), (1, 'c', 'z') AS t(idx, v1, v2)
        """
      Then query result
        | result |
        | a      |
        | y      |
        | c      |

    Scenario: elt with integer values casts to string
      When query
        """
        SELECT elt(1, 42) AS result
        """
      Then query result
        | result |
        | 42     |
