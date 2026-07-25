@split
Feature: split output schema

  @spark_null
  Rule: Output schema

    @sail-bug
Scenario: a non-null literal input to split yields the schema Spark declares
      When query
        """
        SELECT split('oneAtwoBthreeC', '[ABC]') AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = false)
        """

    @sail-bug
Scenario: a non-null column input to split yields the schema Spark declares
      When query
        """
        SELECT split(CAST(id AS STRING), '[ABC]') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = false)
        """

    @sail-bug
Scenario: a nullable column input to split stays nullable
      When query
        """
        SELECT split(c, '[ABC]') AS result FROM VALUES ('oneAtwoBthreeC'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = false)
        """

  Rule: Result values (migrated from test_split.txt doctests)

    Scenario: split doctest #1 (result)
      When query
        """
        SELECT split('oneAtwoBthreeC', '[ABC]') AS result, typeof(split('oneAtwoBthreeC', '[ABC]')) AS type
        """
      Then query result
        | result | type |
        | [one, two, three, ] | array<string> |

    Scenario: split doctest #2 (result)
      When query
        """
        SELECT split('1A2B3C', '[1-9]+') AS result, typeof(split('1A2B3C', '[1-9]+')) AS type
        """
      Then query result
        | result | type |
        | [, A, B, C] | array<string> |

    Scenario: split doctest #3 (result)
      When query
        """
        SELECT split('aa2bb3cc4', NULL) AS result, typeof(split('aa2bb3cc4', NULL)) AS type
        """
      Then query result
        | result | type |
        | NULL | array<string> |

    Scenario: split doctest #4 (result)
      When query
        """
        SELECT split(NULL, '[1-9]+') AS result, typeof(split(NULL, '[1-9]+')) AS type
        """
      Then query result
        | result | type |
        | NULL | array<string> |

    Scenario: split doctest #5 (result)
      When query
        """
        SELECT split('oneAtwoBthreeC', '[ABC]', -4) AS result, typeof(split('oneAtwoBthreeC', '[ABC]', -4)) AS type
        """
      Then query result
        | result | type |
        | [one, two, three, ] | array<string> |

    Scenario: split doctest #6 (result)
      When query
        """
        SELECT split('oneAtwoBthreeC', '[ABC]', -1) AS result, typeof(split('oneAtwoBthreeC', '[ABC]', -1)) AS type
        """
      Then query result
        | result | type |
        | [one, two, three, ] | array<string> |

    Scenario: split doctest #7 (result)
      When query
        """
        SELECT split('oneAtwoBthreeC', '[ABC]', 0) AS result, typeof(split('oneAtwoBthreeC', '[ABC]', 0)) AS type
        """
      Then query result
        | result | type |
        | [one, two, three, ] | array<string> |

    Scenario: split doctest #8 (result)
      When query
        """
        SELECT split('oneAtwoBthreeC', '[ABC]', 1) AS result, typeof(split('oneAtwoBthreeC', '[ABC]', 1)) AS type
        """
      Then query result
        | result | type |
        | [oneAtwoBthreeC] | array<string> |

    Scenario: split doctest #9 (result)
      When query
        """
        SELECT split('oneAtwoBthreeC', '[ABC]', 2) AS result, typeof(split('oneAtwoBthreeC', '[ABC]', 2)) AS type
        """
      Then query result
        | result | type |
        | [one, twoBthreeC] | array<string> |

    Scenario: split doctest #10 (result)
      When query
        """
        SELECT split('oneAtwoBthreeC', '[ABC]', 10) AS result, typeof(split('oneAtwoBthreeC', '[ABC]', 10)) AS type
        """
      Then query result
        | result | type |
        | [one, two, three, ] | array<string> |

    Scenario: split doctest #11 (result)
      When query
        """
        SELECT split('1A2B3C', '[1-9]+', 1) AS result, typeof(split('1A2B3C', '[1-9]+', 1)) AS type
        """
      Then query result
        | result | type |
        | [1A2B3C] | array<string> |

    Scenario: split doctest #12 (result)
      When query
        """
        SELECT split('aa2bb3cc4', '[1-9]+', -1) AS result, typeof(split('aa2bb3cc4', '[1-9]+', -1)) AS type
        """
      Then query result
        | result | type |
        | [aa, bb, cc, ] | array<string> |

    Scenario: split doctest #13 (result)
      When query
        """
        SELECT split('aa2bb3cc4', '[1-9]+', 2) AS result, typeof(split('aa2bb3cc4', '[1-9]+', 2)) AS type
        """
      Then query result
        | result | type |
        | [aa, bb3cc4] | array<string> |

    Scenario: split doctest #14 (result)
      When query
        """
        SELECT split('aa2bb3cc4', '[1-9]+', NULL) AS result, typeof(split('aa2bb3cc4', '[1-9]+', NULL)) AS type
        """
      Then query result
        | result | type |
        | NULL | array<string> |

    Scenario: split doctest #15 (result)
      When query
        """
        SELECT split('aa2bb3cc4', NULL, -1) AS result, typeof(split('aa2bb3cc4', NULL, -1)) AS type
        """
      Then query result
        | result | type |
        | NULL | array<string> |

    Scenario: split doctest #16 (result)
      When query
        """
        SELECT split(NULL, '[1-9]+', -1) AS result, typeof(split(NULL, '[1-9]+', -1)) AS type
        """
      Then query result
        | result | type |
        | NULL | array<string> |

    Scenario: split doctest #17 (result)
      When query
        """
        SELECT split(s, p, l) AS result, typeof(split(s, p, l)) AS type FROM VALUES ('oneAtwoBthreeC', '[ABC]', -4), ('oneAtwoBthreeC', '[ABC]', -1), ('oneAtwoBthreeC', '[ABC]', 0), ('oneAtwoBthreeC', '[ABC]', 1), ('oneAtwoBthreeC', '[ABC]', 2), ('oneAtwoBthreeC', '[ABC]', 10), ('1A2B3C', '[1-9]+', 1), ('aa2bb3cc4', '[1-9]+', -1), ('aa2bb3cc4', '[1-9]+', 2), ('aa2bb3cc4', '[1-9]+', NULL), ('aa2bb3cc4', NULL, -1), (NULL, '[1-9]+', -1) AS T(s, p, l)
        """
      Then query result
        | result | type |
        | [one, two, three, ] | array<string> |
        | [one, two, three, ] | array<string> |
        | [one, two, three, ] | array<string> |
        | [oneAtwoBthreeC] | array<string> |
        | [one, twoBthreeC] | array<string> |
        | [one, two, three, ] | array<string> |
        | [1A2B3C] | array<string> |
        | [aa, bb, cc, ] | array<string> |
        | [aa, bb3cc4] | array<string> |
        | NULL | array<string> |
        | NULL | array<string> |
        | NULL | array<string> |

