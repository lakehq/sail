Feature: split output schema

  @function(nullability)
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

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT split(<args>) AS result, typeof(split(<args>)) AS type
        """
      Then query result
        | result   | type          |
        | <result> | array<string> |

      Examples:
        | case                       | args                          | result              |
        | split doctest #1 (result)  | 'oneAtwoBthreeC', '[ABC]'     | [one, two, three, ] |
        | split doctest #2 (result)  | '1A2B3C', '[1-9]+'            | [, A, B, C]         |
        | split doctest #3 (result)  | 'aa2bb3cc4', NULL             | NULL                |
        | split doctest #4 (result)  | NULL, '[1-9]+'                | NULL                |
        | split doctest #5 (result)  | 'oneAtwoBthreeC', '[ABC]', -4 | [one, two, three, ] |
        | split doctest #6 (result)  | 'oneAtwoBthreeC', '[ABC]', -1 | [one, two, three, ] |
        | split doctest #7 (result)  | 'oneAtwoBthreeC', '[ABC]', 0  | [one, two, three, ] |
        | split doctest #8 (result)  | 'oneAtwoBthreeC', '[ABC]', 1  | [oneAtwoBthreeC]    |
        | split doctest #9 (result)  | 'oneAtwoBthreeC', '[ABC]', 2  | [one, twoBthreeC]   |
        | split doctest #10 (result) | 'oneAtwoBthreeC', '[ABC]', 10 | [one, two, three, ] |
        | split doctest #11 (result) | '1A2B3C', '[1-9]+', 1         | [1A2B3C]            |
        | split doctest #12 (result) | 'aa2bb3cc4', '[1-9]+', -1     | [aa, bb, cc, ]      |
        | split doctest #13 (result) | 'aa2bb3cc4', '[1-9]+', 2      | [aa, bb3cc4]        |
        | split doctest #14 (result) | 'aa2bb3cc4', '[1-9]+', NULL   | NULL                |
        | split doctest #15 (result) | 'aa2bb3cc4', NULL, -1         | NULL                |
        | split doctest #16 (result) | NULL, '[1-9]+', -1            | NULL                |

    Scenario: split doctest #17 (result)
      When query
        """
        SELECT split(s, p, l) AS result, typeof(split(s, p, l)) AS type FROM VALUES ('oneAtwoBthreeC', '[ABC]', -4), ('oneAtwoBthreeC', '[ABC]', -1), ('oneAtwoBthreeC', '[ABC]', 0), ('oneAtwoBthreeC', '[ABC]', 1), ('oneAtwoBthreeC', '[ABC]', 2), ('oneAtwoBthreeC', '[ABC]', 10), ('1A2B3C', '[1-9]+', 1), ('aa2bb3cc4', '[1-9]+', -1), ('aa2bb3cc4', '[1-9]+', 2), ('aa2bb3cc4', '[1-9]+', NULL), ('aa2bb3cc4', NULL, -1), (NULL, '[1-9]+', -1) AS T(s, p, l)
        """
      Then query result
        | result              | type          |
        | [one, two, three, ] | array<string> |
        | [one, two, three, ] | array<string> |
        | [one, two, three, ] | array<string> |
        | [oneAtwoBthreeC]    | array<string> |
        | [one, twoBthreeC]   | array<string> |
        | [one, two, three, ] | array<string> |
        | [1A2B3C]            | array<string> |
        | [aa, bb, cc, ]      | array<string> |
        | [aa, bb3cc4]        | array<string> |
        | NULL                | array<string> |
        | NULL                | array<string> |
        | NULL                | array<string> |
