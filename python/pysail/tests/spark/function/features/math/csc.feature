Feature: csc output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to csc yields the schema Spark declares
      When query
        """
        SELECT csc(1) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to csc yields the schema Spark declares
      When query
        """
        SELECT csc(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to csc stays nullable
      When query
        """
        SELECT csc(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

  Rule: Result values (migrated from test_csc.txt doctests)

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT csc(<input>) AS csc
        """
      Then query result
        | csc      |
        | <result> |

      Examples:
        | case                    | input              | result              |
        | csc doctest #1 (result) | radians(90)        | 1.0                 |
        | csc doctest #2 (result) | 1.5707963267948966 | 1.0                 |
        | csc doctest #3 (result) | 90                 | 1.1185724071637084  |
        | csc doctest #4 (result) | 90.0               | 1.1185724071637084  |
        | csc doctest #5 (result) | -90.0              | -1.1185724071637084 |
        | csc doctest #6 (result) | 0                  | Infinity            |
