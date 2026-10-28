@function(lambda)
Feature: an untyped NULL array with a plain expression in place of the lambda

  # Spark coerces a NULL argument to an array only when the whole call is already
  # resolved, so an explicit lambda still rejects it while a plain expression does not.

  Rule: The result is NULL

    Scenario Outline: <call> over an untyped NULL array is NULL
      When query
        """
        SELECT <call> AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | call                |
        | exists(NULL, true)  |
        | forall(NULL, true)  |
        | filter(NULL, true)  |
        | transform(NULL, 1)  |
        | array_sort(NULL, 1) |

  Rule: The result schema follows the plain expression

    Scenario: exists over an untyped NULL array
      When query
        """
        SELECT exists(NULL, true) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

    Scenario: forall over an untyped NULL array
      When query
        """
        SELECT forall(NULL, true) AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

    Scenario: filter over an untyped NULL array
      When query
        """
        SELECT filter(NULL, true) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: void (containsNull = true)
        """

    Scenario: transform over an untyped NULL array takes its element type from the expression
      When query
        """
        SELECT transform(NULL, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: array_sort over an untyped NULL array
      When query
        """
        SELECT array_sort(NULL, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: void (containsNull = true)
        """

  Rule: An explicit lambda still rejects the untyped NULL array

    Scenario Outline: <call> keeps the argument type error
      When query
        """
        SELECT <call> AS result
        """
      Then query error <name>.*(ARRAY|list)

      Examples:
        | call                    | name      |
        | exists(NULL, x -> true) | exists    |
        | forall(NULL, x -> true) | forall    |
        | filter(NULL, x -> true) | filter    |
        | transform(NULL, x -> 1) | transform |
