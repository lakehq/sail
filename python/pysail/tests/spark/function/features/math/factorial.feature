Feature: factorial output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to factorial yields the schema Spark declares
      When query
        """
        SELECT factorial(5) AS result
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: a non-null column input to factorial yields the schema Spark declares
      When query
        """
        SELECT factorial(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

    Scenario: a nullable column input to factorial stays nullable
      When query
        """
        SELECT factorial(c) AS result FROM VALUES (5), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: long (nullable = true)
        """

  Rule: factorial within range

    Scenario: factorial in range (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT factorial(0) AS a, factorial(5) AS b, factorial(20) AS c
        """
      Then query result
        | a | b   | c                   |
        | 1 | 120 | 2432902008176640000 |

    Scenario: factorial in range (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT factorial(0) AS a, factorial(5) AS b, factorial(20) AS c
        """
      Then query result
        | a | b   | c                   |
        | 1 | 120 | 2432902008176640000 |

  Rule: factorial of a negative number returns NULL

    @sail-bug
    Scenario: factorial of a negative number (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT factorial(-1) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: factorial of a negative number (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT factorial(-1) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: factorial overflow (n > 20) returns NULL

    @sail-bug
    Scenario: factorial overflow (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT factorial(25) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: factorial overflow (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT factorial(25) AS result
        """
      Then query result
        | result |
        | NULL   |
