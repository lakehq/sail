Feature: log output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to log yields the schema Spark declares
      When query
        """
        SELECT log(10, 100) AS result
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a non-null column input to log yields the schema Spark declares
      When query
        """
        SELECT log(CAST(id AS INT), 100) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

    Scenario: a nullable column input to log stays nullable
      When query
        """
        SELECT log(c, 100) AS result FROM VALUES (10), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: double (nullable = true)
        """

  Rule: single-argument log is the natural logarithm

    @sail-bug
    Scenario: single-argument log (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT log(10) AS result
        """
      Then query result
        | result            |
        | 2.302585092994046 |

    @sail-bug
    Scenario: single-argument log (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT log(10) AS result
        """
      Then query result
        | result            |
        | 2.302585092994046 |

  Rule: log of non-positive values returns NULL

    @sail-bug
    Scenario: log of non-positive values (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT log(-1) AS neg, log(0) AS zero
        """
      Then query result
        | neg  | zero |
        | NULL | NULL |

    @sail-bug
    Scenario: log of non-positive values (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT log(-1) AS neg, log(0) AS zero
        """
      Then query result
        | neg  | zero |
        | NULL | NULL |

  Rule: two-argument log uses the given base

    Scenario: two-argument log (ANSI on)
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT log(2.0, 8.0) AS result
        """
      Then query result
        | result |
        | 3.0    |

    Scenario: two-argument log (ANSI off)
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT log(2.0, 8.0) AS result
        """
      Then query result
        | result |
        | 3.0    |
