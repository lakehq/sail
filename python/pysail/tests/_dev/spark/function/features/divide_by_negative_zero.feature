Feature: Division by negative zero

  # Spark treats `-0.0` as a zero divisor (`-0.0 == 0`), so with ANSI mode disabled, division
  # and remainder by `-0.0` return NULL rather than infinity or NaN.
  Rule: Division by -0.0 returns NULL when ANSI mode is disabled

    Scenario Outline: ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT <expr> AS result FROM VALUES (-0.0D) AS t(x)
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                   | expr                                     |
        | integer divided by a -0.0 column       | 1 / x                                    |
        | double divided by a -0.0 column        | 5.0D / x                                 |
        | remainder by a -0.0 column             | 5 % x                                    |
        | float divided by a float -0.0 column   | CAST(5 AS FLOAT) / CAST(x AS FLOAT)      |
        | float remainder by a float -0.0 column | CAST(5 AS FLOAT) % CAST(x AS FLOAT)      |
        | integer divided by a -0.0 literal      | 1 / -0.0D                                |
        | remainder by a -0.0 literal            | 5 % -0.0D                                |
        | integer divided by a cast -0.0 string  | 1 / CAST('-0.0' AS DOUBLE)               |
        | integer division by a decimal -0.0     | 7 div CAST(-0.0 AS DECIMAL(3, 1))        |
        | pmod by a -0.0 column                  | pmod(5, x)                               |
        | pmod of a negative value by -0.0       | pmod(-5, x)                              |
        | pmod of a double by a -0.0 column      | pmod(5.0D, x)                            |
        | pmod of a float by a float -0.0 column | pmod(CAST(5 AS FLOAT), CAST(x AS FLOAT)) |
        | pmod by a -0.0 literal                 | pmod(5, -0.0D)                           |

    Scenario: ANSI off: division by a computed -0.0
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT x / y AS d, x % y AS m
        FROM (SELECT CAST(id AS DOUBLE) AS x, CAST(id AS DOUBLE) * -0.0D AS y FROM range(1, 3))
        """
      Then query result
        | d    | m    |
        | NULL | NULL |
        | NULL | NULL |

    Scenario: ANSI off: -0.0 divided by a non-zero value keeps its sign
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT x / 2.0D AS d FROM VALUES (-0.0D) AS t(x)
        """
      Then query result
        | d    |
        | -0.0 |

  Rule: Division by -0.0 throws an error when ANSI mode is enabled

    Scenario: ANSI on: division by a -0.0 column
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT 1 / x AS result FROM VALUES (-0.0D) AS t(x)
        """
      Then query error (?i)divi.*zero

    Scenario Outline: ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT <expr> AS result FROM VALUES (-0.0D) AS t(x)
        """
      Then query error (?i)by zero

      Examples:
        | case                   | expr           |
        | pmod by a -0.0 column  | pmod(5, x)     |
        | pmod by a -0.0 literal | pmod(5, -0.0D) |

    Scenario: ANSI on: pmod of NULL by -0.0 is NULL
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT pmod(CAST(NULL AS DOUBLE), x) AS result FROM VALUES (-0.0D) AS t(x)
        """
      Then query result
        | result |
        | NULL   |
