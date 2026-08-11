@spark-4.1
Feature: current_time

  Background:
    Given config spark.sql.timeType.enabled = true

  Rule: Default precision and display name

    Scenario: omitted precision has Spark's non-null TIME(6) schema
      When query
        """
        SELECT current_time() AS result
        """
      Then query schema
        """
        root
         |-- result: time(6) (nullable = false)
        """

    Scenario: omitted precision uses 6 in its display name
      When query
        """
        SELECT `current_time(6)` = current_time(6) AS result
        FROM (SELECT current_time())
        """
      Then query result
        | result |
        | true   |

  Rule: Explicit precision

    Scenario: a foldable precision expression is evaluated and preserved in the display name
      When query
        """
        SELECT `current_time((1 + 2))` = current_time(3) AS result
        FROM (SELECT current_time(1 + 2))
        """
      Then query result
        | result |
        | true   |

    Scenario Outline: foldable integer-compatible precision is accepted: <precision>
      When query
        """
        SELECT current_time(<precision>) = current_time(<expected>) AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | precision                       | expected |
        | CAST(' 0003 ' AS INT)           | 3        |
        | CAST(3.9 AS DOUBLE)             | 3        |

    Scenario Outline: supported precision has Spark's exact schema: <precision>
      When query
        """
        SELECT current_time(<precision>) AS result
        """
      Then query schema
        """
        root
         |-- result: time(<precision>) (nullable = false)
        """

      Examples:
        | precision |
        | 0         |
        | 1         |
        | 2         |
        | 3         |
        | 4         |
        | 5         |
        | 6         |

    Scenario Outline: typeof preserves the exact precision: <precision>
      When query
        """
        SELECT typeof(current_time(<precision>)) AS result
        """
      Then query result
        | result            |
        | time(<precision>) |

      Examples:
        | precision |
        | 0         |
        | 1         |
        | 2         |
        | 3         |
        | 4         |
        | 5         |
        | 6         |

    Scenario Outline: cast target type overrides current_time precision metadata: <target>
      When query
        """
        SELECT typeof(CAST(current_time(2) AS <target>)) AS result
        """
      Then query result
        | result     |
        | <expected> |

      Examples:
        | target  | expected |
        | STRING  | string   |
        | TIME    | time(6)  |
        | TIME(0) | time(0)  |
        | TIME(3) | time(3)  |
        | TIME(6) | time(6)  |

    Scenario: internal precision metadata does not change a cast display name
      When query
        """
        SELECT `CAST(current_time(2) AS STRING)` = CAST(current_time(2) AS STRING) AS result
        FROM (SELECT CAST(current_time(2) AS STRING))
        """
      Then query result
        | result |
        | true   |

    Scenario Outline: precision <precision> truncates to <factor> microseconds
      When query
        """
        SELECT
          time_diff('MICROSECOND', TIME '00:00:00', current_time(<precision>)) =
            time_diff('MICROSECOND', TIME '00:00:00', current_time(6)) -
            pmod(time_diff('MICROSECOND', TIME '00:00:00', current_time(6)), <factor>) AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | precision | factor  |
        | 0         | 1000000 |
        | 1         | 100000  |
        | 2         | 10000   |
        | 3         | 1000    |
        | 4         | 100     |
        | 5         | 10      |
        | 6         | 1       |

  Rule: Precision validation

    Scenario Outline: invalid precision is rejected: <case>
      When query
        """
        SELECT current_time(<precision>)
        <suffix>
        """
      Then query error <error>

      Examples:
        | case               | precision                 | suffix        | error                             |
        | non-foldable value | rand()                    |               | (?i)precision.*foldable           |
        | null value         | CAST(NULL AS INT)         |               | (?i)precision.*not.*null          |
        | non-integer type   | CAST(true AS BOOLEAN)     |               | (?i)(precision\|first parameter).*(integer\|int) |
        | below range        | -1                        |               | (?i)precision.*between.*0.*6      |
        | above range        | 7                         |               | (?i)precision.*between.*0.*6      |
