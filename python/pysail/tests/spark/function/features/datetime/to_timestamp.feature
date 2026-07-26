@to_timestamp
Feature: to_timestamp (strict variant)
  Strict to_timestamp that throws on invalid input,
  contrasting with try_to_timestamp which returns NULL.

  Rule: Valid input parses

    Scenario Outline: Valid input: <case>
      When query
        """
        SELECT to_timestamp(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                           | args                                         | result              |
        | ISO timestamp                  | '2024-01-15 10:30:45'                        | 2024-01-15 10:30:45 |
        | Date-only parses with midnight | '2024-01-15'                                 | 2024-01-15 00:00:00 |
        | With format                    | '2024-01-15 10:30:45', 'yyyy-MM-dd HH:mm:ss' | 2024-01-15 10:30:45 |
        | Cast from date                 | DATE '2024-01-15'                            | 2024-01-15 00:00:00 |
        | Cast from timestamp            | TIMESTAMP '2024-01-15 10:30:45'              | 2024-01-15 10:30:45 |

  Rule: Invalid input honors ANSI mode
    # to_timestamp errors on invalid input under ANSI and returns NULL otherwise.

    Scenario Outline: ANSI on: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT to_timestamp(<args>) AS result
        """
      Then query error .*

      Examples:
        | case                                 | args                       |
        | Garbage string under ANSI on errors  | 'not-a-timestamp'          |
        | Format mismatch under ANSI on errors | '2024-01-15', 'dd/MM/yyyy' |

    Scenario Outline: ANSI off: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT to_timestamp(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                        | args                       |
        | Garbage string under ANSI off returns NULL  | 'not-a-timestamp'          |
        | Format mismatch under ANSI off returns NULL | '2024-01-15', 'dd/MM/yyyy' |

  Rule: NULL input propagates

    Scenario Outline: NULL propagation: <case>
      When query
        """
        SELECT to_timestamp(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                     | args                        |
        | NULL input returns NULL  | CAST(NULL AS STRING)        |
        | NULL format returns NULL | '2024-01-15 10:30:45', NULL |

  Rule: Timezone handling — LTZ applies offset, NTZ ignores it
    # Validated against Spark JVM with session tz America/New_York.

    Scenario Outline: Session time zone: <case>
      Given config spark.sql.session.timeZone = America/New_York
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                   | fn               | input                       | result              |
        | LTZ applies trailing Z (UTC) and renders in session tz | to_timestamp     | '2024-01-15 10:30:45Z'      | 2024-01-15 05:30:45 |
        | LTZ applies explicit offset                            | to_timestamp     | '2024-06-15 10:30:45-08:00' | 2024-06-15 14:30:45 |
        | NTZ ignores trailing Z (keeps wall clock)              | to_timestamp_ntz | '2024-01-15 10:30:45Z'      | 2024-01-15 10:30:45 |

    Scenario: NTZ ignores explicit offset
      When query
        """
        SELECT to_timestamp_ntz('2024-06-15 10:30:45-08:00') AS result
        """
      Then query result
        | result              |
        | 2024-06-15 10:30:45 |

  Rule: Fractional seconds, separators, boundaries

    Scenario Outline: Fractions and boundaries: <case>
      When query
        """
        SELECT <fn>(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                        | fn               | input                           | result                     |
        | T separator parses                          | to_timestamp     | '2024-01-15T10:30:45'           | 2024-01-15 10:30:45        |
        | Fractional seconds truncate to microseconds | to_timestamp     | '2024-01-15 10:30:45.123456789' | 2024-01-15 10:30:45.123456 |
        | Single-digit fractional second              | to_timestamp     | '2024-01-15 10:30:45.1'         | 2024-01-15 10:30:45.1      |
        | Leap day                                    | to_timestamp_ntz | '2024-02-29 12:00:00'           | 2024-02-29 12:00:00        |
        | Upper boundary                              | to_timestamp_ntz | '9999-12-31 23:59:59'           | 9999-12-31 23:59:59        |

  Rule: Per-row format (column-expression format)

    Scenario: Different format per row all parse
      When query
        """
        SELECT to_timestamp(d, f) AS result FROM VALUES
          ('2024-01-15 10:30:00', 'yyyy-MM-dd HH:mm:ss'),
          ('15/01/2024 10:30:00', 'dd/MM/yyyy HH:mm:ss') AS t(d, f)
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:00 |
        | 2024-01-15 10:30:00 |

  @spark_null
  Rule: Output schema

    Scenario: a non-null string literal yields a timestamp
      When query
        """
        SELECT to_timestamp('2024-01-15 10:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: a non-null string column yields a timestamp
      When query
        """
        SELECT to_timestamp(date_format(CAST(id AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss')) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """

    Scenario: a nullable string column stays nullable
      When query
        """
        SELECT to_timestamp(c) AS result FROM VALUES ('2024-01-15 10:00:00'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
