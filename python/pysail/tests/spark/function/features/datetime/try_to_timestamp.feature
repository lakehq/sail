@try_to_timestamp
Feature: try_to_timestamp
  Safe variant of to_timestamp that returns NULL on parse failure.

  Rule: Single-argument form parses with default formats

    Scenario Outline: Single argument: <case>
      When query
        """
        SELECT try_to_timestamp(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | input                        | result                     |
        | ISO timestamp parses                | '2024-01-15 10:30:45'        | 2024-01-15 10:30:45        |
        | Date-only parses with midnight time | '2024-01-15'                 | 2024-01-15 00:00:00        |
        | Microseconds preserved              | '2024-01-15 10:30:45.123456' | 2024-01-15 10:30:45.123456 |
        | Cast from date                      | DATE '2024-01-15'            | 2024-01-15 00:00:00        |
        | Garbage returns NULL                | 'not-a-timestamp'            | NULL                       |
        | Empty string returns NULL           | ''                           | NULL                       |
        | Invalid month returns NULL          | '2024-13-15 10:30:45'        | NULL                       |
        | NULL input                          | CAST(NULL AS STRING)         | NULL                       |

  Rule: Two-argument form parses with format string

    Scenario Outline: Two arguments: <case>
      When query
        """
        SELECT try_to_timestamp(<value>, <format>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | value                 | format                | result              |
        | Spark format yyyy-MM-dd HH:mm:ss    | '2024-01-15 10:30:45' | 'yyyy-MM-dd HH:mm:ss' | 2024-01-15 10:30:45 |
        | Custom format dd/MM/yyyy            | '15/01/2024 10:30:45' | 'dd/MM/yyyy HH:mm:ss' | 2024-01-15 10:30:45 |
        | Format mismatch returns NULL        | '2024-01-15'          | 'dd/MM/yyyy'          | NULL                |
        | NULL value with format returns NULL | CAST(NULL AS STRING)  | 'yyyy-MM-dd HH:mm:ss' | NULL                |
        | NULL format returns NULL            | '2024-01-15 10:30:45' | NULL                  | NULL                |

  Rule: Non-finite floating-point string literals return NULL

    Scenario Outline: Non-finite literal: <case>
      When query
        """
        SELECT try_to_timestamp(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                  | input       |
        | NaN string returns NULL               | 'NaN'       |
        | Infinity string returns NULL          | 'Infinity'  |
        | Negative Infinity string returns NULL | '-Infinity' |

  Rule: Per-row format (column-expression format)

    Scenario: Different format per row all parse
      When query
        """
        SELECT try_to_timestamp(d, f) AS result FROM VALUES
          ('2024-01-15 10:30:00', 'yyyy-MM-dd HH:mm:ss'),
          ('15/01/2024 10:30:00', 'dd/MM/yyyy HH:mm:ss') AS t(d, f)
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:00 |
        | 2024-01-15 10:30:00 |

    Scenario: Per-row format with NULL format propagates to NULL
      When query
        """
        SELECT try_to_timestamp(d, f) AS result FROM VALUES
          ('2024-01-15 10:30:00', 'yyyy-MM-dd HH:mm:ss'),
          ('2024-01-16 11:00:00', CAST(NULL AS STRING)) AS t(d, f)
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:00 |
        | NULL                |

  Rule: Multi-row arrays handle per-row failures

    Scenario: Mixed valid and invalid in batch
      When query
        """
        SELECT try_to_timestamp(t) AS result FROM VALUES
          ('2024-01-15 10:30:45'),
          ('garbage'),
          ('2024-01-15'),
          (NULL) AS x(t)
        """
      Then query result
        | result              |
        | 2024-01-15 10:30:45 |
        | NULL                |
        | 2024-01-15 00:00:00 |
        | NULL                |

  Rule: Result values (migrated from test_try_to_timestamp.txt doctests)

    Scenario: try_to_timestamp doctest #1 (result) — input LTZ timestamps under Amsterdam
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      When query
        """
        SELECT ts FROM VALUES (TIMESTAMP_LTZ '2023-01-01 10:00:00'), (TIMESTAMP_LTZ '2023-01-01 03:00:00') AS t(ts)
        """
      Then query result
        | ts                  |
        | 2023-01-01 10:00:00 |
        | 2023-01-01 03:00:00 |

    Scenario Outline: Result values: <case>
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      And config spark.sql.timestampType = <timestamp_type>
      When query
        """
        SELECT try_to_timestamp(ts) AS r FROM VALUES (TIMESTAMP_LTZ '2023-01-01 10:00:00'), (TIMESTAMP_LTZ '2023-01-01 03:00:00') AS t(ts)
        """
      Then query result
        | r                   |
        | 2023-01-01 10:00:00 |
        | 2023-01-01 03:00:00 |

      Examples:
        | case                                                               | timestamp_type |
        | try_to_timestamp doctest #2 (result) — timestampType TIMESTAMP_LTZ | TIMESTAMP_LTZ  |
        | try_to_timestamp doctest #4 (result) — timestampType TIMESTAMP_NTZ | TIMESTAMP_NTZ  |

  Rule: Output schema (migrated from test_try_to_timestamp.txt printSchema doctests)

    Scenario: try_to_timestamp doctest #3 (schema) — timestampType TIMESTAMP_LTZ
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      And config spark.sql.timestampType = TIMESTAMP_LTZ
      When query
        """
        SELECT try_to_timestamp(ts) AS r FROM VALUES (TIMESTAMP_LTZ '2023-01-01 10:00:00'), (TIMESTAMP_LTZ '2023-01-01 03:00:00') AS t(ts)
        """
      Then query schema
        """
        root
         |-- r: timestamp (nullable = true)
        """

    Scenario: try_to_timestamp doctest #5 (schema) — timestampType TIMESTAMP_NTZ
      Given config spark.sql.session.timeZone = Europe/Amsterdam
      And config spark.sql.timestampType = TIMESTAMP_NTZ
      When query
        """
        SELECT try_to_timestamp(ts) AS r FROM VALUES (TIMESTAMP_LTZ '2023-01-01 10:00:00'), (TIMESTAMP_LTZ '2023-01-01 03:00:00') AS t(ts)
        """
      Then query schema
        """
        root
         |-- r: timestamp_ntz (nullable = true)
        """
