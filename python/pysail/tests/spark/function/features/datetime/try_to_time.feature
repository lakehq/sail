@try_to_time @spark-4.1
Feature: try_to_time
  Safe variant of to_time that returns NULL on parse failure
  instead of throwing an exception.

  NOTE: TIME type is a preview feature in Spark 4.x; the JVM raises
  `[UNSUPPORTED_TIME_TYPE] The data type TIME is not supported` for
  any TIME usage. The scenarios below reflect Sail's implementation.
  Once Spark stabilises TIME support, remove @sail-only and update
  expected values to match Spark JVM output. — owners to decide.

  Rule: Single-argument form parses with default formats

    @sail-only
    Scenario Outline: Single-arg: <case>
      When query
        """
        SELECT try_to_time(<arg>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                             | arg                  | result          |
        | HH:MM:SS basic                   | '10:30:45'           | 10:30:45        |
        | HH:MM only                       | '10:30'              | 10:30:00        |
        | Microseconds precision           | '10:30:45.123456'    | 10:30:45.123456 |
        | Midnight                         | '00:00:00'           | 00:00:00        |
        | Last second of day               | '23:59:59'           | 23:59:59        |
        | Last microsecond of day          | '23:59:59.999999'    | 23:59:59.999999 |
        | Garbage string returns NULL      | 'not-a-time'         | NULL            |
        | Empty string returns NULL        | ''                   | NULL            |
        | Out-of-range hour returns NULL   | '25:00:00'           | NULL            |
        | Out-of-range minute returns NULL | '10:60:00'           | NULL            |
        | Negative time returns NULL       | '-01:00:00'          | NULL            |
        | Date-only string returns NULL    | '2024-01-15'         | NULL            |
        | NULL input                       | CAST(NULL AS STRING) | NULL            |

  Rule: Two-argument form parses with format string

    @sail-only
    Scenario Outline: Two-arg: <case>
      When query
        """
        SELECT try_to_time(<arg>, <fmt>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                | arg                  | fmt        | result   |
        | HH-MM-SS custom format              | '10-30-45'           | 'HH-mm-ss' | 10:30:45 |
        | HH:MM only with format              | '10:30'              | 'HH:mm'    | 10:30:00 |
        | Format mismatch returns NULL        | '10:30:45'           | 'HH-mm-ss' | NULL     |
        | NULL value with format returns NULL | CAST(NULL AS STRING) | 'HH:mm:ss' | NULL     |
        | NULL format returns NULL            | '10:30:45'           | NULL       | NULL     |

  Rule: Non-finite floating-point string literals return NULL

    @sail-only
    Scenario Outline: Non-finite: <case>
      When query
        """
        SELECT try_to_time(<arg>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                         | arg        |
        | NaN string returns NULL      | 'NaN'      |
        | Infinity string returns NULL | 'Infinity' |

  Rule: Per-row format (column-expression format)

    @sail-only
    Scenario Outline: Per-row format: <case>
      When query
        """
        SELECT try_to_time(t, f) AS result FROM VALUES
          ('10:30:45', 'HH:mm:ss'),
          (<row2>) AS x(t, f)
        """
      Then query result
        | result   |
        | 10:30:45 |
        | <r2>     |

      Examples:
        | case                                               | row2                             | r2       |
        | Different format per row all parse                 | '10-30-45', 'HH-mm-ss'           | 10:30:45 |
        | Per-row format with NULL format propagates to NULL | '10:30:45', CAST(NULL AS STRING) | NULL     |

  Rule: Multi-row arrays handle per-row failures

    @sail-only
    Scenario: Mixed valid and invalid in batch
      When query
        """
        SELECT try_to_time(t) AS result FROM VALUES ('10:30:45'), ('garbage'), ('00:00:00'), (NULL) AS x(t)
        """
      Then query result
        | result   |
        | 10:30:45 |
        | NULL     |
        | 00:00:00 |
        | NULL     |
