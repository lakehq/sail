@try_to_time @spark-4.1
Feature: try_to_time
  Safe variant of to_time that returns NULL on parse failure
  instead of throwing an exception.

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

  Rule: Spark Java datetime pattern contract

    Background:
      Given config spark.sql.timeType.enabled = true

    Scenario: Java datetime pattern contract applies dynamic formats and NULLs with try_to_time
      When query
        """
        SELECT id, try_to_time(
          value,
          CASE id
            WHEN 1 THEN concat('HH', chr(39), 'B', chr(39), 'mm')
            WHEN 2 THEN 'HH-mm-ss.SSS'
            WHEN 3 THEN 'HHBmm'
            WHEN 4 THEN 'HH:mm:ss.SSS'
            WHEN 5 THEN 'HH:mm:ss'
            ELSE CAST(NULL AS STRING)
          END
        ) AS result
        FROM VALUES
          (1, '10B30'),
          (2, '11-31-42.7'),
          (3, '10B30'),
          (4, '10:30:45.1234'),
          (5, CAST(NULL AS STRING)),
          (6, '12:34:56')
          AS t(id, value)
        ORDER BY id
        """
      Then query result
        | id | result      |
        | 1  | 10:30:00    |
        | 2  | 11:31:42.7  |
        | 3  | NULL        |
        | 4  | NULL        |
        | 5  | NULL        |
        | 6  | NULL        |

    Scenario: Java datetime pattern contract makes try_to_time suppress literal failures
      When query
        """
        SELECT
          try_to_time('10:30:45.1234', 'HH:mm:ss.SSS') AS fraction_too_wide,
          try_to_time('10B30', 'HHBmm') AS unsupported_pattern,
          try_to_time(
            '10:30',
            concat('HH', chr(39), 'mm')
          ) AS invalid_pattern,
          try_to_time(CAST(NULL AS STRING), 'HH:mm:ss') AS null_input
        """
      Then query schema
        """
        root
         |-- fraction_too_wide: time(6) (nullable = true)
         |-- unsupported_pattern: time(6) (nullable = true)
         |-- invalid_pattern: time(6) (nullable = true)
         |-- null_input: time(6) (nullable = true)
        """
      And query result
        | fraction_too_wide | unsupported_pattern | invalid_pattern | null_input |
        | NULL              | NULL                | NULL            | NULL       |
