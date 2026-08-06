Feature: make_timestamp_ntz and try_make_timestamp_ntz functions

  Rule: Basic timestamp creation with 6 arguments

    Scenario Outline: Six arguments: <case>
      When query
        """
        SELECT make_timestamp_ntz(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | args                            | result                     |
        | create timestamp from date and time components | 2014, 12, 28, 6, 30, 45.887     | 2014-12-28 06:30:45.887    |
        | create timestamp at midnight                   | 2023, 12, 31, 0, 0, 0.0         | 2023-12-31 00:00:00        |
        | create timestamp near end of valid range       | 9999, 12, 31, 23, 58, 59.999999 | 9999-12-31 23:58:59.999999 |
        | sec=60 adds one minute                         | 2024, 6, 15, 14, 30, 60.0       | 2024-06-15 14:31:00        |

  Rule: Timestamp creation with date and time arguments

    Scenario Outline: Date and time: <case>
      When query
        """
        SELECT make_timestamp_ntz(DATE <date>, TIME <time>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                             | date         | time              | result                     |
        | combine date and time                            | '2024-03-15' | '14:30:00'        | 2024-03-15 14:30:00        |
        | combine date and time with microsecond precision | '2024-01-01' | '12:34:56.123456' | 2024-01-01 12:34:56.123456 |

  Rule: try_make_timestamp_ntz with valid inputs

    Scenario: valid 6-argument call
      When query
        """
        SELECT try_make_timestamp_ntz(2024, 2, 14, 15, 45, 30.5) AS result
        """
      Then query result
        | result                |
        | 2024-02-14 15:45:30.5 |

    Scenario: valid date and time combination
      When query
        """
        SELECT try_make_timestamp_ntz(DATE '2024-07-04', TIME '18:00:00') AS result
        """
      Then query result
        | result              |
        | 2024-07-04 18:00:00 |

  Rule: try_make_timestamp_ntz with invalid inputs returns NULL

    Scenario Outline: Invalid component: <case>
      When query
        """
        SELECT try_make_timestamp_ntz(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case           | args                   |
        | invalid month  | 2024, 13, 1, 0, 0, 0.0 |
        | invalid day    | 2024, 2, 30, 0, 0, 0.0 |
        | invalid hour   | 2024, 1, 1, 24, 0, 0.0 |
        | invalid minute | 2024, 1, 1, 0, 60, 0.0 |
        | invalid second | 2024, 1, 1, 0, 0, 61.0 |

  Rule: NULL handling

    Scenario: make_timestamp_ntz with null date
      When query
        """
        SELECT make_timestamp_ntz(CAST(NULL AS DATE), TIME '10:00:00') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_make_timestamp_ntz with null year
      When query
        """
        SELECT try_make_timestamp_ntz(CAST(NULL AS INT), 1, 1, 0, 0, 0.0) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Multiple rows with mixed valid and invalid inputs

    Scenario: try_make_timestamp_ntz on array of inputs
      When query
        """
        SELECT
          year,
          try_make_timestamp_ntz(year, month, day, hour, min, sec) AS result
        FROM VALUES
          (2024, 1, 1, 0, 0, 0.0),
          (2024, 13, 1, 0, 0, 0.0),
          (2024, 6, 15, 12, 30, 45.5)
        AS t(year, month, day, hour, min, sec)
        ORDER BY year, month, day
        """
      Then query result ordered
        | year | result                |
        | 2024 | 2024-01-01 00:00:00   |
        | 2024 | 2024-06-15 12:30:45.5 |
        | 2024 | NULL                  |

  Rule: Per-element NULL propagation with 6 arguments over columns

    Scenario: try_make_timestamp_ntz null second only, rest valid, returns NULL not a valid timestamp
      When query
        """
        SELECT try_make_timestamp_ntz(year, month, day, hour, min, sec) AS result
        FROM VALUES
          (2020, 1, 1, 0, 0, CAST(0.0 AS DOUBLE)),
          (2020, 1, 1, 0, 0, CAST(NULL AS DOUBLE))
        AS t(year, month, day, hour, min, sec)
        ORDER BY sec NULLS LAST
        """
      Then query result ordered
        | result              |
        | 2020-01-01 00:00:00 |
        | NULL                |

    Scenario: make_timestamp_ntz null second only over columns returns NULL without error
      When query
        """
        SELECT make_timestamp_ntz(year, month, day, hour, min, sec) AS result
        FROM VALUES
          (2020, 1, 1, 0, 0, CAST(0.0 AS DOUBLE)),
          (2020, 1, 1, 0, 0, CAST(NULL AS DOUBLE))
        AS t(year, month, day, hour, min, sec)
        ORDER BY sec NULLS LAST
        """
      Then query result ordered
        | result              |
        | 2020-01-01 00:00:00 |
        | NULL                |

    Scenario: make_timestamp_ntz null year over columns returns NULL without error
      When query
        """
        SELECT make_timestamp_ntz(year, 1, 1, 0, 0, 0.0) AS result
        FROM VALUES (2020), (CAST(NULL AS INT))
        AS t(year)
        ORDER BY year NULLS LAST
        """
      Then query result ordered
        | result              |
        | 2020-01-01 00:00:00 |
        | NULL                |

    Scenario: try_make_timestamp_ntz any null component over columns returns NULL
      When query
        """
        SELECT try_make_timestamp_ntz(year, month, day, hour, min, sec) AS result
        FROM VALUES
          (CAST(NULL AS INT), 1, 1, 0, 0, 0.0),
          (2020, CAST(NULL AS INT), 1, 0, 0, 0.0),
          (2020, 1, CAST(NULL AS INT), 0, 0, 0.0),
          (2020, 1, 1, CAST(NULL AS INT), 0, 0.0),
          (2020, 1, 1, 0, CAST(NULL AS INT), 0.0),
          (2020, 1, 1, 0, 0, CAST(NULL AS DOUBLE)),
          (2020, 1, 1, 0, 0, 0.0)
        AS t(year, month, day, hour, min, sec)
        ORDER BY year NULLS FIRST, month NULLS FIRST, day NULLS FIRST, hour NULLS FIRST, min NULLS FIRST, sec NULLS FIRST
        """
      Then query result ordered
        | result              |
        | NULL                |
        | NULL                |
        | NULL                |
        | NULL                |
        | NULL                |
        | NULL                |
        | 2020-01-01 00:00:00 |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: non-null components yield a timestamp
      When query
        """
        SELECT make_timestamp(2024, 1, 15, 10, 0, 0) AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    @sail-bug
    Scenario: non-null component columns yield a timestamp
      When query
        """
        SELECT make_timestamp(2024, 1, 15, 10, 0, CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = false)
        """

    Scenario: a nullable component column stays nullable
      When query
        """
        SELECT make_timestamp(2024, 1, 15, 10, 0, c) AS result FROM VALUES (CAST(0 AS INT)), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp (nullable = true)
        """
