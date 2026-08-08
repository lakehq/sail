Feature: convert_timezone

  Rule: Type coercion

  Background:
      Given config spark.sql.session.timeZone = Asia/Shanghai

    Scenario Outline: `convert_timezone` with coercible input
      When query
        """
        SELECT convert_timezone('America/Los_Angeles', 'Europe/Amsterdam', <ts>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                                    | result              |
        | TIMESTAMP '2024-06-15 14:30:00'       | 2024-06-15 23:30:00 |
        | TIMESTAMP '2024-06-15 13:30:00+07:00' | 2024-06-15 23:30:00 |
        | TIMESTAMP_NTZ '2024-06-15 14:30:00'   | 2024-06-15 23:30:00 |
        | TIMESTAMP_LTZ '2024-06-15 14:30:00'   | 2024-06-15 23:30:00 |
        | '2024-06-15 14:30:00'                 | 2024-06-15 23:30:00 |
        | '2024-06-15 14:30:00+07:00'           | 2024-06-15 23:30:00 |
        | DATE '2024-06-15'                     | 2024-06-15 09:00:00 |

    Scenario Outline: `convert_timezone` with coercible input and implicit source timezone
      When query
        """
        SELECT convert_timezone('Europe/Amsterdam', <ts>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                                    | result              |
        | TIMESTAMP '2024-06-15 14:30:00'       | 2024-06-15 08:30:00 |
        | TIMESTAMP '2024-06-15 13:30:00+07:00' | 2024-06-15 08:30:00 |
        | TIMESTAMP_NTZ '2024-06-15 14:30:00'   | 2024-06-15 08:30:00 |
        | TIMESTAMP_LTZ '2024-06-15 14:30:00'   | 2024-06-15 08:30:00 |
        | '2024-06-15 14:30:00'                 | 2024-06-15 08:30:00 |
        | '2024-06-15 14:30:00+07:00'           | 2024-06-15 08:30:00 |
        | DATE '2024-06-15'                     | 2024-06-14 18:00:00 |

  Rule: Daylight saving time handling

  Background:
      Given config spark.sql.session.timeZone = Asia/Shanghai

    Scenario Outline: `convert_timezone` around daylight saving time transition
      When query
        """
        SELECT convert_timezone(<from>, <to>, <ts>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | ts                    | from                  | to                    | result              |
        | '2025-03-09 01:30:00' | 'America/Los_Angeles' | 'Europe/Amsterdam'    | 2025-03-09 10:30:00 |
        | '2025-03-09 02:30:00' | 'America/Los_Angeles' | 'Europe/Amsterdam'    | 2025-03-09 11:30:00 |
        | '2025-03-09 03:30:00' | 'America/Los_Angeles' | 'Europe/Amsterdam'    | 2025-03-09 11:30:00 |
        | '2025-03-09 10:30:00' | 'Europe/Amsterdam'    | 'America/Los_Angeles' | 2025-03-09 01:30:00 |
        | '2025-03-09 11:30:00' | 'Europe/Amsterdam'    | 'America/Los_Angeles' | 2025-03-09 03:30:00 |

  Rule: Zone ID parsing

  Background:
      Given config spark.sql.session.timeZone = UTC

    Scenario Outline: `convert_timezone` accepts every zone offset form Java accepts
      When query
        """
        SELECT CAST(convert_timezone('<zone>', 'UTC', TIMESTAMP_NTZ '2024-01-01 12:00:00') AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | zone      | result              |
        | Z         | 2024-01-01 12:00:00 |
        | +8        | 2024-01-01 04:00:00 |
        | -9        | 2024-01-01 21:00:00 |
        | +08       | 2024-01-01 04:00:00 |
        | +0130     | 2024-01-01 10:30:00 |
        | +01:30    | 2024-01-01 10:30:00 |
        | +013045   | 2024-01-01 10:29:15 |
        | +01:30:45 | 2024-01-01 10:29:15 |
        | +18:00    | 2023-12-31 18:00:00 |
        | -18:00    | 2024-01-02 06:00:00 |

    Scenario Outline: `convert_timezone` pads the offset forms supported before Spark 3.0
      When query
        """
        SELECT CAST(convert_timezone('<zone>', 'UTC', TIMESTAMP_NTZ '2024-01-01 12:00:00') AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | zone     | result              |
        | +8:30    | 2024-01-01 03:30:00 |
        | +08:3    | 2024-01-01 03:57:00 |
        | +8:3     | 2024-01-01 03:57:00 |
        | GMT+8:30 | 2024-01-01 03:30:00 |
        | UTC+1:09 | 2024-01-01 10:51:00 |

    Scenario Outline: `convert_timezone` accepts bare and prefixed UTC zone IDs
      When query
        """
        SELECT CAST(convert_timezone('<zone>', 'UTC', TIMESTAMP_NTZ '2024-01-01 12:00:00') AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | zone     | result              |
        | UT       | 2024-01-01 12:00:00 |
        | UTC      | 2024-01-01 12:00:00 |
        | GMT      | 2024-01-01 12:00:00 |
        | UTC+8    | 2024-01-01 04:00:00 |
        | GMT+0130 | 2024-01-01 10:30:00 |
        | UT+01:00 | 2024-01-01 11:00:00 |

    Scenario Outline: `convert_timezone` resolves the legacy short zone IDs
      When query
        """
        SELECT CAST(convert_timezone('<zone>', 'UTC', TIMESTAMP_NTZ '2024-01-01 12:00:00') AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | zone | result              |
        | PST  | 2024-01-01 20:00:00 |
        | EST  | 2024-01-01 17:00:00 |
        | MST  | 2024-01-01 19:00:00 |
        | HST  | 2024-01-01 22:00:00 |

    Scenario Outline: `convert_timezone` parses the session time zone as the implicit source zone
      Given config spark.sql.session.timeZone = <zone>
      When query
        """
        SELECT CAST(convert_timezone('UTC', TIMESTAMP_NTZ '2024-01-01 12:00:00') AS STRING) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | zone      | result              |
        | +8        | 2024-01-01 04:00:00 |
        | +01:30:45 | 2024-01-01 10:29:15 |
        | GMT+8:30  | 2024-01-01 03:30:00 |
        | UT        | 2024-01-01 12:00:00 |
        | UTC+8     | 2024-01-01 04:00:00 |
        | EST       | 2024-01-01 17:00:00 |

    Scenario Outline: `convert_timezone` rejects the zone IDs Java rejects
      When query
        """
        SELECT convert_timezone('<zone>', 'UTC', TIMESTAMP_NTZ '2024-01-01 12:00:00') AS result
        """
      Then query error \[INVALID_TIMEZONE\]

      Examples:
        | zone      |
        | +         |
        | A         |
        | +8:       |
        | +123      |
        | +01:2:03  |
        | +18:00:01 |
        | +19:00    |
        | Foo/Bar   |

    Scenario: an invalid zone ID is reported without quoting
      When query
        """
        SELECT convert_timezone('Foo/Bar', 'UTC', TIMESTAMP_NTZ '2024-01-01 12:00:00') AS result
        """
      Then query error \[INVALID_TIMEZONE\] The timezone: Foo/Bar is invalid\.

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to convert_timezone yields the schema Spark declares
      When query
        """
        SELECT convert_timezone('Europe/Brussels', 'America/Los_Angeles', timestamp_ntz'2021-12-06 00:00:00') AS result
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to convert_timezone yields the schema Spark declares
      When query
        """
        SELECT convert_timezone(CAST(id AS STRING), 'America/Los_Angeles', timestamp_ntz'2021-12-06 00:00:00') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = false)
        """

    Scenario: a nullable column input to convert_timezone stays nullable
      When query
        """
        SELECT convert_timezone(c, 'America/Los_Angeles', timestamp_ntz'2021-12-06 00:00:00') AS result FROM VALUES ('Europe/Brussels'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: timestamp_ntz (nullable = true)
        """
