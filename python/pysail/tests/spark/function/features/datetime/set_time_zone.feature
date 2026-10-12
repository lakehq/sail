Feature: SET TIME ZONE and SET spark.sql.session.timeZone change the session time zone

  # Every expected value was measured on Spark 4.2 over Spark Connect (Java 17), starting from a session pinned to UTC.
  # The zone is restored after each scenario by the `config` step.
  #
  # Only the session time zone is covered. Every other configuration key is outside these statements for now.
  #
  # Known limits, none of them tested here:
  # - A zone that Spark accepts is stored as written, but Sail cannot use all of them in timestamp functions yet
  #   (`PST`, `+5:30`, `GMT+8`, offsets with seconds). The scenarios check `current_timezone()`, not those functions.
  # - `spark.conf.set` does not validate the zone yet: only these statements reject a zone Spark cannot resolve.
  # - `SET TIME ZONE LOCAL`, and the zone that `RESET` restores, are the zone of the host. Their scenarios check
  #   that the zone set before is gone, not which zone replaces it.
  # - `RESET` of any other key, and `RESET` without a key, are `@sail-bug` scenarios at the end.
  # - An unquoted offset with a colon (`SET spark.sql.session.timeZone = +08:00`) does not parse yet: `@sail-bug` scenario
  #   in the rule for `SET` on the zone key. With quotes, `SET TIME ZONE '+08:00'` works.
  # - `SET timezone = ...`: Spark stores an unrelated key and leaves the session zone alone.

  Rule: SET TIME ZONE sets the session zone

    Scenario Outline: SET TIME ZONE <value>
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET TIME ZONE <value>
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone   |
        | <zone> |

      Examples:
        | value                                | zone         |
        | 'Asia/Kolkata'                       | Asia/Kolkata |
        | 'PST'                                | PST          |
        | '+05:30'                             | +05:30       |
        | INTERVAL 1 HOURS                     | +01:00       |
        | INTERVAL '-08:00' HOUR TO MINUTE     | -08:00       |
        | INTERVAL 0 HOURS                     | Z            |
        | INTERVAL 1 HOURS 30 MINUTES          | +01:30       |
        | INTERVAL 1 HOURS 30 SECONDS          | +01:00:30    |

    Scenario: SET TIME ZONE returns the key and the value
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SET TIME ZONE 'Asia/Kolkata'
        """
      Then query result
        | key                        | value        |
        | spark.sql.session.timeZone | Asia/Kolkata |

    Scenario: SET TIME ZONE moves the instant of a timestamp literal
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET TIME ZONE 'Asia/Kolkata'
        """
      When query
        """
        SELECT unix_seconds(TIMESTAMP'2024-01-15 12:34:56') AS result
        """
      Then query result
        | result     |
        | 1705302296 |

    # The message names the part of the interval that Spark checks first: months, then days, then hours, then seconds.
    @spark-4
    Scenario Outline: SET TIME ZONE rejects the interval <value> and keeps the session zone
      Given config spark.sql.session.timeZone = UTC
      Given statement with error INVALID_INTERVAL_FORMAT.TIMEZONE_INTERVAL_OUT_OF_RANGE\] Error parsing '<input>' to interval
        """
        SET TIME ZONE <value>
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone |
        | UTC  |

      Examples:
        | value                                              | input      |
        | INTERVAL 1 MONTHS                                  | 1          |
        | INTERVAL 3 MONTHS                                  | 3          |
        | INTERVAL 1 YEAR                                    | 12         |
        | INTERVAL '1-2' YEAR TO MONTH                       | 14         |
        | INTERVAL '1-0' YEAR TO MONTH                       | 12         |
        | INTERVAL 1 MONTHS 1 DAYS                           | 1          |
        | INTERVAL 0 MONTHS 1 DAYS                           | 1          |
        | INTERVAL 1 DAYS                                    | 1          |
        | INTERVAL 2 DAYS                                    | 2          |
        | INTERVAL 1 DAYS -24 HOURS                          | 1          |
        | INTERVAL 1 DAYS -13 HOURS                          | 1          |
        | INTERVAL 1 DAY -24 HOURS                           | 1          |
        | INTERVAL 1 DAY 19 HOURS                            | 1          |
        | INTERVAL 1 WEEKS -168 HOURS                        | 7          |
        | INTERVAL 5 DAYS -1 DAYS                            | 4          |
        | INTERVAL -106751991 DAYS -14454775808 MICROSECONDS | -106751991 |
        | INTERVAL '25:00' HOUR TO MINUTE                    | 1          |
        | INTERVAL '1 00:00' DAY TO MINUTE                   | 1          |
        | INTERVAL '2 03:00' DAY TO MINUTE                   | 2          |
        | INTERVAL 19 HOURS                                  | 19         |
        | INTERVAL -19 HOURS                                 | 19         |
        | INTERVAL '19:00' HOUR TO MINUTE                    | 19         |
        | INTERVAL '-19:00' HOUR TO MINUTE                   | 19         |
        | INTERVAL 18 HOURS 1 SECONDS                        | 18         |
        | INTERVAL '18:00:01' HOUR TO SECOND                 | 18         |
        | INTERVAL 1 SECONDS 500 MILLISECONDS                | 1          |
        | INTERVAL -1500 MILLISECONDS                        | -1         |
        | INTERVAL 0 SECONDS 1 MICROSECONDS                  | 0          |
        | INTERVAL 1 HOURS 0.5 SECONDS                       | 3600       |
        | INTERVAL 1 YEAR -12 MONTHS 30 HOURS                | 30         |
        | INTERVAL 1 YEAR -12 MONTHS 24 HOURS                | 24         |
        | INTERVAL 1 YEAR -12 MONTHS 2 DAYS                  | 2          |
        | INTERVAL 1 YEAR -12 MONTHS 1 DAYS -24 HOURS        | 1          |
        | INTERVAL 1 MONTHS -1 MONTHS 0.5 SECONDS            | 0          |

    Scenario Outline: SET TIME ZONE accepts the interval <value> with its sign and boundaries
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET TIME ZONE <value>
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone   |
        | <zone> |

      Examples:
        | value                        | zone      |
        | INTERVAL 18 HOURS            | +18:00    |
        | INTERVAL -18 HOURS           | -18:00    |
        | INTERVAL -8 HOURS            | -08:00    |
        | INTERVAL -30 MINUTES         | -00:30    |
        | INTERVAL -1 HOURS 30 SECONDS | -00:59:30 |
        | INTERVAL '1' HOUR            | +01:00    |
        | INTERVAL 1 YEAR -12 MONTHS   | Z         |
        | INTERVAL '0-0' YEAR TO MONTH | Z         |
        | INTERVAL 1 DAYS -1 DAYS      | Z         |
        | INTERVAL 1 WEEKS -7 DAYS     | Z         |
        | INTERVAL 0 DAYS 1 HOURS      | +01:00    |
        | INTERVAL -30 SECONDS         | -00:00:30 |
        | INTERVAL - 1 HOURS           | -01:00    |
        | INTERVAL '-00:00:30' HOUR TO SECOND | -00:00:30 |
        | INTERVAL 1 YEAR -12 MONTHS 5 HOURS  | +05:00    |

    @spark-4
    Scenario: SET TIME ZONE rejects an interval written as a string without a unit
      Given config spark.sql.session.timeZone = UTC
      Given statement with error Invalid time zone displacement value
        """
        SET TIME ZONE INTERVAL '1 hour'
        """

    @spark-4
    Scenario: SET TIME ZONE rejects a missing zone
      Given config spark.sql.session.timeZone = UTC
      Given statement with error Invalid time zone displacement value
        """
        SET TIME ZONE
        """

    Scenario Outline: SET TIME ZONE <value> returns the normalized offset in the row
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SET TIME ZONE <value>
        """
      Then query result
        | key                        | value    |
        | spark.sql.session.timeZone | <offset> |

      Examples:
        | value                        | offset    |
        | INTERVAL 1 HOURS             | +01:00    |
        | INTERVAL 0 HOURS             | Z         |
        | INTERVAL -8 HOURS            | -08:00    |
        | INTERVAL 1 HOURS 30 SECONDS  | +01:00:30 |

    Scenario Outline: SET TIME ZONE written as <shape>
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        <statement>
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone         |
        | Asia/Kolkata |

      Examples:
        | shape                | statement                                   |
        | a double-quoted zone | SET TIME ZONE "Asia/Kolkata"                |
        | lowercase keywords   | set time zone 'Asia/Kolkata'                |
        | a trailing semicolon | SET TIME ZONE 'Asia/Kolkata';               |
        | a trailing comment   | SET TIME ZONE 'Asia/Kolkata' -- comment     |
        | key and value joined | SET spark.sql.session.timeZone=Asia/Kolkata |
        | a quoted key         | SET `spark.sql.session.timeZone`=Asia/Kolkata |
        | a raw string         | SET TIME ZONE r'Asia/Kolkata'               |
        | a semicolon after the unquoted value | SET spark.sql.session.timeZone = Asia/Kolkata; |

    # A backslash in a table cell is an escape of the table, so this statement is in a docstring.
    Scenario: SET TIME ZONE unescapes a backslash in the zone
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET TIME ZONE 'Asia\/Kolkata'
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone         |
        | Asia/Kolkata |

    Scenario: SET TIME ZONE returns a non-nullable key and value
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SET TIME ZONE 'Asia/Kolkata'
        """
      Then query schema
        """
        root
         |-- key: string (nullable = false)
         |-- value: string (nullable = false)
        """

    Scenario Outline: SET TIME ZONE accepts the zone <zone> as Spark does
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET TIME ZONE '<zone>'
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone   |
        | <zone> |

      Examples:
        | zone                |
        | UTC                 |
        | GMT                 |
        | UT                  |
        | Z                   |
        | +8                  |
        | +08                 |
        | +0530               |
        | -08                 |
        | +5:30               |
        | +05:3               |
        | +18:00              |
        | GMT+8               |
        | UTC-08:00           |
        | UT+1                |
        | EST                 |
        | IST                 |
        | America/Los_Angeles |
        | Etc/GMT+5           |
        | +1:30:45            |
        | +01:30:45           |
        | GMT0                |
        | US/Pacific          |
        | Asia/Calcutta       |
        | Etc/UTC             |
        | +0                  |
        | -00                 |
        | +1800               |
        | +180000             |
        | +09:5               |
        | +9:5                |
        | -18:00              |
        | GMT-0               |
        | UT+18               |
        | GMT+9:30            |
        | UTC+0530            |
        | UTC+05:30:15        |
        | GMT+8:00:00         |

    # Spark rejects the zone when the configuration is set, so the session keeps the zone it had.
    @spark-4
    Scenario Outline: SET TIME ZONE rejects the zone <zone> and keeps the session zone
      Given config spark.sql.session.timeZone = Asia/Kolkata
      Given statement with error INVALID_CONF_VALUE.TIME_ZONE
        """
        SET TIME ZONE '<zone>'
        """
      When query
        """
        SELECT current_timezone() AS zone, unix_seconds(TIMESTAMP'2024-01-15 12:34:56') AS result
        """
      Then query result
        | zone         | result     |
        | Asia/Kolkata | 1705302296 |

      Examples:
        | zone         |
        | Foo/Bar      |
        | Nope         |
        | z            |
        | utc          |
        | asia/kolkata |
        | +19:00       |
        | GMT+25       |
        | UTC+         |
        | Z+1          |
        | LOCAL        |
        | UT+19        |
        | UT-          |
        | +18:00:01    |
        | -18:00:01    |
        | +18:01       |
        | +24          |
        | +9:5:7       |
        | + 5          |
        | +123         |
        | +12345       |
        | +1:2:3       |
        | +01:00:5     |
        | +08:60       |
        | +08:00:60    |
        | +Z           |
        | GMT 0        |

    @spark-4
    Scenario: SET TIME ZONE rejects an empty zone and keeps the session zone
      Given config spark.sql.session.timeZone = Asia/Kolkata
      Given statement with error INVALID_CONF_VALUE.TIME_ZONE
        """
        SET TIME ZONE ''
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone         |
        | Asia/Kolkata |

    # The cells of a table are trimmed, so a zone with spaces needs its own statement.
    @spark-4
    Scenario: SET TIME ZONE rejects a zone with surrounding spaces and keeps the session zone
      Given config spark.sql.session.timeZone = Asia/Kolkata
      Given statement with error INVALID_CONF_VALUE.TIME_ZONE
        """
        SET TIME ZONE ' UTC '
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone         |
        | Asia/Kolkata |

    # The adjacent string literals of Spark's `stringLit` are one string.
    @sail-bug
    Scenario: SET TIME ZONE accepts a zone written as adjacent string literals
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET TIME ZONE 'Asia/' 'Kolkata'
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone         |
        | Asia/Kolkata |

    # `intervalValue` takes an optional sign before the number.
    @sail-bug
    Scenario: SET TIME ZONE accepts an interval with a plus sign
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET TIME ZONE INTERVAL +1 HOURS
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone   |
        | +01:00 |

    # `SET TIME ZONE .*?` is the catch-all alternative of the grammar.
    @sail-bug
    @spark-4
    Scenario Outline: SET TIME ZONE rejects <statement> as an invalid displacement
      Given config spark.sql.session.timeZone = UTC
      Given statement with error Invalid time zone displacement value
        """
        <statement>
        """

      Examples:
        | statement                          |
        | SET TIME ZONE UTC                  |
        | SET TIME ZONE 5                    |
        | SET TIME ZONE LOCAL x              |
        | SET TIME ZONE = 'UTC'              |
        | SET TIME ZONE NULL                 |
        | SET TIME ZONE Asia/Kolkata         |
        | SET TIME ZONE 'a' x                |
        | SET TIME ZONE INTERVAL             |
        | SET TIME ZONE (INTERVAL 1 HOURS)   |

    # Spark names the error of an interval that it cannot read before it checks the range.
    @sail-bug
    @spark-4
    Scenario Outline: SET TIME ZONE rejects the interval <value> with the error of Spark
      Given config spark.sql.session.timeZone = UTC
      Given statement with error <class>
        """
        SET TIME ZONE INTERVAL <value>
        """

      Examples:
        | value                          | class                                                  |
        | 1.5 HOURS                      | INVALID_INTERVAL_FORMAT.INVALID_FRACTION               |
        | 1000 NANOSECONDS               | INVALID_INTERVAL_FORMAT.INVALID_UNIT                   |
        | 9999999999999 HOURS            | INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION           |
        | 3000000000 DAYS                | INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION           |
        | 9223372036854775807 SECONDS    | INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION           |
        | '1 day' HOUR                   | Can only use numbers in the interval value part                |
        | 1 HOUR TO MINUTE               | The value of from-to unit must be a string                     |
        | '1-0' SECOND TO DAY            | INVALID_INTERVAL_FORMAT.UNSUPPORTED_FROM_TO_EXPRESSION |
        | '1' MINUTE TO HOUR             | INVALID_INTERVAL_FORMAT.UNSUPPORTED_FROM_TO_EXPRESSION |
        | '1' YEAR TO DAY                | INVALID_INTERVAL_FORMAT.UNSUPPORTED_FROM_TO_EXPRESSION |
        | '1' DAY TO MONTH               | INVALID_INTERVAL_FORMAT.UNSUPPORTED_FROM_TO_EXPRESSION |
        | '1' HOUR TO MINUTE 1 HOUR      | Can only have a single from-to unit in the interval literal    |
        | 1 HOUR 1 HOUR TO MINUTE        | Can only have a single from-to unit in the interval literal    |
        | '1:2' HOUR                     | INVALID_INTERVAL_FORMAT.INVALID_VALUE                          |
        | '' HOUR                        | INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER                    |
        | 1.1234567891 SECONDS           | INVALID_INTERVAL_FORMAT.INVALID_PRECISION                      |
        | 'abc' HOUR                     | Can only use numbers in the interval value part                |

    # Spark checks the range of every field of a `FROM ... TO ...` string (minute and second below 60, hour below 24 after a day).
    # The interval literal of Sail adds the overflow to the next field, also in `SELECT INTERVAL '8:61:17' HOUR TO SECOND`.
    @sail-bug
    Scenario Outline: SET TIME ZONE rejects the field out of range in the interval <value>
      Given config spark.sql.session.timeZone = UTC
      Given statement with error outside range
        """
        SET TIME ZONE INTERVAL <value>
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone |
        | UTC  |

      Examples:
        | value                          |
        | '8:61:17' HOUR TO SECOND       |
        | '0 5:60:24' DAY TO SECOND      |
        | '40:60' MINUTE TO SECOND       |
        | '-29:60' MINUTE TO SECOND      |
        | '3 27' DAY TO HOUR             |

    Scenario Outline: SET TIME ZONE accepts the short id <zone>
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET TIME ZONE '<zone>'
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone   |
        | <zone> |

      Examples:
        | zone |
        | ACT |
        | AET |
        | AGT |
        | ART |
        | AST |
        | BET |
        | BST |
        | CAT |
        | CNT |
        | CST |
        | CTT |
        | EAT |
        | ECT |
        | EST |
        | HST |
        | IET |
        | IST |
        | JST |
        | MIT |
        | MST |
        | NET |
        | NST |
        | PLT |
        | PNT |
        | PRT |
        | PST |
        | SST |
        | VST |

    Scenario: SET TIME ZONE LOCAL drops the zone that was set
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET TIME ZONE 'Pacific/Kiritimati'
        """
      Given statement
        """
        SET TIME ZONE LOCAL
        """
      When query
        """
        SELECT current_timezone() <> 'Pacific/Kiritimati' AS changed
        """
      Then query result
        | changed |
        | true    |

  Rule: SET on the zone key sets the session zone

    Scenario: SET spark.sql.session.timeZone with an unquoted value
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET spark.sql.session.timeZone = Asia/Kolkata
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone         |
        | Asia/Kolkata |

    Scenario: SET spark.sql.session.timeZone returns the key and the value
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SET spark.sql.session.timeZone = Asia/Kolkata
        """
      Then query result
        | key                        | value        |
        | spark.sql.session.timeZone | Asia/Kolkata |

    Scenario: SET spark.sql.session.timeZone moves the instant of a timestamp literal
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET spark.sql.session.timeZone = Asia/Kolkata
        """
      When query
        """
        SELECT unix_seconds(TIMESTAMP'2024-01-15 12:34:56') AS result
        """
      Then query result
        | result     |
        | 1705302296 |

    Scenario: SET spark.sql.session.timeZone returns a non-nullable key and value
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        SET spark.sql.session.timeZone = Asia/Kolkata
        """
      Then query schema
        """
        root
         |-- key: string (nullable = false)
         |-- value: string (nullable = false)
        """

    @spark-4
    Scenario: SET on the zone key needs an equals sign
      Given config spark.sql.session.timeZone = UTC
      Given statement with error INVALID_SET_SYNTAX
        """
        SET spark.sql.session.timeZone Asia/Kolkata
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone |
        | UTC  |

    @spark-4
    Scenario: SET spark.sql.session.timeZone rejects a zone Spark cannot resolve and keeps the session zone
      Given config spark.sql.session.timeZone = Asia/Kolkata
      Given statement with error INVALID_CONF_VALUE.TIME_ZONE
        """
        SET spark.sql.session.timeZone = Nope
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone         |
        | Asia/Kolkata |

    # Spark keeps the quotes as part of the value, so the zone is not found.
    @spark-4
    Scenario: SET spark.sql.session.timeZone with a quoted value is rejected and keeps the session zone
      Given config spark.sql.session.timeZone = UTC
      Given statement with error INVALID_CONF_VALUE.TIME_ZONE
        """
        SET spark.sql.session.timeZone = 'Asia/Kolkata'
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone |
        | UTC  |

    # The quotes are part of the value, and a value that is not a time zone is rejected whatever its type.
    @spark-4
    Scenario Outline: SET spark.sql.session.timeZone with the value <value> is rejected and keeps the session zone
      Given config spark.sql.session.timeZone = UTC
      Given statement with error INVALID_CONF_VALUE.TIME_ZONE
        """
        SET spark.sql.session.timeZone = <value>
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone |
        | UTC  |

      Examples:
        | value              |
        | "Asia/Kolkata"     |
        | r'Asia/Kolkata'    |
        | U&'Asia/Kolkata'   |
        | 'UTC'              |
        | ''                 |
        | 5                  |
        | true               |

    @spark-4
    Scenario: SET with a quoted key and a quoted value is rejected like the unquoted key and keeps the session zone
      Given config spark.sql.session.timeZone = UTC
      Given statement with error INVALID_CONF_VALUE.TIME_ZONE
        """
        SET `spark.sql.session.timeZone` = 'Asia/Kolkata'
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone |
        | UTC  |

    # Spark takes the key as it is written, so a key written as a string is not a configuration key.
    @spark-4
    Scenario: SET with a key written as a string and an unquoted value is rejected and keeps the session zone
      Given config spark.sql.session.timeZone = UTC
      Given statement with error INVALID_SET_SYNTAX
        """
        SET 'spark.sql.session.timeZone' = Asia/Kolkata
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone |
        | UTC  |

    Scenario: SET spark.sql.session.timeZone with a negative hour sets the zone as it is written
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET spark.sql.session.timeZone = -1
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone |
        | -1   |

    # Spark takes the statement as `SET key=value` with the key written as one word.
    @spark-4
    Scenario Outline: SET on the zone key rejects <statement> as a malformed SET and keeps the session zone
      Given config spark.sql.session.timeZone = UTC
      Given statement with error INVALID_SET_SYNTAX\] Expected format is 'SET', 'SET key', or 'SET key=value'
        """
        <statement>
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone |
        | UTC  |

      Examples:
        | statement                                              |
        | SET spark.sql.session.timeZone -1                      |
        | SET spark.sql.session.timeZone +8                      |
        | SET spark.sql.session.timeZone 'Asia/Kolkata'          |
        | SET spark.sql.session.timeZone "Asia/Kolkata"          |
        | SET 'spark.sql.session.timeZone' = -1                  |
        | SET 'spark.sql.session.timeZone' = 'Asia/Kolkata'      |
        | SET 'spark.sql.session.timeZone' = true                |
        | SET spark.sql . session.timeZone = Asia/Kolkata        |
        | SET spark . sql . session . timeZone = UTC             |
        | SET spark.`sql`.session.timeZone = UTC                 |

    # TODO: The unquoted value is cut at the first colon. See `SetPropertyValue` in the SQL parser.
    @sail-bug
    Scenario Outline: SET spark.sql.session.timeZone accepts the unquoted offset <value>
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET spark.sql.session.timeZone = <value>
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone    |
        | <value> |

      Examples:
        | value     |
        | +08:00    |
        | -08:00    |
        | -8:00     |
        | -08:00:00 |

    Scenario Outline: SET spark.sql.session.timeZone = <value> sets the zone as it is written
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET spark.sql.session.timeZone = <value>
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone    |
        | <value> |

      Examples:
        | value     |
        | -08       |
        | +8        |
        | Etc/GMT+5 |
        | GMT+8     |
        | UTC-08:00 |

    # Spark takes the backquoted value and strips the backquotes.
    @sail-bug
    Scenario Outline: SET accepts the backquoted value in <statement>
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        <statement>
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone   |
        | <zone> |

      Examples:
        | statement                                           | zone         |
        | SET spark.sql.session.timeZone = `Asia/Kolkata`     | Asia/Kolkata |
        | SET `spark.sql.session.timeZone`=`UTC`              | UTC          |

    # Spark takes everything after `=` up to `;` as the value, so the value below is not a time zone.
    @sail-bug
    @spark-4
    Scenario Outline: SET spark.sql.session.timeZone rejects the value in <statement> as a zone
      Given config spark.sql.session.timeZone = Asia/Kolkata
      Given statement with error INVALID_CONF_VALUE.TIME_ZONE
        """
        <statement>
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone         |
        | Asia/Kolkata |

      Examples:
        | statement                                               |
        | SET spark.sql.session.timeZone = Asia/Kolkata foo       |
        | SET spark.sql.session.timeZone = UTC +1                 |
        | SET spark.sql.session.timeZone =                        |
        | SET spark.sql.session.timeZone = UTC -- comment         |
        | SET spark.sql.session.timeZone = UTC /* comment */      |
        | SET spark.sql.session.timeZone = - 1                    |
        | SET spark.sql.session.timeZone = +  5                   |

    # `SET <key>` shows the value of the key.
    @sail-bug
    Scenario: SET spark.sql.session.timeZone without a value returns the current zone
      Given config spark.sql.session.timeZone = Asia/Kolkata
      When query
        """
        SET spark.sql.session.timeZone
        """
      Then query result
        | key                        | value        |
        | spark.sql.session.timeZone | Asia/Kolkata |

  Rule: RESET on the zone key restores the default zone

    Scenario: RESET spark.sql.session.timeZone drops the zone that was set
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET TIME ZONE 'Pacific/Kiritimati'
        """
      Given statement
        """
        RESET spark.sql.session.timeZone
        """
      When query
        """
        SELECT current_timezone() <> 'Pacific/Kiritimati' AS changed
        """
      Then query result
        | changed |
        | true    |

  Rule: RESET returns no columns

    Scenario: RESET spark.sql.session.timeZone returns an empty relation
      Given config spark.sql.session.timeZone = UTC
      When query
        """
        RESET spark.sql.session.timeZone
        """
      Then query schema
        """
        root
        """

    @sail-bug
    Scenario Outline: RESET of the key <key> is accepted
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        RESET <key>
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone |
        | UTC  |

      Examples:
        | key                           |
        | spark.sql.shuffle.partitions  |
        | foo.bar                       |
        | timezone                      |
        | spark.sql.session.timezone    |
        | `a b`                         |
        | a:b                           |
        | a.                            |

    # A static configuration cannot be reset, and neither can a configuration of the core of Spark.
    @sail-bug
    @spark-4
    Scenario Outline: RESET of the key <key> is rejected with <class>
      Given statement with error <class>
        """
        RESET <key>
        """

      Examples:
        | key                     | class                         |
        | spark.sql.warehouse.dir | CANNOT_MODIFY_STATIC_CONFIG   |
        | spark.executor.memory   | CANNOT_MODIFY_CONFIG          |

    # The comment is part of the statement for Spark.
    @sail-bug
    @spark-4
    Scenario: RESET spark.sql.session.timeZone with a comment is rejected
      Given config spark.sql.session.timeZone = Asia/Kolkata
      Given statement with error INVALID_RESET_COMMAND_FORMAT
        """
        RESET spark.sql.session.timeZone -- comment
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone         |
        | Asia/Kolkata |

    @spark-4
    Scenario: RESET rejects a key written as a string and keeps the session zone
      Given config spark.sql.session.timeZone = Asia/Kolkata
      Given statement with error INVALID_RESET_COMMAND_FORMAT\] Expected format is 'RESET' or 'RESET key'
        """
        RESET 'spark.sql.session.timeZone'
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone         |
        | Asia/Kolkata |

    @spark-4
    Scenario Outline: RESET rejects the statement <statement> and keeps the session zone
      Given config spark.sql.session.timeZone = Asia/Kolkata
      Given statement with error INVALID_RESET_COMMAND_FORMAT\] Expected format is 'RESET' or 'RESET key'
        """
        <statement>
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone         |
        | Asia/Kolkata |

      Examples:
        | statement                                                |
        | RESET a b                                                |
        | RESET spark.sql.session.timeZone spark.sql.ansi.enabled  |
        | RESET spark.sql.session.timeZone =                       |

    # `RESET` without a key is not implemented yet.
    @sail-bug
    Scenario: RESET without a key drops the zone that was set
      Given config spark.sql.session.timeZone = UTC
      Given statement
        """
        SET TIME ZONE 'Pacific/Kiritimati'
        """
      Given statement
        """
        RESET
        """
      When query
        """
        SELECT current_timezone() <> 'Pacific/Kiritimati' AS changed
        """
      Then query result
        | changed |
        | true    |

    @spark-4
    Scenario Outline: RESET rejects the key <statement> and keeps the session zone
      Given config spark.sql.session.timeZone = Asia/Kolkata
      Given statement with error INVALID_RESET_COMMAND_FORMAT\] Expected format is 'RESET' or 'RESET key'
        """
        <statement>
        """
      When query
        """
        SELECT current_timezone() AS zone
        """
      Then query result
        | zone         |
        | Asia/Kolkata |

      Examples:
        | statement                                   |
        | RESET spark . sql . session . timeZone      |
        | RESET spark.`sql`.session.timeZone          |

