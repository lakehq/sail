Feature: Interval type qualifiers

  Rule: Year-month interval literal qualifiers

    Scenario Outline: year-month interval literal preserves its schema qualifier
      When query
      """
      SELECT <expression> AS result
      """
      Then query schema
      """
      root
       |-- result: <schema_type> (nullable = false)
      """

      Examples:
      | expression                             | schema_type            |
      | INTERVAL '10' YEAR                     | interval year          |
      | INTERVAL '-10' YEAR                    | interval year          |
      | INTERVAL '15' MONTH                    | interval month         |
      | INTERVAL '-15' MONTH                   | interval month         |
      | INTERVAL '10-8' YEAR TO MONTH          | interval year to month |
      | INTERVAL '-10-8' YEAR TO MONTH         | interval year to month |
      | INTERVAL '0' YEAR                      | interval year          |
      | INTERVAL '0' MONTH                     | interval month         |
      | INTERVAL '0-0' YEAR TO MONTH           | interval year to month |

  Rule: Day-time interval literal qualifiers

    Scenario Outline: day-time interval literal preserves its schema qualifier
      When query
      """
      SELECT <expression> AS result
      """
      Then query schema
      """
      root
       |-- result: <schema_type> (nullable = false)
      """

      Examples:
      | expression                                      | schema_type               |
      | INTERVAL '3' DAY                                | interval day              |
      | INTERVAL '-3' DAY                               | interval day              |
      | INTERVAL '4' HOUR                               | interval hour             |
      | INTERVAL '-4' HOUR                              | interval hour             |
      | INTERVAL '5' MINUTE                             | interval minute           |
      | INTERVAL '-5' MINUTE                            | interval minute           |
      | INTERVAL '6.123456' SECOND                      | interval second           |
      | INTERVAL '-6.123456' SECOND                     | interval second           |
      | INTERVAL '1 02' DAY TO HOUR                     | interval day to hour      |
      | INTERVAL '-1 02' DAY TO HOUR                    | interval day to hour      |
      | INTERVAL '1 02:03' DAY TO MINUTE                | interval day to minute    |
      | INTERVAL '-1 02:03' DAY TO MINUTE               | interval day to minute    |
      | INTERVAL '1 02:03:04.123456' DAY TO SECOND      | interval day to second    |
      | INTERVAL '-1 02:03:04.123456' DAY TO SECOND     | interval day to second    |
      | INTERVAL '02:03' HOUR TO MINUTE                 | interval hour to minute   |
      | INTERVAL '02:03:04.123456' HOUR TO SECOND       | interval hour to second   |
      | INTERVAL '03:04.123456' MINUTE TO SECOND        | interval minute to second |

  Rule: Interval casts

    Scenario Outline: numeric cast to interval preserves the requested schema qualifier
      When query
      """
      SELECT CAST(<value> AS <cast_type>) AS result
      """
      Then query schema
      """
      root
       |-- result: <schema_type> (nullable = false)
      """

      Examples:
      | value | cast_type              | schema_type            |
      | 10    | INTERVAL YEAR          | interval year          |
      | -10   | INTERVAL YEAR          | interval year          |
      | 15    | INTERVAL MONTH         | interval month         |
      | -15   | INTERVAL MONTH         | interval month         |
      | 128   | INTERVAL YEAR TO MONTH | interval year to month |
      | 3     | INTERVAL DAY           | interval day           |
      | 4     | INTERVAL HOUR          | interval hour          |
      | 5     | INTERVAL MINUTE        | interval minute        |
      | 6     | INTERVAL SECOND        | interval second        |
      | 90061 | INTERVAL DAY TO SECOND | interval day to second |

    Scenario Outline: string cast to interval preserves the requested schema qualifier
      When query
      """
      SELECT CAST(<value> AS <cast_type>) AS result
      """
      Then query schema
      """
      root
       |-- result: <schema_type> (nullable = true)
      """

      Examples:
      | value               | cast_type              | schema_type            |
      | '10'                | INTERVAL YEAR          | interval year          |
      | '15'                | INTERVAL MONTH         | interval month         |
      | '10-8'              | INTERVAL YEAR TO MONTH | interval year to month |
      | '3'                 | INTERVAL DAY           | interval day           |
      | '4'                 | INTERVAL HOUR          | interval hour          |
      | '5'                 | INTERVAL MINUTE        | interval minute        |
      | '6.123456'          | INTERVAL SECOND        | interval second        |
      | '1 02:03:04.123456' | INTERVAL DAY TO SECOND | interval day to second |

  Rule: Interval qualifiers through projections

    Scenario Outline: unaliased interval literal keeps literal column name
      When query
      """
      SELECT <expression>
      """
      Then query result
      | <column_name> |
      | <result>      |

      Examples: Standard ANSI form preserves the user's quoted text
      | expression                                 | column_name                                | result                                     |
      | INTERVAL '10' YEAR                         | INTERVAL '10' YEAR                         | INTERVAL '10' YEAR                         |
      | INTERVAL '10-8' YEAR TO MONTH              | INTERVAL '10-8' YEAR TO MONTH              | INTERVAL '10-8' YEAR TO MONTH              |
      | INTERVAL '3' DAY                           | INTERVAL '3' DAY                           | INTERVAL '3' DAY                           |
      | INTERVAL '1 02:03:04.123456' DAY TO SECOND | INTERVAL '1 02:03:04.123456' DAY TO SECOND | INTERVAL '1 02:03:04.123456' DAY TO SECOND |
      | INTERVAL '-10' YEAR                        | INTERVAL '-10' YEAR                        | INTERVAL '-10' YEAR                        |

      Examples: Multi-unit single-unit form normalizes to quoted ANSI form
      | expression        | column_name          | result               |
      | INTERVAL 10 YEAR  | INTERVAL '10' YEAR   | INTERVAL '10' YEAR   |
      | INTERVAL 10 YEARS | INTERVAL '10' YEAR   | INTERVAL '10' YEAR   |
      | INTERVAL 3 DAY    | INTERVAL '3' DAY     | INTERVAL '3' DAY     |
      | INTERVAL 3 DAYS   | INTERVAL '3' DAY     | INTERVAL '3' DAY     |
      | INTERVAL 5 MINUTE | INTERVAL '05' MINUTE | INTERVAL '05' MINUTE |

      Examples: WEEK converts to DAY and MILLISECOND/MICROSECOND convert to fractional SECOND
      | expression                | column_name              | result                   |
      | INTERVAL 1 WEEK           | INTERVAL '7' DAY          | INTERVAL '7' DAY          |
      | INTERVAL 2 WEEKS          | INTERVAL '14' DAY         | INTERVAL '14' DAY         |
      | INTERVAL 100 MILLISECOND  | INTERVAL '00.1' SECOND    | INTERVAL '00.1' SECOND    |
      | INTERVAL 100 MILLISECONDS | INTERVAL '00.1' SECOND    | INTERVAL '00.1' SECOND    |
      | INTERVAL 100 MICROSECOND  | INTERVAL '00.0001' SECOND | INTERVAL '00.0001' SECOND |
      | INTERVAL 100 MICROSECONDS | INTERVAL '00.0001' SECOND | INTERVAL '00.0001' SECOND |

      Examples: Multi-unit multiple-unit form normalizes to a range
      | expression                                | column_name                              | result                                   |
      | INTERVAL 10 YEARS 8 MONTHS                | INTERVAL '10-8' YEAR TO MONTH            | INTERVAL '10-8' YEAR TO MONTH            |
      | INTERVAL 1 DAY 2 HOURS                    | INTERVAL '1 02' DAY TO HOUR              | INTERVAL '1 02' DAY TO HOUR              |
      | INTERVAL 1 DAY 2 HOURS 3 MINUTES          | INTERVAL '1 02:03' DAY TO MINUTE         | INTERVAL '1 02:03' DAY TO MINUTE         |
      | INTERVAL 1 DAY 2 HOURS 3 MINUTES 4 SECONDS| INTERVAL '1 02:03:04' DAY TO SECOND      | INTERVAL '1 02:03:04' DAY TO SECOND      |
      | INTERVAL 1 HOUR 2 MINUTES                 | INTERVAL '01:02' HOUR TO MINUTE          | INTERVAL '01:02' HOUR TO MINUTE          |
      | INTERVAL 1 HOUR 2 MINUTES 3 SECONDS       | INTERVAL '01:02:03' HOUR TO SECOND       | INTERVAL '01:02:03' HOUR TO SECOND       |
      | INTERVAL 1 HOUR 2 MINUTES 3.456789 SECONDS| INTERVAL '01:02:03.456789' HOUR TO SECOND| INTERVAL '01:02:03.456789' HOUR TO SECOND|
      | INTERVAL 1 MINUTE 2 SECONDS               | INTERVAL '01:02' MINUTE TO SECOND        | INTERVAL '01:02' MINUTE TO SECOND        |
      | INTERVAL -10 YEARS 8 MONTHS               | INTERVAL '-9-4' YEAR TO MONTH            | INTERVAL '-9-4' YEAR TO MONTH            |
      | INTERVAL 10 YEARS -8 MONTHS               | INTERVAL '9-4' YEAR TO MONTH             | INTERVAL '9-4' YEAR TO MONTH             |
      | INTERVAL 0 DAYS 0 HOURS 0 MINUTES 0 SECONDS| INTERVAL '0 00:00:00' DAY TO SECOND     | INTERVAL '0 00:00:00' DAY TO SECOND      |

      Examples: Multi-unit out-of-order units still normalize to a canonical range
      | expression                            | column_name                        | result                             |
      | INTERVAL 8 MONTHS 10 YEARS            | INTERVAL '10-8' YEAR TO MONTH      | INTERVAL '10-8' YEAR TO MONTH      |
      | INTERVAL 3 MINUTES 1 DAY 2 HOURS      | INTERVAL '1 02:03' DAY TO MINUTE   | INTERVAL '1 02:03' DAY TO MINUTE   |
      | INTERVAL 4 SECONDS 1 DAY 2 HOURS 3 MINUTES | INTERVAL '1 02:03:04' DAY TO SECOND | INTERVAL '1 02:03:04' DAY TO SECOND |
      | INTERVAL 2 MINUTES 1 HOUR             | INTERVAL '01:02' HOUR TO MINUTE    | INTERVAL '01:02' HOUR TO MINUTE    |
      | INTERVAL 3 SECONDS 2 MINUTES 1 HOUR   | INTERVAL '01:02:03' HOUR TO SECOND | INTERVAL '01:02:03' HOUR TO SECOND |
      | INTERVAL 2 HOURS 1 DAY                | INTERVAL '1 02' DAY TO HOUR        | INTERVAL '1 02' DAY TO HOUR        |

      Examples: Multi-unit with WEEK collapses WEEK into DAY in the range
      | expression              | column_name                 | result                      |
      | INTERVAL 1 WEEK 2 DAYS  | INTERVAL '9' DAY            | INTERVAL '9' DAY            |
      | INTERVAL 1 WEEK 2 HOURS | INTERVAL '7 02' DAY TO HOUR | INTERVAL '7 02' DAY TO HOUR |

      Examples: Multi-unit with sub-second units collapses MILLISECOND/MICROSECOND into fractional SECOND
      | expression                                  | column_name                              | result                                   |
      | INTERVAL 1 SECOND 100 MILLISECONDS          | INTERVAL '01.1' SECOND                   | INTERVAL '01.1' SECOND                   |
      | INTERVAL 100 MILLISECONDS 200 MICROSECONDS  | INTERVAL '00.1002' SECOND                | INTERVAL '00.1002' SECOND                |
      | INTERVAL 1 HOUR 100 MILLISECONDS            | INTERVAL '01:00:00.1' HOUR TO SECOND     | INTERVAL '01:00:00.1' HOUR TO SECOND     |
      | INTERVAL 1 DAY 100 MICROSECONDS             | INTERVAL '1 00:00:00.0001' DAY TO SECOND | INTERVAL '1 00:00:00.0001' DAY TO SECOND |

      Examples: Multi-unit skipping intermediate units still infers the range
      | expression                 | column_name                        | result                             |
      | INTERVAL 1 DAY 5 MINUTES   | INTERVAL '1 00:05' DAY TO MINUTE   | INTERVAL '1 00:05' DAY TO MINUTE   |
      | INTERVAL 1 DAY 5 SECONDS   | INTERVAL '1 00:00:05' DAY TO SECOND| INTERVAL '1 00:00:05' DAY TO SECOND|
      | INTERVAL 1 HOUR 5 SECONDS  | INTERVAL '01:00:05' HOUR TO SECOND | INTERVAL '01:00:05' HOUR TO SECOND |

      Examples: Multi-unit with the same unit repeated combines into a single-field interval
      | expression               | column_name        | result             |
      | INTERVAL 1 DAY 2 DAYS    | INTERVAL '3' DAY   | INTERVAL '3' DAY   |
      | INTERVAL 1 YEAR 1 YEAR   | INTERVAL '2' YEAR  | INTERVAL '2' YEAR  |
      | INTERVAL 1 HOUR 1 HOUR   | INTERVAL '02' HOUR | INTERVAL '02' HOUR |

      Examples: Zero and large single-unit values
      | expression         | column_name        | result             |
      | INTERVAL '0' YEAR  | INTERVAL '0' YEAR  | INTERVAL '0' YEAR  |
      | INTERVAL '100' DAY | INTERVAL '100' DAY | INTERVAL '100' DAY |

      Examples: Outer unary minus wraps the column in parens and moves the sign inside the value
      | expression                                  | column_name                                   | result                                      |
      | -INTERVAL '10' YEAR                         | (- INTERVAL '10' YEAR)                        | INTERVAL '-10' YEAR                         |
      | -INTERVAL '10-8' YEAR TO MONTH              | (- INTERVAL '10-8' YEAR TO MONTH)             | INTERVAL '-10-8' YEAR TO MONTH              |
      | -INTERVAL '3' DAY                           | (- INTERVAL '3' DAY)                          | INTERVAL '-3' DAY                           |
      | -INTERVAL '1 02:03:04.123456' DAY TO SECOND | (- INTERVAL '1 02:03:04.123456' DAY TO SECOND)| INTERVAL '-1 02:03:04.123456' DAY TO SECOND |
      | -INTERVAL 10 YEARS                          | (- INTERVAL '10' YEAR)                        | INTERVAL '-10' YEAR                         |
      | -INTERVAL 10 YEARS 8 MONTHS                 | (- INTERVAL '10-8' YEAR TO MONTH)             | INTERVAL '-10-8' YEAR TO MONTH              |

    Scenario Outline: interval qualifier survives aliases and subqueries
      When query
      """
      SELECT renamed AS result
      FROM (
        SELECT value AS renamed
        FROM (
          SELECT <expression> AS value
        ) source
      ) projected
      """
      Then query schema
      """
      root
       |-- result: <schema_type> (nullable = false)
      """

      Examples:
      | expression                                 | schema_type            |
      | INTERVAL '10' YEAR                         | interval year          |
      | INTERVAL '15' MONTH                        | interval month         |
      | INTERVAL '10-8' YEAR TO MONTH              | interval year to month |
      | INTERVAL '3' DAY                           | interval day           |
      | INTERVAL '4' HOUR                          | interval hour          |
      | INTERVAL '5' MINUTE                        | interval minute        |
      | INTERVAL '6.123456' SECOND                 | interval second        |
      | INTERVAL '1 02:03:04.123456' DAY TO SECOND | interval day to second |

  Rule: Invalid interval qualifiers

    Scenario Outline: invalid interval qualifier ranges are rejected
      When query
      """
      SELECT CAST(1 AS <cast_type>) AS result
      """
      Then query error (?i)interval

      Examples:
      | cast_type                 |
      | INTERVAL MONTH TO YEAR    |
      | INTERVAL HOUR TO DAY      |
      | INTERVAL SECOND TO HOUR   |
