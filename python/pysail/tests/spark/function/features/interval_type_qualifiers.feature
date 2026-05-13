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
      Then query result
      | result   |
      | <result> |

      Examples:
      | expression                              | schema_type            | result                              |
      | INTERVAL '10' YEAR                     | interval year          | INTERVAL '10' YEAR                  |
      | INTERVAL '-10' YEAR                    | interval year          | INTERVAL '-10' YEAR                 |
      | INTERVAL '15' MONTH                    | interval month         | INTERVAL '15' MONTH                 |
      | INTERVAL '-15' MONTH                   | interval month         | INTERVAL '-15' MONTH                |
      | INTERVAL '10-8' YEAR TO MONTH          | interval year to month | INTERVAL '10-8' YEAR TO MONTH       |
      | INTERVAL '-10-8' YEAR TO MONTH         | interval year to month | INTERVAL '-10-8' YEAR TO MONTH      |
      | INTERVAL '0' YEAR                      | interval year          | INTERVAL '0' YEAR                   |
      | INTERVAL '0' MONTH                     | interval month         | INTERVAL '0' MONTH                  |
      | INTERVAL '0-0' YEAR TO MONTH           | interval year to month | INTERVAL '0-0' YEAR TO MONTH        |

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
      Then query result
      | result   |
      | <result> |

      Examples:
      | expression                                      | schema_type            | result                                      |
      | INTERVAL '3' DAY                                | interval day           | INTERVAL '3' DAY                            |
      | INTERVAL '-3' DAY                               | interval day           | INTERVAL '-3' DAY                           |
      | INTERVAL '4' HOUR                               | interval hour          | INTERVAL '4' HOUR                           |
      | INTERVAL '-4' HOUR                              | interval hour          | INTERVAL '-4' HOUR                          |
      | INTERVAL '5' MINUTE                             | interval minute        | INTERVAL '5' MINUTE                         |
      | INTERVAL '-5' MINUTE                            | interval minute        | INTERVAL '-5' MINUTE                        |
      | INTERVAL '6.123456' SECOND                      | interval second        | INTERVAL '6.123456' SECOND                  |
      | INTERVAL '-6.123456' SECOND                     | interval second        | INTERVAL '-6.123456' SECOND                 |
      | INTERVAL '1 02' DAY TO HOUR                     | interval day to hour   | INTERVAL '1 02' DAY TO HOUR                 |
      | INTERVAL '-1 02' DAY TO HOUR                    | interval day to hour   | INTERVAL '-1 02' DAY TO HOUR                |
      | INTERVAL '1 02:03' DAY TO MINUTE                | interval day to minute | INTERVAL '1 02:03' DAY TO MINUTE            |
      | INTERVAL '-1 02:03' DAY TO MINUTE               | interval day to minute | INTERVAL '-1 02:03' DAY TO MINUTE           |
      | INTERVAL '1 02:03:04.123456' DAY TO SECOND      | interval day to second | INTERVAL '1 02:03:04.123456' DAY TO SECOND  |
      | INTERVAL '-1 02:03:04.123456' DAY TO SECOND     | interval day to second | INTERVAL '-1 02:03:04.123456' DAY TO SECOND |
      | INTERVAL '02:03' HOUR TO MINUTE                 | interval hour to minute | INTERVAL '02:03' HOUR TO MINUTE            |
      | INTERVAL '02:03:04.123456' HOUR TO SECOND       | interval hour to second | INTERVAL '02:03:04.123456' HOUR TO SECOND  |
      | INTERVAL '03:04.123456' MINUTE TO SECOND        | interval minute to second | INTERVAL '03:04.123456' MINUTE TO SECOND |

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
      Then query result
      | result   |
      | <result> |

      Examples:
      | value | cast_type               | schema_type               | result                               |
      | 10    | INTERVAL YEAR           | interval year             | INTERVAL '10' YEAR                   |
      | -10   | INTERVAL YEAR           | interval year             | INTERVAL '-10' YEAR                  |
      | 15    | INTERVAL MONTH          | interval month            | INTERVAL '15' MONTH                  |
      | -15   | INTERVAL MONTH          | interval month            | INTERVAL '-15' MONTH                 |
      | 128   | INTERVAL YEAR TO MONTH  | interval year to month    | INTERVAL '10-8' YEAR TO MONTH        |
      | 3     | INTERVAL DAY            | interval day              | INTERVAL '3' DAY                     |
      | 4     | INTERVAL HOUR           | interval hour             | INTERVAL '4' HOUR                    |
      | 5     | INTERVAL MINUTE         | interval minute           | INTERVAL '5' MINUTE                  |
      | 6     | INTERVAL SECOND         | interval second           | INTERVAL '6' SECOND                  |
      | 90061 | INTERVAL DAY TO SECOND  | interval day to second    | INTERVAL '1 01:01:01' DAY TO SECOND  |

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
      Then query result
      | result   |
      | <result> |

      Examples:
      | value                  | cast_type                  | schema_type               | result                              |
      | '10'                   | INTERVAL YEAR              | interval year             | INTERVAL '10' YEAR                  |
      | '15'                   | INTERVAL MONTH             | interval month            | INTERVAL '15' MONTH                 |
      | '10-8'                 | INTERVAL YEAR TO MONTH     | interval year to month    | INTERVAL '10-8' YEAR TO MONTH       |
      | '3'                    | INTERVAL DAY               | interval day              | INTERVAL '3' DAY                    |
      | '4'                    | INTERVAL HOUR              | interval hour             | INTERVAL '4' HOUR                   |
      | '5'                    | INTERVAL MINUTE            | interval minute           | INTERVAL '5' MINUTE                 |
      | '6.123456'             | INTERVAL SECOND            | interval second           | INTERVAL '6.123456' SECOND          |
      | '1 02:03:04.123456'    | INTERVAL DAY TO SECOND     | interval day to second    | INTERVAL '1 02:03:04.123456' DAY TO SECOND |

  Rule: Interval qualifiers through projections

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
      Then query result
      | result   |
      | <result> |

      Examples:
      | expression                                  | schema_type              | result                                  |
      | INTERVAL '10' YEAR                         | interval year            | INTERVAL '10' YEAR                      |
      | INTERVAL '15' MONTH                        | interval month           | INTERVAL '15' MONTH                     |
      | INTERVAL '10-8' YEAR TO MONTH              | interval year to month   | INTERVAL '10-8' YEAR TO MONTH           |
      | INTERVAL '3' DAY                           | interval day             | INTERVAL '3' DAY                        |
      | INTERVAL '4' HOUR                          | interval hour            | INTERVAL '4' HOUR                       |
      | INTERVAL '5' MINUTE                        | interval minute          | INTERVAL '5' MINUTE                     |
      | INTERVAL '6.123456' SECOND                 | interval second          | INTERVAL '6.123456' SECOND              |
      | INTERVAL '1 02:03:04.123456' DAY TO SECOND | interval day to second   | INTERVAL '1 02:03:04.123456' DAY TO SECOND |

    Scenario Outline: interval qualifier survives struct fields
      When query
      """
      SELECT named_struct('value', <expression>) AS result
      """
      Then query schema
      """
      root
       |-- result: struct (nullable = false)
       |    |-- value: <schema_type> (nullable = false)
      """
      Then query result
      | result     |
      | <result>   |

      Examples:
      | expression                                  | schema_type              | result                                  |
      | INTERVAL '10' YEAR                         | interval year            | {10 years}                              |
      | INTERVAL '15' MONTH                        | interval month           | {15 months}                             |
      | INTERVAL '10-8' YEAR TO MONTH              | interval year to month   | {10 years 8 months}                     |
      | INTERVAL '3' DAY                           | interval day             | {3 days}                                |
      | INTERVAL '4' HOUR                          | interval hour            | {4 hours}                               |
      | INTERVAL '5' MINUTE                        | interval minute          | {5 minutes}                             |
      | INTERVAL '6.123456' SECOND                 | interval second          | {6.123456 seconds}                      |
      | INTERVAL '1 02:03:04.123456' DAY TO SECOND | interval day to second   | {1 days 2 hours 3 minutes 4.123456 seconds} |

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
