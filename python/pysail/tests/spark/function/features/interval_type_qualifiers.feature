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
      | expression                              | schema_type            |
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
      | expression                                  | schema_type            |
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
