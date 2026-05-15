@interval-qualifiers
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

      Examples: Unqualified string literals normalize to the canonical typed form
      | expression                              | column_name                        | result                             |
      | INTERVAL '1 month'                      | INTERVAL '1' MONTH                 | INTERVAL '1' MONTH                 |
      | INTERVAL '1 day'                        | INTERVAL '1' DAY                   | INTERVAL '1' DAY                   |
      | INTERVAL '5.5 seconds'                  | INTERVAL '05.5' SECOND             | INTERVAL '05.5' SECOND             |
      | INTERVAL '1 hour'                       | INTERVAL '01' HOUR                 | INTERVAL '01' HOUR                 |
      | INTERVAL '1 minute'                     | INTERVAL '01' MINUTE               | INTERVAL '01' MINUTE               |
      | INTERVAL '2 years 3 months'             | INTERVAL '2-3' YEAR TO MONTH       | INTERVAL '2-3' YEAR TO MONTH       |
      | INTERVAL '1 day 2 hours'                | INTERVAL '1 02' DAY TO HOUR        | INTERVAL '1 02' DAY TO HOUR        |
      | INTERVAL '1 day 2 hours 30 minutes'     | INTERVAL '1 02:30' DAY TO MINUTE   | INTERVAL '1 02:30' DAY TO MINUTE   |
      | INTERVAL '1 hour 30 minutes 15 seconds' | INTERVAL '01:30:15' HOUR TO SECOND | INTERVAL '01:30:15' HOUR TO SECOND |
      | INTERVAL '1 week'                       | INTERVAL '7' DAY                   | INTERVAL '7' DAY                   |
      | INTERVAL '100 milliseconds'             | INTERVAL '00.1' SECOND             | INTERVAL '00.1' SECOND             |
      | INTERVAL '100 microseconds'             | INTERVAL '00.0001' SECOND          | INTERVAL '00.0001' SECOND          |
      | INTERVAL '-1 day'                       | INTERVAL '-1' DAY                  | INTERVAL '-1' DAY                  |
      | INTERVAL '-2 years 3 months'            | INTERVAL '-1-9' YEAR TO MONTH      | INTERVAL '-1-9' YEAR TO MONTH      |
      | INTERVAL '0 day'                        | INTERVAL '0' DAY                   | INTERVAL '0' DAY                   |

      Examples: Outer unary minus wraps the column in parens and moves the sign inside the value
      | expression                                  | column_name                                   | result                                      |
      | -INTERVAL '10' YEAR                         | (- INTERVAL '10' YEAR)                        | INTERVAL '-10' YEAR                         |
      | -INTERVAL '10-8' YEAR TO MONTH              | (- INTERVAL '10-8' YEAR TO MONTH)             | INTERVAL '-10-8' YEAR TO MONTH              |
      | -INTERVAL '3' DAY                           | (- INTERVAL '3' DAY)                          | INTERVAL '-3' DAY                           |
      | -INTERVAL '1 02:03:04.123456' DAY TO SECOND | (- INTERVAL '1 02:03:04.123456' DAY TO SECOND)| INTERVAL '-1 02:03:04.123456' DAY TO SECOND |
      | -INTERVAL 10 YEARS                          | (- INTERVAL '10' YEAR)                        | INTERVAL '-10' YEAR                         |
      | -INTERVAL 10 YEARS 8 MONTHS                 | (- INTERVAL '10-8' YEAR TO MONTH)             | INTERVAL '-10-8' YEAR TO MONTH              |
      | -INTERVAL 1 WEEK                            | (- INTERVAL '7' DAY)                          | INTERVAL '-7' DAY                           |
      | -INTERVAL 5 MINUTE                          | (- INTERVAL '05' MINUTE)                      | INTERVAL '-05' MINUTE                       |
      | -INTERVAL 100 MILLISECOND                   | (- INTERVAL '00.1' SECOND)                    | INTERVAL '-00.1' SECOND                     |
      | -INTERVAL '1 day'                           | (- INTERVAL '1' DAY)                          | INTERVAL '-1' DAY                           |
      | -INTERVAL '5.5 seconds'                     | (- INTERVAL '05.5' SECOND)                    | INTERVAL '-05.5' SECOND                     |
      | -INTERVAL '2 years 3 months'                | (- INTERVAL '2-3' YEAR TO MONTH)              | INTERVAL '-2-3' YEAR TO MONTH               |

  Rule: Operations on intervals preserve qualifier metadata

    Scenario Outline: binary arithmetic on intervals normalizes to the widest covering qualifier
      When query
      """
      SELECT <expression>
      """
      Then query result
      | <column_name> |
      | <result>      |

      Examples: Year-month +/- widens to YEAR TO MONTH when start/end differ
      | expression                                | column_name                                 | result                        |
      | INTERVAL '10' YEAR + INTERVAL '5' MONTH   | (INTERVAL '10' YEAR + INTERVAL '5' MONTH)   | INTERVAL '10-5' YEAR TO MONTH |
      | INTERVAL '10' YEAR + INTERVAL '5' YEAR    | (INTERVAL '10' YEAR + INTERVAL '5' YEAR)    | INTERVAL '15' YEAR            |
      | INTERVAL '10' MONTH + INTERVAL '5' MONTH  | (INTERVAL '10' MONTH + INTERVAL '5' MONTH)  | INTERVAL '15' MONTH           |
      | INTERVAL '10' YEAR - INTERVAL '5' MONTH   | (INTERVAL '10' YEAR - INTERVAL '5' MONTH)   | INTERVAL '9-7' YEAR TO MONTH  |

      Examples: Day-time +/- widens to the broadest range; HOUR literals format as zero-padded in the column header
      | expression                                            | column_name                                              | result                              |
      | INTERVAL '1' DAY + INTERVAL '2' HOUR                  | (INTERVAL '1' DAY + INTERVAL '02' HOUR)                  | INTERVAL '1 02' DAY TO HOUR         |
      | INTERVAL '1' DAY + INTERVAL '5' SECOND                | (INTERVAL '1' DAY + INTERVAL '05' SECOND)                | INTERVAL '1 00:00:05' DAY TO SECOND |
      | INTERVAL '01:02' HOUR TO MINUTE + INTERVAL '5' SECOND | (INTERVAL '01:02' HOUR TO MINUTE + INTERVAL '05' SECOND) | INTERVAL '01:02:05' HOUR TO SECOND  |

    Scenario Outline: interval times/divided by numeric widens to the widest range qualifier
      When query
      """
      SELECT <expression>
      """
      Then query result
      | <column_name> |
      | <result>      |

      Examples:
      | expression                              | column_name                               | result                              |
      | INTERVAL '10' YEAR * 2                  | (INTERVAL '10' YEAR * 2)                  | INTERVAL '20-0' YEAR TO MONTH       |
      | 2 * INTERVAL '10' YEAR                  | (INTERVAL '10' YEAR * 2)                  | INTERVAL '20-0' YEAR TO MONTH       |
      | INTERVAL '10' YEAR / 2                  | (INTERVAL '10' YEAR / 2)                  | INTERVAL '5-0' YEAR TO MONTH        |
      | INTERVAL '3' DAY * 4                    | (INTERVAL '3' DAY * 4)                    | INTERVAL '12 00:00:00' DAY TO SECOND|
      | INTERVAL '1 02:03:04' DAY TO SECOND * 2 | (INTERVAL '1 02:03:04' DAY TO SECOND * 2) | INTERVAL '2 04:06:08' DAY TO SECOND |

    Scenario: SUM / MIN / MAX preserve the input qualifier; AVG widens to the broadest range
      When query
      """
      SELECT SUM(c) AS sum_c, MIN(c) AS min_c, MAX(c) AS max_c, AVG(c) AS avg_c
      FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(c)
      """
      Then query result ordered
      | sum_c            | min_c            | max_c            | avg_c                               |
      | INTERVAL '6' DAY | INTERVAL '1' DAY | INTERVAL '3' DAY | INTERVAL '2 00:00:00' DAY TO SECOND |

    @sail-bug
    # Sail's SUM signature does not yet accept Interval(YearMonth) — DataFusion's
    # built-in `sum` rejects it, and Arrow's `Interval(YearMonth) -> Int*` cast
    # is declared in `can_cast_types` but not actually implemented, so the usual
    # bridge-through-int trick doesn't work. A custom UDAF (similar to the one
    # in `aggregate/try_avg.rs`) is the cleanest path forward.
    Scenario: SUM over typed YEAR intervals stays as YEAR
      When query
      """
      SELECT SUM(c) AS s
      FROM VALUES (INTERVAL '1' YEAR), (INTERVAL '2' YEAR), (INTERVAL '3' YEAR) AS t(c)
      """
      Then query result
      | s                |
      | INTERVAL '6' YEAR |

    @sail-bug
    # Same root cause as `SUM over typed YEAR intervals stays as YEAR` above —
    # Sail's SUM does not yet accept Interval(YearMonth). YEAR TO MONTH fails
    # the same way as the single-field YEAR form.
    Scenario: SUM over YEAR TO MONTH stays as YEAR TO MONTH
      When query
      """
      SELECT SUM(c)
      FROM VALUES (INTERVAL '1-2' YEAR TO MONTH), (INTERVAL '3-4' YEAR TO MONTH) AS t(c)
      """
      Then query result
      | sum(c)                       |
      | INTERVAL '4-6' YEAR TO MONTH |

    Scenario: window MIN / MAX preserve the input qualifier
      When query
      """
      SELECT c, MIN(c) OVER () AS mn, MAX(c) OVER () AS mx
      FROM VALUES (INTERVAL '1' DAY), (INTERVAL '5' DAY) AS t(c)
      ORDER BY c
      """
      Then query result ordered
      | c                | mn               | mx               |
      | INTERVAL '1' DAY | INTERVAL '1' DAY | INTERVAL '5' DAY |
      | INTERVAL '5' DAY | INTERVAL '1' DAY | INTERVAL '5' DAY |

    @sail-bug
    # Same root cause as `SUM over typed YEAR intervals stays as YEAR` above —
    # Sail's SUM does not yet accept Interval(YearMonth). The window form fails
    # the same way as the non-window form.
    Scenario: running window SUM over YEAR intervals preserves the YEAR qualifier
      When query
      """
      SELECT c, SUM(c) OVER (ORDER BY c) AS running
      FROM VALUES (INTERVAL '1' YEAR), (INTERVAL '2' YEAR), (INTERVAL '3' YEAR) AS t(c)
      """
      Then query result ordered
      | c                 | running           |
      | INTERVAL '1' YEAR | INTERVAL '1' YEAR |
      | INTERVAL '2' YEAR | INTERVAL '3' YEAR |
      | INTERVAL '3' YEAR | INTERVAL '6' YEAR |

    Scenario Outline: CAST between qualified interval types follows the target qualifier
      When query
      """
      SELECT <expression>
      """
      Then query result
      | <column_name> |
      | <result>      |

      Examples:
      | expression                                                           | column_name                                                                | result                          |
      | CAST(INTERVAL '10' YEAR AS INTERVAL MONTH)                           | CAST(INTERVAL '10' YEAR AS INTERVAL MONTH)                                 | INTERVAL '120' MONTH            |
      | CAST(INTERVAL '10-8' YEAR TO MONTH AS INTERVAL YEAR)                 | CAST(INTERVAL '10-8' YEAR TO MONTH AS INTERVAL YEAR)                       | INTERVAL '10' YEAR              |
      | CAST(INTERVAL '10-8' YEAR TO MONTH AS INTERVAL MONTH)                | CAST(INTERVAL '10-8' YEAR TO MONTH AS INTERVAL MONTH)                      | INTERVAL '128' MONTH            |
      | CAST(INTERVAL '1' DAY AS INTERVAL HOUR)                              | CAST(INTERVAL '1' DAY AS INTERVAL HOUR)                                    | INTERVAL '24' HOUR              |
      | CAST(INTERVAL '1' DAY AS INTERVAL SECOND)                            | CAST(INTERVAL '1' DAY AS INTERVAL SECOND)                                  | INTERVAL '86400' SECOND         |
      | CAST(INTERVAL '1 02:03:04.123456' DAY TO SECOND AS INTERVAL HOUR)    | CAST(INTERVAL '1 02:03:04.123456' DAY TO SECOND AS INTERVAL HOUR)          | INTERVAL '26' HOUR              |
      | CAST(INTERVAL '1 02:03:04.123456' DAY TO SECOND AS INTERVAL HOUR TO MINUTE) | CAST(INTERVAL '1 02:03:04.123456' DAY TO SECOND AS INTERVAL HOUR TO MINUTE) | INTERVAL '26:03' HOUR TO MINUTE |

    Scenario Outline: column references to typed interval columns preserve the qualifier
      When query
      """
      SELECT <projection> FROM VALUES (<value>) AS t(c)
      """
      Then query result
      | <header>  |
      | <result>  |

      Examples:
      | projection      | value                       | header                      | result                      |
      | c               | INTERVAL '10' YEAR          | c                           | INTERVAL '10' YEAR          |
      | c AS aliased    | INTERVAL '10' YEAR          | aliased                     | INTERVAL '10' YEAR          |
      | c               | INTERVAL '1' DAY            | c                           | INTERVAL '1' DAY            |
      | c               | INTERVAL '1 02' DAY TO HOUR | c                           | INTERVAL '1 02' DAY TO HOUR |

    Scenario Outline: IF / CASE / COALESCE widen to the broadest covering interval qualifier
      When query
      """
      SELECT <expression>
      """
      Then query result
      | <column_name> |
      | <result>      |

      Examples:
      | expression                                                            | column_name                                                             | result                       |
      | IF(true, INTERVAL '10' YEAR, INTERVAL '5' YEAR)                       | (IF(true, INTERVAL '10' YEAR, INTERVAL '5' YEAR))                       | INTERVAL '10' YEAR           |
      | IF(false, INTERVAL '10' YEAR, INTERVAL '5' MONTH)                     | (IF(false, INTERVAL '10' YEAR, INTERVAL '5' MONTH))                     | INTERVAL '0-5' YEAR TO MONTH |
      | CASE WHEN 1=1 THEN INTERVAL '10' YEAR ELSE INTERVAL '5' YEAR END      | CASE WHEN (1 = 1) THEN INTERVAL '10' YEAR ELSE INTERVAL '5' YEAR END    | INTERVAL '10' YEAR           |
      | CASE WHEN 1=1 THEN INTERVAL '10' YEAR ELSE INTERVAL '5' MONTH END     | CASE WHEN (1 = 1) THEN INTERVAL '10' YEAR ELSE INTERVAL '5' MONTH END   | INTERVAL '10-0' YEAR TO MONTH |
      | COALESCE(NULL, INTERVAL '10' YEAR)                                    | coalesce(NULL, INTERVAL '10' YEAR)                                      | INTERVAL '10' YEAR           |

    Scenario: SELECT * preserves the qualifier of each typed interval column through a subquery
      When query
      """
      SELECT * FROM (
        SELECT INTERVAL '10' YEAR AS a, INTERVAL '5' DAY AS b
      )
      """
      Then query result
      | a                  | b                |
      | INTERVAL '10' YEAR | INTERVAL '5' DAY |

    Scenario: binary `+` on subquery columns preserves each side's qualifier
      When query
      """
      SELECT t.a + t.a, t.b + t.b FROM (
        SELECT INTERVAL '10' YEAR AS a, INTERVAL '5' DAY AS b
      ) t
      """
      Then query result
      | (a + a)            | (b + b)           |
      | INTERVAL '20' YEAR | INTERVAL '10' DAY |

    Scenario: CTE preserves the qualifier on the underlying interval literal
      When query
      """
      WITH t AS (SELECT INTERVAL '10' YEAR AS x)
      SELECT x FROM t
      """
      Then query result
      | x                  |
      | INTERVAL '10' YEAR |

    Scenario: doubly-nested subquery rename preserves the qualifier
      When query
      """
      SELECT inner_col FROM (
        SELECT renamed AS inner_col FROM (
          SELECT INTERVAL '10' YEAR AS renamed
        )
      )
      """
      Then query result
      | inner_col          |
      | INTERVAL '10' YEAR |

    Scenario: schema reflects the widened YEAR TO MONTH qualifier for year-month addition
      When query
      """
      SELECT INTERVAL '10' YEAR + INTERVAL '5' MONTH
      """
      Then query schema
      """
      root
       |-- (INTERVAL '10' YEAR + INTERVAL '5' MONTH): interval year to month (nullable = false)
      """

    Scenario: schema reflects the widened YEAR TO MONTH qualifier for year-month multiplication
      When query
      """
      SELECT INTERVAL '10' YEAR * 2
      """
      Then query schema
      """
      root
       |-- (INTERVAL '10' YEAR * 2): interval year to month (nullable = false)
      """

    @sail-bug
    # Same root cause as `SUM over typed YEAR intervals stays as YEAR` —
    # Sail's SUM does not yet accept Interval(YearMonth), so the schema check
    # never reaches the qualifier propagation step.
    Scenario: schema reflects the preserved YEAR qualifier on SUM
      When query
      """
      SELECT SUM(c) FROM VALUES (INTERVAL '1' YEAR), (INTERVAL '2' YEAR) AS t(c)
      """
      Then query schema
      """
      root
       |-- sum(c): interval year (nullable = true)
      """

    Scenario: schema reflects the preserved DAY qualifier on a window MIN
      When query
      """
      SELECT MIN(c) OVER () FROM VALUES (INTERVAL '1' DAY) AS t(c)
      """
      Then query schema
      """
      root
       |-- min(c) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING): interval day (nullable = true)
      """

    Scenario: schema reflects the preserved YEAR qualifier on a window MIN
      When query
      """
      SELECT MIN(c) OVER () FROM VALUES (INTERVAL '1' YEAR) AS t(c)
      """
      Then query schema
      """
      root
       |-- min(c) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING): interval year (nullable = true)
      """

    Scenario: schema reflects the alias replacing the long window header
      When query
      """
      SELECT MIN(c) OVER () AS mn FROM VALUES (INTERVAL '1' DAY) AS t(c)
      """
      Then query schema
      """
      root
       |-- mn: interval day (nullable = true)
      """

    Scenario: schema reflects the ORDER BY window frame in the header
      When query
      """
      SELECT c, MIN(c) OVER (ORDER BY c)
      FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY) AS t(c)
      """
      Then query schema
      """
      root
       |-- c: interval day (nullable = false)
       |-- min(c) OVER (ORDER BY c ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW): interval day (nullable = true)
      """

    Scenario: schema reflects each side's qualifier on binary `+` over subquery columns
      When query
      """
      SELECT t.a + t.a, t.b + t.b FROM (
        SELECT INTERVAL '10' YEAR AS a, INTERVAL '5' DAY AS b
      ) t
      """
      Then query schema
      """
      root
       |-- (a + a): interval year (nullable = false)
       |-- (b + b): interval day (nullable = false)
      """

    Scenario: schema reflects the preserved DAY qualifier on SUM / MIN / MAX
      When query
      """
      SELECT SUM(c) AS sum_c, MIN(c) AS min_c, MAX(c) AS max_c
      FROM VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY), (INTERVAL '3' DAY) AS t(c)
      """
      Then query schema
      """
      root
       |-- sum_c: interval day (nullable = true)
       |-- min_c: interval day (nullable = true)
       |-- max_c: interval day (nullable = true)
      """

    Scenario: schema reflects the preserved YEAR qualifier on IF with matching branches
      When query
      """
      SELECT IF(true, INTERVAL '10' YEAR, INTERVAL '5' YEAR)
      """
      Then query schema
      """
      root
       |-- (IF(true, INTERVAL '10' YEAR, INTERVAL '5' YEAR)): interval year (nullable = false)
      """

    Scenario: schema reflects the target MONTH qualifier of a CAST
      When query
      """
      SELECT CAST(INTERVAL '10' YEAR AS INTERVAL MONTH)
      """
      Then query schema
      """
      root
       |-- CAST(INTERVAL '10' YEAR AS INTERVAL MONTH): interval month (nullable = false)
      """

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
