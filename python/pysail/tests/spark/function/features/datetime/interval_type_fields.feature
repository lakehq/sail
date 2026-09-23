# The PySpark 3.5 client renders every interval type as a bare `interval`, without its
# leading and trailing fields, so these schema assertions only hold from PySpark 4.
@spark-4
Feature: Leading and trailing fields of the interval types

  @function(nullability)
  Rule: A year-month interval only spans the fields that it is declared with

    Scenario Outline: Year-month interval: <case>
      When query
        """
        SELECT <lit> AS result
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = false)
        """

      Examples:
        | case          | lit                            | fields        |
        | year to month | INTERVAL '10-8' YEAR TO MONTH  | year to month |

    @sail-bug
    Scenario Outline: Year-month interval: <case> (known Sail bug)
      When query
        """
        SELECT <lit> AS result
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = false)
        """

      Examples:
        | case          | lit                            | fields        |
        | year only     | INTERVAL '10' YEAR             | year          |
        | month only    | INTERVAL '8' MONTH             | month         |

  @function(nullability)
  Rule: A day-time interval only spans the fields that it is declared with

    Scenario Outline: Day-time interval: <case>
      When query
        """
        SELECT <lit> AS result
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = false)
        """

      Examples:
        | case           | lit                              | fields         |
        | day to second  | INTERVAL '1 2:3:4' DAY TO SECOND | day to second  |

    @sail-bug
    Scenario Outline: Day-time interval: <case> (known Sail bug)
      When query
        """
        SELECT <lit> AS result
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = false)
        """

      Examples:
        | case           | lit                              | fields         |
        | day only       | INTERVAL '5' DAY                 | day            |
        | hour only      | INTERVAL '7' HOUR                | hour           |
        | minute only    | INTERVAL '9' MINUTE              | minute         |
        | second only    | INTERVAL '3' SECOND              | second         |
        | day to hour    | INTERVAL '1 2' DAY TO HOUR       | day to hour    |
        | day to minute  | INTERVAL '1 2:3' DAY TO MINUTE   | day to minute  |
        | hour to minute | INTERVAL '2:3' HOUR TO MINUTE    | hour to minute |
        | hour to second | INTERVAL '2:3:4' HOUR TO SECOND  | hour to second |
        | minute to second | INTERVAL '3:4' MINUTE TO SECOND | minute to second |

  @function(nullability)
  Rule: A multi-unit interval with a single unit only spans that unit

    @sail-bug
    Scenario Outline: Multi-unit interval: <case>
      When query
        """
        SELECT <lit> AS result
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = false)
        """

      Examples:
        | case         | lit               | fields |
        | years        | INTERVAL 3 YEARS  | year   |
        | months       | INTERVAL 2 MONTHS | month  |
        | days         | INTERVAL 5 DAYS   | day    |
        | hours        | INTERVAL 4 HOURS  | hour   |
        | seconds      | INTERVAL 6 SECONDS | second |

  @function(nullability)
  Rule: A week folds into the day field and sub-second units fold into the second field

    @sail-bug
    Scenario Outline: Coarser field for <case>
      When query
        """
        SELECT <lit> AS result
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = false)
        """

      Examples:
        | case          | lit                     | fields |
        | week          | INTERVAL 1 WEEK         | day    |
        | weeks days    | INTERVAL 2 WEEKS 3 DAYS | day    |
        | millisecond   | INTERVAL '-7' MILLISECOND | second |
        | microsecond   | INTERVAL '-7' MICROSECOND | second |
        | microseconds  | INTERVAL 5 MICROSECONDS | second |

  @function(nullability)
  Rule: A multi-unit interval spans from its coarsest to its finest unit

    Scenario Outline: Multi-unit span: <case>
      When query
        """
        SELECT <lit> AS result
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = false)
        """

      Examples:
        | case                  | lit                                         | fields           |
        | year to month         | INTERVAL 1 YEAR 2 MONTHS                    | year to month    |
        | day to second         | INTERVAL 3 DAYS 4 HOURS 5 MINUTES 6 SECONDS | day to second    |

    @sail-bug
    Scenario Outline: Multi-unit span: <case> (known Sail bug)
      When query
        """
        SELECT <lit> AS result
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = false)
        """

      Examples:
        | case                  | lit                                         | fields           |
        | day to hour           | INTERVAL 1 DAY 2 HOURS                      | day to hour      |
        | hour to minute        | INTERVAL 2 HOURS 3 MINUTES                  | hour to minute   |
        | hour to second        | INTERVAL 1 HOUR 2 SECONDS                   | hour to second   |
        | minute to second      | INTERVAL 1 MINUTE 5 MICROSECONDS            | minute to second |
        | second only           | INTERVAL 1 SECOND 2 MILLISECONDS            | second           |

    @sail-bug
    Scenario: The span ignores the order the units are written in
      When query
        """
        SELECT INTERVAL 2 HOURS 1 DAY AS result
        """
      Then query schema
        """
        root
         |-- result: interval day to hour (nullable = false)
        """

  @function(nullability)
  Rule: A cast to an interval type only spans the fields that it is declared with

    Scenario Outline: Cast of NULL to an interval type: <case>
      When query
        """
        SELECT CAST(NULL AS INTERVAL <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = true)
        """

      Examples:
        | case          | type          | fields        |
        | year to month | YEAR TO MONTH | year to month |

    @sail-bug
    Scenario Outline: Cast of NULL to an interval type: <case> (known Sail bug)
      When query
        """
        SELECT CAST(NULL AS INTERVAL <type>) AS result
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = true)
        """

      Examples:
        | case          | type          | fields        |
        | year only     | YEAR          | year          |
        | month only    | MONTH         | month         |
        | hour only     | HOUR          | hour          |
        | minute only   | MINUTE        | minute        |

    @sail-bug
    Scenario: Cast narrows a year-month interval to its leading field
      When query
        """
        SELECT CAST(INTERVAL '10-8' YEAR TO MONTH AS INTERVAL YEAR) AS result
        """
      Then query schema
        """
        root
         |-- result: interval year (nullable = false)
        """

    @sail-bug
    Scenario: Cast of a day-time interval keeps the declared field
      When query
        """
        SELECT CAST(INTERVAL '3' DAY AS INTERVAL DAY) AS result
        """
      Then query schema
        """
        root
         |-- result: interval day (nullable = false)
        """

  Rule: A cast to a narrower interval type truncates the value toward zero

    @sail-bug
    Scenario Outline: Cast truncates a day-time value: <case>
      When query
        """
        SELECT CAST(INTERVAL '<lit>' DAY TO SECOND AS INTERVAL <type>) AS result
        """
      Then query result collected
        | result  |
        | <value> |

      Examples:
        | case              | lit         | type   | value             |
        | to hour           | 1 02:03:04  | HOUR   | 1 day, 2:00:00    |
        | to day            | 1 02:03:04  | DAY    | 1 day, 0:00:00    |
        | to minute         | 1 02:03:04  | MINUTE | 1 day, 2:03:00    |
        | negative to hour  | -1 02:03:04 | HOUR   | -2 days, 22:00:00 |

    @sail-bug
    Scenario Outline: Cast truncates a year-month value: <case>
      When query
        """
        SELECT CAST(INTERVAL '<lit>' YEAR TO MONTH AS INTERVAL <type>) AS result
        """
      Then query result
        | result  |
        | <value> |

      Examples:
        | case             | lit   | type  | value              |
        | to year          | 10-8  | YEAR  | INTERVAL '10' YEAR  |
        | negative to year | -10-8 | YEAR  | INTERVAL '-10' YEAR |
        | to month         | 10-8  | MONTH | INTERVAL '128' MONTH |

  @function(nullability)
  Rule: A multi-unit interval takes its family from the units written, not from its value

    @sail-bug
    Scenario Outline: Zero-valued year-month multi-unit interval: <case>
      When query
        """
        SELECT <lit> AS result
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = false)
        """

      Examples:
        | case                 | lit                        | fields        |
        | year cancels month   | INTERVAL 1 YEAR -12 MONTHS | year to month |
        | all year-month zeros | INTERVAL 0 YEARS 0 MONTHS  | year to month |
        | month cancels month  | INTERVAL 1 MONTH -1 MONTH  | month         |

    @sail-bug
    Scenario Outline: Zero-valued interval keeps its units: <case>
      When query
        """
        SELECT <lit> AS result
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = false)
        """

      Examples:
        | case               | lit                      | fields      |
        | single zero year   | INTERVAL 0 YEARS         | year        |
        | single zero month  | INTERVAL 0 MONTHS        | month       |
        | day cancels hour   | INTERVAL 1 DAY -24 HOURS | day to hour |
        | single zero day    | INTERVAL 0 DAYS          | day         |
        | single zero second | INTERVAL 0 SECONDS       | second      |

  Rule: Mixing year-month and day-time units is rejected

    @sail-bug
    Scenario Outline: Mixed interval units: <case>
      When query
        """
        SELECT <lit> AS result
        """
      Then query error Cannot mix year-month and day-time fields

      Examples:
        | case            | lit                       |
        | year with day   | INTERVAL 1 YEAR 2 DAYS    |
        | month with hour | INTERVAL 1 MONTH 3 HOURS  |

  @function(nullability)
  Rule: The interval qualifier survives the expressions that produce interval values

    @sail-bug
    Scenario Outline: Qualifier of a derived interval: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = false)
        """

      Examples:
        | case             | expr                                                            | fields      |
        | negate year      | -INTERVAL '1' YEAR                                              | year        |
        | negate hour      | -INTERVAL '3' HOUR                                              | hour        |
        | add same year    | INTERVAL '1' YEAR + INTERVAL '2' YEAR                           | year        |
        | add day and hour | INTERVAL '1' DAY + INTERVAL '2' HOUR                            | day to hour |
        | coalesce hour    | coalesce(INTERVAL '3' HOUR)                                     | hour        |
        | case when hour   | CASE WHEN true THEN INTERVAL '3' HOUR ELSE INTERVAL '4' HOUR END | hour        |
        | explode month    | explode(array(INTERVAL '1' MONTH))                              | month       |

    Scenario: Adding a month and a year interval widens to year to month
      When query
        """
        SELECT INTERVAL '1' MONTH + INTERVAL '2' YEAR AS result
        """
      Then query schema
        """
        root
         |-- result: interval year to month (nullable = false)
        """

    @sail-bug
    Scenario Outline: Qualifier of an interval column: <case>
      When query
        """
        SELECT result FROM VALUES (<lit>) AS t(result)
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = false)
        """

      Examples:
        | case | lit                | fields |
        | year | INTERVAL '10' YEAR | year   |
        | hour | INTERVAL '3' HOUR  | hour   |

    @sail-bug
    Scenario: The qualifier survives an aggregate
      When query
        """
        SELECT max(result) AS result FROM VALUES (INTERVAL '10' YEAR) AS t(result)
        """
      Then query schema
        """
        root
         |-- result: interval year (nullable = true)
        """

    @sail-bug
    Scenario: The qualifier survives an array
      When query
        """
        SELECT array(INTERVAL '1' MONTH) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: interval month (containsNull = false)
        """

    @sail-bug
    Scenario: The qualifier survives a struct
      When query
        """
        SELECT named_struct('a', INTERVAL '3' HOUR) AS result
        """
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- a: interval hour (nullable = false)
        """

    @sail-bug
    Scenario: The qualifier survives a map
      When query
        """
        SELECT map('k', INTERVAL '3' HOUR) AS result
        """
      Then query schema
        """
        root
         |-- result: map (nullable = false)
         |    |-- key: string
         |    |-- value: interval hour (valueContainsNull = false)
        """

    Scenario: A union widens the qualifier to cover both sides
      When query
        """
        SELECT INTERVAL '1' YEAR AS result UNION ALL SELECT INTERVAL '1' MONTH
        """
      Then query schema
        """
        root
         |-- result: interval year to month (nullable = false)
        """

  Rule: An interval renders with the fields it is declared with

    @sail-bug
    Scenario Outline: Rendering of a single-field interval: <case>
      When query
        """
        SELECT CAST(<lit> AS STRING) AS result
        """
      Then query result collected
        | result  |
        | <value> |

      Examples:
        | case           | lit                           | value                           |
        | month only     | INTERVAL '1' MONTH            | INTERVAL '1' MONTH              |
        | year only      | INTERVAL '10' YEAR            | INTERVAL '10' YEAR              |
        | hour only      | INTERVAL '10' HOUR            | INTERVAL '10' HOUR              |
        | day only       | INTERVAL '5' DAY              | INTERVAL '5' DAY                |
        | second only    | INTERVAL '3' SECOND           | INTERVAL '03' SECOND            |
        | hour to minute | INTERVAL '2:3' HOUR TO MINUTE | INTERVAL '02:03' HOUR TO MINUTE |

    Scenario Outline: Rendering of a full-span interval: <case>
      When query
        """
        SELECT CAST(<lit> AS STRING) AS result
        """
      Then query result collected
        | result  |
        | <value> |

      Examples:
        | case          | lit                              | value                               |
        | year to month | INTERVAL '10-8' YEAR TO MONTH    | INTERVAL '10-8' YEAR TO MONTH       |
        | day to second | INTERVAL '1 2:3:4' DAY TO SECOND | INTERVAL '1 02:03:04' DAY TO SECOND |

  # Spark 4.2.0: `Abs`, `UnaryMinus`, `Add` / `Subtract` of intervals keep (or widen) the
  # input qualifiers, `Cast` to a single-field interval keeps that field, while
  # intervalExpressions.scala `MultiplyDTInterval` / `DivideDTInterval` declare the default
  # `DayTimeIntervalType()` (day to second) and `MultiplyYMInterval` / `DivideYMInterval`
  # the default `YearMonthIntervalType()` (year to month).
  @function(nullability)
  Rule: Qualifiers of interval arithmetic and casts from numbers

    @sail-bug
    Scenario Outline: Qualifier of interval arithmetic: <case>
      When query
        """
        SELECT <expr> AS result
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = false)
        """

      Examples:
        | case                 | expr                                  | fields         |
        | abs of hour          | abs(INTERVAL '-3' HOUR)               | hour           |
        | abs of month         | abs(INTERVAL '-1' MONTH)              | month          |
        | negate minute        | -INTERVAL '3' MINUTE                  | minute         |
        | subtract day hour    | INTERVAL '1' DAY - INTERVAL '3' HOUR  | day to hour    |
        | add hour minute      | INTERVAL '1' HOUR + INTERVAL '30' MINUTE | hour to minute |
        | divide hour resets   | INTERVAL '2' HOUR / 2                 | day to second  |

    Scenario: Multiplying a day-time interval resets to the default day to second
      When query
        """
        SELECT INTERVAL '1' DAY * 2 AS result
        """
      Then query schema
        """
        root
         |-- result: interval day to second (nullable = false)
        """

    @sail-bug
    Scenario: Qualifier of interval arithmetic over columns
      When query
        """
        SELECT i1 + i2 AS s, abs(i1) AS a
        FROM VALUES (INTERVAL '1' HOUR, INTERVAL '30' MINUTE), (INTERVAL '-2' HOUR, INTERVAL '45' MINUTE) AS t(i1, i2)
        """
      Then query schema
        """
        root
         |-- s: interval hour to minute (nullable = false)
         |-- a: interval hour (nullable = false)
        """

    @sail-bug
    Scenario Outline: Qualifier of a cast from a number: <case>
      When query
        """
        SELECT CAST(n AS INTERVAL <type>) AS result FROM VALUES (2), (-3) AS t(n)
        """
      Then query schema
        """
        root
         |-- result: interval <fields> (nullable = false)
        """

      Examples:
        | case   | type   | fields |
        | year   | YEAR   | year   |
        | month  | MONTH  | month  |
        | day    | DAY    | day    |
        | hour   | HOUR   | hour   |
        | minute | MINUTE | minute |
