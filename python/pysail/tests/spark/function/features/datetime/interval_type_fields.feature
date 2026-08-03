@interval_type_fields
Feature: Leading and trailing fields of the interval types

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
        | year only     | INTERVAL '10' YEAR             | year          |
        | month only    | INTERVAL '8' MONTH             | month         |

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
        | day only       | INTERVAL '5' DAY                 | day            |
        | hour only      | INTERVAL '7' HOUR                | hour           |
        | minute only    | INTERVAL '9' MINUTE              | minute         |
        | second only    | INTERVAL '3' SECOND              | second         |
        | day to hour    | INTERVAL '1 2' DAY TO HOUR       | day to hour    |
        | day to minute  | INTERVAL '1 2:3' DAY TO MINUTE   | day to minute  |
        | hour to minute | INTERVAL '2:3' HOUR TO MINUTE    | hour to minute |
        | hour to second | INTERVAL '2:3:4' HOUR TO SECOND  | hour to second |
        | minute to second | INTERVAL '3:4' MINUTE TO SECOND | minute to second |

  Rule: A multi-unit interval with a single unit only spans that unit

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

  Rule: A week folds into the day field and sub-second units fold into the second field

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
        | day to hour           | INTERVAL 1 DAY 2 HOURS                      | day to hour      |
        | hour to minute        | INTERVAL 2 HOURS 3 MINUTES                  | hour to minute   |
        | hour to second        | INTERVAL 1 HOUR 2 SECONDS                   | hour to second   |
        | minute to second      | INTERVAL 1 MINUTE 5 MICROSECONDS            | minute to second |
        | second only           | INTERVAL 1 SECOND 2 MILLISECONDS            | second           |
        | day to second         | INTERVAL 3 DAYS 4 HOURS 5 MINUTES 6 SECONDS | day to second    |

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
        | year only     | YEAR          | year          |
        | month only    | MONTH         | month         |
        | hour only     | HOUR          | hour          |
        | minute only   | MINUTE        | minute        |
        | year to month | YEAR TO MONTH | year to month |

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
