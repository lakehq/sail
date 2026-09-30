# CAST scenarios imported from test/bug_catalog (0886a9e7f): datetime/interval_type_fields.feature
@spark-4
Feature: Additional CAST coverage from interval_type_fields

  @function(nullability)
  Rule: A cast to an interval type only spans the fields that it is declared with

    Scenario Outline: interval_type_fields catalog: Cast of NULL to an interval type: <case>
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

    Scenario Outline: interval_type_fields catalog: Cast of NULL to an interval type: <case> (known Sail bug)
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

    Scenario: interval_type_fields catalog: Cast narrows a year-month interval to its leading field
      When query
        """
        SELECT CAST(INTERVAL '10-8' YEAR TO MONTH AS INTERVAL YEAR) AS result
        """
      Then query schema
        """
        root
         |-- result: interval year (nullable = false)
        """

    Scenario: interval_type_fields catalog: Cast of a day-time interval keeps the declared field
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

    Scenario Outline: interval_type_fields catalog: Cast truncates a day-time value: <case>
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

  Rule: show renders an interval using its field range at every nesting depth

    # Spark's ToPrettyString recursively formats containers while retaining the
    # DayTimeIntervalType(startField, endField) of every child. These three
    # shapes are deliberately cumulative: array, struct<array>, then
    # array<struct<array<struct>>>.
    Scenario Outline: interval_type_fields catalog: show preserves nested interval fields: <case>
      When query
        """
        <query>
        """
      Then query result
        | v       |
        | <shown> |

      Examples:
        | case                       | query                                                                                                                                                           | shown                                               |
        | array                       | SELECT array(INTERVAL '02:00:00' HOUR TO SECOND) AS v                                                                                                        | [INTERVAL '02:00:00' HOUR TO SECOND]                |
        | struct containing an array  | SELECT named_struct('items', array(INTERVAL '02:00:00' HOUR TO SECOND), 'tag', 7) AS v                                                                       | {[INTERVAL '02:00:00' HOUR TO SECOND], 7}           |
        | array struct array struct   | SELECT array(named_struct('items', array(named_struct('leaf', INTERVAL '02:00:00' HOUR TO SECOND)), 'tag', 7)) AS v                                          | [{[{INTERVAL '02:00:00' HOUR TO SECOND}], 7}]        |

    Scenario Outline: interval_type_fields catalog: Cast truncates a year-month value: <case>
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

    # The scenarios below isolate the *value* (via the interval's underlying month
    # count, read through CAST(... AS INT) and displayed as a collision-free hex
    # string) as a second angle on the same truncation, independent of display.
    Scenario Outline: interval_type_fields catalog: Cast truncates a year-month value (checked via its raw integer, not display): <case>
      When query
        """
        SELECT hex(CAST(CAST(INTERVAL '<lit>' YEAR TO MONTH AS INTERVAL <type>) AS INT)) AS result
        """
      Then query result
        | result  |
        | <value> |

      Examples:
        | case             | lit   | type  | value              |
        | to year          | 10-8  | YEAR  | A                  |
        | negative to year | -10-8 | YEAR  | FFFFFFFFFFFFFFF6   |
        | to month         | 10-8  | MONTH | 80                 |

    # `typeof()` and `query schema` also read the narrowed field metadata, confirming
    # the narrowing itself -- not just the value -- actually took effect.
    Scenario Outline: interval_type_fields catalog: Cast truncates a year-month value (checked via typeof, not display): <case>
      When query
        """
        SELECT typeof(CAST(INTERVAL '<lit>' YEAR TO MONTH AS INTERVAL <type>)) AS result
        """
      Then query result
        | result           |
        | <value>          |

      Examples:
        | case             | lit   | type  | value          |
        | to year          | 10-8  | YEAR  | interval year  |
        | negative to year | -10-8 | YEAR  | interval year  |
        | to month         | 10-8  | MONTH | interval month |

  @function(nullability)
  Rule: Qualifiers of interval arithmetic and casts from numbers

    Scenario Outline: interval_type_fields catalog: Qualifier of a cast from a number: <case>
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
