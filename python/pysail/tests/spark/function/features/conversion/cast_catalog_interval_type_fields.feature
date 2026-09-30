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

    # TODO: display only (see the comment after this scenario for the confirmed-correct
    #   value, verified via hex()). Sail's `.show()`/`query result` path formats a
    #   year-month interval straight from the raw Arrow array, with no access to the
    #   Sail-only field metadata (`SAIL::spark::interval`) that narrows the printed
    #   range; it always prints the full YEAR-TO-MONTH range regardless of the CAST's
    #   narrower target field. Same root cause as the `.show()`/ArrayFormatter gap
    #   deprioritized elsewhere this session -- fixing it means threading that
    #   metadata into `sail-common-datafusion`'s `ArrayFormatter`/`DisplayIndex`,
    #   which is a separate display engine from the `CAST ... AS STRING` path
    #   (`SparkToUtf8` family) that already reads the metadata correctly.
    @sail-bug
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

    # The scenario above shows the display bug: Sail's `show()`/`query result` path
    # formats a year-month interval from the raw array alone (no field metadata), so
    # it always prints the full YEAR-TO-MONTH range regardless of the narrower field
    # the CAST declared. The scenarios below isolate the *value* (via the interval's
    # underlying month count, read through CAST(... AS INT) and displayed as a
    # collision-free hex string) to show the truncation itself is already correct.
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

    # `typeof()` and `query schema` both read the narrowed field metadata correctly
    # (only the raw `show()`/`query result` rendering path above does not), so this
    # is a second angle confirming the narrowing itself -- not just the value --
    # actually took effect, without depending on the still-broken display.
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
