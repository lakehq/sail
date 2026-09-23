Feature: make_date output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to make_date yields the schema Spark declares
      When query
        """
        SELECT make_date(2013, 7, 15) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

    @sail-bug
    Scenario: a non-null column input to make_date yields the schema Spark declares
      When query
        """
        SELECT make_date(CAST(id AS INT), 7, 15) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

    Scenario: a nullable column input to make_date stays nullable
      When query
        """
        SELECT make_date(c, 7, 15) AS result FROM VALUES (2013), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

  @function(nullability)
  Rule: Nullability through Spark's implicit casts
  # String -> * is force-nullable (Cast.scala:458)

    @sail-bug
    Scenario Outline: make_date without an implicit cast keeps its non-nullable schema
      When query
        """
        SELECT make_date(<input>, 7, 15) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

      Examples:
        | case    | input |
        | no cast | 2013  |

    Scenario Outline: make_date through a force-nullable implicit cast: <case>
      When query
        """
        SELECT make_date(<input>, 7, 15) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

      Examples:
        | case          | input  |
        | STRING -> INT | '2013' |

  # Spark 4.2.0 datetimeExpressions.scala, MakeDate: `LocalDate.of(year, month, day)`; a
  # `DateTimeException` becomes `ansiDateTimeArgumentOutOfRange` when failOnError (= ANSI)
  # and NULL otherwise. The year range is java.time's, not 0001..9999.
  Rule: Components are validated by java.time and honour ANSI mode

    Scenario Outline: make_date builds dates outside 0001..9999: <case>
      When query
        """
        SELECT make_date(<year>, <month>, <day>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | year  | month | day | result       |
        | leap day on leap year | 2024  | 2     | 29  | 2024-02-29   |
        | year zero             | 0     | 1     | 1   | 0000-01-01   |
        | negative year         | -1    | 1     | 1   | -0001-01-01  |
        | five-digit year       | 10000 | 1     | 1   | +10000-01-01 |

    @sail-bug
    Scenario Outline: make_date rejects an invalid component under ANSI: <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_date(<year>, <month>, <day>) AS result
        """
      Then query error \[DATETIME_FIELD_OUT_OF_BOUNDS

      Examples:
        | case                      | year       | month | day |
        | month 13                  | 2024       | 13    | 1   |
        | month 0                   | 2024       | 0     | 1   |
        | day 0                     | 2024       | 1     | 0   |
        | day 31 in a 30-day month  | 2024       | 4     | 31  |
        | leap day on non-leap year | 2023       | 2     | 29  |
        | year beyond java.time     | 1000000000 | 1     | 1   |

    @sail-bug
    Scenario Outline: make_date returns NULL for an invalid component without ANSI: <case>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_date(<year>, <month>, <day>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                      | year       | month | day |
        | month 13                  | 2024       | 13    | 1   |
        | day 31 in a 30-day month  | 2024       | 4     | 31  |
        | leap day on non-leap year | 2023       | 2     | 29  |
        | year beyond java.time     | 1000000000 | 1     | 1   |

    # Row by row: one invalid row must become NULL, not fail the whole batch.
    @sail-bug
    Scenario: make_date resolves invalid rows to NULL without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_date(y, m, d) AS result
        FROM VALUES (1, 2024, 2, 29), (2, 2023, 2, 29), (3, 2024, 13, 1), (4, 1999, 7, 4) AS t(i, y, m, d)
        ORDER BY i
        """
      Then query result ordered
        | result     |
        | 2024-02-29 |
        | NULL       |
        | NULL       |
        | 1999-07-04 |

    Scenario: make_date resolves valid rows from columns under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_date(y, m, d) AS result
        FROM VALUES (1, 2024, 2, 29), (2, 2023, 12, 31), (3, 1999, 7, 4), (4, NULL, 1, 1) AS t(i, y, m, d)
        ORDER BY i
        """
      Then query result ordered
        | result     |
        | 2024-02-29 |
        | 2023-12-31 |
        | 1999-07-04 |
        | NULL       |

    # Without ANSI, failOnError is false and MakeDate.nullable is always true.
    Scenario: make_date is nullable without ANSI even for non-null literals
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_date(2024, 1, 15) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

  # MakeDate is ImplicitCastInputTypes over (INT, INT, INT): a fractional or BIGINT
  # argument goes through Cast, which truncates, overflows under ANSI and is NULL otherwise.
  Rule: Arguments are implicitly cast to INT

    @sail-bug
    Scenario Outline: make_date truncates a fractional year: <case>
      When query
        """
        SELECT make_date(<year>, 2, 3) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case    | year                   | result     |
        | decimal | 1.5                    | 0001-02-03 |
        | double  | CAST(2024.9 AS DOUBLE) | 2024-02-03 |

    @sail-bug
    Scenario: make_date rejects a BIGINT year that overflows INT under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT make_date(3000000000, 1, 1) AS result
        """
      Then query error \[CAST_OVERFLOW

    @sail-bug
    Scenario: make_date resolves uncastable year rows to NULL without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_date(y, 7, 4) AS result
        FROM VALUES (1, '2024'), (2, 'x'), (3, '1999') AS t(i, y)
        ORDER BY i
        """
      Then query result ordered
        | result     |
        | 2024-07-04 |
        | NULL       |
        | 1999-07-04 |

    @sail-bug
    Scenario: make_date resolves an INT-overflowing BIGINT year to NULL without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT make_date(y, 7, 4) AS result
        FROM VALUES (1, CAST(3000000000 AS BIGINT)), (2, CAST(1999 AS BIGINT)) AS t(i, y)
        ORDER BY i
        """
      Then query result ordered
        | result     |
        | NULL       |
        | 1999-07-04 |

  @function(nullability)
  Rule: An untyped NULL argument keeps the DATE result type

    @sail-bug
    Scenario Outline: make_date with an untyped NULL <case> is a nullable date
      When query
        """
        SELECT make_date(<args>) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

      Examples:
        | case  | args          |
        | year  | NULL, 1, 1    |
        | month | 2024, NULL, 1 |
        | day   | 2024, 1, NULL |
