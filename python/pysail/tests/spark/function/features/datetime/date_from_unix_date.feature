Feature: date_from_unix_date output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to date_from_unix_date yields the schema Spark declares
      When query
        """
        SELECT date_from_unix_date(1) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

    Scenario: a non-null column input to date_from_unix_date yields the schema Spark declares
      When query
        """
        SELECT date_from_unix_date(CAST(id AS INT)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

    Scenario: a nullable column input to date_from_unix_date stays nullable
      When query
        """
        SELECT date_from_unix_date(c) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

  @function(nullability)
  Rule: Nullability through Spark's implicit casts
  # String -> * is force-nullable (Cast.scala:458)
  # Float/Double -> Integral is force-nullable (Cast.scala:471)

    @sail-bug
    Scenario Outline: date_from_unix_date loses non-nullability through Spark's implicit cast: <case>
      When query
        """
        SELECT date_from_unix_date(<input>) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = true)
        """

      Examples:
        | case          | input             |
        | STRING -> INT | '1'               |
        | DOUBLE -> INT | CAST(1 AS DOUBLE) |

    Scenario Outline: date_from_unix_date without an implicit cast keeps its non-nullable schema
      When query
        """
        SELECT date_from_unix_date(<input>) AS result
        """
      Then query schema
        """
        root
         |-- result: date (nullable = false)
        """

      Examples:
        | case    | input |
        | no cast | 1     |

  # Spark 4.2.0 datetimeExpressions.scala, DateFromUnixDate: the INT day count IS the
  # DATE value, so every INT is a valid date (-5877641-06-23 .. +5881580-07-11).
  Rule: date_from_unix_date covers the whole INT range

    Scenario Outline: date_from_unix_date of <case>
      When query
        """
        SELECT date_from_unix_date(<days>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                   | days    | result       |
        | the day before epoch   | -1      | 1969-12-31   |
        | the last 4-digit year  | 2932896 | 9999-12-31   |
        | the first 5-digit year | 2932897 | +10000-01-01 |
        | the day before year 1  | -719163 | 0000-12-31   |

    @sail-bug
    Scenario Outline: date_from_unix_date at the <case>
      When query
        """
        SELECT date_from_unix_date(<days>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case        | days        | result         |
        | INT maximum | 2147483647  | +5881580-07-11 |
        | INT minimum | -2147483648 | -5877641-06-23 |

    Scenario: date_from_unix_date resolves each row from a column
      When query
        """
        SELECT date_from_unix_date(c) AS result
        FROM VALUES (1, 0), (2, -1), (3, 19797), (4, NULL) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result     |
        | 1970-01-01 |
        | 1969-12-31 |
        | 2024-03-15 |
        | NULL       |

  # DateFromUnixDate is ImplicitCastInputTypes over INT: the argument goes through Cast.
  Rule: date_from_unix_date implicitly casts its argument to INT

    @sail-bug
    Scenario Outline: date_from_unix_date casts a <case> argument to INT
      When query
        """
        SELECT date_from_unix_date(<days>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case    | days | result     |
        | decimal | 1.5  | 1970-01-02 |
        | string  | '7'  | 1970-01-08 |

    @sail-bug
    Scenario Outline: date_from_unix_date rejects an uncastable <case> under ANSI
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT date_from_unix_date(<days>) AS result
        """
      Then query error \[<error>

      Examples:
        | case            | days                       | error              |
        | string          | 'x'                        | CAST_INVALID_INPUT |
        | overflow BIGINT | CAST(3000000000 AS BIGINT) | CAST_OVERFLOW      |

    # Without ANSI the BIGINT -> INT cast wraps around (3000000000 -> -1294967296).
    @sail-bug
    Scenario Outline: date_from_unix_date casts an uncastable <case> without ANSI
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT date_from_unix_date(<days>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case            | days                       | result         |
        | string          | 'x'                        | NULL           |
        | overflow BIGINT | CAST(3000000000 AS BIGINT) | -3543531-12-19 |

  # Spark 4.2.0 datetimeExpressions.scala, UnixDate: ExpectsInputTypes(DATE), no implicit cast.
  Rule: unix_date

    Scenario: unix_date resolves each row from a column
      When query
        """
        SELECT unix_date(c) AS result
        FROM VALUES (1, DATE '1970-01-01'), (2, DATE '1969-12-31'), (3, DATE '2024-03-17'), (4, NULL) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | 0      |
        | -1     |
        | 19799  |
        | NULL   |

    Scenario Outline: unix_date at the edge of the 4-digit year range: <case>
      When query
        """
        SELECT unix_date(DATE '<date>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case      | date       | result  |
        | first day | 0001-01-01 | -719162 |
        | last day  | 9999-12-31 | 2932896 |

    @sail-bug
    Scenario Outline: unix_date rejects a <case> argument
      When query
        """
        SELECT unix_date(<input>) AS result
        """
      Then query error \[DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE

      Examples:
        | case      | input                           |
        | string    | '2024-03-17'                    |
        | timestamp | TIMESTAMP '2024-03-17 10:00:00' |

    @function(nullability)
    Scenario: unix_date of a non-null DATE is a non-nullable int
      When query
        """
        SELECT unix_date(DATE '2024-03-17') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """
