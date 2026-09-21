Feature: CAST expressions

  Rule: Timestamp timezone conversion

    Scenario: casting TIMESTAMP_NTZ to TIMESTAMP resolves the session-zone gap and overlap
      Given config spark.sql.session.timeZone = America/Los_Angeles
      When query
        """
        SELECT label, unix_micros(CAST(value AS TIMESTAMP)) AS result
        FROM VALUES
          ('gap', TIMESTAMP_NTZ '2021-03-14 02:30:00'),
          ('overlap', TIMESTAMP_NTZ '2021-11-07 01:30:00')
          AS t(label, value)
        ORDER BY label
        """
      Then query result ordered
        | label   | result           |
        | gap     | 1615717800000000 |
        | overlap | 1636273800000000 |

    Scenario Outline: casting TIMESTAMP_NTZ to TIMESTAMP supports fixed offset <timezone>
      Given config spark.sql.session.timeZone = <timezone>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT
          unix_micros(CAST(value AS TIMESTAMP)) AS cast_result,
          unix_micros(TRY_CAST(value AS TIMESTAMP)) AS try_result,
          CAST(CAST(NULL AS TIMESTAMP_NTZ) AS TIMESTAMP) IS NULL AS null_result
        FROM VALUES (TIMESTAMP_NTZ '1970-01-01 00:00:00') AS t(value)
        """
      Then query result
        | cast_result | try_result | null_result |
        | <result>    | <result>   | true        |

      Examples:
        | timezone | ansi  | result       |
        | +01      | true  | -3600000000  |
        | +0130    | false | -5400000000  |
        | +01:30   | true  | -5400000000  |
        | -0130    | false | 5400000000   |
        | -00:00   | true  | 0            |
        | +18:00   | false | -64800000000 |

  Rule: DATE cast ANSI behavior

    Scenario: casting a malformed string to DATE returns null when ANSI mode is disabled
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT CAST('not-a-date' AS DATE) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: casting a malformed string to DATE fails when ANSI mode is enabled
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST('not-a-date' AS DATE) AS result
        """
      Then query error CAST_INVALID_INPUT

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to cast yields the schema Spark declares
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT cast('10' as int) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Legacy STRING to INT casts

    Scenario: decimal strings truncate and overflowing strings return NULL
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT id, CAST(value AS INT) AS result
        FROM VALUES
          (0, '100'),
          (1, '1.23'),
          (2, '-4.56'),
          (3, '2147483647.999'),
          (4, '-2147483648.999'),
          (5, '2178802287'),
          (6, '2147483648'),
          (7, '-2147483649'),
          (8, '2147483648.0'),
          (9, '123.a'),
          (10, CAST(NULL AS STRING))
        AS data(id, value)
        ORDER BY id
        """
      Then query result ordered
        | id | result      |
        | 0  | 100         |
        | 1  | 1           |
        | 2  | -4          |
        | 3  | 2147483647  |
        | 4  | -2147483648 |
        | 5  | NULL        |
        | 6  | NULL        |
        | 7  | NULL        |
        | 8  | NULL        |
        | 9  | NULL        |
        | 10 | NULL        |

    Scenario: overflowing strings do not abort a filter predicate
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT value
        FROM VALUES ('2178802287'), ('100'), ('2147483648') AS data(value)
        WHERE CAST(value AS INT) = 100
        """
      Then query result
        | value |
        | 100   |

  Rule: ANSI and TRY casts stay strict

    Scenario Outline: ANSI CAST rejects <case>
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT CAST(<input> AS INT) AS result
        """
      Then query error <error>

      Examples:
        | case                    | input        | error      |
        | a decimal string        | '1.23'       | 1.23       |
        | an overflowing integer  | '2147483648' | 2147483648 |

    Scenario: TRY_CAST returns NULL for decimal and overflowing strings
      When query
        """
        SELECT id, TRY_CAST(value AS INT) AS result
        FROM VALUES
          (0, '100'),
          (1, '1.23'),
          (2, '2147483648')
        AS data(id, value)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 0  | 100    |
        | 1  | NULL   |
        | 2  | NULL   |

  Rule: a numeric cast to BOOLEAN is the value compared with zero

    # `canAnsiCast` takes every numeric to BOOLEAN (`Cast.scala:105`, and `canCast:239` with ANSI
    # off), and `Cast.castToBoolean` is `value != 0` (`Cast.scala:840-847`). Arrow has no DECIMAL to
    # BOOLEAN kernel, so the comparison is spelled out; the other widths already had one.
    Scenario Outline: <case> cast to BOOLEAN is <result>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT CAST(<value> AS BOOLEAN) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | ansi  | value                          | result |
        | a negative decimal    | false | -1.5                           | true   |
        | a negative decimal    | true  | -1.5                           | true   |
        | a positive decimal    | true  | 1.5                            | true   |
        | a zero decimal        | true  | CAST(0.00 AS DECIMAL(10,2))    | false  |
        | a negative zero       | true  | CAST(-0.0 AS DECIMAL(10,2))    | false  |
        | a BD literal          | true  | -1.0BD                         | true   |
        | a wide decimal        | true  | CAST(1 AS DECIMAL(38,0))       | true   |
        | a NULL decimal        | true  | CAST(NULL AS DECIMAL(10,2))    | NULL   |
        | a double              | true  | -1.5D                          | true   |
        | an int                | true  | -1                             | true   |

    Scenario: try_cast of a decimal to BOOLEAN is the same comparison
      When query
        """
        SELECT TRY_CAST(-1.5 AS BOOLEAN) AS a, TRY_CAST(CAST(0.0 AS DECIMAL(10,2)) AS BOOLEAN) AS b
        """
      Then query result
        | a    | b     |
        | true | false |

    Scenario: a decimal column cast to BOOLEAN keeps every row
      When query
        """
        SELECT CAST(d AS BOOLEAN) AS result
        FROM VALUES (CAST(-1.5 AS DECIMAL(10,2))), (CAST(0.00 AS DECIMAL(10,2))), (CAST(NULL AS DECIMAL(10,2))) AS t(d)
        """
      Then query result
        | result |
        | true   |
        | false  |
        | NULL   |

  Rule: Spark refuses a numeric cast to DATE, and to BINARY unless it is a plain integral CAST

    # `canCast` has no arm from a numeric to DATE (`Cast.scala:223-255`) and neither has
    # `canAnsiCast`, so it is refused whatever the mode and whatever the spelling.
    Scenario Outline: <expression> is refused with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expression> AS result
        """
      Then query error (?i)cannot cast

      Examples:
        | expression                | ansi  |
        | CAST(-1 AS DATE)          | false |
        | CAST(-1 AS DATE)          | true  |
        | TRY_CAST(-1 AS DATE)      | false |
        | TRY_CAST(-1 AS DATE)      | true  |
        | -1::DATE                  | false |
        | CAST(1.5 AS DATE)         | true  |
        | CAST(-1L AS DATE)         | false |

    # `canCast` takes an integral to BINARY (`Cast.scala:234`) but `canAnsiCast` does not
    # (`Cast.scala:92-122`), and `TRY_CAST` is governed by `canAnsiCast` in both modes, so only a
    # plain `CAST` of an INTEGRAL with ANSI off is accepted. A FLOAT or a DECIMAL never is.
    Scenario Outline: <expression> is refused with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT <expression> AS result
        """
      Then query error (?i)cannot cast

      Examples:
        | expression                | ansi  |
        | CAST(-1 AS BINARY)        | true  |
        | CAST(-1L AS BINARY)       | true  |
        | TRY_CAST(-1 AS BINARY)    | false |
        | TRY_CAST(-1 AS BINARY)    | true  |
        | CAST(-1.5D AS BINARY)     | false |
        | CAST(-1.5 AS BINARY)      | false |
        | -1::BINARY                | true  |

    Scenario Outline: an integral CAST to BINARY is accepted with ANSI off: <expression>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT hex(<expression>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | expression         | result   |
        | CAST(-1 AS BINARY) | FFFFFFFF |
        | -1::BINARY         | FFFFFFFF |

    # TODO: `Cast.castToBinary` writes the integer BIG-endian (`Cast.scala`, `NumberConverter`),
    #  and Sail writes Arrow's native little-endian order, so only a symmetric value agrees.
    #  Pre-existing: `main` answers `01000000` for `CAST(1 AS BINARY)` too.
    @sail-bug
    Scenario Outline: an integral CAST to BINARY keeps Spark's byte order: <expression>
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT hex(<expression>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | expression         | result           |
        | CAST(1 AS BINARY)  | 00000001         |
        | CAST(1L AS BINARY) | 0000000000000001 |

    Scenario: a string to BINARY is not touched by the guard
      When query
        """
        SELECT hex(CAST('a' AS BINARY)) AS a, hex(TRY_CAST('a' AS BINARY)) AS b
        """
      Then query result
        | a  | b  |
        | 61 | 61 |
