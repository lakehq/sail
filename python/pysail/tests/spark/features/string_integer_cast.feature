Feature: String casts to integer types respect ANSI mode

  # TODO: Sail preserves the source column name for these nested casts, while
  # Spark includes the complete cast expression. This predates legacy parsing.
  @sail-bug
  Scenario: nested legacy integer casts use the full Spark expression name
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST(CAST(value AS SMALLINT) AS BIGINT)
      FROM VALUES ('42') AS data(value)
      """
    Then query result
      | CAST(CAST(value AS SMALLINT) AS BIGINT) |
      | 42                                    |

  Scenario: malformed string literals return NULL for every signed integer width
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST('USD' AS TINYINT) AS int8,
             CAST('USD' AS SMALLINT) AS int16,
             CAST('USD' AS INT) AS int32,
             CAST('USD' AS BIGINT) AS int64
      """
    Then query result
      | int8 | int16 | int32 | int64 |
      | NULL | NULL  | NULL  | NULL  |

  Scenario Outline: legacy string parsing for <type> columns
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, CAST(value AS <type>) AS result
      FROM VALUES
        (0, 'USD'),
        (1, ''),
        (2, '  '),
        (3, '+'),
        (4, '-'),
        (5, '42'),
        (6, ' +0042 '),
        (7, '-42'),
        (8, '1.99'),
        (9, '-1.99'),
        (10, '.9'),
        (11, '-.9'),
        (12, '.'),
        (13, '-.'),
        (14, '1.'),
        (15, '1.2.3'),
        (16, '1.2x'),
        (17, '1e1'),
        (18, '1 2'),
        (19, '１２'),
        (20, concat(chr(9), '42', chr(127))),
        (21, concat(chr(160), '42')),
        (22, CAST(NULL AS STRING))
      AS data(id, value)
      ORDER BY id
      """
    Then query result ordered
      | id | result |
      | 0  | NULL   |
      | 1  | NULL   |
      | 2  | NULL   |
      | 3  | NULL   |
      | 4  | NULL   |
      | 5  | 42     |
      | 6  | 42     |
      | 7  | -42    |
      | 8  | 1      |
      | 9  | -1     |
      | 10 | 0      |
      | 11 | 0      |
      | 12 | 0      |
      | 13 | 0      |
      | 14 | 1      |
      | 15 | NULL   |
      | 16 | NULL   |
      | 17 | NULL   |
      | 18 | NULL   |
      | 19 | NULL   |
      | 20 | 42     |
      | 21 | NULL   |
      | 22 | NULL   |

    Examples:
      | type     |
      | TINYINT  |
      | SMALLINT |
      | INT      |
      | BIGINT   |

  Scenario Outline: legacy string casts check <type> bounds after truncation
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, CAST(value AS <type>) AS result
      FROM VALUES
        (0, '<min>'), (1, '<max>'),
        (2, '<min>.999'), (3, '<max>.999'),
        (4, '<below>'), (5, '<above>'),
        (6, '<below>.0'), (7, '<above>.0'),
        (8, '999999999999999999999999999999999999999999')
      AS data(id, value)
      ORDER BY id
      """
    Then query result ordered
      | id | result |
      | 0  | <min>  |
      | 1  | <max>  |
      | 2  | <min>  |
      | 3  | <max>  |
      | 4  | NULL   |
      | 5  | NULL   |
      | 6  | NULL   |
      | 7  | NULL   |
      | 8  | NULL   |

    Examples:
      | type     | min                  | max                 | below                | above               |
      | TINYINT  | -128                 | 127                 | -129                 | 128                 |
      | SMALLINT | -32768               | 32767               | -32769               | 32768               |
      | INT      | -2147483648          | 2147483647          | -2147483649          | 2147483648          |
      | BIGINT   | -9223372036854775808 | 9223372036854775807 | -9223372036854775809 | 9223372036854775808 |

  Scenario Outline: casting a non-null string column to <type> is nullable in legacy mode
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT CAST(value AS <type>) AS result
      FROM VALUES ('42'), ('USD') AS data(value)
      """
    Then query schema
      """
      root
       |-- result: <schema_type> (nullable = true)
      """

    Examples:
      | type     | schema_type |
      | TINYINT  | byte        |
      | SMALLINT | short       |
      | INT      | integer     |
      | BIGINT   | long        |

  Scenario Outline: invalid string casts to <type> do not abort legacy filters
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT value
      FROM VALUES ('USD'), ('42'), ('42.9'), ('<overflow>') AS data(value)
      WHERE CAST(value AS <type>) = 42
      """
    Then query result
      | value |
      | 42    |
      | 42.9  |

    Examples:
      | type     | overflow            |
      | TINYINT  | 128                 |
      | SMALLINT | 32768               |
      | INT      | 2147483648          |
      | BIGINT   | 9223372036854775808 |

  Scenario Outline: ANSI casts to <type> reject <input>
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST('<input>' AS <type>) AS result
      """
    Then query error <input>

    Examples:
      | type     | input               |
      | TINYINT  | USD                 |
      | SMALLINT | USD                 |
      | INT      | USD                 |
      | BIGINT   | USD                 |
      | TINYINT  | 1.99                |
      | SMALLINT | 1.99                |
      | INT      | 1.99                |
      | BIGINT   | 1.99                |
      | TINYINT  | 128                 |
      | SMALLINT | 32768               |
      | INT      | 2147483648          |
      | BIGINT   | 9223372036854775808 |

  Scenario Outline: TRY_CAST to <type> remains strict with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, TRY_CAST(value AS <type>) AS result
      FROM VALUES
        (0, '42'), (1, 'USD'), (2, '1.99'),
        (3, '<overflow>'), (4, ''), (5, CAST(NULL AS STRING))
      AS data(id, value)
      ORDER BY id
      """
    Then query result ordered
      | id | result |
      | 0  | 42     |
      | 1  | NULL   |
      | 2  | NULL   |
      | 3  | NULL   |
      | 4  | NULL   |
      | 5  | NULL   |

    Examples:
      | type     | ansi  | overflow            |
      | TINYINT  | true  | 128                 |
      | SMALLINT | true  | 32768               |
      | INT      | true  | 2147483648          |
      | BIGINT   | true  | 9223372036854775808 |
      | TINYINT  | false | 128                 |
      | SMALLINT | false | 32768               |
      | INT      | false | 2147483648          |
      | BIGINT   | false | 9223372036854775808 |

  @sail-only
  Scenario Outline: legacy string casts support existing unsigned <type> targets
    Given config spark.sql.ansi.enabled = false
    When query
      """
      SELECT id, CAST(value AS <type>) AS result
      FROM VALUES
        (0, 'USD'), (1, ''), (2, CAST(NULL AS STRING)),
        (3, '0'), (4, ' +0042 '), (5, '42.99'),
        (6, '<max>'), (7, '<max>.999'), (8, '<above>'),
        (9, '-1'), (10, '1.2x'), (11, '1e1')
      AS data(id, value)
      ORDER BY id
      """
    Then query result ordered
      | id | result |
      | 0  | NULL   |
      | 1  | NULL   |
      | 2  | NULL   |
      | 3  | 0      |
      | 4  | 42     |
      | 5  | 42     |
      | 6  | <max>  |
      | 7  | <max>  |
      | 8  | NULL   |
      | 9  | NULL   |
      | 10 | NULL   |
      | 11 | NULL   |

    Examples:
      | type   | max                  | above                |
      | UINT8  | 255                  | 256                  |
      | UINT16 | 65535                | 65536                |
      | UINT32 | 4294967295           | 4294967296           |
      | UINT64 | 18446744073709551615 | 18446744073709551616 |

  @sail-only
  Scenario Outline: ANSI casts to unsigned <type> reject malformed strings
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(CAST('USD' AS <type>) AS STRING) AS result
      """
    Then query error USD

    Examples:
      | type   |
      | UINT8  |
      | UINT16 |
      | UINT32 |
      | UINT64 |

  @sail-only
  Scenario Outline: TRY_CAST to unsigned <type> remains strict with ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, TRY_CAST(value AS <type>) AS result
      FROM VALUES
        (0, '42'), (1, 'USD'), (2, '42.99'), (3, CAST(NULL AS STRING))
      AS data(id, value)
      ORDER BY id
      """
    Then query result ordered
      | id | result |
      | 0  | 42     |
      | 1  | NULL   |
      | 2  | NULL   |
      | 3  | NULL   |

    Examples:
      | type   | ansi  |
      | UINT8  | true  |
      | UINT16 | true  |
      | UINT32 | true  |
      | UINT64 | true  |
      | UINT8  | false |
      | UINT16 | false |
      | UINT32 | false |
      | UINT64 | false |
