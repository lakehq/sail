Feature: array output schema

  Rule: Void columns preserve rows

    Scenario Outline: Void columns in array(<arguments>)
      When query
        """
        SELECT array(<arguments>) AS result FROM VALUES (NULL), (NULL) AS t(v)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: void (containsNull = true)
        """
      Then query result
        | result   |
        | <result> |
        | <result> |

      Examples:
        | arguments | result       |
        | v         | [NULL]       |
        | v, v      | [NULL, NULL] |
        | v, NULL   | [NULL, NULL] |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to array yields the schema Spark declares
      When query
        """
        SELECT array(1, 2, 3) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: a non-null column input to array yields the schema Spark declares
      When query
        """
        SELECT array(CAST(id AS INT), 2, 3) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = false)
        """

    Scenario: a nullable column input to array stays nullable
      When query
        """
        SELECT array(c, 2, 3) AS result FROM VALUES (1), (CAST(NULL AS INT)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: integer (containsNull = true)
        """

  # Existing array-index bugs retained after reverting the PR implementation.
  @sail-bug
  Rule: Array subscripts require integral types

    Scenario Outline: Non-integral array subscript <index> is rejected with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT array(10, 20)[<index>] AS result
        """
      Then query error (?i)DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE

      Examples:
        | index        | ansi  |
        | '4294967296' | true  |
        | '4294967296' | false |

    Scenario: Integral and NULL subscripts retain their existing behavior
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT array(10, 20)[0L] AS first,
               array(10, 20)[4294967296L] AS narrowed,
               array(10, 20)[NULL] AS missing
        """
      Then query result
        | first | narrowed | missing |
        | 10    | 10       | NULL    |

    Scenario Outline: Array subscript columns use their <type> values with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT a[`0`] AS result, t.a[t.`0`] AS qualified
        FROM VALUES (array(10, 20), CAST(0 AS <type>)),
                    (array(30, 40), CAST(1 AS <type>)),
                    (array(50, 60), CAST(NULL AS <type>)),
                    (CAST(NULL AS ARRAY<INT>), CAST(-1 AS <type>)) AS t(a, `0`)
        """
      Then query result
        | result | qualified |
        | 10     | 10        |
        | 40     | 40        |
        | NULL   | NULL      |
        | NULL   | NULL      |

      Examples:
        | type     | ansi  |
        | TINYINT  | true  |
        | TINYINT  | false |
        | SMALLINT | true  |
        | SMALLINT | false |
        | INT      | true  |
        | INT      | false |
        | BIGINT   | true  |
        | BIGINT   | false |

    Scenario Outline: BIGINT subscript columns narrow to signed INT with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT a[i] AS result
        FROM VALUES (array(10, 20), 4294967296L),
                    (array(30, 40), -4294967295L),
                    (array(50, 60), CAST('-9223372036854775808' AS BIGINT)) AS t(a, i)
        """
      Then query result
        | result |
        | 10     |
        | 40     |
        | 50     |

      Examples:
        | ansi  |
        | true  |
        | false |

    Scenario Outline: Non-integral subscript column <index> is rejected with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT a[`0`] AS result FROM VALUES (array(10, 20), <index>) AS t(a, `0`)
        """
      Then query error (?i)DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE

      Examples:
        | index | ansi  |
        | '0'   | true  |
        | '0'   | false |
        | 0.5   | true  |
        | 0.5   | false |
        | 0.5D  | true  |
        | 0.5D  | false |

    Scenario Outline: Projected subscript columns widened to <value> are rejected with ANSI <ansi>
      Given config spark.sql.ansi.enabled = <ansi>
      When query
        """
        SELECT a[`0`] AS result
        FROM (SELECT array(10, 20) AS a,
                     CASE WHEN id = 0 THEN 0 ELSE <value> END AS `0`
              FROM range(2)) t
        """
      Then query error (?i)DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE

      Examples:
        | value | ansi  |
        | 0.5   | true  |
        | 0.5   | false |
        | 0.5D  | true  |
        | 0.5D  | false |

    Scenario Outline: Out-of-bounds subscript column <index> errors in ANSI mode
      Given config spark.sql.ansi.enabled = true
      When query
        """
        SELECT a[`0`] AS result FROM VALUES (array(10, 20), <index>) AS t(a, `0`)
        """
      Then query error (?i)INVALID_ARRAY_INDEX

      Examples:
        | index       |
        | -1          |
        | 2           |
        | 2147483647  |
        | 2147483648L |
        | 4294967295L |

    Scenario: Out-of-bounds subscript columns return NULL outside ANSI mode
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT a[`0`] AS result
        FROM VALUES (array(10, 20), -1L),
                    (array(10, 20), 2L),
                    (array(10, 20), 2147483647L),
                    (array(10, 20), 2147483648L),
                    (array(10, 20), 4294967295L) AS t(a, `0`)
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |
        | NULL   |
        | NULL   |
        | NULL   |
