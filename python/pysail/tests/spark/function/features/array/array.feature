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
        | -0.5         | true  |
        | -0.5         | false |
        | -0.5D        | true  |
        | -0.5D        | false |
        | '4294967296' | true  |
        | '4294967296' | false |

    Scenario: Integral and NULL subscripts retain their existing behavior
      Given config spark.sql.ansi.enabled = false
      When query
        """
        SELECT array(10, 20)[0L] AS first,
               array(10, 20)[4294967296L] AS narrowed,
               array(10, 20)[NULL] AS missing,
               array(10, 20)[-1] AS out_of_bounds
        """
      Then query result
        | first | narrowed | missing | out_of_bounds |
        | 10    | 10       | NULL    | NULL          |
