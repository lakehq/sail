Feature: array_repeat output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to array_repeat yields the schema Spark declares
      When query
        """
        SELECT array_repeat('123', 2) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = false)
        """

    @sail-bug
    Scenario: a non-null column input to array_repeat yields the schema Spark declares
      When query
        """
        SELECT array_repeat(CAST(id AS STRING), 2) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = false)
        """

    @sail-bug
    Scenario: a nullable column input to array_repeat stays nullable
      When query
        """
        SELECT array_repeat(c, 2) AS result FROM VALUES ('123'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = true)
        """


  Rule: Qualified nested elements survive repetition

    Scenario Outline: Repeating an interval array <case>
      When query
        """
        SELECT transform(array_repeat(a, <count>), x -> CAST(x[0] AS INT)) AS result
        FROM (SELECT <value> AS a)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case       | value                                                     | count | result       |
        | non-null   | CAST(array(INTERVAL '1' DAY) AS ARRAY<INTERVAL DAY>)       | 2     | [1, 1]       |
        | null array | CAST(NULL AS ARRAY<INTERVAL DAY>)                         | 2     | [NULL, NULL] |
        | zero count | CAST(array(INTERVAL '1' DAY) AS ARRAY<INTERVAL DAY>)       | 0     | []           |
        | null count | CAST(array(INTERVAL '1' DAY) AS ARRAY<INTERVAL DAY>)       | NULL  | NULL         |

    Scenario: Padding an array of qualified interval arrays
      When query
        """
        SELECT transform(array_insert(array(a), 3, a), x -> CAST(x[0] AS INT)) AS result
        FROM (SELECT CAST(array(INTERVAL '1' DAY) AS ARRAY<INTERVAL DAY>) AS a)
        """
      Then query result
        | result       |
        | [1, NULL, 1] |
