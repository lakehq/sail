Feature: map_entries function

  Rule: Basic usage

    Scenario: map_entries returns entries for a non-nullable map value
      When query
        """
        SELECT
          map_entries(map(1, 'a', 2, 'b')) AS result,
          typeof(map_entries(map(1, 'a', 2, 'b'))) AS type
        """
      Then query result
        | result           | type                                |
        | [{1, a}, {2, b}] | array<struct<key:int,value:string>> |

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null map literal yields a non-nullable array
      When query
        """
        SELECT map_entries(map('a', 1)) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: struct (containsNull = false)
         |    |    |-- key: string (nullable = false)
         |    |    |-- value: integer (nullable = false)
        """

    @sail-bug
    Scenario: a non-null map column yields a non-nullable array
      When query
        """
        SELECT map_entries(map('a', id)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: struct (containsNull = false)
         |    |    |-- key: string (nullable = false)
         |    |    |-- value: long (nullable = false)
        """

    Scenario: a nullable map column stays nullable
      When query
        """
        SELECT map_entries(c) AS result FROM VALUES (map('a', 1)), (CAST(NULL AS MAP<STRING,INT>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: struct (containsNull = false)
         |    |    |-- key: string (nullable = false)
         |    |    |-- value: integer (nullable = true)
        """

    @sail-bug
    Scenario: a nullable map value propagates to the struct value field
      When query
        """
        SELECT map_entries(c) AS result FROM VALUES (map('a', CAST(NULL AS INT))) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: struct (containsNull = false)
         |    |    |-- key: string (nullable = false)
         |    |    |-- value: integer (nullable = true)
        """
