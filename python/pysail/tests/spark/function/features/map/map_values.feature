Feature: map_values output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to map_values yields the schema Spark declares
      When query
        """
        SELECT map_values(map(1, 'a', 2, 'b')) AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = false)
         |    |-- element: string (containsNull = true)
        """

    Scenario: a nullable column input to map_values stays nullable
      When query
        """
        SELECT map_values(c) AS result FROM VALUES (map(1, 'a', 2, 'b')), (CAST(NULL AS MAP<INT,STRING>)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """

  Rule: Qualified map values

    Scenario Outline: Map values retain interval qualifiers for <case>
      When query
        """
        SELECT transform(map_values(<map>), x -> CAST(x AS INT)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case            | map                                                                                        | result |
        | literal entries | map_from_entries(array(struct(1, INTERVAL '2' SECOND)))                                         | [2]    |
        | typed empty     | map_from_entries(CAST(array() AS ARRAY<STRUCT<k:INT,v:INTERVAL DAY>>))                       | []     |
        | concatenated    | map_concat(map_from_entries(array(struct(1, INTERVAL '2' SECOND))), map_from_entries(array(struct(2, INTERVAL '3' SECOND)))) | [2, 3]    |
