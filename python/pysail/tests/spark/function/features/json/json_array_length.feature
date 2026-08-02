@json_array_length
Feature: json_array_length output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to json_array_length yields the schema Spark declares
      When query
        """
        SELECT json_array_length('[1,2,3,4]') AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a non-null column input to json_array_length yields the schema Spark declares
      When query
        """
        SELECT json_array_length(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

    Scenario: a nullable column input to json_array_length stays nullable
      When query
        """
        SELECT json_array_length(c) AS result FROM VALUES ('[1,2,3,4]'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = true)
        """

  Rule: Result values (migrated from test_json_array_length.txt doctests)

    Scenario Outline: Result values: <case>
      When query
        """
        SELECT json_array_length(<arg>) AS arr_len, typeof(json_array_length(<arg>)) AS type
        """
      Then query result
        | arr_len   | type |
        | <arr_len> | int  |

      Examples:
        | case                                  | arg       | arr_len |
        | json_array_length doctest #1 (result) | '[1,2,3]' | 3       |
        | json_array_length doctest #2 (result) | '[]'      | 0       |

    Scenario: json_array_length doctest #3 (result)
      When query
        """
        SELECT
  json_array_length(CAST(NULL AS STRING)) AS null_json,
  json_array_length('not a json') AS invalid_json,
  json_array_length('1') AS scalar_json

        """
      Then query result
        | null_json | invalid_json | scalar_json |
        | NULL      | NULL         | NULL        |
