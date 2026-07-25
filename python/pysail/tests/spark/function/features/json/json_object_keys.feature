@json_object_keys
Feature: json_object_keys output schema

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to json_object_keys yields the schema Spark declares
      When query
        """
        SELECT json_object_keys('{}') AS result
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """

    Scenario: a non-null column input to json_object_keys yields the schema Spark declares
      When query
        """
        SELECT json_object_keys(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """

    Scenario: a nullable column input to json_object_keys stays nullable
      When query
        """
        SELECT json_object_keys(c) AS result FROM VALUES ('{}'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: array (nullable = true)
         |    |-- element: string (containsNull = true)
        """

  Rule: Result values (migrated from test_json_object_keys.txt doctests)

    Scenario: json_object_keys doctest #1 (result)
      When query
        """
        SELECT
  json_object_keys('{"f1":"abc","f2":{"f3":"a","f4":"b"}}') AS keys,
  typeof(json_object_keys('{"f1":"abc","f2":{"f3":"a","f4":"b"}}')) AS type

        """
      Then query result
        | keys     | type          |
        | [f1, f2] | array<string> |

    Scenario: json_object_keys doctest #2 (result)
      When query
        """
        SELECT json_object_keys('{}') AS keys, typeof(json_object_keys('{}')) AS type
        """
      Then query result
        | keys | type          |
        | []   | array<string> |

    Scenario: json_object_keys doctest #3 (result)
      When query
        """
        SELECT
  json_object_keys('[1,2]') AS non_object,
  json_object_keys(CAST(NULL AS STRING)) AS null_json,
  json_object_keys('not a json') AS invalid_json

        """
      Then query result
        | non_object | null_json | invalid_json |
        | NULL       | NULL      | NULL         |
