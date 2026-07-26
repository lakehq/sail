@get_json_object
Feature: get_json_object extracts values via a Spark JSONPath

  # Spark `get_json_object(json, path)` walks a JSONPath subset: a leading `$`,
  # dot notation (`$.a.b`), array indexing (`$.a[0]`, `$[0]`), and single-quoted
  # bracket notation (`$['a']`). A bare `$` returns the whole document. The
  # result is the matched value rendered as text; a path that does not match (or
  # cannot be parsed) returns NULL.

  Rule: Dot notation walks object keys

    Scenario Outline: Dot: <case>
      When query
        """
        SELECT get_json_object(<json>, <path>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                          | json            | path    | result  |
        | top-level key                                 | '{"a":1}'       | '$.a'   | 1       |
        | nested keys                                   | '{"a":{"b":1}}' | '$.a.b' | 1       |
        | object value is returned as compact JSON text | '{"a":{"b":1}}' | '$.a'   | {"b":1} |
        | string value is returned unquoted             | '{"a":"hi"}'    | '$.a'   | hi      |
        | boolean value                                 | '{"a":true}'    | '$.a'   | true    |

  Rule: A bare $ returns the whole document

    Scenario Outline: Bare dollar: <case>
      When query
        """
        SELECT get_json_object(<json>, '$') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case         | json      | result  |
        | whole object | '{"a":1}' | {"a":1} |
        | whole array  | '[1,2,3]' | [1,2,3] |

  Rule: Array indexing selects array elements

    Scenario Outline: Array index: <case>
      When query
        """
        SELECT get_json_object(<json>, <path>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                  | json                  | path     | result |
        | index after a key                                     | '{"a":[10,20,30]}'    | '$.a[1]' | 20     |
        | index at the root                                     | '[1,2,3]'             | '$[0]'   | 1      |
        | nested array element is returned as compact JSON text | '{"a":[[1,2],[3,4]]}' | '$.a[1]' | [3,4]  |

  Rule: Mixed key and index paths

    Scenario Outline: Mixed path: <case>
      When query
        """
        SELECT get_json_object(<json>, <path>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                    | json                      | path       | result |
        | key then index then key | '{"a":[{"b":7}]}'         | '$.a[0].b' | 7      |
        | index then key          | '{"a":[{"c":1},{"c":9}]}' | '$.a[1].c' | 9      |

  Rule: Single-quoted bracket notation

    # @sail-bug: the parser handles `['key']`, but Sail's SQL parser collapses
    # the escaped `''`, so these reach the function as `$[a]` and return NULL.
    # They pass on Spark JVM. Remove the tag once the SQL `''` escaping is fixed.

    @sail-bug
    Scenario Outline: Bracket: <case>
      When query
        """
        SELECT get_json_object(<json>, <path>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                      | json        | path         | result |
        | single-quoted bracket key                 | '{"a":1}'   | '$[''a'']'   | 1      |
        | single-quoted bracket key containing dots | '{"a.b":5}' | '$[''a.b'']' | 5      |

  Rule: Non-matching and invalid paths return NULL

    Scenario Outline: No match: <case>
      When query
        """
        SELECT get_json_object(<json>, <path>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                                      | json          | path     |
        | missing key                               | '{"a":1}'     | '$.x'    |
        | descending into a scalar                  | '{"a":1}'     | '$.a.b'  |
        | array index out of bounds                 | '{"a":[1,2]}' | '$.a[5]' |
        | path not anchored at dollar returns NULL  | '{"a":1}'     | 'a'      |
        | empty path returns NULL                   | '{"a":1}'     | ''       |
        | dollar followed by empty key returns NULL | '{"a":1}'     | '$.'     |

  Rule: get_json_object — the argument is resolved per row, not taken from the first row

    @column_args
    Scenario: get_json_object with the argument as a literal
      When query
        """
        SELECT get_json_object('[{"a":"b"},{"a":"c"}]', '$[0].a') AS result
        """
      Then query result ordered
        | result |
        | b      |

    # Sail returns the wrong value on the column path: Sail returns NULL for every row.
    @column_args @sail-bug
    Scenario Outline: Get_json_object: <case>
      When query
        """
        SELECT get_json_object('[{"a":"b"},{"a":"c"}]', c) AS result FROM VALUES (1, '$[0].a'), (2, <v2>) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result |
        | b      |
        | <r2>   |

      Examples:
        | case                                                                        | v2       | r2   |
        | get_json_object takes argument 2 from a column holding two different values | '$.a'    | NULL |
        | get_json_object takes argument 2 from a column containing NULL              | NULL     | NULL |
        | get_json_object takes argument 2 from a column                              | '$[0].a' | b    |

  @spark_null
  Rule: Output schema

    Scenario: a non-null json literal yields a string
      When query
        """
        SELECT get_json_object('{"a":1}', '$.a') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null json column yields a string
      When query
        """
        SELECT get_json_object(CONCAT('{"n":', CAST(id AS STRING), '}'), '$.n') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable json column stays nullable
      When query
        """
        SELECT get_json_object(c, '$.a') AS result FROM VALUES ('{"a":1}'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Result values (migrated from test_get_json_object.txt doctests)

    Scenario: get_json_object doctest #1 (result)
      When query
        """
        SELECT
  get_json_object('{"a":"x","b":1,"c":{"d":true},"e":[10,20]}', '$.a') AS a,
  get_json_object('{"a":"x","b":1,"c":{"d":true},"e":[10,20]}', '$.b') AS b,
  get_json_object('{"a":"x","b":1,"c":{"d":true},"e":[10,20]}', '$.c') AS c,
  get_json_object('{"a":"x","b":1,"c":{"d":true},"e":[10,20]}', '$.e') AS e

        """
      Then query result
        | a | b | c          | e       |
        | x | 1 | {"d":true} | [10,20] |

    Scenario: get_json_object doctest #2 (result)
      When query
        """
        SELECT
  get_json_object('{"a":null}', '$.a') AS null_value,
  get_json_object('{"a":1}', '$.missing') AS missing_key,
  get_json_object(CAST(NULL AS STRING), '$.a') AS null_json,
  get_json_object('{"a":1}', CAST(NULL AS STRING)) AS null_path,
  get_json_object('not a json', '$.a') AS invalid_json,
  get_json_object('{"a":1}', 'a') AS invalid_path

        """
      Then query result
        | null_value | missing_key | null_json | null_path | invalid_json | invalid_path |
        | NULL       | NULL        | NULL      | NULL      | NULL         | NULL         |
