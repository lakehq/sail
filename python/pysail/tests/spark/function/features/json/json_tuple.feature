Feature: json_tuple function

  Background:
    Given a Spark session

  Scenario: Extract a single field
    When I execute the SQL query:
      """
      SELECT json_tuple('{"a":"hello"}', 'a') AS c0
      """
    Then the result should be:
      | c0    |
      | hello |

  Scenario: Extract multiple fields
    When I execute the SQL query:
      """
      SELECT json_tuple('{"f1":"value1","f2":"value2"}', 'f1', 'f2') AS (c0, c1)
      """
    Then the result should be:
      | c0     | c1     |
      | value1 | value2 |

  Scenario: Extract three fields
    When I execute the SQL query:
      """
      SELECT json_tuple('{"a":"1","b":"2","c":"3"}', 'a', 'b', 'c') AS (c0, c1, c2)
      """
    Then the result should be:
      | c0 | c1 | c2 |
      | 1  | 2  | 3  |

  Scenario: Numeric values are returned as strings
    When I execute the SQL query:
      """
      SELECT json_tuple('{"a":1,"b":2}', 'a', 'b') AS (c0, c1)
      """
    Then the result should be:
      | c0 | c1 |
      | 1  | 2  |

  Scenario: Boolean values are returned as strings
    When I execute the SQL query:
      """
      SELECT json_tuple('{"flag":true,"other":false}', 'flag', 'other') AS (c0, c1)
      """
    Then the result should be:
      | c0   | c1    |
      | true | false |

  Scenario: Float values are returned as strings
    When I execute the SQL query:
      """
      SELECT json_tuple('{"price":3.14}', 'price') AS c0
      """
    Then the result should be:
      | c0   |
      | 3.14 |

  Scenario: Missing key returns NULL
    When I execute the SQL query:
      """
      SELECT json_tuple('{"a":"1","b":"2"}', 'a', 'c') AS (c0, c1)
      """
    Then the result should be:
      | c0 | c1   |
      | 1  | NULL |

  Scenario: All missing keys return NULL
    When I execute the SQL query:
      """
      SELECT json_tuple('{"a":"1"}', 'x', 'y', 'z') AS (c0, c1, c2)
      """
    Then the result should be:
      | c0   | c1   | c2   |
      | NULL | NULL | NULL |

  Scenario: JSON null returns NULL
    When I execute the SQL query:
      """
      SELECT json_tuple('{"a":null}', 'a') AS c0
      """
    Then the result should be:
      | c0   |
      | NULL |

  Scenario: NULL input returns NULL values
    When I execute the SQL query:
      """
      SELECT json_tuple(NULL, 'a', 'b') AS (c0, c1)
      """
    Then the result should be:
      | c0   | c1   |
      | NULL | NULL |

  Scenario: Invalid JSON returns NULL
    When I execute the SQL query:
      """
      SELECT json_tuple('not_json', 'a') AS c0
      """
    Then the result should be:
      | c0   |
      | NULL |

  Scenario: Empty string input
    When I execute the SQL query:
      """
      SELECT json_tuple('', 'a') AS c0
      """
    Then the result should be:
      | c0   |
      | NULL |

  Scenario: Empty JSON object
    When I execute the SQL query:
      """
      SELECT json_tuple('{}', 'a', 'b') AS (c0, c1)
      """
    Then the result should be:
      | c0   | c1   |
      | NULL | NULL |

  Scenario: Duplicate keys in arguments
    When I execute the SQL query:
      """
      SELECT json_tuple('{"a":"hello"}', 'a', 'a') AS (c0, c1)
      """
    Then the result should be:
      | c0    | c1    |
      | hello | hello |

  Scenario: Key with dot character
    When I execute the SQL query:
      """
      SELECT json_tuple('{"a.b":"dotted"}', 'a.b') AS c0
      """
    Then the result should be:
      | c0     |
      | dotted |

  Scenario: Key with spaces
    When I execute the SQL query:
      """
      SELECT json_tuple('{"my key":"spaced"}', 'my key') AS c0
      """
    Then the result should be:
      | c0     |
      | spaced |

  Scenario: Value with escaped characters
    When I execute the SQL query:
      """
      SELECT json_tuple('{"msg":"hello\\nworld"}', 'msg') AS c0
      """
    Then the result should be:
      | c0           |
      | hello\nworld |

  Scenario: Nested object is returned as JSON string
    When I execute the SQL query:
      """
      SELECT json_tuple('{"a":{"b":1}}', 'a') AS c0
      """
    Then the result should be:
      | c0      |
      | {"b":1} |

  Scenario: Array is returned as JSON string
    When I execute the SQL query:
      """
      SELECT json_tuple('{"arr":[1,2,3]}', 'arr') AS c0
      """
    Then the result should be:
      | c0       |
      | [1,2,3]  |

  Scenario: Nested path is not resolved
    When I execute the SQL query:
      """
      SELECT json_tuple('{"a":{"b":"deep"}}', 'a.b') AS c0
      """
    Then the result should be:
      | c0   |
      | NULL |

  Scenario: Extract values from multiple rows
    Given a temporary view "json_data" with data:
      | json_col                      |
      | {"f1":"value1","f2":"value2"} |
      | {"f1":"value12"}              |
    When I execute the SQL query:
      """
      SELECT json_tuple(json_col, 'f1', 'f2') AS (c0, c1)
      FROM json_data
      """
    Then the result should be:
      | c0      | c1     |
      | value1  | value2 |
      | value12 | NULL   |

  Scenario: Extract values with NULLs across rows
    Given a temporary view "json_nums" with data:
      | json_col            |
      | {"a":1,"b":2,"c":3} |
      | {"a":4,"b":5}       |
      | {"b":6}             |
      | NULL                |
    When I execute the SQL query:
      """
      SELECT json_tuple(json_col, 'a', 'b', 'c') AS (c0, c1, c2)
      FROM json_nums
      """
    Then the result should be:
      | c0   | c1   | c2   |
      | 1    | 2    | 3    |
      | 4    | 5    | NULL |
      | NULL | 6    | NULL |
      | NULL | NULL | NULL |

  Scenario: Use json_tuple with other columns
    Given a temporary view "events" with data:
      | id | payload                        |
      | 1  | {"action":"click","item":"42"} |
      | 2  | {"action":"view"}              |
      | 3  | NULL                           |
    When I execute the SQL query:
      """
      SELECT id, json_tuple(payload, 'action', 'item') AS (action, item)
      FROM events
      """
    Then the result should be:
      | id | action | item |
      | 1  | click  | 42   |
      | 2  | view   | NULL |
      | 3  | NULL   | NULL |

  Scenario: Use json_tuple with LATERAL VIEW
    Given a temporary view "src" with data:
      | json_col                    |
      | {"name":"Alice","age":"30"} |
      | {"name":"Bob","age":"25"}   |
    When I execute the SQL query:
      """
      SELECT jt.name, jt.age
      FROM src
      LATERAL VIEW json_tuple(json_col, 'name', 'age') jt AS name, age
      """
    Then the result should be:
      | name  | age |
      | Alice | 30  |
      | Bob   | 25  |

  Scenario: LATERAL VIEW with missing values
    Given a temporary view "partial" with data:
      | json_col           |
      | {"x":"10"}         |
      | {"x":"20","y":"5"} |
    When I execute the SQL query:
      """
      SELECT jt.x, jt.y
      FROM partial
      LATERAL VIEW json_tuple(json_col, 'x', 'y') jt AS x, y
      """
    Then the result should be:
      | x  | y    |
      | 10 | NULL |
      | 20 | 5    |

  Scenario: Extract five fields
    When I execute the SQL query:
      """
      SELECT json_tuple(
        '{"a":"1","b":"2","c":"3","d":"4","e":"5"}',
        'a', 'b', 'c', 'd', 'e'
      ) AS (c0, c1, c2, c3, c4)
      """
    Then the result should be:
      | c0 | c1 | c2 | c3 | c4 |
      | 1  | 2  | 3  | 4  | 5  |

  Scenario: Mix existing and missing fields
    When I execute the SQL query:
      """
      SELECT json_tuple(
        '{"a":"1","c":"3","e":"5"}',
        'a', 'b', 'c', 'd', 'e'
      ) AS (c0, c1, c2, c3, c4)
      """
    Then the result should be:
      | c0 | c1   | c2 | c3   | c4 |
      | 1  | NULL | 3  | NULL | 5  |

  Scenario: Requested fields in different order
    When I execute the SQL query:
      """
      SELECT json_tuple('{"b":"2","a":"1"}', 'a', 'b') AS (c0, c1)
      """
    Then the result should be:
      | c0 | c1 |
      | 1  | 2  |

  Scenario: JSON with whitespace
    When I execute the SQL query:
      """
      SELECT json_tuple('{ "a" : "hello" , "b" : "world" }', 'a', 'b') AS (c0, c1)
      """
    Then the result should be:
      | c0    | c1    |
      | hello | world |

  Scenario: Empty string value
    When I execute the SQL query:
      """
      SELECT json_tuple('{"a":""}', 'a') AS c0
      """
    Then the result should be:
      | c0 |
      |    |

  Scenario: Root JSON array returns NULL
    When I execute the SQL query:
      """
      SELECT json_tuple('[1,2,3]', 'a') AS c0
      """
    Then the result should be:
      | c0   |
      | NULL |