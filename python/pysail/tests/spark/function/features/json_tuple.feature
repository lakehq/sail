@json_tuple
Feature: json_tuple function

  Rule: Basic extraction
    Scenario: Extract single field
      When query
        """
        SELECT json_tuple('{"a":1}', 'a')
        """
      Then query result
        | c0 |
        | 1  |

    Scenario: Extract multiple fields
      When query
        """
        SELECT json_tuple('{"a":1,"b":"hello","c":true}', 'a', 'b', 'c')
        """
      Then query result
        | c0 | c1    | c2   |
        | 1  | hello | true |

    Scenario: Extract five fields
      When query
        """
        SELECT json_tuple('{"a":1,"b":2,"c":3,"d":4,"e":5}', 'a', 'b', 'c', 'd', 'e')
        """
      Then query result
        | c0 | c1 | c2 | c3 | c4 |
        | 1  | 2  | 3  | 4  | 5  |

    Scenario: Extract from subquery column
      When query
        """
        SELECT json_tuple(js, 'a', 'b')
        FROM (SELECT '{"a":1,"b":2}' AS js)
        """
      Then query result
        | c0 | c1 |
        | 1  | 2  |

  Rule: Missing and null fields
    Scenario: Missing field returns NULL
      When query
        """
        SELECT json_tuple('{"a":1}', 'a', 'b')
        """
      Then query result
        | c0 | c1   |
        | 1  | NULL |

    Scenario: All fields missing returns all NULLs
      When query
        """
        SELECT json_tuple('{"a":1}', 'x', 'y', 'z')
        """
      Then query result
        | c0   | c1   | c2   |
        | NULL | NULL | NULL |

    Scenario: JSON null value returns NULL
      When query
        """
        SELECT json_tuple('{"a":null}', 'a')
        """
      Then query result
        | c0   |
        | NULL |

    Scenario: Mixed null and non-null JSON values
      When query
        """
        SELECT json_tuple('{"a":1,"b":null,"c":"x"}', 'a', 'b', 'c')
        """
      Then query result
        | c0 | c1   | c2 |
        | 1  | NULL | x  |

  Rule: NULL and invalid JSON input
    Scenario: NULL JSON string returns NULL
      When query
        """
        SELECT json_tuple(CAST(NULL AS STRING), 'a')
        """
      Then query result
        | c0   |
        | NULL |

    Scenario: NULL JSON string with multiple fields returns all NULLs
      When query
        """
        SELECT json_tuple(CAST(NULL AS STRING), 'a', 'b', 'c')
        """
      Then query result
        | c0   | c1   | c2   |
        | NULL | NULL | NULL |

    Scenario: Empty string JSON returns NULL
      When query
        """
        SELECT json_tuple('', 'a')
        """
      Then query result
        | c0   |
        | NULL |

    Scenario: Invalid JSON returns NULL
      When query
        """
        SELECT json_tuple('not json', 'a')
        """
      Then query result
        | c0   |
        | NULL |

    Scenario: Whitespace-only string returns NULL
      When query
        """
        SELECT json_tuple('   ', 'a')
        """
      Then query result
        | c0   |
        | NULL |

    Scenario: Array as top-level JSON returns NULL
      When query
        """
        SELECT json_tuple('[1,2,3]', 'a')
        """
      Then query result
        | c0   |
        | NULL |

    Scenario: Empty object returns NULL for any field
      When query
        """
        SELECT json_tuple('{}', 'a')
        """
      Then query result
        | c0   |
        | NULL |

  Rule: Nested structures returned as JSON strings
    Scenario: Nested object returned as JSON string
      When query
        """
        SELECT json_tuple('{"a":{"b":1,"c":2}}', 'a')
        """
      Then query result
        | c0            |
        | {"b":1,"c":2} |

    Scenario: Nested array returned as JSON string
      When query
        """
        SELECT json_tuple('{"a":[1,2,3]}', 'a')
        """
      Then query result
        | c0      |
        | [1,2,3] |

    Scenario: Deeply nested object returned as JSON string
      When query
        """
        SELECT json_tuple('{"a":{"b":{"c":1}}}', 'a')
        """
      Then query result
        | c0            |
        | {"b":{"c":1}} |

    Scenario: Nested array of objects returned as JSON string
      When query
        """
        SELECT json_tuple('{"a":[{"x":1},{"x":2}]}', 'a')
        """
      Then query result
        | c0                |
        | [{"x":1},{"x":2}] |

    Scenario: Empty array value returned as JSON string
      When query
        """
        SELECT json_tuple('{"a":[]}', 'a')
        """
      Then query result
        | c0 |
        | [] |

    Scenario: Empty nested object returned as JSON string
      When query
        """
        SELECT json_tuple('{"a":{}}', 'a')
        """
      Then query result
        | c0 |
        | {} |

  Rule: Value types
    Scenario: Boolean values
      When query
        """
        SELECT json_tuple('{"a":true,"b":false}', 'a', 'b')
        """
      Then query result
        | c0   | c1    |
        | true | false |

    Scenario: Integer and float values
      When query
        """
        SELECT json_tuple('{"i":42,"f":3.14,"neg":-7}', 'i', 'f', 'neg')
        """
      Then query result
        | c0 | c1   | c2 |
        | 42 | 3.14 | -7 |

    @xfail
    Scenario: Scientific notation positive
      When query
        """
        SELECT json_tuple('{"a":1.5e10}', 'a')
        """
      Then query result
        | c0     |
        | 1.5E10 |

    Scenario: Scientific notation negative
      When query
        """
        SELECT json_tuple('{"a":-1.5e-3}', 'a')
        """
      Then query result
        | c0      |
        | -0.0015 |

    @xfail
    Scenario: Very large number
      When query
        """
        SELECT json_tuple('{"a":99999999999999999999}', 'a')
        """
      Then query result
        | c0                   |
        | 99999999999999999999 |

    Scenario: Numeric precision
      When query
        """
        SELECT json_tuple('{"a":0.1234567890123456789}', 'a')
        """
      Then query result
        | c0                  |
        | 0.12345678901234568 |

    @xfail
    Scenario: Zero and negative zero
      When query
        """
        SELECT json_tuple('{"a":0,"b":-0,"c":0.0}', 'a', 'b', 'c')
        """
      Then query result
        | c0 | c1 | c2  |
        | 0  | 0  | 0.0 |

    Scenario: Empty string value
      When query
        """
        SELECT json_tuple('{"a":""}', 'a')
        """
      Then query result
        | c0 |
        |    |

    Scenario: String value with content
      When query
        """
        SELECT json_tuple('{"a":"hello world"}', 'a')
        """
      Then query result
        | c0          |
        | hello world |

    Scenario: Numeric string value
      When query
        """
        SELECT json_tuple('{"a":"123"}', 'a')
        """
      Then query result
        | c0  |
        | 123 |

  Rule: Key behavior
    Scenario: Case-sensitive key lookup
      When query
        """
        SELECT json_tuple('{"Name":"Alice","name":"bob"}', 'Name', 'name', 'NAME')
        """
      Then query result
        | c0    | c1  | c2   |
        | Alice | bob | NULL |

    Scenario: Duplicate keys returns last value
      When query
        """
        SELECT json_tuple('{"a":1,"a":2}', 'a')
        """
      Then query result
        | c0 |
        | 2  |

    Scenario: Keys with special characters
      When query
        """
        SELECT json_tuple('{"a.b":1,"c d":2}', 'a.b', 'c d')
        """
      Then query result
        | c0 | c1 |
        | 1  | 2  |

  Rule: Unicode and special content
    Scenario: Unicode value
      When query
        """
        SELECT json_tuple('{"b":"café"}', 'b')
        """
      Then query result
        | c0   |
        | café |

    Scenario: Unicode key
      When query
        """
        SELECT json_tuple('{"nombre":"Juan"}', 'nombre')
        """
      Then query result
        | c0   |
        | Juan |

  Rule: Whitespace handling
    Scenario: Whitespace in JSON string
      When query
        """
        SELECT json_tuple('{  "a" : 1 ,  "b" : 2  }', 'a', 'b')
        """
      Then query result
        | c0 | c1 |
        | 1  | 2  |

    Scenario: JSON with embedded newlines
      When query
        """
        SELECT json_tuple('{"a":1,\n"b":2}', 'a', 'b')
        """
      Then query result
        | c0 | c1 |
        | 1  | 2  |

  Rule: LATERAL VIEW pattern
    Scenario: LATERAL VIEW with single row
      When query
        """
        SELECT t.id, jt.c0, jt.c1
        FROM (SELECT 1 AS id, '{"name":"Alice","age":"30"}' AS json_str) t
        LATERAL VIEW json_tuple(t.json_str, 'name', 'age') jt AS c0, c1
        """
      Then query result
        | id | c0    | c1 |
        | 1  | Alice | 30 |

    Scenario: LATERAL VIEW with multiple rows including NULL
      When query
        """
        SELECT t.id, jt.c0, jt.c1
        FROM (
          SELECT 1 AS id, '{"x":"a","y":"b"}' AS js
          UNION ALL
          SELECT 2 AS id, '{"x":"c","y":"d"}' AS js
          UNION ALL
          SELECT 3 AS id, CAST(NULL AS STRING) AS js
        ) t
        LATERAL VIEW json_tuple(t.js, 'x', 'y') jt AS c0, c1
        ORDER BY t.id
        """
      Then query result
        | id | c0   | c1   |
        | 1  | a    | b    |
        | 2  | c    | d    |
        | 3  | NULL | NULL |

  Rule: Additional edge cases
    @xfail
    Scenario: Escaped backslash in value
      When query
        """
        SELECT json_tuple('{"a":"hello\\nworld"}', 'a')
        """
      Then query result
        | c0           |
        | hello\nworld |

    @xfail
    Scenario: Long string value (1000 chars)
      When query
        """
        SELECT length(c0) FROM (SELECT json_tuple('{"v":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}', 'v') AS c0)
        """
      Then query result
        | length(c0) |
        | 1000       |

    Scenario: Many fields extract (20 fields)
      When query
        """
        SELECT json_tuple('{"f0":0,"f1":1,"f2":2,"f3":3,"f4":4,"f5":5,"f6":6,"f7":7,"f8":8,"f9":9,"f10":10,"f11":11,"f12":12,"f13":13,"f14":14,"f15":15,"f16":16,"f17":17,"f18":18,"f19":19}', 'f0', 'f1', 'f2', 'f3', 'f4', 'f5', 'f6', 'f7', 'f8', 'f9', 'f10', 'f11', 'f12', 'f13', 'f14', 'f15', 'f16', 'f17', 'f18', 'f19')
        """
      Then query result
        | c0 | c1 | c2 | c3 | c4 | c5 | c6 | c7 | c8 | c9 | c10 | c11 | c12 | c13 | c14 | c15 | c16 | c17 | c18 | c19 |
        | 0  | 1  | 2  | 3  | 4  | 5  | 6  | 7  | 8  | 9  | 10  | 11  | 12  | 13  | 14  | 15  | 16  | 17  | 18  | 19  |

    Scenario: Trailing comma in JSON returns null
      When query
        """
        SELECT json_tuple('{"a":1,}', 'a')
        """
      Then query result
        | c0   |
        | NULL |

    Scenario: Null as key name
      When query
        """
        SELECT json_tuple('{"null":1}', 'null')
        """
      Then query result
        | c0 |
        | 1  |

    Scenario: Empty string as key
      When query
        """
        SELECT json_tuple('{"":1}', '')
        """
      Then query result
        | c0 |
        | 1  |

    Scenario: Interleaved existing and missing fields
      When query
        """
        SELECT json_tuple('{"a":1,"c":3}', 'a', 'b', 'c', 'd')
        """
      Then query result
        | c0 | c1   | c2 | c3   |
        | 1  | NULL | 3  | NULL |
