Feature: from_json function parses JSON strings into structured types

  Rule: Basic struct parsing
    Scenario: Parse simple struct from JSON
      When query
        """
        SELECT from_json('{"a":1, "b":0.8}', 'a INT, b DOUBLE') AS result
        """
      Then query result
        | result       |
        | {1, 0.8}     |

    Scenario: Parse struct with string fields
      When query
        """
        SELECT from_json('{"name":"Alice", "age":30}', 'name STRING, age INT') AS result
        """
      Then query result
        | result          |
        | {Alice, 30}     |

    Scenario: Parse nested struct from JSON
      When query
        """
        SELECT from_json('{"a":1, "b":{"c":3}}', 'a INT, b STRUCT<c: INT>') AS result
        """
      Then query result
        | result          |
        | {1, {3}}        |

  Rule: Struct with STRUCT<> schema syntax
    Scenario: Parse struct using explicit STRUCT syntax
      When query
        """
        SELECT from_json('{"teacher":"Alice","student":[{"name":"Bob","rank":1},{"name":"Charlie","rank":2}]}', 'STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>') AS result
        """
      Then query result
        | result                                         |
        | {Alice, [{Bob, 1}, {Charlie, 2}]}              |

  Rule: Null and error handling (PERMISSIVE mode)
    Scenario: Null input returns null struct
      When query
        """
        SELECT from_json(NULL, 'a INT, b STRING') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Invalid JSON returns struct with null fields
      When query
        """
        SELECT from_json('not valid json', 'a INT') AS result
        """
      Then query result
        | result  |
        | {NULL}  |

    Scenario: Missing fields return null values
      When query
        """
        SELECT from_json('{"a":1}', 'a INT, b STRING') AS result
        """
      Then query result
        | result      |
        | {1, NULL}   |

  Rule: Timestamp formatting with options
    Scenario: Parse struct with timestamp using custom format
      When query
        """
        SELECT from_json('{"time":"26/08/2015"}', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy')) AS result
        """
      Then query result
        | result                       |
        | {2015-08-26 00:00:00}        |

  Rule: Boolean and numeric types
    Scenario: Parse boolean values
      When query
        """
        SELECT from_json('{"flag":true}', 'flag BOOLEAN') AS result
        """
      Then query result
        | result  |
        | {true}  |

    Scenario: Parse various numeric types
      When query
        """
        SELECT from_json('{"a":1, "b":2.5}', 'a BIGINT, b DOUBLE') AS result
        """
      Then query result
        | result    |
        | {1, 2.5}  |

    Scenario: Parse tinyint smallint and float types
      When query
        """
        SELECT from_json('{"a":1, "b":2, "c":3.5}', 'a TINYINT, b SMALLINT, c FLOAT') AS result
        """
      Then query result
        | result      |
        | {1, 2, 3.5} |

  Rule: Array parsing
    Scenario: Parse top-level array of integers
      When query
        """
        SELECT from_json('[1, 2, 3]', 'ARRAY<INT>') AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: Parse empty array
      When query
        """
        SELECT from_json('[]', 'ARRAY<INT>') AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: Null input returns null for array
      When query
        """
        SELECT from_json(CAST(NULL AS STRING), 'ARRAY<INT>') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Invalid JSON returns null for array
      When query
        """
        SELECT from_json('not json', 'ARRAY<INT>') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Map parsing
    Scenario: Parse top-level map
      When query
        """
        SELECT from_json('{"a":1, "b":2}', 'MAP<STRING, INT>') AS result
        """
      Then query result
        | result          |
        | {a -> 1, b -> 2} |

    Scenario: Null input returns null for map
      When query
        """
        SELECT from_json(CAST(NULL AS STRING), 'MAP<STRING, INT>') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Date parsing
    Scenario: Parse date field
      When query
        """
        SELECT from_json('{"d":"2024-01-15"}', 'd DATE') AS result
        """
      Then query result
        | result       |
        | {2024-01-15} |

    Scenario: Parse date with custom format
      When query
        """
        SELECT from_json('{"d":"15/01/2024"}', 'd DATE', map('dateFormat', 'dd/MM/yyyy')) AS result
        """
      Then query result
        | result       |
        | {2024-01-15} |

  Rule: Decimal parsing
    Scenario: Parse decimal from number
      When query
        """
        SELECT from_json('{"v":3.14}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result |
        | {3.14} |

    Scenario: Parse decimal from string
      When query
        """
        SELECT from_json('{"v":"3.14"}', 'v DECIMAL(10,2)') AS result
        """
      Then query result
        | result |
        | {3.14} |

  Rule: Nested collections in struct
    Scenario: Parse struct with nested array
      When query
        """
        SELECT from_json('{"items":[1,2,3]}', 'STRUCT<items: ARRAY<INT>>') AS result
        """
      Then query result
        | result        |
        | {[1, 2, 3]}   |

    Scenario: Parse struct with nested map
      When query
        """
        SELECT from_json('{"m":{"x":1,"y":2}}', 'STRUCT<m: MAP<STRING, INT>>') AS result
        """
      Then query result
        | result               |
        | {{x -> 1, y -> 2}}   |

  Rule: String coercion
    Scenario: Parse number as string type
      When query
        """
        SELECT from_json('{"a":123}', 'a STRING') AS result
        """
      Then query result
        | result |
        | {123}  |
