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
