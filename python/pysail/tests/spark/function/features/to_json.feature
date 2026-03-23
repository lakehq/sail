Feature: to_json function converts complex types to JSON strings

  Rule: Basic struct conversion
    Scenario: Convert named_struct to JSON
      When query
        """
        SELECT to_json(named_struct('a', 1, 'b', 2)) AS result
        """
      Then query result
        | result         |
        | {"a":1,"b":2}  |

    Scenario: Convert nested struct to JSON
      When query
        """
        SELECT to_json(named_struct('a', 1, 'b', named_struct('c', 3))) AS result
        """
      Then query result
        | result                 |
        | {"a":1,"b":{"c":3}}    |

  Rule: Map conversion
    Scenario: Convert simple map to JSON
      When query
        """
        SELECT to_json(map('a', 1)) AS result
        """
      Then query result
        | result    |
        | {"a":1}   |

    Scenario: Convert map with struct value to JSON
      When query
        """
        SELECT to_json(map('a', named_struct('b', 1))) AS result
        """
      Then query result
        | result            |
        | {"a":{"b":1}}     |

    Scenario: Convert map with struct key to JSON
      When query
        """
        SELECT to_json(map(named_struct('a', 1), named_struct('b', 2))) AS result
        """
      Then query result
        | result              |
        | {"[1]":{"b":2}}     |

  Rule: Array conversion
    Scenario: Convert array of maps to JSON
      When query
        """
        SELECT to_json(array(map('a', 1))) AS result
        """
      Then query result
        | result      |
        | [{"a":1}]   |

    Scenario: Convert array of structs to JSON
      When query
        """
        SELECT to_json(array(named_struct('a', 1, 'b', 2))) AS result
        """
      Then query result
        | result            |
        | [{"a":1,"b":2}]   |

  Rule: Timestamp formatting with options
    Scenario: Convert struct with timestamp using custom format
      When query
        """
        SELECT to_json(
          named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')),
          map('timestampFormat', 'dd/MM/yyyy')
        ) AS result
        """
      Then query result
        | result                  |
        | {"time":"26/08/2015"}   |

  Rule: Null handling
    Scenario: Null fields are omitted from JSON output
      When query
        """
        SELECT to_json(named_struct('a', 1, 'b', CAST(NULL AS INT))) AS result
        """
      Then query result
        | result    |
        | {"a":1}   |

    Scenario: Null input returns null
      When query
        """
        SELECT to_json(CAST(NULL AS STRUCT<a: INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Decimal conversion
    Scenario: Convert struct with decimal to JSON
      When query
        """
        SELECT to_json(named_struct('price', CAST(123.45 AS DECIMAL(10,2)))) AS result
        """
      Then query result
        | result             |
        | {"price":123.45}   |

  Rule: Boolean conversion
    Scenario: Convert struct with boolean to JSON
      When query
        """
        SELECT to_json(named_struct('active', true, 'deleted', false)) AS result
        """
      Then query result
        | result                          |
        | {"active":true,"deleted":false} |

  Rule: Date formatting
    Scenario: Convert struct with date using custom format
      When query
        """
        SELECT to_json(
          named_struct('date', to_date('2024-01-15')),
          map('dateFormat', 'dd/MM/yyyy')
        ) AS result
        """
      Then query result
        | result                  |
        | {"date":"15/01/2024"}   |

  Rule: Float conversion
    Scenario: Convert struct with float to JSON
      When query
        """
        SELECT to_json(named_struct('value', CAST(3.14159 AS DOUBLE))) AS result
        """
      Then query result
        | result               |
        | {"value":3.14159}    |

  Rule: String conversion
    Scenario: Convert struct with string to JSON
      When query
        """
        SELECT to_json(named_struct('name', 'hello world')) AS result
        """
      Then query result
        | result                    |
        | {"name":"hello world"}    |

  Rule: Array of primitives
    Scenario: Array of integers
      When query
        """
        SELECT to_json(array(1, 2, 3)) AS result
        """
      Then query result
        | result  |
        | [1,2,3] |

    Scenario: Array of strings
      When query
        """
        SELECT to_json(array('a', 'b', 'c')) AS result
        """
      Then query result
        | result          |
        | ["a","b","c"]   |

  Rule: Empty structures

    Scenario: Empty map
      When query
        """
        SELECT to_json(map()) AS result
        """
      Then query result
        | result |
        | {}     |

    Scenario: Empty array
      When query
        """
        SELECT to_json(array()) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: Special float values
    Scenario: NaN value rendered as string
      When query
        """
        SELECT to_json(named_struct('v', CAST('NaN' AS DOUBLE))) AS result
        """
      Then query result
        | result        |
        | {"v":"NaN"}   |

    Scenario: Infinity value rendered as string
      When query
        """
        SELECT to_json(named_struct('v', CAST('Infinity' AS DOUBLE))) AS result
        """
      Then query result
        | result             |
        | {"v":"Infinity"}   |

    Scenario: Negative Infinity value rendered as string
      When query
        """
        SELECT to_json(named_struct('v', CAST('-Infinity' AS DOUBLE))) AS result
        """
      Then query result
        | result              |
        | {"v":"-Infinity"}   |

  Rule: Integer types
    Scenario: Tinyint zero
      When query
        """
        SELECT to_json(named_struct('v', CAST(0 AS TINYINT))) AS result
        """
      Then query result
        | result  |
        | {"v":0} |

    Scenario: Tinyint max
      When query
        """
        SELECT to_json(named_struct('v', CAST(127 AS TINYINT))) AS result
        """
      Then query result
        | result    |
        | {"v":127} |

    Scenario: Tinyint min
      When query
        """
        SELECT to_json(named_struct('v', CAST(-128 AS TINYINT))) AS result
        """
      Then query result
        | result     |
        | {"v":-128} |

  Rule: Default date and timestamp format
    Scenario: Date default format
      When query
        """
        SELECT to_json(named_struct('d', to_date('2024-01-15'))) AS result
        """
      Then query result
        | result                |
        | {"d":"2024-01-15"}   |

  Rule: Null in complex types
    Scenario: Array of nulls
      When query
        """
        SELECT to_json(named_struct('a', array(CAST(NULL AS INT), CAST(NULL AS INT)))) AS result
        """
      Then query result
        | result              |
        | {"a":[null,null]}   |

    Scenario: Map with null value
      When query
        """
        SELECT to_json(map('a', CAST(NULL AS INT), 'b', 1)) AS result
        """
      Then query result
        | result              |
        | {"a":null,"b":1}    |

  Rule: Deeply nested structures
    Scenario: Four levels of nesting
      When query
        """
        SELECT to_json(named_struct('a', named_struct('b', named_struct('c', named_struct('d', 42))))) AS result
        """
      Then query result
        | result                      |
        | {"a":{"b":{"c":{"d":42}}}}  |

    Scenario: Array of arrays
      When query
        """
        SELECT to_json(named_struct('v', array(array(1,2), array(3,4)))) AS result
        """
      Then query result
        | result                |
        | {"v":[[1,2],[3,4]]}   |

    Scenario: Nested maps
      When query
        """
        SELECT to_json(map('outer', map('inner', 1))) AS result
        """
      Then query result
        | result                    |
        | {"outer":{"inner":1}}     |

  Rule: String with special characters
    Scenario: Empty string value
      When query
        """
        SELECT to_json(named_struct('a', '')) AS result
        """
      Then query result
        | result     |
        | {"a":""}   |

  Rule: Error handling
    Scenario: Invalid options type returns error
      When query
        """
        SELECT to_json(named_struct('a', 1), 'invalid_options') AS result
        """
      Then query error INVALID_OPTIONS.NON_MAP_FUNCTION
