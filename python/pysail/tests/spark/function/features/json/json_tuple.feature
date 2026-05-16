Feature: json_tuple function extracts multiple fields from a JSON string as columns

  Rule: Basic extraction

    Scenario: Extract a single field from a JSON string
      When query
        """
        SELECT json_tuple('{"a":"hello"}', 'a') AS c0
        """
      Then query result
        | c0    |
        | hello |

    Scenario: Extract multiple fields from a JSON string
      When query
        """
        SELECT json_tuple('{"f1":"value1","f2":"value2"}', 'f1', 'f2')
        """
      Then query result
        | c0     | c1     |
        | value1 | value2 |

    Scenario: Extract three fields from a JSON string
      When query
        """
        SELECT json_tuple('{"a":"1","b":"2","c":"3"}', 'a', 'b', 'c')
        """
      Then query result
        | c0 | c1 | c2 |
        | 1  | 2  | 3  |

  Rule: NULL handling

    Scenario: Extract from NULL JSON string returns NULL
      When query
        """
        SELECT json_tuple(NULL, 'a', 'b')
        """
      Then query result
        | c0   | c1   |
        | NULL | NULL |

    Scenario: Extract non-existent field returns NULL
      When query
        """
        SELECT json_tuple('{"a":"hello"}', 'b') AS c0
        """
      Then query result
        | c0   |
        | NULL |

    Scenario: Extract from empty JSON object returns NULL for all fields
      When query
        """
        SELECT json_tuple('{}', 'a', 'b', 'c')
        """
      Then query result
        | c0   | c1   | c2   |
        | NULL | NULL | NULL |

    Scenario: Extract from invalid JSON returns NULL for all fields
      When query
        """
        SELECT json_tuple('{invalid json}', 'a', 'b')
        """
      Then query result
        | c0   | c1   |
        | NULL | NULL |

    Scenario: Extract some existing and some non-existent fields
      When query
        """
        SELECT json_tuple('{"a":"hello","b":"world"}', 'a', 'c', 'b')
        """
      Then query result
        | c0    | c1   | c2    |
        | hello | NULL | world |

  Rule: Data type conversion

    Scenario: Extract numeric values as strings
      When query
        """
        SELECT json_tuple('{"a":123,"b":45.67,"c":true}', 'a', 'b', 'c')
        """
      Then query result
        | c0  | c1    | c2   |
        | 123 | 45.67 | true |

    Scenario: Extract nested JSON object as JSON string
      When query
        """
        SELECT json_tuple('{"a":{"b":"c"},"d":42}', 'a', 'd')
        """
      Then query result
        | c0        | c1 |
        | {"b":"c"} | 42 |

    Scenario: Extract array values as JSON string
      When query
        """
        SELECT json_tuple('{"a":[1,2,3],"b":"hello"}', 'a', 'b')
        """
      Then query result
        | c0      | c1    |
        | [1,2,3] | hello |

  Rule: Empty string handling

    Scenario: Extract field with empty string value
      When query
        """
        SELECT json_tuple('{"a":"","b":"value"}', 'a', 'b')
        """
      Then query result
        | c0 | c1    |
        |    | value |

    Scenario: Extract field with JSON null value
      When query
        """
        SELECT json_tuple('{"a":null,"b":"value"}', 'a', 'b')
        """
      Then query result
        | c0   | c1    |
        | NULL | value |

  Rule: Special characters and escaping

    Scenario: Extract field with special characters in key
      When query
        """
        SELECT json_tuple('{"a-b":"value1","a b":"value2"}', 'a-b', 'a b')
        """
      Then query result
        | c0     | c1     |
        | value1 | value2 |

    Scenario: Extract field with Unicode characters
      When query
        """
        SELECT json_tuple('{"名称":"值","name":"value"}', '名称', 'name')
        """
      Then query result
        | c0 | c1    |
        | 值 | value |

    Scenario: Extract field with escaped characters in value
      When query
        """
        SELECT encode(c0, 'utf-8') AS v0, encode(c1, 'utf-8') AS v1 FROM (SELECT json_tuple('{"a":"x\\ny","b":"m\\tn"}', 'a', 'b'))
        """
      Then query result
        |         v0 |         v1 |
        | [78 0A 79] | [6D 09 6E] |

  Rule: Column naming behavior

    Scenario: Output columns are always named c0, c1, c2, etc.
      When query
        """
        SELECT json_tuple('{"x":"val1","y":"val2"}', 'x', 'y')
        """
      Then query result
        | c0   | c1   |
        | val1 | val2 |

    Scenario: Column naming with different number of fields
      When query
        """
        SELECT json_tuple('{"p1":"v1","p2":"v2","p3":"v3","p4":"v4"}', 'p1', 'p2', 'p3', 'p4')
        """
      Then query result
        | c0 | c1 | c2 | c3 |
        | v1 | v2 | v3 | v4 |

  Rule: Edge cases

    Scenario: Root JSON array returns NULL for all fields
      When query
        """
        SELECT json_tuple('[{"a":1},{"a":2}]', 'a') AS c0
        """
      Then query result
        | c0   |
        | NULL |

    Scenario: JSON string with whitespace
      When query
        """
        SELECT json_tuple('  {"a": "value1", "b": "value2"}  ', 'a', 'b')
        """
      Then query result
        | c0     | c1     |
        | value1 | value2 |

    Scenario: Single-quoted JSON (should fail gracefully)
      When query
        """
        SELECT json_tuple("{'a':'value'}", 'a') AS c0
        """
      Then query result
        | c0   |
        | NULL |
