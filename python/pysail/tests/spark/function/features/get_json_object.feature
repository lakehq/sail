Feature: get_json_object extracts values via a Spark JSONPath

  # Spark `get_json_object(json, path)` walks a JSONPath subset: a leading `$`,
  # dot notation (`$.a.b`), array indexing (`$.a[0]`, `$[0]`), and single-quoted
  # bracket notation (`$['a']`). A bare `$` returns the whole document. The
  # result is the matched value rendered as text; a path that does not match (or
  # cannot be parsed) returns NULL.

  Rule: Dot notation walks object keys

    Scenario: top-level key
      When query
        """
        SELECT get_json_object('{"a":1}', '$.a') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: nested keys
      When query
        """
        SELECT get_json_object('{"a":{"b":1}}', '$.a.b') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: object value is returned as compact JSON text
      When query
        """
        SELECT get_json_object('{"a":{"b":1}}', '$.a') AS result
        """
      Then query result
        | result  |
        | {"b":1} |

    Scenario: string value is returned unquoted
      When query
        """
        SELECT get_json_object('{"a":"hi"}', '$.a') AS result
        """
      Then query result
        | result |
        | hi     |

    Scenario: boolean value
      When query
        """
        SELECT get_json_object('{"a":true}', '$.a') AS result
        """
      Then query result
        | result |
        | true   |

  Rule: A bare $ returns the whole document

    Scenario: whole object
      When query
        """
        SELECT get_json_object('{"a":1}', '$') AS result
        """
      Then query result
        | result  |
        | {"a":1} |

    Scenario: whole array
      When query
        """
        SELECT get_json_object('[1,2,3]', '$') AS result
        """
      Then query result
        | result  |
        | [1,2,3] |

  Rule: Array indexing selects array elements

    Scenario: index after a key
      When query
        """
        SELECT get_json_object('{"a":[10,20,30]}', '$.a[1]') AS result
        """
      Then query result
        | result |
        | 20     |

    Scenario: index at the root
      When query
        """
        SELECT get_json_object('[1,2,3]', '$[0]') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: nested array element is returned as compact JSON text
      When query
        """
        SELECT get_json_object('{"a":[[1,2],[3,4]]}', '$.a[1]') AS result
        """
      Then query result
        | result |
        | [3,4]  |

  Rule: Mixed key and index paths

    Scenario: key then index then key
      When query
        """
        SELECT get_json_object('{"a":[{"b":7}]}', '$.a[0].b') AS result
        """
      Then query result
        | result |
        | 7      |

    Scenario: index then key
      When query
        """
        SELECT get_json_object('{"a":[{"c":1},{"c":9}]}', '$.a[1].c') AS result
        """
      Then query result
        | result |
        | 9      |

  Rule: Single-quoted bracket notation

    # @sail-bug: the parser handles `['key']`, but Sail's SQL parser collapses
    # the escaped `''`, so these reach the function as `$[a]` and return NULL.
    # They pass on Spark JVM. Remove the tag once the SQL `''` escaping is fixed.

    @sail-bug
    Scenario: single-quoted bracket key
      When query
        """
        SELECT get_json_object('{"a":1}', '$[''a'']') AS result
        """
      Then query result
        | result |
        | 1      |

    @sail-bug
    Scenario: single-quoted bracket key containing dots
      When query
        """
        SELECT get_json_object('{"a.b":5}', '$[''a.b'']') AS result
        """
      Then query result
        | result |
        | 5      |

  Rule: Non-matching and invalid paths return NULL

    Scenario: missing key
      When query
        """
        SELECT get_json_object('{"a":1}', '$.x') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: descending into a scalar
      When query
        """
        SELECT get_json_object('{"a":1}', '$.a.b') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: array index out of bounds
      When query
        """
        SELECT get_json_object('{"a":[1,2]}', '$.a[5]') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: path not anchored at dollar returns NULL
      When query
        """
        SELECT get_json_object('{"a":1}', 'a') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: empty path returns NULL
      When query
        """
        SELECT get_json_object('{"a":1}', '') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: dollar followed by empty key returns NULL
      When query
        """
        SELECT get_json_object('{"a":1}', '$.') AS result
        """
      Then query result
        | result |
        | NULL   |
