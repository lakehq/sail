Feature: struct function

  Rule: Basic struct construction

    Scenario: struct with literal values
      When query
        """
        SELECT struct(1, 'hello') AS result
        """
      Then query result
        | result     |
        | {1, hello} |

    Scenario: struct with named columns
      When query
        """
        SELECT struct(a, b) AS result
        FROM VALUES (1, 'x'), (2, 'y') AS t(a, b)
        ORDER BY a
        """
      Then query result ordered
        | result |
        | {1, x} |
        | {2, y} |

  Rule: Struct field names

    Scenario Outline: struct preserves names extracted from computed structs
      Given config spark.sql.caseSensitive = false
      When query
        """
        SELECT to_json(struct(<argument>)) AS result
        """
      Then query result
        | result     |
        | <expected> |

      Examples:
        | argument                                              | expected          |
        | named_struct('Temperature', 1).Temperature              | {"Temperature":1} |
        | named_struct('Temperature', 1).TEMPERATURE              | {"TEMPERATURE":1} |
        | named_struct('Temperature', 1).TEMPERATURE AS renamed   | {"renamed":1}     |

    Scenario: struct preserves computed field reference spelling in a lambda
      Given config spark.sql.caseSensitive = false
      When query
        """
        SELECT to_json(transform(array(-1, 2), reading ->
          struct(named_struct('Temperature', reading).TEMPERATURE))) AS result
        """
      Then query result
        | result                                |
        | [{"TEMPERATURE":-1},{"TEMPERATURE":2}] |

    Scenario Outline: struct preserves <case> in HAVING
      Given config spark.sql.caseSensitive = false
      When query
        """
        SELECT max(id) AS foo
        FROM range(1)
        HAVING to_json(struct(<argument>)) = '<expected>'
        """
      Then query result
        | foo |
        | 0   |

      Examples:
        | case                       | argument       | expected      |
        | alias reference spelling   | FOO            | {"FOO":0}     |
        | alias declaration spelling | foo            | {"foo":0}     |
        | explicit field alias       | FOO AS renamed | {"renamed":0} |

  Rule: Struct nullability — struct itself is never NULL

    Scenario: struct with NULL fields is not NULL
      When query
        """
        SELECT struct(NULL, 'hello') IS NOT NULL AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: struct with all NULL fields is not NULL
      When query
        """
        SELECT struct(CAST(NULL AS INT), CAST(NULL AS STRING)) IS NOT NULL AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: struct with nullable column inputs is not NULL
      When query
        """
        SELECT struct(a, b) IS NOT NULL AS result
        FROM VALUES (NULL, 'x'), (1, NULL), (NULL, NULL) AS t(a, b)
        """
      Then query result
        | result |
        | true   |
        | true   |
        | true   |

    Scenario: struct contains NULL fields but struct value exists
      When query
        """
        SELECT struct(a, b) AS result
        FROM VALUES (NULL, 'y') AS t(a, b)
        """
      Then query result
        | result    |
        | {NULL, y} |

  Rule: Nested structs

    Scenario: nested struct
      When query
        """
        SELECT struct(1, struct(2, 3)) AS result
        """
      Then query result
        | result      |
        | {1, {2, 3}} |

  Rule: named_struct

    Scenario: named_struct basic
      When query
        """
        SELECT named_struct('a', 1, 'b', 'hello') AS result
        """
      Then query result
        | result     |
        | {1, hello} |

    Scenario: named_struct with NULL value is not NULL
      When query
        """
        SELECT named_struct('a', CAST(NULL AS INT), 'b', 'hello') IS NOT NULL AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Struct field extraction

    Scenario Outline: struct field extraction rejects a column selector on <child>
      When query
        """
        SELECT <child>[selector]
        FROM VALUES (named_struct('selector', 1), 'selector') AS t(payload, selector)
        """
      Then query error (?i)(INVALID_EXTRACT_FIELD_TYPE|extraction must be a literal)

      Examples:
        | child                       |
        | payload                     |
        | named_struct('selector', 1)  |
        | coalesce(payload, payload)  |

    Scenario Outline: struct field extraction preserves a literal selector in <expression>
      When query
        """
        SELECT <expression> AS result
        """
      Then query result
        | result |
        | 1      |

      Examples:
        | expression                                                 |
        | named_struct('a', 1).a                                      |
        | named_struct('a', 1)['a']                                   |
        | named_struct('nested', named_struct('a', 1)).nested.a        |
        | named_struct('a.b', 1).`a.b`                                |

    Scenario: map extraction distinguishes literal fields from column selectors
      When query
        """
        SELECT map('selector', 1, 'other', 2).selector AS literal_key,
               map('selector', 1, 'other', 2)[selector] AS column_key
        FROM VALUES ('other') AS t(selector)
        """
      Then query result
        | literal_key | column_key |
        | 1           | 2          |

    Scenario Outline: qualified field extraction ignores an ambiguous interpretation of the table alias
      Given config spark.sql.caseSensitive = false
      When query
        """
        SELECT t.s.x AS result
        FROM (
          SELECT <value> AS s, named_struct('s', 2, 'S', 3) AS t
        ) t
        """
      Then query result
        | result     |
        | <expected> |

      Examples:
        | value                                                 | expected |
        | named_struct('x', 1)                                   | 1        |
        | array(named_struct('x', 1), named_struct('x', 4))       | [1, 4]   |

    Scenario Outline: field extraction preserves ambiguity in the selected root
      Given config spark.sql.caseSensitive = false
      When query
        """
        SELECT <reference>
        FROM (
          SELECT named_struct('x', 1, 'X', 2) AS s,
                 named_struct('s', named_struct('x', 9)) AS t
        ) t
        """
      Then query error (?i)(AMBIGUOUS_REFERENCE_TO_FIELDS|ambiguous reference to the field)

      Examples:
        | reference |
        | t.s.x     |
        | s.x       |

    @sail-bug
    Scenario: qualified field extraction prefers the qualified root when both interpretations are valid
      Given config spark.sql.caseSensitive = false
      When query
        """
        SELECT t.s.x AS result
        FROM (
          SELECT named_struct('x', 1) AS s,
                 named_struct('s', named_struct('x', 9)) AS t
        ) t
        """
      Then query result
        | result |
        | 1      |

    @sail-bug
    Scenario: a missing qualified struct field prevents fallback to an unqualified root
      Given config spark.sql.caseSensitive = false
      When query
        """
        SELECT t.s.x
        FROM (
          SELECT named_struct('y', 1) AS s,
                 named_struct('s', named_struct('x', 9)) AS t
        ) t
        """
      Then query error (?i)(FIELD_NOT_FOUND|cannot resolve attribute)
