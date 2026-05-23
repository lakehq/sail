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
        | result       |
        | {1, {2, 3}}  |

  Rule: Struct casts

    Scenario: struct cast maps fields by position when names do not overlap
      When query
        """
        SELECT CAST(named_struct('a', 1, 'b', '2') AS STRUCT<x: BIGINT, y: INT>) AS result
        """
      Then query result
        | result |
        | {1, 2} |
      Then query schema
        """
        root
         |-- result: struct (nullable = false)
         |    |-- x: long (nullable = true)
         |    |-- y: integer (nullable = true)
        """

    Scenario: struct cast remains positional when one target name overlaps
      When query
        """
        SELECT CAST(named_struct('x', 1, 'b', 2) AS STRUCT<a: STRING, b: STRING>) AS result
        """
      Then query result
        | result |
        | {1, 2} |

    Scenario: struct cast preserves a NULL top-level struct
      When query
        """
        SELECT CAST(CAST(NULL AS STRUCT<a: INT, b: INT>) AS STRUCT<x: BIGINT, y: BIGINT>) IS NULL AS result
        """
      Then query result
        | result |
        | true   |

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
