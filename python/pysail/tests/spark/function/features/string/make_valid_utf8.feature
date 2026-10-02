Feature: make_valid_utf8 output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to make_valid_utf8 yields the schema Spark declares
      When query
        """
        SELECT make_valid_utf8('Spark') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to make_valid_utf8 yields the schema Spark declares
      When query
        """
        SELECT make_valid_utf8(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to make_valid_utf8 stays nullable
      When query
        """
        SELECT make_valid_utf8(c) AS result FROM VALUES ('Spark'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Atomic non-string input is cast to string

    # All four UTF-8 functions declare the same argument in Spark
    # (`StringTypeWithCollation` + `ImplicitCastInputTypes`), so the analyzer casts every
    # `AtomicType` to STRING first. The rendering must be Spark's own, not Arrow's.
    Scenario Outline: atomic input: <case>
      When query
        """
        SELECT make_valid_utf8(<argument>) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case      | argument                 | result |
        | char      | CAST('ab' AS CHAR(2))    | ab     |
        | varchar   | CAST('ab' AS VARCHAR(4)) | ab     |

    # `MakeValidUtf8` is registered with `Signature::any`, so it has no coercion hook and rejects
    # every atomic type Spark implicitly casts to STRING. Measured 2026-08-26: Sail raises
    # "expected string array for `make_valid_utf8`" for all of these.
    @sail-bug
    Scenario Outline: atomic input is cast to string: <case>
      When query
        """
        SELECT make_valid_utf8(<argument>) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case      | argument                            | result              |
        | tinyint   | CAST(1 AS TINYINT)                  | 1                   |
        | int       | 1                                   | 1                   |
        | bigint    | CAST(1 AS BIGINT)                   | 1                   |
        | double    | CAST(1.5 AS DOUBLE)                 | 1.5                 |
        | decimal   | CAST(1.5 AS DECIMAL(10,2))          | 1.50                |
        | boolean   | true                                | true                |
        | date      | DATE '2024-01-15'                   | 2024-01-15          |
        | timestamp | TIMESTAMP '2024-01-15 12:00:00'     | 2024-01-15 12:00:00 |
        | ntz       | TIMESTAMP_NTZ '2024-01-15 12:00:00' | 2024-01-15 12:00:00 |

  Rule: NULL input returns NULL

    Scenario Outline: NULL input: <case>
      When query
        """
        SELECT make_valid_utf8(<argument>) AS result
        """
      Then query result collected
        | result |
        | NULL   |

      Examples:
        | case        | argument             |
        | NULL string | CAST(NULL AS STRING) |
        | NULL binary | CAST(NULL AS BINARY) |

    # Same root cause: with no coercion hook an untyped NULL has no string type to become.
    @sail-bug
    Scenario: an untyped NULL returns NULL
      When query
        """
        SELECT make_valid_utf8(NULL) AS result
        """
      Then query result collected
        | result |
        | NULL   |

  Rule: Binary input

    Scenario: valid UTF-8 bytes pass through
      When query
        """
        SELECT make_valid_utf8(X'61') AS result
        """
      Then query result collected
        | result |
        | a      |

    Scenario: invalid bytes are replaced with U+FFFD
      When query
        """
        SELECT make_valid_utf8(X'80') AS result
        """
      Then query result collected
        | result |
        | �      |

  Rule: Non-atomic input is rejected

    # ARRAY/MAP/STRUCT are not `AtomicType`, so Spark rejects them at analysis time.
    # Sail rejects these, but with its own error rather than Spark's DATATYPE_MISMATCH class.
    @sail-bug
    Scenario Outline: non-atomic input is an error: <case>
      When query
        """
        SELECT make_valid_utf8(<argument>) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE|expects STRING or BINARY)

      Examples:
        | case   | argument             |
        | array  | array('a')           |
        | map    | map('a', 'b')        |
        | struct | named_struct('a', 1) |

    # The accepted half of the same rule: it is the CONTAINER that is rejected, not a string
    # reached through one.
    Scenario: a string reached through a container is accepted
      When query
        """
        SELECT make_valid_utf8(element_at(array('a'), 1)) AS result
        """
      Then query result collected
        | result |
        | a      |

  Rule: Default output column name

    # Spark builds it from `nodeName` plus the rendered argument, independent of how the function
    # is implemented internally, so a rewrite must not change it.
    Scenario Outline: the column name comes from the function name and its argument: <case>
      When query
        """
        SELECT make_valid_utf8(<argument>)
        """
      Then query schema
        """
        root
         |-- <name>: string (nullable = true)
        """

      Examples:
        | case           | argument | name           | type   |
        | string literal | 'abc'    | make_valid_utf8(abc)        | string |
        | binary literal | X'61'    | make_valid_utf8(X'61')      | string |
        | empty string   | ''       | make_valid_utf8()           | string |

    # Spark's `prettyName` is `nodeName.toLowerCase(Locale.ROOT)`, so the user's spelling never
    # reaches the column name. Sail interpolates the name as written. Measured 2026-08-26:
    # Spark `make_valid_utf8(abc)`, Sail `MAKE_VALID_UTF8(abc)`. Sail-wide behaviour, not specific to this function.
    @sail-bug
    Scenario: the column name is lower-cased regardless of how the call was spelled
      When query
        """
        SELECT MAKE_VALID_UTF8('abc')
        """
      Then query schema
        """
        root
         |-- make_valid_utf8(abc): string (nullable = true)
        """

  Rule: Composes with other expressions

    Scenario: the result can be collected into an array
      When query
        """
        SELECT array(make_valid_utf8('a'), make_valid_utf8(X'62')) AS result
        """
      Then query result collected
        | result   |
        | ['a', 'b'] |

    Scenario: the result can be consumed by another string function
      When query
        """
        SELECT upper(make_valid_utf8('ab')) AS result
        """
      Then query result collected
        | result |
        | AB     |

    Scenario: a nullable column is resolved row by row
      When query
        """
        SELECT make_valid_utf8(c) AS result FROM VALUES ('a'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query result collected
        | result |
        | a      |
        | NULL   |
