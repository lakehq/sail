Feature: is_valid_utf8 output schema

  @function(nullability)
  Rule: Output schema

    @sail-bug
    Scenario: a non-null literal input to is_valid_utf8 yields the schema Spark declares
      When query
        """
        SELECT is_valid_utf8('Spark') AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

    @sail-bug
    Scenario: a non-null column input to is_valid_utf8 yields the schema Spark declares
      When query
        """
        SELECT is_valid_utf8(CAST(id AS STRING)) AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

    @sail-bug
    Scenario: a nullable column input to is_valid_utf8 stays nullable
      When query
        """
        SELECT is_valid_utf8(c) AS result FROM VALUES ('Spark'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

    @sail-bug
    Scenario: a non-null binary literal input yields the schema Spark declares
      When query
        """
        SELECT is_valid_utf8(X'80') AS result
        """
      Then query schema
        """
        root
         |-- result: boolean (nullable = true)
        """

  Rule: NULL input returns NULL

    @sail-bug
    Scenario Outline: NULL input returns NULL: <case>
      When query
        """
        SELECT is_valid_utf8(<argument>) AS result
        """
      Then query result collected
        | result |
        | NULL   |

      Examples:
        | case          | argument             |
        | untyped NULL  | NULL                 |
        | NULL string   | CAST(NULL AS STRING) |
        | NULL binary   | CAST(NULL AS BINARY) |
        | NULL integer  | CAST(NULL AS INT)    |

    @sail-bug
    Scenario: NULL string input
      When query
        """
        SELECT is_valid_utf8(CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    Scenario: NULL binary input
      When query
        """
        SELECT is_valid_utf8(CAST(NULL AS BINARY)) AS result
        """
      Then query result
        | result |
        | NULL   |

  @function(nullability)
  Rule: Default output column name

    # Spark derives it from `nodeName` (`override def nodeName = "is_valid_utf8"`), not from the
    # expression the function desugars to, so rewriting the implementation must not change it.
    @sail-bug
    Scenario Outline: the column name comes from the function name and its argument: <case>
      When query
        """
        SELECT is_valid_utf8(<argument>)
        """
      Then query schema
        """
        root
         |-- <name>: boolean (nullable = true)
        """

      Examples:
        | case           | argument | name                 |
        | string literal | 'abc'    | is_valid_utf8(abc)   |
        | binary literal | X'80'    | is_valid_utf8(X'80') |
        | empty string   | ''       | is_valid_utf8()      |

    # Spark's `prettyName` is `nodeName.toLowerCase(Locale.ROOT)`, so the user's spelling never
    # reaches the column name. Sail interpolates the name as written. Measured 2026-08-26: Spark
    # `is_valid_utf8(abc)`, Sail `IS_VALID_UTF8(abc)`. This is Sail-wide behaviour rather than
    # something specific to this function, but it is the discriminating case for this Rule.
    @sail-bug
    Scenario: the column name is lower-cased regardless of how the call was spelled
      When query
        """
        SELECT IS_VALID_UTF8('abc')
        """
      Then query schema
        """
        root
         |-- is_valid_utf8(abc): boolean (nullable = true)
        """

  Rule: String input

    Scenario Outline: string input: <case>
      When query
        """
        SELECT is_valid_utf8(<argument>) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case                    | argument | result |
        | a valid string          | 'hello'  | true   |
        | the empty string        | ''       | true   |
        | an emoji                | '😀'     | true   |

    Scenario: a valid string is valid
      When query
        """
        SELECT is_valid_utf8('hello') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: the empty string is valid
      When query
        """
        SELECT is_valid_utf8('') AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Atomic non-string input is cast to string

    # Every `AtomicType` is implicitly cast to STRING, and the rendered text is always
    # valid UTF-8, so these are all `true`.
    Scenario Outline: atomic input is read as a string: <case>
      When query
        """
        SELECT is_valid_utf8(<argument>) AS result
        """
      Then query result collected
        | result |
        | true   |

      Examples:
        | case                  | argument                        |
        | integer               | 123                             |
        | decimal               | 1.5                             |
        | double                | CAST(1.5 AS DOUBLE)             |
        | boolean               | true                            |
        | timestamp             | TIMESTAMP '2024-01-15 12:00:00' |
        | date                  | DATE '2024-01-15'               |
        | year-month interval   | INTERVAL '1' YEAR               |
        | day-time interval     | INTERVAL '1' DAY                |

    # TIME is Spark 4.1+, so it needs a tighter gate than the file's `@spark-4`. It cannot live
    # in the table above: pytest-bdd applies `Examples:` tags as raw markers without running
    # `pytest_bdd_apply_tag`, so a tag there would never produce a skip.
    @spark-4.1
    Scenario: time input is read as a string
      When query
        """
        SELECT is_valid_utf8(TIME '12:30:00') AS result
        """
      Then query result collected
        | result |
        | true   |

    # `VariantType` extends `AtomicType`, so Spark casts it to STRING and returns true.
    # Sail represents VARIANT as a struct, which its coercion rejects.
    # Measured 2026-08-25: Spark JVM 4.2.0 -> true; Sail -> coercion error.
    @sail-bug
    Scenario: a variant input is read as a string
      When query
        """
        SELECT is_valid_utf8(parse_json('{"a":1}')) AS result
        """
      Then query result collected
        | result |
        | true   |

  Rule: Non-atomic input is rejected

    # ARRAY/MAP/STRUCT are not `AtomicType`, so Spark rejects them at analysis time
    # rather than rendering them as a string.
    @sail-bug
    Scenario Outline: non-atomic input is an error: <case>
      When query
        """
        SELECT is_valid_utf8(<argument>) AS result
        """
      Then query error (?i)(DATATYPE_MISMATCH\.UNEXPECTED_INPUT_TYPE|expects STRING or BINARY)

      Examples:
        | case   | argument             |
        | array  | array('a', 'b')      |
        | map    | map('a', 'b')        |
        | struct | named_struct('a', 1) |

    # The accepted half of the same rule: it is the CONTAINER type that is rejected, not a string
    # reached through one. Without this, an implementation that rejected everything would pass.
    Scenario: a string reached through a container is accepted
      When query
        """
        SELECT is_valid_utf8(element_at(array('a', 'b'), 1)) AS result
        """
      Then query result collected
        | result |
        | true   |

  Rule: Binary input is validated byte by byte

    Scenario Outline: binary input: <case>
      When query
        """
        SELECT is_valid_utf8(<argument>) AS result
        """
      Then query result collected
        | result   |
        | <result> |

      Examples:
        | case                          | argument      | result |
        | valid ASCII bytes             | X'68656C6C6F' | true   |
        | a NUL byte                    | X'00'         | true   |
        | the empty binary              | X''           | true   |
        | a lone continuation byte      | X'80'         | false  |
        | an overlong encoding          | X'C080'       | false  |
        | a truncated multibyte segment | X'C3'         | false  |
        | a valid two-byte sequence     | X'C3A9'       | true   |
        | a valid three-byte sequence   | X'E282AC'     | true   |
        | a valid four-byte sequence    | X'F09F9880'   | true   |
        | a UTF-8 BOM                   | X'EFBBBF'     | true   |
        | a surrogate-range encoding    | X'EDA080'     | false  |
        | a code point above U+10FFFF   | X'F4908080'   | false  |
        | an invalid byte inside ASCII  | X'61C262'     | false  |

    Scenario: valid UTF-8 bytes are valid
      When query
        """
        SELECT is_valid_utf8(X'68656C6C6F') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: the empty binary is valid
      When query
        """
        SELECT is_valid_utf8(X'') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: a lone continuation byte is invalid
      When query
        """
        SELECT is_valid_utf8(X'80') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: an overlong encoding is invalid
      When query
        """
        SELECT is_valid_utf8(X'C080') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: a truncated multibyte sequence is invalid
      When query
        """
        SELECT is_valid_utf8(X'C3') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: a valid two-byte sequence is valid
      When query
        """
        SELECT is_valid_utf8(X'C3A9') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: a valid four-byte sequence is valid
      When query
        """
        SELECT is_valid_utf8(X'F09F9880') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: a valid three-byte sequence is valid
      When query
        """
        SELECT is_valid_utf8(X'E282AC') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: a UTF-8 BOM is valid content
      When query
        """
        SELECT is_valid_utf8(X'EFBBBF') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: a surrogate-range encoding is invalid
      When query
        """
        SELECT is_valid_utf8(X'EDA080') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: a code point above U+10FFFF is invalid
      When query
        """
        SELECT is_valid_utf8(X'F4908080') AS result
        """
      Then query result
        | result |
        | false  |

  Rule: Values must be validated individually, not as one buffer

    # Validating the whole values buffer in one pass is only sound if every offset also lands on a
    # character boundary. In these fixtures the CONCATENATION is valid UTF-8 while no single value
    # is, so an implementation that validated the buffer and skipped the boundary check would
    # answer `true` for every row.
    Scenario: a two-byte character split across two rows is invalid in both
      When query
        """
        SELECT is_valid_utf8(b) AS result FROM VALUES (X'C3'), (X'A9') AS t(b)
        """
      Then query result collected
        | result |
        | false  |
        | false  |

    Scenario: a three-byte character split across three rows is invalid in all
      When query
        """
        SELECT is_valid_utf8(b) AS result FROM VALUES (X'E2'), (X'82'), (X'AC') AS t(b)
        """
      Then query result collected
        | result |
        | false  |
        | false  |
        | false  |

    Scenario: a valid row followed by a split character
      When query
        """
        SELECT is_valid_utf8(b) AS result
        FROM VALUES (X'68656C6C6F'), (X'C3'), (X'A9') AS t(b)
        """
      Then query result collected
        | result |
        | true   |
        | false  |
        | false  |

    @sail-bug
    Scenario: a split character with a NULL row between the halves
      When query
        """
        SELECT is_valid_utf8(b) AS result
        FROM VALUES (X'C3'), (CAST(NULL AS BINARY)), (X'A9') AS t(b)
        """
      Then query result collected
        | result |
        | false  |
        | NULL   |
        | false  |

  Rule: Column expressions over multiple rows

    @sail-bug
    Scenario: string column with a NULL row
      When query
        """
        SELECT is_valid_utf8(s) AS result
        FROM VALUES ('hello'), (CAST(NULL AS STRING)), ('') AS t(s)
        """
      Then query result collected
        | result |
        | true   |
        | NULL   |
        | true   |

    @sail-bug
    Scenario: binary column mixing valid, invalid and NULL rows
      When query
        """
        SELECT is_valid_utf8(b) AS result
        FROM VALUES (X'68656C6C6F'), (X'80'), (CAST(NULL AS BINARY)) AS t(b)
        """
      Then query result collected
        | result |
        | true   |
        | false  |
        | NULL   |

    # Negated on purpose: `NOT NULL` is NULL, so the NULL row is filtered out. Had a NULL input
    # yielded `false`, `NOT false` would be true and that row would come back.
    @sail-bug
    Scenario: a negated predicate keeps NULL out of the result
      When query
        """
        SELECT hex(b) AS b FROM VALUES (X'68656C6C6F'), (X'80'), (CAST(NULL AS BINARY)) AS t(b)
        WHERE NOT is_valid_utf8(b)
        """
      Then query result collected
        | b  |
        | 80 |

  Rule: Composes with other expressions

    Scenario: the result can be collected into an array
      When query
        """
        SELECT array(is_valid_utf8('a'), is_valid_utf8(X'80')) AS result
        """
      Then query result collected
        | result        |
        | [True, False] |

    Scenario: the result is usable as an aggregate FILTER predicate
      When query
        """
        SELECT count(*) FILTER (WHERE is_valid_utf8(b)) AS result
        FROM VALUES (X'61'), (X'80') AS t(b)
        """
      Then query result collected
        | result |
        | 1      |

  Rule: Argument count

    # The accepted arity belongs in the same Rule: without it, an implementation that rejected
    # every call would satisfy the two rejection scenarios below.
    Scenario: one argument is accepted
      When query
        """
        SELECT is_valid_utf8('a') AS result
        """
      Then query result collected
        | result |
        | true   |

    @sail-bug
    Scenario: zero arguments is an error
      When query
        """
        SELECT is_valid_utf8() AS result
        """
      Then query error (?i)(WRONG_NUM_ARGS|requires 1 argument)

    @sail-bug
    Scenario: two arguments is an error
      When query
        """
        SELECT is_valid_utf8('a', 'b') AS result
        """
      Then query error (?i)(WRONG_NUM_ARGS|requires 1 argument)

  Rule: Non-string castable input is cast to string

    Scenario: integer input is read as a string
      When query
        """
        SELECT is_valid_utf8(123) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: double input is read as a string
      When query
        """
        SELECT is_valid_utf8(1.5) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: boolean input is read as a string
      When query
        """
        SELECT is_valid_utf8(true) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: timestamp input is read as a string
      When query
        """
        SELECT is_valid_utf8(TIMESTAMP '2024-01-15 12:00:00') AS result
        """
      Then query result
        | result |
        | true   |
