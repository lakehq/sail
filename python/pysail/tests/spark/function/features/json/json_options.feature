Feature: JSON expression functions handle Spark's JSON options

  Spark builds `JSONOptions` eagerly, so every option is parsed as soon as the options map is read,
  even by a function that never uses the resulting value. A bad option is rejected by `from_json`,
  `to_json`, and `schema_of_json` alike, including for options that function ignores.

  Unlike `CSVOptions`, which reads its flags with `getBool`, `JSONOptions` reads them with Scala's
  `String.toBoolean`, so the error quotes the offending value instead of naming the option.

  Rule: A non-boolean value is rejected by from_json

    @sail-bug
    Scenario Outline: from_json rejects a non-boolean <option>
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('<option>', 'garbage')) AS result
        """
      Then query error For input string: "garbage"

      Examples:
        | option                             |
        | allowComments                      |
        | allowUnquotedFieldNames            |
        | allowSingleQuotes                  |
        | allowNumericLeadingZeros           |
        | allowNonNumericNumbers             |
        | allowBackslashEscapingAnyCharacter |
        | allowUnquotedControlChars          |
        | dropFieldIfAllNull                 |
        | multiLine                          |
        | prefersDecimal                     |
        | primitivesAsString                 |
        | inferTimestamp                     |
        | ignoreNullFields                   |
        | pretty                             |
        | writeNonAsciiCharacterAsCodePoint  |
        | useUnsafeRow                       |
        | enableDateTimeParsingFallback      |

  Rule: A non-boolean value is rejected by to_json

    @sail-bug
    Scenario Outline: to_json rejects a non-boolean <option>
      When query
        """
        SELECT to_json(named_struct('a', 1), map('<option>', 'garbage')) AS result
        """
      Then query error For input string: "garbage"

      Examples:
        | option                             |
        | allowComments                      |
        | allowUnquotedFieldNames            |
        | allowSingleQuotes                  |
        | allowNumericLeadingZeros           |
        | allowNonNumericNumbers             |
        | allowBackslashEscapingAnyCharacter |
        | allowUnquotedControlChars          |
        | dropFieldIfAllNull                 |
        | multiLine                          |
        | prefersDecimal                     |
        | primitivesAsString                 |
        | inferTimestamp                     |
        | ignoreNullFields                   |
        | pretty                             |
        | writeNonAsciiCharacterAsCodePoint  |
        | useUnsafeRow                       |
        | enableDateTimeParsingFallback      |

  Rule: A non-boolean value is rejected by schema_of_json

    @sail-bug
    Scenario Outline: schema_of_json rejects a non-boolean <option>
      When query
        """
        SELECT schema_of_json('{"a":1}', map('<option>', 'garbage')) AS result
        """
      Then query error For input string: "garbage"

      Examples:
        | option                             |
        | allowComments                      |
        | allowUnquotedFieldNames            |
        | allowSingleQuotes                  |
        | allowNumericLeadingZeros           |
        | allowNonNumericNumbers             |
        | allowBackslashEscapingAnyCharacter |
        | allowUnquotedControlChars          |
        | dropFieldIfAllNull                 |
        | multiLine                          |
        | prefersDecimal                     |
        | primitivesAsString                 |
        | inferTimestamp                     |
        | ignoreNullFields                   |
        | pretty                             |
        | writeNonAsciiCharacterAsCodePoint  |
        | useUnsafeRow                       |
        | enableDateTimeParsingFallback      |

  Rule: Values that only look boolean are rejected

    @sail-bug
    Scenario Outline: from_json rejects the allowComments value <value>
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('allowComments', '<value>')) AS result
        """
      Then query error For input string: "<value>"

      Examples:
        | value |
        | 1     |
        | 0     |
        | yes   |
        | t     |

    # Gherkin trims table cells, so the empty and whitespace cases are spelled out.
    @sail-bug
    Scenario: from_json rejects an empty allowComments value
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('allowComments', '')) AS result
        """
      Then query error For input string: ""

    @sail-bug
    Scenario: from_json rejects an allowComments value with a leading space
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('allowComments', ' true')) AS result
        """
      Then query error For input string: " true"

  Rule: Boolean values are case-insensitive

    Scenario Outline: from_json accepts the boolean value <value>
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('allowComments', '<value>')) AS result
        """
      Then query result
        | result |
        | {1}    |

      Examples:
        | value |
        | true  |
        | false |
        | TRUE  |
        | FALSE |
        | FaLsE |

  Rule: Unknown options are ignored

    Scenario: from_json ignores an unknown option
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('unknownOption', 'garbage')) AS result
        """
      Then query result
        | result |
        | {1}    |

    Scenario: to_json ignores an unknown option
      When query
        """
        SELECT to_json(named_struct('a', 1), map('unknownOption', 'garbage')) AS result
        """
      Then query result
        | result  |
        | {"a":1} |

  Rule: A NULL option value is rejected

    # Spark rejects the call outright, even under a key it does not know, so this is a property of
    # the options map rather than of the individual option.

    @sail-bug
    Scenario: from_json rejects a NULL value for a known option
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('allowComments', CAST(NULL AS STRING))) AS result
        """
      Then query error For input string

    @spark-4.2
    # Spark 4.1 raised FAILED_FUNCTION_CALL here; 4.2 ignores an unknown option
    # whose value is NULL and parses normally. Measured on 4.2.0.
    Scenario: from_json ignores a NULL value for an unknown option
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('unknownOption', CAST(NULL AS STRING))) AS result
        """
      Then query result
        | result |
        | {1}    |

    @sail-bug
    Scenario: to_json rejects a NULL option value
      When query
        """
        SELECT to_json(named_struct('a', 1), map('ignoreNullFields', CAST(NULL AS STRING))) AS result
        """
      Then query error For input string

  Rule: Non-boolean options are validated too

    @sail-bug
    Scenario: from_json rejects an unknown encoding
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('encoding', 'garbage')) AS result
        """
      Then query error INVALID_PARAMETER_VALUE.CHARSET

    @sail-bug
    Scenario: from_json rejects an unknown timeZone
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('timeZone', 'garbage')) AS result
        """
      Then query error INVALID_TIMEZONE

    @sail-bug
    Scenario: from_json rejects an unknown compression codec
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('compression', 'garbage')) AS result
        """
      Then query error CODEC_NOT_AVAILABLE

    @sail-bug
    Scenario: from_json rejects an empty lineSep
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('lineSep', '')) AS result
        """
      Then query error 'lineSep' cannot be an empty string

    @sail-bug
    Scenario: from_json rejects a non-numeric samplingRatio
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('samplingRatio', 'garbage')) AS result
        """
      Then query error For input string: "garbage"

    @sail-bug
    Scenario: from_json rejects the DROPMALFORMED mode
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('mode', 'DROPMALFORMED')) AS result
        """
      Then query error doesn't support the DROPMALFORMED mode

    Scenario: from_json accepts an unknown mode
      # Spark does not validate the mode: it warns and falls back to PERMISSIVE.
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('mode', 'garbage')) AS result
        """
      Then query result
        | result |
        | {1}    |

  Rule: An option that is set must actually take effect

    # These are the dangerous ones: the value is valid and accepted by both engines, but Sail
    # ignores the option and silently returns different data. Each scenario is paired with the
    # no-option baseline so that it discriminates rather than merely passing.

    Scenario: to_json drops a null field by default
      When query
        """
        SELECT to_json(named_struct('a', 1, 'b', CAST(NULL AS INT))) AS result
        """
      Then query result
        | result  |
        | {"a":1} |

    @sail-bug
    Scenario: to_json keeps a null field when ignoreNullFields is false
      When query
        """
        SELECT to_json(named_struct('a', 1, 'b', CAST(NULL AS INT)), map('ignoreNullFields', 'false')) AS result
        """
      Then query result
        | result             |
        | {"a":1,"b":null} |

    @sail-bug
    Scenario: to_json honors an uppercase ignoreNullFields key
      When query
        """
        SELECT to_json(named_struct('a', 1, 'b', CAST(NULL AS INT)), map('IGNORENULLFIELDS', 'false')) AS result
        """
      Then query result
        | result             |
        | {"a":1,"b":null} |

    Scenario: from_json rejects a comment by default
      When query
        """
        SELECT from_json('{"a":1 /*c*/}', 'a INT') AS result
        """
      Then query result
        | result |
        | {NULL} |

    @sail-bug
    Scenario: from_json accepts a comment when allowComments is true
      When query
        """
        SELECT from_json('{"a":1 /*c*/}', 'a INT', map('allowComments', 'true')) AS result
        """
      Then query result
        | result |
        | {1}    |

    @sail-bug
    Scenario: from_json honors an uppercase allowComments key
      When query
        """
        SELECT from_json('{"a":1 /*c*/}', 'a INT', map('ALLOWCOMMENTS', 'true')) AS result
        """
      Then query result
        | result |
        | {1}    |

  Rule: The options apply to every row of a column, not just to a literal

    # A row-0 broadcast would be invisible with a single value, so each row differs.

    @sail-bug
    Scenario: to_json keeps the null of each row when ignoreNullFields is false
      When query
        """
        SELECT to_json(named_struct('a', c), map('ignoreNullFields', 'false')) AS result
        FROM VALUES (1), (CAST(NULL AS INT)), (3) AS t(c)
        """
      Then query result
        | result       |
        | {"a":1}      |
        | {"a":null}   |
        | {"a":3}      |

    Scenario: from_json parses every row of a column
      When query
        """
        SELECT from_json(c, 'a INT') AS result
        FROM VALUES ('{"a":1}'), ('{"a":2}'), ('{"a":garbage}'), (NULL) AS t(c)
        """
      Then query result
        | result |
        | {1}    |
        | {2}    |
        | {NULL} |
        | NULL   |

  Rule: Integer options are validated

    # `maxNestingDepth`, `maxNumLen` and `maxStringLen` are read straight off the parameter map
    # with Scala's `String.toInt`, so they report the offending value like the booleans do.

    @sail-bug
    Scenario Outline: from_json rejects a non-integer <option>
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('<option>', 'garbage')) AS result
        """
      Then query error For input string: "garbage"

      Examples:
        | option          |
        | maxNestingDepth |
        | maxNumLen       |
        | maxStringLen    |

    @sail-bug
    Scenario: from_json rejects a negative maxNestingDepth
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('maxNestingDepth', '-1')) AS result
        """
      Then query error Cannot set maxNestingDepth to a negative value

    Scenario: from_json accepts an integer maxNestingDepth
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('maxNestingDepth', '10')) AS result
        """
      Then query result
        | result |
        | {1}    |

  Rule: An option Spark merely ignores must not fail the query

    # `schema_of_json` refuses these outright rather than ignoring them, so a query Spark answers
    # fails in Sail. The refusal is deliberate — the option would change the inferred schema — but
    # it still diverges: Spark infers the schema and returns it.

    @sail-bug
    Scenario Outline: schema_of_json accepts <option> set to true
      When query
        """
        SELECT schema_of_json('{"a":1}', map('<option>', 'true')) AS result
        """
      Then query result
        | result           |
        | STRUCT<a: BIGINT> |

      Examples:
        | option                             |
        | allowComments                      |
        | allowBackslashEscapingAnyCharacter |
        | allowUnquotedControlChars          |
        | dropFieldIfAllNull                 |

  Rule: Options Spark does not validate are accepted

    # Guards against over-validating: these carry a value Spark never parses, so rejecting them
    # would be its own divergence.

    Scenario: from_json accepts an unparseable timestampNTZFormat
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('timestampNTZFormat', 'garbage')) AS result
        """
      Then query result
        | result |
        | {1}    |

    Scenario: from_json accepts any columnNameOfCorruptRecord
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('columnNameOfCorruptRecord', 'garbage')) AS result
        """
      Then query result
        | result |
        | {1}    |

  Rule: A datetime format is validated only where Spark actually uses it

    # These are NOT eager, unlike the flags: Spark parses the pattern when it formats or infers
    # with it, so the same garbage value is rejected by one function and accepted by another.
    # `from_json` with an INT schema never touches dates, so it never parses the pattern. The
    # accepting scenarios are guards: validating these eagerly would be its own divergence.

    @sail-bug
    Scenario: from_json accepts an unparseable dateFormat
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('dateFormat', 'garbage')) AS result
        """
      Then query result
        | result |
        | {1}    |

    @sail-bug
    Scenario: from_json accepts an unparseable timestampFormat
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('timestampFormat', 'garbage')) AS result
        """
      Then query result
        | result |
        | {1}    |

    Scenario: schema_of_json accepts an unparseable dateFormat
      When query
        """
        SELECT schema_of_json('{"a":1}', map('dateFormat', 'garbage')) AS result
        """
      Then query result
        | result            |
        | STRUCT<a: BIGINT> |

    @sail-bug
    Scenario: to_json rejects an unparseable dateFormat
      When query
        """
        SELECT to_json(named_struct('a', 1), map('dateFormat', 'garbage')) AS result
        """
      Then query error INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_WEEK_BASED_PATTERN

    @sail-bug
    Scenario: to_json rejects an unparseable timestampFormat
      When query
        """
        SELECT to_json(named_struct('a', 1), map('timestampFormat', 'garbage')) AS result
        """
      Then query error INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_WEEK_BASED_PATTERN

    @sail-bug
    Scenario: schema_of_json rejects an unparseable timestampFormat
      # It infers timestamps, so it does parse this pattern — unlike dateFormat just above.
      When query
        """
        SELECT schema_of_json('{"a":1}', map('timestampFormat', 'garbage')) AS result
        """
      Then query error INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_WEEK_BASED_PATTERN

  Rule: The remaining option classes are rejected

    @sail-bug
    Scenario: from_json rejects a singleVariantColumn that the schema cannot hold
      When query
        """
        SELECT from_json('{"a":1}', 'a INT', map('singleVariantColumn', 'v')) AS result
        """
      Then query error Literal must have a corresponding value to

    @sail-bug
    Scenario: schema_of_json rejects a NULL option value
      When query
        """
        SELECT schema_of_json('{"a":1}', map('allowComments', CAST(NULL AS STRING))) AS result
        """
      Then query error For input string

  Rule: A boolean option that is set must change what the function does

    # The worst class: the value is valid, both engines accept it, and Sail silently returns
    # different data. Each is paired with its no-option baseline, so the pair discriminates —
    # without the baseline the scenario could pass for the wrong reason.
    #
    # Measured as working and deliberately not re-tested here: allowSingleQuotes,
    # allowNonNumericNumbers, inferTimestamp, prefersDecimal, primitivesAsString.
    # Measured as having no observable effect on an expression (a reader-only or internal
    # option), so no scenario can discriminate: multiLine, useUnsafeRow,
    # allowBackslashEscapingAnyCharacter.

    Scenario: from_json rejects an unquoted field name by default
      When query
        """
        SELECT from_json('{a:1}', 'a INT') AS result
        """
      Then query result
        | result |
        | {NULL} |

    @sail-bug
    Scenario: from_json accepts an unquoted field name when allowUnquotedFieldNames is true
      When query
        """
        SELECT from_json('{a:1}', 'a INT', map('allowUnquotedFieldNames', 'true')) AS result
        """
      Then query result
        | result |
        | {1}    |

    Scenario: from_json rejects a leading zero by default
      When query
        """
        SELECT from_json('{"a":01}', 'a INT') AS result
        """
      Then query result
        | result |
        | {NULL} |

    @sail-bug
    Scenario: from_json accepts a leading zero when allowNumericLeadingZeros is true
      When query
        """
        SELECT from_json('{"a":01}', 'a INT', map('allowNumericLeadingZeros', 'true')) AS result
        """
      Then query result
        | result |
        | {1}    |

    Scenario: from_json rejects an unquoted control character by default
      # Asserted through IS NOT NULL: a raw control character cannot be written in a table cell.
      When query
        """
        SELECT from_json(concat('{"a":"', char(1), '"}'), 'a STRING').a IS NOT NULL AS result
        """
      Then query result
        | result |
        | false  |

    @sail-bug
    Scenario: from_json accepts an unquoted control character when allowUnquotedControlChars is true
      When query
        """
        SELECT from_json(concat('{"a":"', char(1), '"}'), 'a STRING', map('allowUnquotedControlChars', 'true')).a IS NOT NULL AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: to_json writes one line by default
      # `pretty` is asserted through the newline rather than the text, because a pretty-printed
      # document cannot be written in a Gherkin table cell.
      When query
        """
        SELECT to_json(named_struct('a', 1, 'b', 2)) LIKE '%\n%' AS result
        """
      Then query result
        | result |
        | false  |

    @sail-bug
    Scenario: to_json writes several lines when pretty is true
      When query
        """
        SELECT to_json(named_struct('a', 1, 'b', 2), map('pretty', 'true')) LIKE '%\n%' AS result
        """
      Then query result
        | result |
        | true   |

    # Asserted through the length: the escaped form carries a backslash, and a Gherkin table cell
    # cannot hold one verbatim. The verbatim form is 9 characters, the escaped one is 14.
    Scenario: to_json writes a non-ASCII character verbatim by default
      When query
        """
        SELECT length(to_json(named_struct('a', 'ñ'))) AS result
        """
      Then query result
        | result |
        | 9      |

    @sail-bug
    Scenario: to_json escapes a non-ASCII character when writeNonAsciiCharacterAsCodePoint is true
      When query
        """
        SELECT length(to_json(named_struct('a', 'ñ'), map('writeNonAsciiCharacterAsCodePoint', 'true'))) AS result
        """
      Then query result
        | result |
        | 14     |

    @sail-bug
    Scenario: from_json returns NULL for a date the dateFormat cannot parse
      When query
        """
        SELECT from_json('{"a":"2024-01-15"}', 'a DATE', map('dateFormat', 'yyyy/MM/dd')) AS result
        """
      Then query result
        | result |
        | {NULL} |

    @sail-bug
    Scenario: from_json falls back to the default date parser when enableDateTimeParsingFallback is true
      When query
        """
        SELECT from_json('{"a":"2024-01-15"}', 'a DATE', map('dateFormat', 'yyyy/MM/dd', 'enableDateTimeParsingFallback', 'true')) AS result
        """
      Then query result
        | result       |
        | {2024-01-15} |
