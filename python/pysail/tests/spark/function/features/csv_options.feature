@csv
Feature: CSV expression functions handle Spark's CSV options

  Spark builds `CSVOptions` eagerly, so every option is parsed as soon as the options map is read,
  even by a function that never uses the resulting value. A bad option is rejected by `from_csv`,
  `to_csv`, and `schema_of_csv` alike, including for options that function ignores.

  Rule: A non-boolean value is rejected by from_csv

    Scenario Outline: from_csv rejects a non-boolean <option>
      When query
        """
        SELECT from_csv('1', 'a INT', map('<option>', 'garbage')) AS result
        """
      Then query error <option> flag can be true or false

      Examples:
        | option                   |
        | header                   |
        | inferSchema              |
        | ignoreLeadingWhiteSpace  |
        | ignoreTrailingWhiteSpace |
        | escapeQuotes             |
        | quoteAll                 |
        | enforceSchema            |

  Rule: A non-boolean value is rejected by to_csv

    Scenario Outline: to_csv rejects a non-boolean <option>
      When query
        """
        SELECT to_csv(named_struct('a', 1), map('<option>', 'garbage')) AS result
        """
      Then query error <option> flag can be true or false

      Examples:
        | option                   |
        | header                   |
        | inferSchema              |
        | ignoreLeadingWhiteSpace  |
        | ignoreTrailingWhiteSpace |
        | escapeQuotes             |
        | quoteAll                 |
        | enforceSchema            |

  Rule: A non-boolean value is rejected by schema_of_csv

    Scenario Outline: schema_of_csv rejects a non-boolean <option>
      When query
        """
        SELECT schema_of_csv('1', map('<option>', 'garbage')) AS result
        """
      Then query error <option> flag can be true or false

      Examples:
        | option                   |
        | header                   |
        | inferSchema              |
        | ignoreLeadingWhiteSpace  |
        | ignoreTrailingWhiteSpace |
        | escapeQuotes             |
        | quoteAll                 |
        | enforceSchema            |

  Rule: multiLine reports the offending value instead of the option name

    # Spark reads `multiLine` with Scala's `String.toBoolean` rather than `CSVOptions.getBool`,
    # so it accepts the same values as the other boolean options but reports a different error.

    Scenario: from_csv rejects a non-boolean multiLine
      When query
        """
        SELECT from_csv('1', 'a INT', map('multiLine', 'garbage')) AS result
        """
      Then query error For input string: "garbage"

    Scenario: to_csv rejects a non-boolean multiLine
      When query
        """
        SELECT to_csv(named_struct('a', 1), map('multiLine', 'garbage')) AS result
        """
      Then query error For input string: "garbage"

    Scenario: schema_of_csv rejects a non-boolean multiLine
      When query
        """
        SELECT schema_of_csv('1', map('multiLine', 'garbage')) AS result
        """
      Then query error For input string: "garbage"

    Scenario: from_csv rejects an empty multiLine
      When query
        """
        SELECT from_csv('1', 'a INT', map('multiLine', '')) AS result
        """
      Then query error For input string: ""

    Scenario: from_csv accepts an uppercase multiLine
      When query
        """
        SELECT from_csv('1', 'a INT', map('multiLine', 'TRUE')) AS result
        """
      Then query result
        | result |
        | {1}    |

  Rule: Boolean values are case-insensitive

    Scenario Outline: from_csv accepts the boolean value <value>
      When query
        """
        SELECT from_csv('1', 'a INT', map('header', '<value>')) AS result
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
        | True  |
        | FaLsE |

    Scenario: to_csv accepts an uppercase boolean value
      When query
        """
        SELECT to_csv(named_struct('a', 'x'), map('quoteAll', 'TRUE')) AS result
        """
      Then query result
        | result |
        | "x"    |

  Rule: Values that only look boolean are rejected

    Scenario Outline: from_csv rejects the header value <value>
      When query
        """
        SELECT from_csv('1', 'a INT', map('header', '<value>')) AS result
        """
      Then query error header flag can be true or false

      Examples:
        | value |
        | 1     |
        | 0     |
        | yes   |
        | t     |
        | f     |

    # The whitespace and empty cases are spelled out because Gherkin trims table cells.
    Scenario: from_csv rejects an empty header value
      When query
        """
        SELECT from_csv('1', 'a INT', map('header', '')) AS result
        """
      Then query error header flag can be true or false

    Scenario: from_csv rejects a header value with a leading space
      When query
        """
        SELECT from_csv('1', 'a INT', map('header', ' true')) AS result
        """
      Then query error header flag can be true or false

    Scenario: from_csv rejects a header value with a trailing space
      When query
        """
        SELECT from_csv('1', 'a INT', map('header', 'true ')) AS result
        """
      Then query error header flag can be true or false

    Scenario: from_csv rejects a boolean value spelled with a non-ASCII lookalike
      # Spark folds option values with `toLowerCase(Locale.ROOT)`, which does not map U+017F to
      # `s`, so this is not the word `false`.
      When query
        """
        SELECT from_csv('1', 'a INT', map('header', 'falſe')) AS result
        """
      Then query error header flag can be true or false

  Rule: Option keys are matched case-insensitively

    # The error always names the canonical option, never the key the user wrote.

    Scenario Outline: from_csv rejects a non-boolean value for the key <key>
      When query
        """
        SELECT from_csv('1', 'a INT', map('<key>', 'garbage')) AS result
        """
      Then query error header flag can be true or false

      Examples:
        | key    |
        | HEADER |
        | Header |
        | hEaDeR |

  Rule: Unknown options are ignored

    Scenario: from_csv ignores an unknown option
      When query
        """
        SELECT from_csv('1', 'a INT', map('unknownOption', 'garbage')) AS result
        """
      Then query result
        | result |
        | {1}    |

    Scenario: to_csv ignores an unknown option
      When query
        """
        SELECT to_csv(named_struct('a', 1), map('unknownOption', 'garbage')) AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: An option key is matched case-insensitively when the option applies

    # Not just when it is rejected: the option must actually take effect under any spelling.

    Scenario: to_csv honors an uppercase sep
      When query
        """
        SELECT to_csv(named_struct('a', 1, 'b', 'x'), map('SEP', '|')) AS result
        """
      Then query result
        | result |
        | 1\|x   |

    Scenario: from_csv honors an uppercase sep
      When query
        """
        SELECT from_csv('1|2', 'a INT, b INT', map('SEP', '|')) AS result
        """
      Then query result
        | result |
        | {1, 2} |

    Scenario: from_csv honors a mixed-case delimiter
      When query
        """
        SELECT from_csv('1|2', 'a INT, b INT', map('DeLiMiTeR', '|')) AS result
        """
      Then query result
        | result |
        | {1, 2} |

    Scenario: schema_of_csv honors an uppercase sep
      When query
        """
        SELECT schema_of_csv('1|abc', map('SEP', '|')) AS result
        """
      Then query result
        | result                        |
        | STRUCT<_c0: INT, _c1: STRING> |

    # Without a timestampFormat the value infers as STRING, so these three together prove the
    # option is really applied under either spelling rather than silently dropped.
    Scenario: schema_of_csv infers a string without a timestampFormat
      When query
        """
        SELECT schema_of_csv('15/01/2024 12:00:00') AS result
        """
      Then query result
        | result              |
        | STRUCT<_c0: STRING> |

    Scenario: schema_of_csv honors a canonical timestampFormat
      When query
        """
        SELECT schema_of_csv('15/01/2024 12:00:00', map('timestampFormat', 'dd/MM/yyyy HH:mm:ss')) AS result
        """
      Then query result
        | result                 |
        | STRUCT<_c0: TIMESTAMP> |

    Scenario: schema_of_csv honors an uppercase timestampFormat
      When query
        """
        SELECT schema_of_csv('15/01/2024 12:00:00', map('TIMESTAMPFORMAT', 'dd/MM/yyyy HH:mm:ss')) AS result
        """
      Then query result
        | result                 |
        | STRUCT<_c0: TIMESTAMP> |

    Scenario: schema_of_csv infers a date with the default format
      When query
        """
        SELECT schema_of_csv('2024-01-15') AS result
        """
      Then query result
        | result            |
        | STRUCT<_c0: DATE> |

    @sail-bug
    Scenario: schema_of_csv honors a dateFormat
      # Sail hardcodes the DATE inference format to `%Y-%m-%d` and never reads `dateFormat`.
      When query
        """
        SELECT schema_of_csv('15/01/2024', map('dateFormat', 'dd/MM/yyyy')) AS result
        """
      Then query result
        | result            |
        | STRUCT<_c0: DATE> |

    @sail-bug
    Scenario: schema_of_csv honors a timestampFormat that carries no time
      # Sail parses the value with a date-time parser that requires a time component, so a
      # date-only format never matches and the field falls back to STRING.
      When query
        """
        SELECT schema_of_csv('15/01/2024', map('timestampFormat', 'dd/MM/yyyy')) AS result
        """
      Then query result
        | result                 |
        | STRUCT<_c0: TIMESTAMP> |

  Rule: A malformed record yields NULL instead of failing the query

    # Spark's default parse mode is PERMISSIVE: a field that does not fit the schema becomes NULL
    # and every field that parsed fine keeps its value. Sail reads no `mode` at all and aborts the
    # query on the first bad field, so it always behaves as FAILFAST and one bad row kills an
    # otherwise valid read.

    @sail-bug
    Scenario: from_csv nulls only the field that failed to parse
      When query
        """
        SELECT from_csv('1,garbage', 'a INT, b INT') AS result
        """
      Then query result
        | result    |
        | {1, NULL} |

    @sail-bug
    Scenario: from_csv keeps a good field that follows a bad one
      When query
        """
        SELECT from_csv('garbage,2', 'a INT, b INT') AS result
        """
      Then query result
        | result    |
        | {NULL, 2} |

    @sail-bug
    Scenario: from_csv nulls the fields missing from a short record
      When query
        """
        SELECT from_csv('1', 'a INT, b INT') AS result
        """
      Then query result
        | result    |
        | {1, NULL} |

    @sail-bug
    Scenario: from_csv ignores the extra fields of a long record
      When query
        """
        SELECT from_csv('1,2,3', 'a INT, b INT') AS result
        """
      Then query result
        | result |
        | {1, 2} |

    @sail-bug
    Scenario: from_csv fails the query under the FAILFAST mode
      When query
        """
        SELECT from_csv('1,garbage', 'a INT, b INT', map('mode', 'FAILFAST')) AS result
        """
      Then query error Malformed records are detected in record parsing

  Rule: The parse mode applies to every row of a column, not just to a literal

    # An option argument is a literal map, but the input is a column. These cover all three modes
    # over multiple rows with a DIFFERENT value per row, so a kernel that broadcast row 0 would
    # show up instead of silently agreeing.

    @sail-bug
    Scenario: from_csv nulls the bad fields of each row independently
      When query
        """
        SELECT from_csv(c, 'a INT, b INT') AS result
        FROM VALUES ('1,2'), ('3,garbage'), ('garbage,6'), ('7,8'), (NULL) AS t(c)
        """
      Then query result
        | result    |
        | {1, 2}    |
        | {3, NULL} |
        | {NULL, 6} |
        | {7, 8}    |
        | NULL      |

    @sail-bug
    Scenario: from_csv nulls the bad fields of each row under an explicit PERMISSIVE mode
      When query
        """
        SELECT from_csv(c, 'a INT, b INT', map('mode', 'PERMISSIVE')) AS result
        FROM VALUES ('1,2'), ('3,garbage'), ('9,10') AS t(c)
        """
      Then query result
        | result    |
        | {1, 2}    |
        | {3, NULL} |
        | {9, 10}   |

    Scenario: from_csv parses every row under the FAILFAST mode when all rows are well formed
      When query
        """
        SELECT from_csv(c, 'a INT, b INT', map('mode', 'FAILFAST')) AS result
        FROM VALUES ('1,2'), ('3,4'), ('5,6') AS t(c)
        """
      Then query result
        | result |
        | {1, 2} |
        | {3, 4} |
        | {5, 6} |

    @sail-bug
    Scenario: from_csv fails the whole column under the FAILFAST mode when one row is bad
      When query
        """
        SELECT from_csv(c, 'a INT, b INT', map('mode', 'FAILFAST')) AS result
        FROM VALUES ('1,2'), ('3,garbage'), ('5,6') AS t(c)
        """
      Then query error Malformed records are detected in record parsing

    Scenario: from_csv rejects the DROPMALFORMED mode over a column too
      When query
        """
        SELECT from_csv(c, 'a INT', map('mode', 'DROPMALFORMED')) AS result
        FROM VALUES ('1'), ('2') AS t(c)
        """
      Then query error doesn't support the DROPMALFORMED mode

    Scenario: from_csv applies a separator to every row of a column
      When query
        """
        SELECT from_csv(c, 'a INT, b INT', map('sep', '|')) AS result
        FROM VALUES ('1|2'), ('3|4'), ('5|6') AS t(c)
        """
      Then query result
        | result |
        | {1, 2} |
        | {3, 4} |
        | {5, 6} |

    Scenario: to_csv applies an option to every row of a column
      When query
        """
        SELECT to_csv(named_struct('a', c, 'b', 'x'), map('sep', ';', 'quoteAll', 'true')) AS result
        FROM VALUES (1), (2), (3) AS t(c)
        """
      Then query result
        | result  |
        | "1";"x" |
        | "2";"x" |
        | "3";"x" |

  Rule: A NULL option value is rejected

    # Spark rejects the call outright, even when the key is one it does not know, so this is not
    # per-option validation but a property of the options map itself.

    Scenario: from_csv rejects a NULL value for a known option
      When query
        """
        SELECT from_csv('1', 'a INT', map('header', CAST(NULL AS STRING))) AS result
        """
      Then query error Failed preparing of the function .from_csv. for call

    Scenario: from_csv rejects a NULL value for an unknown option
      When query
        """
        SELECT from_csv('1', 'a INT', map('unknownOption', CAST(NULL AS STRING))) AS result
        """
      Then query error Failed preparing of the function .from_csv. for call

    Scenario: to_csv rejects a NULL option value
      When query
        """
        SELECT to_csv(named_struct('a', 1), map('sep', CAST(NULL AS STRING))) AS result
        """
      Then query error Failed preparing of the function .to_csv. for call

    Scenario: schema_of_csv rejects a NULL option value
      When query
        """
        SELECT schema_of_csv('1', map('sep', CAST(NULL AS STRING))) AS result
        """
      Then query error Failed preparing of the function .schema_of_csv. for call

  Rule: A field is not trimmed before it is parsed

    @sail-bug
    Scenario: from_csv does not trim the whitespace around a field
      # Spark keeps the surrounding whitespace, so ` 1 ` is not the number 1 and the field is
      # malformed. Sail trims every token, which silently accepts it. Observing Spark's NULL
      # here needs PERMISSIVE, which is why this travels with the parse-mode work.
      When query
        """
        SELECT from_csv(' 1 , 2 ', 'a INT, b INT') AS result
        """
      Then query result
        | result       |
        | {NULL, NULL} |

  Rule: Single-character options are validated

    Scenario: from_csv rejects a multi-character quote
      When query
        """
        SELECT from_csv('1,2', 'a INT, b INT', map('quote', 'ab')) AS result
        """
      Then query error quote cannot be more than one character

    Scenario: to_csv rejects a multi-character quote with Spark's message
      # Sail already rejects this, but says "CSV option `quote` must be a single character".
      When query
        """
        SELECT to_csv(named_struct('a', 1), map('quote', 'ab')) AS result
        """
      Then query error quote cannot be more than one character

    # Spark measures the value with Java's `String.length`, which counts UTF-16 code units, not
    # characters: an emoji is two units and is rejected, but `ñ` is one and is accepted.
    Scenario: to_csv accepts a single non-ASCII quote character
      When query
        """
        SELECT to_csv(named_struct('a', 'x'), map('quote', 'ñ')) AS result
        """
      Then query result
        | result |
        | x      |

    Scenario: to_csv rejects a quote that is one character but two UTF-16 units
      When query
        """
        SELECT to_csv(named_struct('a', 'x'), map('quote', '😀')) AS result
        """
      Then query error quote cannot be more than one character

    Scenario: from_csv rejects a multi-character comment
      When query
        """
        SELECT from_csv('1,2', 'a INT, b INT', map('comment', 'ab')) AS result
        """
      Then query error comment cannot be more than one character

    Scenario: to_csv rejects a multi-character comment
      When query
        """
        SELECT to_csv(named_struct('a', 1), map('comment', 'ab')) AS result
        """
      Then query error comment cannot be more than one character

  Rule: An empty separator is rejected

    Scenario: from_csv rejects an empty sep
      When query
        """
        SELECT from_csv('1,2', 'a INT, b INT', map('sep', '')) AS result
        """
      Then query error Delimiter cannot be empty

    Scenario: to_csv rejects an empty sep
      When query
        """
        SELECT to_csv(named_struct('a', 1), map('sep', '')) AS result
        """
      Then query error Delimiter cannot be empty

    Scenario: to_csv rejects an empty lineSep
      When query
        """
        SELECT to_csv(named_struct('a', 1), map('lineSep', '')) AS result
        """
      Then query error 'lineSep' cannot be an empty string

  Rule: Numeric options are validated

    Scenario: from_csv rejects a non-numeric maxColumns
      When query
        """
        SELECT from_csv('1,2', 'a INT, b INT', map('maxColumns', 'garbage')) AS result
        """
      Then query error maxColumns should be an integer. Found garbage

    Scenario: from_csv rejects a non-numeric samplingRatio
      When query
        """
        SELECT from_csv('1,2', 'a INT, b INT', map('samplingRatio', 'garbage')) AS result
        """
      Then query error For input string: "garbage"

  Rule: Enum options are validated

    Scenario: from_csv rejects an unknown unescapedQuoteHandling
      When query
        """
        SELECT from_csv('1,2', 'a INT, b INT', map('unescapedQuoteHandling', 'garbage')) AS result
        """
      Then query error No enum constant

    Scenario: from_csv rejects the DROPMALFORMED mode
      # Only PERMISSIVE and FAILFAST are supported by the expression, unlike the CSV reader.
      When query
        """
        SELECT from_csv('1,2', 'a INT, b INT', map('mode', 'DROPMALFORMED')) AS result
        """
      Then query error doesn't support the DROPMALFORMED mode

    Scenario: from_csv accepts an unknown mode
      # Spark does not validate the mode: it warns and falls back to PERMISSIVE.
      When query
        """
        SELECT from_csv('1', 'a INT', map('mode', 'garbage')) AS result
        """
      Then query result
        | result |
        | {1}    |
