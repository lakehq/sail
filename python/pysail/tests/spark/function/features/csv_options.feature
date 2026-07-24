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

    Scenario: schema_of_csv gives sep precedence over delimiter
      # sep wins over delimiter, matching from_csv/to_csv (and Spark's read order).
      When query
        """
        SELECT schema_of_csv('1|2', map('sep', '|', 'delimiter', ';')) AS result
        """
      Then query result
        | result                     |
        | STRUCT<_c0: INT, _c1: INT> |

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

  Rule: An option value is validated lazily, per Spark's null propagation

    # Spark builds the parser inside the non-null evaluation path, so a bad option VALUE is not
    # seen when every input row is NULL: it returns NULL rather than erroring. A structurally
    # invalid map (a NULL key or value) stays eager — see the next Rule.

    Scenario: from_csv returns NULL for a NULL input even with a bad option
      When query
        """
        SELECT from_csv(CAST(NULL AS STRING), 'a INT', map('header', 'garbage')) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: to_csv returns NULL for a NULL input even with a bad option
      When query
        """
        SELECT to_csv(CAST(NULL AS STRUCT<a: INT>), map('header', 'garbage')) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: from_csv does not validate options when every row is NULL
      When query
        """
        SELECT from_csv(c, 'a INT', map('header', 'garbage')) AS result
        FROM VALUES (CAST(NULL AS STRING)), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |

    Scenario: from_csv still validates the option when a non-null row is present
      When query
        """
        SELECT from_csv(c, 'a INT', map('header', 'garbage')) AS result
        FROM VALUES ('1'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query error header flag can be true or false

  Rule: Options inherited from the file-source options are validated too

    # These are not in CSVOptions itself but in its parent, and Spark validates them eagerly all
    # the same. `preferDate` and `columnPruning` go through getBool (naming the option); the rest
    # through Scala's toBoolean / toInt (quoting the value).

    Scenario Outline: from_csv rejects a non-boolean <option>
      When query
        """
        SELECT from_csv('1', 'a INT', map('<option>', 'garbage')) AS result
        """
      Then query error <option> flag can be true or false

      Examples:
        | option        |
        | preferDate    |
        | columnPruning |

    Scenario Outline: from_csv rejects a non-boolean <option> quoting the value
      When query
        """
        SELECT from_csv('1', 'a INT', map('<option>', 'garbage')) AS result
        """
      Then query error For input string: "garbage"

      Examples:
        | option                        |
        | enableDateTimeParsingFallback |
        | ignoreCorruptFiles            |
        | ignoreMissingFiles            |

    Scenario: from_csv rejects a non-integer inputBufferSize
      When query
        """
        SELECT from_csv('1', 'a INT', map('inputBufferSize', 'garbage')) AS result
        """
      Then query error For input string: "garbage"

    Scenario: from_csv accepts a valid preferDate
      When query
        """
        SELECT from_csv('1', 'a INT', map('preferDate', 'true')) AS result
        """
      Then query result
        | result |
        | {1}    |

    @sail-bug
    Scenario: from_csv skips preferDate validation under the legacy time-parser policy
      # Spark reads preferDate only when the parser policy is CORRECTED
      # (`if (legacy) false else getBool(PREFER_DATE)`), so under LEGACY a bad value is never read
      # nor validated. Sail does not plumb the policy, so it validates it regardless.
      Given config spark.sql.legacy.timeParserPolicy = LEGACY
      When query
        """
        SELECT from_csv('1', 'a INT', map('preferDate', 'garbage')) AS result
        """
      Then query result
        | result |
        | {1}    |

  Rule: Boolean folding follows the option's parser

    # `multiLine` reads through Scala toBoolean, which upper-case-folds Unicode: `ſ` (U+017F)
    # upper-cases to `S`, so `falſe` is accepted. `header` reads through getBool, which lower-cases
    # with Locale.ROOT — `ſ` does not lower-case to `s`, so `falſe` is rejected.

    Scenario: from_csv accepts a Unicode-folded multiLine value
      When query
        """
        SELECT from_csv('1', 'a INT', map('multiLine', 'falſe')) AS result
        """
      Then query result
        | result |
        | {1}    |

    Scenario: from_csv rejects the same Unicode value for a getBool option
      When query
        """
        SELECT from_csv('1', 'a INT', map('header', 'falſe')) AS result
        """
      Then query error header flag can be true or false

  Rule: The writer rejects a lineSep over two UTF-16 code units

    # Spark measures the length in UTF-16 code units, not characters: `ab` is two units and is
    # accepted, while `abc` is three and is rejected.

    Scenario: to_csv accepts a two-unit lineSep
      When query
        """
        SELECT to_csv(named_struct('a', 1), map('lineSep', 'ab')) AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: to_csv rejects a three-character lineSep
      When query
        """
        SELECT to_csv(named_struct('a', 1), map('lineSep', 'abc')) AS result
        """
      Then query error 'lineSep' can contain only 1 character

  # ---------------------------------------------------------------------------
  # Deferred findings from the #2255 review. Each passes against Spark and fails
  # against Sail, so they are tagged @sail-bug and tracked until fixed in their
  # own PR. They are NOT addressed here — see the PR description for scope.
  # ---------------------------------------------------------------------------

  Rule: Duplicate/aliased option keys follow Spark's effective-map precedence

    # Spark collapses the options into a case-insensitive map before reading, so the LAST
    # case-variant wins and a shadowed invalid value is never seen. Sail validates every raw entry
    # and reads the first match, so it rejects the shadowed value or picks the wrong one.

    Scenario: from_csv uses the last case-variant of a duplicated key
      When query
        """
        SELECT from_csv('1', 'a INT', map('header', 'garbage', 'HEADER', 'true')) AS result
        """
      Then query result
        | result |
        | {1}    |

    Scenario: to_csv uses the last case-variant of a duplicated key
      When query
        """
        SELECT to_csv(named_struct('a', 'x'), map('quoteAll', 'false', 'QUOTEALL', 'true')) AS result
        """
      Then query result
        | result |
        | "x"    |

    Scenario: to_csv gives sep precedence over a shadowed invalid delimiter alias
      When query
        """
        SELECT to_csv(named_struct('a', 1, 'b', 'x'), map('sep', '|', 'delimiter', '')) AS result
        """
      Then query result
        | result |
        | 1\|x   |

  Rule: The separator follows Spark's escape grammar (F4, deferred)

    # Spark runs the separator through CSVExprUtils.toDelimiterStr: `\q` is an unsupported special
    # character and is rejected, while `\t` means an actual tab. Sail keeps the raw string, so it
    # accepts `\q` and treats `\t` as two literal characters.

    @sail-bug
    Scenario: to_csv rejects an unsupported special character in the separator
      When query
        """
        SELECT to_csv(named_struct('a', 1, 'b', 'x'), map('sep', '\\q')) AS result
        """
      Then query error UNSUPPORTED_SPECIAL_CHARACTER

    @sail-bug
    Scenario: to_csv unescapes a tab separator
      # Asserted through the length: Spark turns `\t` into one real tab, so `1<tab>x` is 3 chars;
      # Sail keeps the two literal characters, so `1\tx` is 4. A raw tab cannot go in a table cell.
      When query
        """
        SELECT length(to_csv(named_struct('a', 1, 'b', 'x'), map('sep', '\\t'))) AS result
        """
      Then query result
        | result |
        | 3      |

  Rule: Numeric options follow Java's parsing (F5, deferred)

    # Java's `Integer.parseInt` goes through `Character.digit`, which accepts any Unicode Nd digit
    # (`٥` is Arabic-Indic five, `١٢` is twelve). Rust's `parse::<i32>()` is ASCII-only, so Sail
    # rejects a value Spark reads fine. `maxCharsPerColumn` behaves identically.

    @sail-only
    Scenario: from_csv accepts a negative maxColumns
      # Deliberate divergence, NOT a @sail-bug — do not "fix" this by matching Spark.
      #
      #   Spark: throws a raw `java.lang.NegativeArraySizeException: -5` from univocity, which
      #          allocates the column array with that size. That is a crash, not a validation:
      #          there is no error class and no message designed for a user.
      #   Sail:  accepts it and ignores the option (Sail does not implement `maxColumns`), so the
      #          row parses as if the option were absent.
      #
      # Matching Spark here would mean replicating a crash, so the scenario is `@sail-only`
      # (skipped on the JVM) rather than `@sail-bug` (which would mean "fix this").
      # For contrast, `maxCharsPerColumn=-1` IS valid in Spark, where a negative means "no limit".
      When query
        """
        SELECT from_csv('1', 'a INT', map('maxColumns', '-5')) AS result
        """
      Then query result
        | result |
        | {1}    |

    @sail-bug
    Scenario: from_csv accepts a Unicode decimal digit in an integer option
      When query
        """
        SELECT from_csv('1', 'a INT', map('maxColumns', '٥')) AS result
        """
      Then query result
        | result |
        | {1}    |

  Rule: samplingRatio uses Java's float grammar (F5, deferred)

    # Spark parses it with Double.parseDouble, which accepts `1d` and rejects `inf`. Sail uses
    # Rust's f64 parser, which does the opposite.

    @sail-bug
    Scenario: from_csv accepts a Java-style float samplingRatio
      When query
        """
        SELECT from_csv('1', 'a INT', map('samplingRatio', '1d')) AS result
        """
      Then query result
        | result |
        | {1}    |

    @sail-bug
    Scenario: from_csv rejects the Rust-only float value inf
      When query
        """
        SELECT from_csv('1', 'a INT', map('samplingRatio', 'inf')) AS result
        """
      Then query error For input string: "inf"

  Rule: The options map must be a foldable map<string,string> (F7, deferred)

    # Spark rejects a non-literal or non-string options map during analysis. Sail reads row zero of
    # whatever map it is given, so it accepts these instead of rejecting them.

    @sail-bug
    Scenario: from_csv rejects a non-literal options map
      When query
        """
        SELECT from_csv('1', 'a INT', map_from_arrays(array('header'), array('garbage'))) AS result
        """
      Then query error NON_MAP_FUNCTION

    @sail-bug
    Scenario: from_csv rejects a non-string options map
      When query
        """
        SELECT from_csv('1', 'a INT', map('header', 1)) AS result
        """
      Then query error NON_STRING_TYPE

  Rule: String options with a validated domain are checked (F1 remainder, deferred)

    # Spark validates the charset, time zone, and codec names eagerly in the CSVOptions
    # constructor, so a non-null input makes them throw. Sail accepts any string. These are the
    # part of the missing-option finding that needs domain validation rather than a bool/int parse,
    # so they are deferred with the effective-map work.

    @sail-bug
    Scenario: from_csv rejects an unknown encoding
      When query
        """
        SELECT from_csv('1', 'a INT', map('encoding', 'utf-99')) AS result
        """
      Then query error INVALID_PARAMETER_VALUE.CHARSET

    @sail-bug
    Scenario: from_csv rejects an unknown timeZone
      When query
        """
        SELECT from_csv('1', 'a INT', map('timeZone', 'garbage')) AS result
        """
      Then query error INVALID_TIMEZONE

    @sail-bug
    Scenario: from_csv rejects an unknown compression codec
      When query
        """
        SELECT from_csv('1', 'a INT', map('compression', 'garbage')) AS result
        """
      Then query error CODEC_NOT_AVAILABLE

    Scenario: from_csv skips charset validation when the input is NULL
      # The domain check is lazy too: a NULL input never reaches the parser, so a bad charset is
      # not seen. This must keep passing even once the divergences above are fixed.
      When query
        """
        SELECT from_csv(CAST(NULL AS STRING), 'a INT', map('encoding', 'utf-99')) AS result
        """
      Then query result
        | result |
        | NULL   |
