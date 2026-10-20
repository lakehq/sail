Feature: xpath_boolean/double/float/int/long/number/short/string extract typed values from XML

  Rule: xpath_boolean evaluates XPath to a boolean

    Scenario Outline: Boolean: <case>
      When query
        """
        SELECT xpath_boolean('<a><b>1</b></a>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                 | path | result |
        | xpath_boolean returns true when node exists          | a/b  | true   |
        | xpath_boolean returns false when node does not exist | a/c  | false  |

  Rule: xpath_double and xpath_number evaluate XPath to a double

    Scenario Outline: Double: <case>
      When query
        """
        SELECT <fn>('<xml>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | fn           | xml                     | path     | result |
        | xpath_double returns a sum as double           | xpath_double | <a><b>1</b><b>2</b></a> | sum(a/b) | 3.0    |
        | xpath_number returns a sum as double           | xpath_number | <a><b>1</b><b>2</b></a> | sum(a/b) | 3.0    |
        | xpath_double returns NaN for non-numeric value | xpath_double | <a><b>text</b></a>      | a/b      | NaN    |

  Rule: xpath_float evaluates XPath to a float

    Scenario Outline: Float: <case>
      When query
        """
        SELECT xpath_float('<xml>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                          | xml                     | path     | result |
        | xpath_float returns a sum as float            | <a><b>1</b><b>2</b></a> | sum(a/b) | 3.0    |
        | xpath_float returns NaN for non-numeric value | <a><b>text</b></a>      | a/b      | NaN    |

  Rule: xpath_int evaluates XPath to an integer

    Scenario Outline: Int: <case>
      When query
        """
        SELECT xpath_int('<xml>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                         | xml                     | path     | result |
        | xpath_int returns a sum as integer           | <a><b>1</b><b>2</b></a> | sum(a/b) | 3      |
        | xpath_int returns zero for non-numeric value | <a><b>text</b></a>      | a/b      | 0      |

  Rule: xpath_long evaluates XPath to a long integer

    Scenario Outline: Long: <case>
      When query
        """
        SELECT xpath_long('<xml>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                          | xml                     | path     | result |
        | xpath_long returns a sum as long              | <a><b>1</b><b>2</b></a> | sum(a/b) | 3      |
        | xpath_long returns zero for non-numeric value | <a><b>text</b></a>      | a/b      | 0      |

  Rule: xpath_short evaluates XPath to a short integer

    Scenario Outline: Short: <case>
      When query
        """
        SELECT xpath_short('<xml>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                           | xml                     | path     | result |
        | xpath_short returns a sum as short             | <a><b>1</b><b>2</b></a> | sum(a/b) | 3      |
        | xpath_short returns zero for non-numeric value | <a><b>text</b></a>      | a/b      | 0      |

  Rule: xpath_string evaluates XPath to a string

    Scenario: xpath_string returns text content
      When query
        """
        SELECT xpath_string('<a><b>b</b><c>cc</c></a>', 'a/c') AS result
        """
      Then query result
        | result |
        | cc     |

    Scenario: xpath_string returns empty string when no match
      When query
        """
        SELECT xpath_string('<a><b>b</b></a>', 'a/c') AS result
        """
      Then query result
        | result |
        |        |

  Rule: Empty or null inputs return NULL

    Scenario: typed xpath returns NULL for empty xml
      When query
        """
        SELECT
          xpath_boolean('', 'a/b') AS bool_result,
          xpath_double('', 'a/b') AS double_result,
          xpath_int('', 'a/b') AS int_result,
          xpath_string('', 'a/b') AS string_result
        """
      Then query result
        | bool_result | double_result | int_result | string_result |
        | NULL        | NULL          | NULL       | NULL          |

    Scenario: typed xpath returns NULL for empty path
      When query
        """
        SELECT
          xpath_boolean('<a><b>1</b></a>', '') AS bool_result,
          xpath_double('<a><b>1</b></a>', '') AS double_result,
          xpath_int('<a><b>1</b></a>', '') AS int_result,
          xpath_string('<a><b>1</b></a>', '') AS string_result
        """
      Then query result
        | bool_result | double_result | int_result | string_result |
        | NULL        | NULL          | NULL       | NULL          |

    Scenario: typed xpath returns NULL for null xml or path
      When query
        """
        SELECT
          xpath_boolean(CAST(NULL AS STRING), 'a/b') AS null_xml,
          xpath_boolean('<a><b>1</b></a>', CAST(NULL AS STRING)) AS null_path
        """
      Then query result
        | null_xml | null_path |
        | NULL     | NULL      |

  Rule: Invalid XML or XPath fails

    # Spark wraps the parse failure as "(java.lang.RuntimeException) Error loading expression
    # 'a/b'"; Sail reports its own "Invalid XML" wording.
    @sail-bug
    Scenario: typed xpath fails on invalid XML
      When query
        """
        SELECT xpath_int('<a><b>1</b>', 'a/b') AS result
        """
      Then query error (?s).*Error loading expression

    Scenario: typed xpath fails on invalid XPath
      When query
        """
        SELECT xpath_int('<a><b>1</b></a>', '!!!') AS result
        """
      Then query error (?s).*Invalid XPath.*

  Rule: a path matching several nodes takes the first one

    Scenario: xpath_string on a single element node
      When query
        """
        SELECT xpath_string('<a><b>b1</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | b1     |

    @sail-bug
    Scenario: xpath_string on several element nodes takes the first
      When query
        """
        SELECT xpath_string('<a><b>b1</b><b>b2</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | b1     |

    @sail-bug
    Scenario: xpath_int on several element nodes takes the first
      When query
        """
        SELECT xpath_int('<a><b>7</b><b>8</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | 7      |

    @sail-bug
    Scenario: xpath_double on several element nodes takes the first
      When query
        """
        SELECT xpath_double('<a><b>1.5</b><b>2</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | 1.5    |

    Scenario: xpath_boolean on several element nodes tests the node-set
      When query
        """
        SELECT xpath_boolean('<a><b>1</b><b>2</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | true   |

  Rule: narrowing to a SHORT wraps, it does not saturate

    @sail-bug
    Scenario: xpath_short wraps a value above the short range
      When query
        """
        SELECT xpath_short('<a><b>99999</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | -31073 |

    @sail-bug
    Scenario: xpath_short wraps at the exact upper boundary
      When query
        """
        SELECT xpath_short('<a><b>32768</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | -32768 |

    @sail-bug
    Scenario: xpath_short wraps a value below the short range
      When query
        """
        SELECT xpath_short('<a><b>-40000</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | 25536  |

    Scenario: xpath_int saturates a value above the int range
      When query
        """
        SELECT xpath_int('<a><b>99999999999</b></a>', 'a/b') AS result
        """
      Then query result
        | result     |
        | 2147483647 |

  Rule: a number outside XPath 1.0's lexical space is NaN

    # Spark evaluates the path with XPath 1.0, whose number grammar is only [-]digits[.digits]:
    # no exponent, no INF. Anything else is NaN. Sail uses XPath 2.0 (xee), which accepts both.

    @sail-bug
    Scenario: xpath_double of INF is NaN
      When query
        """
        SELECT xpath_double('<a><b>INF</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
    Scenario: xpath_double of -INF is NaN
      When query
        """
        SELECT xpath_double('<a><b>-INF</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
    Scenario: xpath_double of scientific notation is NaN
      When query
        """
        SELECT xpath_double('<a><b>1.5e2</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
    Scenario: xpath_double of an exponent that overflows is NaN
      When query
        """
        SELECT xpath_double('<a><b>1e400</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
    Scenario: xpath_float of an exponent is NaN
      When query
        """
        SELECT xpath_float('<a><b>1e300</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
    Scenario: xpath_int of INF is zero
      When query
        """
        SELECT xpath_int('<a><b>INF</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | 0      |

    @sail-bug
    Scenario: xpath_long of INF is zero
      When query
        """
        SELECT xpath_long('<a><b>INF</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | 0      |

    @sail-bug
    Scenario: xpath_short of INF is zero
      When query
        """
        SELECT xpath_short('<a><b>INF</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: xpath_boolean of INF tests the node, not the number
      When query
        """
        SELECT xpath_boolean('<a><b>INF</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | true   |

  Rule: a default namespace does not hide the nodes

    # Spark parses the document WITHOUT namespace awareness, so an unprefixed path still matches a
    # node in a default namespace. Sail evaluates with XPath 2.0 (xee), which honours the namespace:
    # `a/b` matches nothing and the result comes back empty (or 0, or an empty array).
    #
    # This is not a laboratory case: any real-world document carrying an `xmlns` -- SOAP, RSS, SVG,
    # Atom, most industry schemas -- returns empty in Sail today.
    #
    # Deferred rather than patched, because none of the ways out is free:
    #   - Rewrite the path (`a/b` -> `*:a/*:b`, "any namespace" in XPath 2.0). Emulates Spark well,
    #     but it means parsing the XPath expression to rewrite only the name tests, leaving string
    #     literals and function names alone.
    #   - Strip the `xmlns` declarations from the document before parsing. Simple, but it breaks
    #     documents that use prefixes, and it changes what the user actually passed in.
    #   - Accept the divergence and document it.
    # It needs a decision, so it gets its own PR.

    # Sail returns an empty string.
    @sail-bug
    Scenario: xpath_string matches through a default namespace
      When query
        """
        SELECT xpath_string('<a xmlns="http://x"><b>v</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | v      |

    # Sail returns 0.
    @sail-bug
    Scenario: xpath_int matches through a default namespace
      When query
        """
        SELECT xpath_int('<a xmlns="http://x"><b>7</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | 7      |

  Rule: XPath 1.0 converts a non-node result to a number

    # The path need not evaluate to a node. XPath 1.0's `number()` also converts a boolean (true is
    # 1, false is 0) and a string (read by the 1.0 lexical rules). `xee` can only turn a numeric
    # atomic into a number, so these have to be converted by hand. All values captured on Spark JVM.

    Scenario: xpath_double of a boolean comparison is 1.0
      When query
        """
        SELECT xpath_double('<a><b>10</b></a>', 'a/b = 10') AS result
        """
      Then query result
        | result |
        | 1.0    |

    Scenario: xpath_number of a boolean comparison is 1.0
      When query
        """
        SELECT xpath_number('<a><b>10</b></a>', 'a/b = 10') AS result
        """
      Then query result
        | result |
        | 1.0    |

    Scenario: xpath_float of a boolean comparison is 1.0
      When query
        """
        SELECT xpath_float('<a><b>10</b></a>', 'a/b = 10') AS result
        """
      Then query result
        | result |
        | 1.0    |

    Scenario: xpath_double of true() is 1.0
      When query
        """
        SELECT xpath_double('<a><b>10</b></a>', 'true()') AS result
        """
      Then query result
        | result |
        | 1.0    |

    Scenario: xpath_int of a boolean comparison is 1
      When query
        """
        SELECT xpath_int('<a><b>10</b></a>', 'a/b = 10') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: xpath_long of a boolean comparison is 1
      When query
        """
        SELECT xpath_long('<a><b>10</b></a>', 'a/b = 10') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: xpath_short of a boolean comparison is 1
      When query
        """
        SELECT xpath_short('<a><b>10</b></a>', 'a/b = 10') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: xpath_double of string() reads the string as a number
      When query
        """
        SELECT xpath_double('<a><b>10</b></a>', 'string(a/b)') AS result
        """
      Then query result
        | result |
        | 10.0   |

    Scenario: xpath_double of a string literal reads it as a number
      When query
        """
        SELECT xpath_double('<a><b>10</b></a>', 'string(10)') AS result
        """
      Then query result
        | result |
        | 10.0   |

  Rule: Unicode whitespace around a number is not trimmed

    # XPath 1.0 whitespace is only #x20, #x9, #xD, #xA. A number wrapped in other Unicode whitespace
    # -- NBSP (U+00A0), EM SPACE (U+2003), NEL (U+0085) -- is therefore NaN in Spark. Rust's
    # `str::trim`, which strips all Unicode whitespace, would have accepted them. All values captured
    # on Spark JVM. The scenarios below embed the actual whitespace characters between `<b>` and the
    # digits, so they look like ordinary spaces but are not.

    @sail-bug
    Scenario: xpath_double does not trim a NBSP around a number
      When query
        """
        SELECT xpath_double('<a><b> 42 </b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
    Scenario: xpath_double does not trim an EM SPACE around a number
      When query
        """
        SELECT xpath_double('<a><b> 42 </b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
    Scenario: xpath_double does not trim a NEL around a number
      When query
        """
        SELECT xpath_double('<a><b>42</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
    Scenario: xpath_int does not trim a NBSP around a number
      When query
        """
        SELECT xpath_int('<a><b> 42 </b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: an explicit XPath function is evaluated with XPath 2.0 rules

    # Spark evaluates the WHOLE path with XPath 1.0, so an explicit `number()`/`sum()` also follows
    # the 1.0 number grammar (no exponent, no INF -> NaN) and takes the first node of a node-set.
    # Sail delegates the expression to `xee`, which evaluates it with XPath 2.0: it reads `1.5e2` and
    # `INF`, and it rejects a multi-node argument to `number()` with XPTY0004. Fixing this means
    # intercepting `xee`'s own `number()`/`sum()`, so it is deferred. The common case -- a bare path
    # like `a/b` over `1.5e2` -- is already NaN (see the rule above), only an explicit function is
    # affected. Values below are Spark's.

    # Sail returns 150.0.
    @sail-bug
    Scenario: xpath_double of an explicit number() over scientific notation is NaN
      When query
        """
        SELECT xpath_double('<a><b>1.5e2</b></a>', 'number(a/b)') AS result
        """
      Then query result
        | result |
        | NaN    |

    # Sail returns 150.0.
    @sail-bug
    Scenario: xpath_double of an explicit sum() over scientific notation is NaN
      When query
        """
        SELECT xpath_double('<a><b>1.5e2</b></a>', 'sum(a/b)') AS result
        """
      Then query result
        | result |
        | NaN    |

    # Sail returns Infinity.
    @sail-bug
    Scenario: xpath_double of an explicit number() over INF is NaN
      When query
        """
        SELECT xpath_double('<a><b>INF</b></a>', 'number(a/b)') AS result
        """
      Then query result
        | result |
        | NaN    |

    # Sail errors with XPTY0004 because xee's number() rejects a sequence of more than one item.
    @sail-bug
    Scenario: xpath_double of an explicit number() over several nodes takes the first
      When query
        """
        SELECT xpath_double('<a><b>1</b><b>2</b></a>', 'number(a/b)') AS result
        """
      Then query result
        | result |
        | 1.0    |

  Rule: a comma sequence in the path is rejected

    # Spark evaluates the path with XPath 1.0, which has no sequence constructor, so a comma is a
    # syntax error. Sail wraps the path as `(path)[1]`, which turns `1, 2` into the valid XPath 2.0
    # `(1, 2)[1]` and quietly returns the first item. Deferred: rejecting a top-level comma means
    # parsing the XPath, since a comma inside a function call (`concat(a, b)`) is legitimate.

    # Sail returns 1.0.
    Scenario: xpath_double of a comma sequence is an error
      When query
        """
        SELECT xpath_double('<a><b>10</b></a>', '1, 2') AS result
        """
      Then query error (?s).*Invalid XPath.*
