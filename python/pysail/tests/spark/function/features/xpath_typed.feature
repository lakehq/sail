@xpath
Feature: xpath_boolean/double/float/int/long/number/short/string extract typed values from XML

  Rule: xpath_boolean evaluates XPath to a boolean

    Scenario: xpath_boolean returns true when node exists
      When query
        """
        SELECT xpath_boolean('<a><b>1</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: xpath_boolean returns false when node does not exist
      When query
        """
        SELECT xpath_boolean('<a><b>1</b></a>', 'a/c') AS result
        """
      Then query result
        | result |
        | false  |

  Rule: xpath_double and xpath_number evaluate XPath to a double

    Scenario: xpath_double returns a sum as double
      When query
        """
        SELECT xpath_double('<a><b>1</b><b>2</b></a>', 'sum(a/b)') AS result
        """
      Then query result
        | result |
        | 3.0    |

    Scenario: xpath_number returns a sum as double
      When query
        """
        SELECT xpath_number('<a><b>1</b><b>2</b></a>', 'sum(a/b)') AS result
        """
      Then query result
        | result |
        | 3.0    |

    Scenario: xpath_double returns NaN for non-numeric value
      When query
        """
        SELECT xpath_double('<a><b>text</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

  Rule: xpath_float evaluates XPath to a float

    Scenario: xpath_float returns a sum as float
      When query
        """
        SELECT xpath_float('<a><b>1</b><b>2</b></a>', 'sum(a/b)') AS result
        """
      Then query result
        | result |
        | 3.0    |

    Scenario: xpath_float returns NaN for non-numeric value
      When query
        """
        SELECT xpath_float('<a><b>text</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

  Rule: xpath_int evaluates XPath to an integer

    Scenario: xpath_int returns a sum as integer
      When query
        """
        SELECT xpath_int('<a><b>1</b><b>2</b></a>', 'sum(a/b)') AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: xpath_int returns zero for non-numeric value
      When query
        """
        SELECT xpath_int('<a><b>text</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: xpath_long evaluates XPath to a long integer

    Scenario: xpath_long returns a sum as long
      When query
        """
        SELECT xpath_long('<a><b>1</b><b>2</b></a>', 'sum(a/b)') AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: xpath_long returns zero for non-numeric value
      When query
        """
        SELECT xpath_long('<a><b>text</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | 0      |

  Rule: xpath_short evaluates XPath to a short integer

    Scenario: xpath_short returns a sum as short
      When query
        """
        SELECT xpath_short('<a><b>1</b><b>2</b></a>', 'sum(a/b)') AS result
        """
      Then query result
        | result |
        | 3      |

    Scenario: xpath_short returns zero for non-numeric value
      When query
        """
        SELECT xpath_short('<a><b>text</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | 0      |

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

    # Both engines reject the document, with different wording: Spark says "Error loading
    # expression", Sail says "Invalid XML document". Accept either, but still require the error
    # to be about the document.
    Scenario: typed xpath fails on invalid XML
      When query
        """
        SELECT xpath_int('<a><b>1</b>', 'a/b') AS result
        """
      Then query error (?s).*(Invalid XML|Error loading expression).*

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

    Scenario: xpath_string on several element nodes takes the first
      When query
        """
        SELECT xpath_string('<a><b>b1</b><b>b2</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | b1     |

    Scenario: xpath_int on several element nodes takes the first
      When query
        """
        SELECT xpath_int('<a><b>7</b><b>8</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | 7      |

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

    Scenario: xpath_short wraps a value above the short range
      When query
        """
        SELECT xpath_short('<a><b>99999</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | -31073 |

    Scenario: xpath_short wraps at the exact upper boundary
      When query
        """
        SELECT xpath_short('<a><b>32768</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | -32768 |

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

    Scenario: xpath_double of INF is NaN
      When query
        """
        SELECT xpath_double('<a><b>INF</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: xpath_double of -INF is NaN
      When query
        """
        SELECT xpath_double('<a><b>-INF</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: xpath_double of scientific notation is NaN
      When query
        """
        SELECT xpath_double('<a><b>1.5e2</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: xpath_double of an exponent that overflows is NaN
      When query
        """
        SELECT xpath_double('<a><b>1e400</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: xpath_float of an exponent is NaN
      When query
        """
        SELECT xpath_float('<a><b>1e300</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: xpath_int of INF is zero
      When query
        """
        SELECT xpath_int('<a><b>INF</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: xpath_long of INF is zero
      When query
        """
        SELECT xpath_long('<a><b>INF</b></a>', 'a/b') AS result
        """
      Then query result
        | result |
        | 0      |

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
