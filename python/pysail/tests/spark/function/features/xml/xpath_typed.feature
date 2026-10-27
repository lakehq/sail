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
