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

    Scenario: typed xpath fails on invalid XML
      When query
        """
        SELECT xpath_int('<a><b>1</b>', 'a/b') AS result
        """
      Then query error (?s).*Invalid XML.*

    Scenario: typed xpath fails on invalid XPath
      When query
        """
        SELECT xpath_int('<a><b>1</b></a>', '!!!') AS result
        """
      Then query error (?s).*Invalid XPath.*
