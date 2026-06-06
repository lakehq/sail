Feature: to_xml converts a struct value to an XML string

  Rule: Basic serialization

    Scenario: Convert a struct with two integer fields
      When query
        """
        SELECT
          xpath_string(to_xml(named_struct('a', 1, 'b', 2)), '/ROW/a') AS a,
          xpath_string(to_xml(named_struct('a', 1, 'b', 2)), '/ROW/b') AS b
        """
      Then query result
        | a | b |
        | 1 | 2 |

    Scenario: Convert a struct with mixed types
      When query
        """
        SELECT
          xpath_string(to_xml(named_struct('a', 1, 'b', 2.5, 'c', true)), '/ROW/a') AS a,
          xpath_string(to_xml(named_struct('a', 1, 'b', 2.5, 'c', true)), '/ROW/b') AS b,
          xpath_string(to_xml(named_struct('a', 1, 'b', 2.5, 'c', true)), '/ROW/c') AS c
        """
      Then query result
        | a | b   | c    |
        | 1 | 2.5 | true |

    Scenario: Convert a struct from a table column
      When query
        """
        SELECT
          xpath_string(to_xml(named_struct('a', value, 'b', value)), '/ROW/a') AS a,
          xpath_string(to_xml(named_struct('a', value, 'b', value)), '/ROW/b') AS b
        FROM VALUES (1), (2), (3) AS t(value)
        ORDER BY value
        """
      Then query result ordered
        | a | b |
        | 1 | 1 |
        | 2 | 2 |
        | 3 | 3 |

    Scenario: XML declaration is present by default
      When query
        """
        SELECT to_xml(named_struct('a', 1)) LIKE '<?xml%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Root tag defaults to ROW
      When query
        """
        SELECT to_xml(named_struct('a', 1)) LIKE '%<ROW>%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Single field struct
      When query
        """
        SELECT xpath_string(to_xml(named_struct('x', 42)), '/ROW/x') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: Empty struct produces self-closing ROW element
      When query
        """
        SELECT to_xml(named_struct('ts', CAST(NULL AS TIMESTAMP))) LIKE '%<ROW/>%' AS result
        """
      Then query result
        | result |
        | true   |

  Rule: NULL handling

    Scenario: NULL struct returns NULL
      When query
        """
        SELECT to_xml(CAST(NULL AS STRUCT<a:INT, b:INT>)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Struct with NULL field omits that field
      When query
        """
        SELECT
          xpath_string(to_xml(named_struct('a', 1, 'b', CAST(NULL AS INT))), '/ROW/a') AS a,
          xpath_string(to_xml(named_struct('a', 1, 'b', CAST(NULL AS INT))), '/ROW/b') AS b
        """
      Then query result
        | a | b    |
        | 1 | NULL |

    Scenario: Multiple consecutive NULL fields with default behavior omits all
      When query
        """
        SELECT
          xpath_string(to_xml(named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS INT), 'c', 3)), '/ROW/a') AS a,
          xpath_string(to_xml(named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS INT), 'c', 3)), '/ROW/b') AS b,
          xpath_string(to_xml(named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS INT), 'c', 3)), '/ROW/c') AS c
        """
      Then query result
        | a    | b    | c |
        | NULL | NULL | 3 |

    Scenario: Integer zero is not treated as NULL
      When query
        """
        SELECT xpath_string(to_xml(named_struct('count', 0)), '/ROW/count') AS result
        """
      Then query result
        | result |
        | 0      |

    Scenario: Empty string is not treated as NULL
      When query
        """
        SELECT to_xml(named_struct('text', '')) LIKE '%<text></text>%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: False boolean is not treated as NULL
      When query
        """
        SELECT xpath_string(to_xml(named_struct('flag', false)), '/ROW/flag') AS result
        """
      Then query result
        | result |
        | false  |

  Rule: Arrays

    Scenario: Primitive array becomes repeated elements
      When query
        """
        SELECT xpath(to_xml(named_struct('numbers', array(1, 2, 3))), '/ROW/numbers/item/text()') AS result
        """
      Then query result
        | result      |
        | [1, 2, 3]   |

    Scenario: Empty array produces self-closing ROW element
      When query
        """
        SELECT to_xml(named_struct('items', array())) LIKE '%<ROW/>%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Array with single element
      When query
        """
        SELECT xpath_string(to_xml(named_struct('items', array(1))), '/ROW/items/item') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: Array of structs repeats elements
      When query
        """
        SELECT xpath(to_xml(named_struct('items', array(named_struct('x', 1), named_struct('x', 2)))), '/ROW/items/x/text()') AS result
        """
      Then query result
        | result |
        | [1, 2] |

    Scenario: Array containing NULL elements skips nulls
      When query
        """
        SELECT xpath(to_xml(named_struct('items', array(1, CAST(NULL AS INT), 3))), '/ROW/items/item/text()') AS result
        """
      Then query result
        | result |
        | [1, 3] |

    Scenario: Array with boolean values preserves false
      When query
        """
        SELECT xpath(to_xml(named_struct('flags', array(true, false, true))), '/ROW/flags/item/text()') AS result
        """
      Then query result
        | result            |
        | [true, false, true] |

  Rule: Nested structures

    Scenario: Nested struct becomes nested XML elements
      When query
        """
        SELECT xpath_string(to_xml(named_struct('outer', named_struct('inner', 42))), '/ROW/outer/inner') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: Deeply nested structures (3 levels)
      When query
        """
        SELECT xpath_string(to_xml(named_struct('a', named_struct('b', named_struct('c', 1)))), '/ROW/a/b/c') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: Nested struct with NULL inner value produces self-closing ROW
      When query
        """
        SELECT to_xml(named_struct('outer', CAST(NULL AS STRUCT<inner:INT>))) LIKE '%<ROW/>%' AS result
        """
      Then query result
        | result |
        | true   |

  Rule: XML escaping

    Scenario: Less-than character is escaped
      When query
        """
        SELECT xpath_string(to_xml(named_struct('msg', 'a < b')), '/ROW/msg') AS result
        """
      Then query result
        | result |
        | a < b  |

    Scenario: Greater-than character is escaped
      When query
        """
        SELECT xpath_string(to_xml(named_struct('msg', 'a > b')), '/ROW/msg') AS result
        """
      Then query result
        | result |
        | a > b  |

    Scenario: Ampersand character is escaped
      When query
        """
        SELECT xpath_string(to_xml(named_struct('msg', 'a & b')), '/ROW/msg') AS result
        """
      Then query result
        | result |
        | a & b  |

    Scenario: Multiple special characters together
      When query
        """
        SELECT xpath_string(to_xml(named_struct('msg', 'a < b & c > d')), '/ROW/msg') AS result
        """
      Then query result
        | result        |
        | a < b & c > d |

    Scenario: Script tag content is safely escaped
      When query
        """
        SELECT xpath_string(to_xml(named_struct('msg', '<script>alert("xss")</script>')), '/ROW/msg') AS result
        """
      Then query result
        | result                       |
        | <script>alert("xss")</script> |

  Rule: Timestamp and date formatting

    Scenario: Timestamp field uses ISO 8601 UTC format
      When query
        """
        SELECT xpath_string(to_xml(named_struct('ts', to_timestamp('2026-06-06', 'yyyy-MM-dd'))), '/ROW/ts') AS result
        """
      Then query result
        | result                   |
        | 2026-06-06T00:00:00.000Z |

    Scenario: Pre-epoch timestamp is formatted correctly
      When query
        """
        SELECT xpath_string(to_xml(named_struct('ts', to_timestamp('1969-12-31', 'yyyy-MM-dd'))), '/ROW/ts') AS result
        """
      Then query result
        | result                   |
        | 1969-12-31T00:00:00.000Z |

    Scenario: NULL timestamp is omitted
      When query
        """
        SELECT to_xml(named_struct('ts', CAST(NULL AS TIMESTAMP))) LIKE '%<ROW/>%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Date field uses default yyyy-MM-dd format
      When query
        """
        SELECT xpath_string(to_xml(named_struct('d', DATE '2026-06-06')), '/ROW/d') AS result
        """
      Then query result
        | result     |
        | 2026-06-06 |

  Rule: Decimal and special values

    Scenario: Decimal field is formatted as fixed-point string
      When query
        """
        SELECT xpath_string(to_xml(named_struct('price', CAST(2.99 AS DECIMAL(5,2)))), '/ROW/price') AS result
        """
      Then query result
        | result |
        | 2.99   |

    Scenario: Negative decimal preserves sign
      When query
        """
        SELECT xpath_string(to_xml(named_struct('price', CAST(-0.83 AS DECIMAL(5,2)))), '/ROW/price') AS result
        """
      Then query result
        | result |
        | -0.83  |

    Scenario: Very large decimal value
      When query
        """
        SELECT xpath_string(to_xml(named_struct('big', CAST(999999.99 AS DECIMAL(10,2)))), '/ROW/big') AS result
        """
      Then query result
        | result    |
        | 999999.99 |

    Scenario: Very small decimal value
      When query
        """
        SELECT xpath_string(to_xml(named_struct('tiny', CAST(0.0001 AS DECIMAL(5,4)))), '/ROW/tiny') AS result
        """
      Then query result
        | result |
        | 0.0001 |

    Scenario: Floating-point NaN value
      When query
        """
        SELECT xpath_string(to_xml(named_struct('nan', CAST('NaN' AS DOUBLE))), '/ROW/nan') AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: Floating-point positive infinity
      When query
        """
        SELECT xpath_string(to_xml(named_struct('pos', CAST('Infinity' AS DOUBLE))), '/ROW/pos') AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: Floating-point negative infinity
      When query
        """
        SELECT xpath_string(to_xml(named_struct('neg', CAST('-Infinity' AS DOUBLE))), '/ROW/neg') AS result
        """
      Then query result
        | result    |
        | -Infinity |

  Rule: Complex structures

    Scenario: Multiple data types in single struct
      When query
        """
        SELECT
          xpath_string(to_xml(named_struct('int_val', 42, 'str_val', 'hello', 'bool_val', true, 'float_val', 2.71)), '/ROW/int_val') AS i,
          xpath_string(to_xml(named_struct('int_val', 42, 'str_val', 'hello', 'bool_val', true, 'float_val', 2.71)), '/ROW/str_val') AS s,
          xpath_string(to_xml(named_struct('int_val', 42, 'str_val', 'hello', 'bool_val', true, 'float_val', 2.71)), '/ROW/bool_val') AS b,
          xpath_string(to_xml(named_struct('int_val', 42, 'str_val', 'hello', 'bool_val', true, 'float_val', 2.71)), '/ROW/float_val') AS f
        """
      Then query result
        | i  | s     | b    | f    |
        | 42 | hello | true | 2.71 |

    Scenario: Boolean true value
      When query
        """
        SELECT xpath_boolean(to_xml(named_struct('flag', true)), '/ROW/flag') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Boolean false value
      When query
        """
        SELECT xpath_string(to_xml(named_struct('flag', false)), '/ROW/flag') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: Struct with array and nested struct
      When query
        """
        SELECT
          xpath(to_xml(named_struct('items', array(1, 2), 'nested', named_struct('value', 42))), '/ROW/items/item/text()') AS items,
          xpath_string(to_xml(named_struct('items', array(1, 2), 'nested', named_struct('value', 42))), '/ROW/nested/value') AS nested
        """
      Then query result
        | items  | nested |
        | [1, 2] | 42     |

  Rule: Options

    Scenario: Custom rowTag changes root element name
      When query
        """
        SELECT to_xml(named_struct('a', 1), map('rowTag', 'Person')) LIKE '%<Person>%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Suppressed declaration when empty string passed
      When query
        """
        SELECT to_xml(named_struct('a', 1), map('declaration', '')) LIKE '%<?xml%' AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: nullValue option renders null fields as configured string
      When query
        """
        SELECT to_xml(named_struct('a', CAST(NULL AS INT)), map('nullValue', 'N/A')) LIKE '%<a>N/A</a>%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Attribute prefix fields become XML attributes
      When query
        """
        SELECT to_xml(named_struct('_id', 99, 'name', 'Alice')) LIKE '%id="99"%' AS result
        """
      Then query result
        | result |
        | true   |
