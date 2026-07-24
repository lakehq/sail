@to_xml
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

    Scenario: Child elements are indented with four spaces
      When query
        """
        SELECT to_xml(named_struct('val', 1)) LIKE '%    <val>1</val>%' AS result
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
        | a | b |
        | 1 |   |

    Scenario: Multiple consecutive NULL fields with default behavior omits all
      When query
        """
        SELECT
          xpath_string(to_xml(named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS INT), 'c', 3)), '/ROW/a') AS a,
          xpath_string(to_xml(named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS INT), 'c', 3)), '/ROW/b') AS b,
          xpath_string(to_xml(named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS INT), 'c', 3)), '/ROW/c') AS c
        """
      Then query result
        | a | b | c |
        |   |   | 3 |

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

    Scenario: Empty array produces self-closing ROW element
      When query
        """
        SELECT to_xml(named_struct('items', array())) LIKE '%<ROW/>%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Array of structs repeats elements
      When query
        """
        SELECT xpath(to_xml(named_struct('items', array(named_struct('x', 1), named_struct('x', 2)))), '/ROW/items/x/text()') AS result
        """
      Then query result
        | result |
        | [1, 2] |

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

    Scenario: Custom timestampFormat applies to TIMESTAMP_LTZ
      When query
        """
        SELECT xpath_string(
          to_xml(named_struct('ts', to_timestamp('2026-06-06', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy')),
          '/ROW/ts'
        ) AS result
        """
      Then query result
        | result     |
        | 06/06/2026 |

    Scenario: Custom dateFormat applies to DATE fields
      When query
        """
        SELECT xpath_string(
          to_xml(named_struct('d', DATE '2026-06-06'), map('dateFormat', 'dd/MM/yyyy')),
          '/ROW/d'
        ) AS result
        """
      Then query result
        | result     |
        | 06/06/2026 |

    Scenario: TIMESTAMP_NTZ has no timezone offset
      When query
        """
        SELECT to_xml(named_struct('ts', CAST('2026-06-06 00:00:00' AS TIMESTAMP_NTZ))) LIKE '%2026-06-06T00:00:00.000%' AS has_time,
               to_xml(named_struct('ts', CAST('2026-06-06 00:00:00' AS TIMESTAMP_NTZ))) LIKE '%+%' AS has_plus,
               to_xml(named_struct('ts', CAST('2026-06-06 00:00:00' AS TIMESTAMP_NTZ))) LIKE '%Z%' AS has_z
        """
      Then query result
        | has_time | has_plus | has_z |
        | true     | false    | false |

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

  Rule: Options

    Scenario: Custom rowTag changes root element name
      When query
        """
        SELECT to_xml(named_struct('a', 1), map('rowTag', 'Person')) LIKE '%<Person>%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: valueTag field is written as inline text content on the parent element
      When query
        """
        SELECT to_xml(named_struct('_id', 1, '_VALUE', 'hello')) LIKE '%hello%' AS has_content,
               to_xml(named_struct('_id', 1, '_VALUE', 'hello')) LIKE '%<_VALUE>%' AS has_element
        """
      Then query result
        | has_content | has_element |
        | true        | false       |

    Scenario: Custom valueTag option controls which field becomes text content
      When query
        """
        SELECT to_xml(named_struct('_id', 7, 'body', 'world'), map('valueTag', 'body')) LIKE '%world%' AS has_content,
               to_xml(named_struct('_id', 7, 'body', 'world'), map('valueTag', 'body')) LIKE '%<body>%' AS has_element
        """
      Then query result
        | has_content | has_element |
        | true        | false       |

    Scenario: Option keys are case-insensitive
      When query
        """
        SELECT to_xml(named_struct('a', 1), map('ROWTAG', 'Item')) LIKE '%<Item>%' AS result
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

  Rule: Map fields

    Scenario: Map keys become child tag names
      When query
        """
        SELECT
          xpath_string(to_xml(named_struct('m', map('k1', 1, 'k2', 2))), '/ROW/m/k1') AS k1,
          xpath_string(to_xml(named_struct('m', map('k1', 1, 'k2', 2))), '/ROW/m/k2') AS k2
        """
      Then query result
        | k1 | k2 |
        | 1  | 2  |

    Scenario: Map with null value omits that entry by default
      When query
        """
        SELECT
          xpath_string(to_xml(named_struct('m', map('k1', 1, 'k2', CAST(NULL AS INT)))), '/ROW/m/k1') AS k1,
          xpath_string(to_xml(named_struct('m', map('k1', 1, 'k2', CAST(NULL AS INT)))), '/ROW/m/k2') AS k2
        """
      Then query result
        | k1 | k2 |
        | 1  |    |

    Scenario: Map with null value renders with nullValue option
      When query
        """
        SELECT to_xml(named_struct('m', map('k1', CAST(NULL AS INT))), map('nullValue', 'N/A')) LIKE '%<k1>N/A</k1>%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Map with string value
      When query
        """
        SELECT xpath_string(to_xml(named_struct('m', map('hello', 'world'))), '/ROW/m/hello') AS result
        """
      Then query result
        | result |
        | world  |

    Scenario: Map with struct value nests struct inside key tag
      When query
        """
        SELECT xpath_string(to_xml(named_struct('m', map('person', named_struct('age', 30)))), '/ROW/m/person/age') AS result
        """
      Then query result
        | result |
        | 30     |

    Scenario: Map with array value repeats key as sibling tags
      When query
        """
        SELECT xpath(to_xml(named_struct('m', map('nums', array(1, 2, 3)))), '/ROW/m/nums/text()') AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: Map field nested inside struct
      When query
        """
        SELECT xpath_string(to_xml(named_struct('outer', named_struct('m', map('k', 1)))), '/ROW/outer/m/k') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: Map inside array of structs expands per element
      When query
        """
        SELECT xpath(to_xml(named_struct('items', array(named_struct('m', map('k', 1)), named_struct('m', map('k', 2))))), '/ROW/items/m/k/text()') AS result
        """
      Then query result
        | result |
        | [1, 2] |

  # ============================================================
  # Bug-hunt (2026-07-10): JVM-validated exact-output scenarios.
  # Flatten with replace(..., '\n', '~'); '~' marks a newline so the
  # exact bytes (declaration, indentation, tag structure) are asserted.
  # ============================================================

  Rule: Serialization envelope

    Scenario: No XML declaration, no trailing newline, four-space indent
      When query
        """
        SELECT replace(to_xml(named_struct('a', 1, 'b', 2)), '\n', '~') AS r
        """
      Then query result
        | r                                    |
        | <ROW>~    <a>1</a>~    <b>2</b>~</ROW> |

    Scenario: Struct whose only field is NULL is self-closing
      When query
        """
        SELECT replace(to_xml(named_struct('a', CAST(NULL AS INT))), '\n', '~') AS r
        """
      Then query result
        | r       |
        | <ROW/>  |

    Scenario: declaration option is ignored for string output
      When query
        """
        SELECT replace(to_xml(named_struct('a', 1), map('declaration', 'version="1.0"')), '\n', '~') AS r
        """
      Then query result
        | r                        |
        | <ROW>~    <a>1</a>~</ROW> |

  Rule: Primitive arrays are repeated flat tags

    Scenario: Primitive array repeats the field tag without an item wrapper
      When query
        """
        SELECT replace(to_xml(named_struct('numbers', array(1, 2, 3))), '\n', '~') AS r
        """
      Then query result
        | r                                                                        |
        | <ROW>~    <numbers>1</numbers>~    <numbers>2</numbers>~    <numbers>3</numbers>~</ROW> |

    Scenario: Single-element primitive array
      When query
        """
        SELECT replace(to_xml(named_struct('items', array(1))), '\n', '~') AS r
        """
      Then query result
        | r                              |
        | <ROW>~    <items>1</items>~</ROW> |

    Scenario: NULL elements are dropped from a primitive array
      When query
        """
        SELECT replace(to_xml(named_struct('items', array(1, CAST(NULL AS INT), 3))), '\n', '~') AS r
        """
      Then query result
        | r                                                    |
        | <ROW>~    <items>1</items>~    <items>3</items>~</ROW> |

    Scenario: Nested arrays wrap inner elements in item, outer keeps field tag
      When query
        """
        SELECT replace(to_xml(named_struct('m', array(array(1, 2), array(3)))), '\n', '~') AS r
        """
      Then query result
        | r                                                                                                       |
        | <ROW>~    <m>~        <item>1</item>~        <item>2</item>~    </m>~    <m>~        <item>3</item>~    </m>~</ROW> |

    Scenario: Array of structs repeats the field tag around struct fields
      When query
        """
        SELECT replace(to_xml(named_struct('items', array(named_struct('x', 1), named_struct('x', 2)))), '\n', '~') AS r
        """
      Then query result
        | r                                                                                       |
        | <ROW>~    <items>~        <x>1</x>~    </items>~    <items>~        <x>2</x>~    </items>~</ROW> |

  Rule: Binary fields

    Scenario: Binary renders as bracketed uppercase hex
      When query
        """
        SELECT replace(to_xml(named_struct('b', X'48656C6C6F')), '\n', '~') AS r
        """
      Then query result
        | r                                     |
        | <ROW>~    <b>[48 65 6C 6C 6F]</b>~</ROW> |

    Scenario: Empty binary renders as empty brackets
      When query
        """
        SELECT replace(to_xml(named_struct('b', X'')), '\n', '~') AS r
        """
      Then query result
        | r                        |
        | <ROW>~    <b>[]</b>~</ROW> |

  Rule: XML escaping

    Scenario: Text escapes ampersand and less-than but leaves greater-than literal
      When query
        """
        SELECT replace(to_xml(named_struct('msg', 'a < b > c & d')), '\n', '~') AS r
        """
      Then query result
        | r                                            |
        | <ROW>~    <msg>a &lt; b > c &amp; d</msg>~</ROW> |

    Scenario: Attribute escapes ampersand, less-than and double-quote but not greater-than
      When query
        """
        SELECT replace(to_xml(named_struct('_a', 'x<y>z&"q')), '\n', '~') AS r
        """
      Then query result
        | r                                   |
        | <ROW a="x&lt;y>z&amp;&quot;q"/> |

  Rule: valueTag is inline text content

    Scenario: valueTag-only struct glues text with no surrounding whitespace
      When query
        """
        SELECT replace(to_xml(named_struct('_VALUE', 'hello')), '\n', '~') AS r
        """
      Then query result
        | r                  |
        | <ROW>hello</ROW>   |

    Scenario: valueTag with an attribute sibling
      When query
        """
        SELECT replace(to_xml(named_struct('_id', 1, '_VALUE', 'hello')), '\n', '~') AS r
        """
      Then query result
        | r                        |
        | <ROW id="1">hello</ROW>  |

    Scenario: valueTag text precedes element siblings inline
      When query
        """
        SELECT replace(to_xml(named_struct('_VALUE', 'hi', 'child', 5)), '\n', '~') AS r
        """
      Then query result
        | r                                     |
        | <ROW>hi~    <child>5</child>~</ROW>    |

  Rule: Maps

    Scenario: Map keys become child tags
      When query
        """
        SELECT replace(to_xml(named_struct('m', map('k1', 1, 'k2', 2))), '\n', '~') AS r
        """
      Then query result
        | r                                                              |
        | <ROW>~    <m>~        <k1>1</k1>~        <k2>2</k2>~    </m>~</ROW> |

    Scenario: Map with array value repeats the key tag
      When query
        """
        SELECT replace(to_xml(named_struct('m', map('nums', array(1, 2, 3)))), '\n', '~') AS r
        """
      Then query result
        | r                                                                                    |
        | <ROW>~    <m>~        <nums>1</nums>~        <nums>2</nums>~        <nums>3</nums>~    </m>~</ROW> |

  Rule: NULL handling and options

    Scenario: NULL struct input returns NULL exact
      When query
        """
        SELECT to_xml(CAST(NULL AS STRUCT<a:INT, b:INT>)) AS r
        """
      Then query result
        | r    |
        | NULL |

    Scenario: NULL field is omitted by default
      When query
        """
        SELECT replace(to_xml(named_struct('a', 1, 'b', CAST(NULL AS INT))), '\n', '~') AS r
        """
      Then query result
        | r                        |
        | <ROW>~    <a>1</a>~</ROW> |

    Scenario: Empty string field is an open-close pair, not self-closing
      When query
        """
        SELECT replace(to_xml(named_struct('text', '')), '\n', '~') AS r
        """
      Then query result
        | r                                |
        | <ROW>~    <text></text>~</ROW>   |

    Scenario: nullValue option renders NULL fields with the configured string
      When query
        """
        SELECT replace(to_xml(named_struct('a', CAST(NULL AS INT)), map('nullValue', 'NA')), '\n', '~') AS r
        """
      Then query result
        | r                          |
        | <ROW>~    <a>NA</a>~</ROW>  |

  Rule: Temporal and decimal formatting

    Scenario: Date uses default yyyy-MM-dd format
      When query
        """
        SELECT replace(to_xml(named_struct('d', DATE '2026-06-06')), '\n', '~') AS r
        """
      Then query result
        | r                                |
        | <ROW>~    <d>2026-06-06</d>~</ROW> |

    Scenario: Decimal keeps trailing zeros to scale
      When query
        """
        SELECT replace(to_xml(named_struct('price', CAST(2.99 AS DECIMAL(5,2)))), '\n', '~') AS r
        """
      Then query result
        | r                                  |
        | <ROW>~    <price>2.99</price>~</ROW> |

    Scenario: TIMESTAMP_NTZ uses ISO 8601 with millisecond precision and no offset
      When query
        """
        SELECT replace(to_xml(named_struct('ts', CAST('2026-06-06 12:00:00' AS TIMESTAMP_NTZ))), '\n', '~') AS r
        """
      Then query result
        | r                                                  |
        | <ROW>~    <ts>2026-06-06T12:00:00.000</ts>~</ROW>   |

  Rule: Special float values

    Scenario: NaN renders literally
      When query
        """
        SELECT replace(to_xml(named_struct('d', CAST('NaN' AS DOUBLE))), '\n', '~') AS r
        """
      Then query result
        | r                        |
        | <ROW>~    <d>NaN</d>~</ROW> |

    Scenario: Positive infinity renders as Infinity
      When query
        """
        SELECT replace(to_xml(named_struct('d', CAST('Infinity' AS DOUBLE))), '\n', '~') AS r
        """
      Then query result
        | r                             |
        | <ROW>~    <d>Infinity</d>~</ROW> |

  Rule: Double and float use Java toString formatting

    Scenario: Whole double keeps a decimal point
      When query
        """
        SELECT replace(to_xml(named_struct('d', CAST(1.0 AS DOUBLE))), '\n', '~') AS r
        """
      Then query result
        | r                        |
        | <ROW>~    <d>1.0</d>~</ROW> |

    Scenario: Large double uses scientific notation
      When query
        """
        SELECT replace(to_xml(named_struct('d', CAST(1e20 AS DOUBLE))), '\n', '~') AS r
        """
      Then query result
        | r                            |
        | <ROW>~    <d>1.0E20</d>~</ROW> |

    Scenario: Small double uses scientific notation
      When query
        """
        SELECT replace(to_xml(named_struct('d', CAST(1e-7 AS DOUBLE))), '\n', '~') AS r
        """
      Then query result
        | r                             |
        | <ROW>~    <d>1.0E-7</d>~</ROW>  |

    Scenario: Medium whole double keeps a decimal point
      When query
        """
        SELECT replace(to_xml(named_struct('d', CAST(123456.0 AS DOUBLE))), '\n', '~') AS r
        """
      Then query result
        | r                             |
        | <ROW>~    <d>123456.0</d>~</ROW> |

    Scenario: Negative zero double normalizes to positive zero
      When query
        """
        SELECT replace(to_xml(named_struct('d', CAST(-0.0 AS DOUBLE))), '\n', '~') AS r
        """
      Then query result
        | r                        |
        | <ROW>~    <d>0.0</d>~</ROW> |

    Scenario: Whole float keeps a decimal point
      When query
        """
        SELECT replace(to_xml(named_struct('f', CAST(1.0 AS FLOAT))), '\n', '~') AS r
        """
      Then query result
        | r                        |
        | <ROW>~    <f>1.0</f>~</ROW> |

    Scenario: Double array repeats the field tag with formatted values
      When query
        """
        SELECT replace(to_xml(named_struct('v', array(CAST(1.0 AS DOUBLE), CAST(2.5 AS DOUBLE)))), '\n', '~') AS r
        """
      Then query result
        | r                                    |
        | <ROW>~    <v>1.0</v>~    <v>2.5</v>~</ROW> |

  Rule: Array element naming option

    Scenario: Custom arrayElementName names the inner elements of nested arrays
      When query
        """
        SELECT replace(to_xml(named_struct('m', array(array(10, 20))), map('arrayElementName', 'val')), '\n', '~') AS r
        """
      Then query result
        | r                                                                    |
        | <ROW>~    <m>~        <val>10</val>~        <val>20</val>~    </m>~</ROW> |

