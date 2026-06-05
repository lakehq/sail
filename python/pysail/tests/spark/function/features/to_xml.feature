Feature: to_xml converts a struct value to an XML string

  Rule: Basic serialization

    Scenario: Convert a struct with two integer fields
      When query
        """
        SELECT to_xml(named_struct('a', 1, 'b', 2))
        """
      Then query result
        | to_xml(named_struct(a, 1, b, 2)) |
        | <ROW>\n    <a>1</a>\n    <b>2</b>\n</ROW> |

    Scenario: Convert a struct with mixed types
      When query
        """
        SELECT to_xml(named_struct('a', 1, 'b', 2.5, 'c', true))
        """
      Then query result
        | to_xml(named_struct(a, 1, b, 2.5, c, true)) |
        | <ROW>\n    <a>1</a>\n    <b>2.5</b>\n    <c>true</c>\n</ROW> |

    Scenario: Convert a struct from a table column
      When query
        """
        SELECT to_xml(named_struct('a', value, 'b', value))
        FROM VALUES (1), (2), (3) AS t(value)
        ORDER BY value
        """
      Then query result
        | to_xml(named_struct(a, value, b, value)) |
        | <ROW>\n    <a>1</a>\n    <b>1</b>\n</ROW> |
        | <ROW>\n    <a>2</a>\n    <b>2</b>\n</ROW> |
        | <ROW>\n    <a>3</a>\n    <b>3</b>\n</ROW> |

    Scenario: Empty struct produces self-closing ROW element
      When query
        """
        SELECT to_xml(named_struct())
        """
      Then query result
        | to_xml(named_struct()) |
        | <ROW/> |

    Scenario: Single field struct
      When query
        """
        SELECT to_xml(named_struct('x', 42))
        """
      Then query result
        | to_xml(named_struct(x, 42)) |
        | <ROW>\n    <x>42</x>\n</ROW> |

  Rule: NULL handling

    Scenario: NULL struct returns NULL
      When query
        """
        SELECT to_xml(CAST(NULL AS STRUCT<a:INT, b:INT>))
        """
      Then query result
        | to_xml(CAST(NULL AS STRUCT<A:INT,B:INT>)) |
        | NULL |

    Scenario: Struct with NULL field omits that field
      When query
        """
        SELECT to_xml(named_struct('a', 1, 'b', CAST(NULL AS INT)))
        """
      Then query result
        | to_xml(named_struct(a, 1, b, CAST(NULL AS INT))) |
        | <ROW>\n    <a>1</a>\n</ROW> |

    Scenario: Multiple consecutive NULL fields with default behavior omits all
      When query
        """
        SELECT to_xml(named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS INT), 'c', 3))
        """
      Then query result
        | to_xml(named_struct(a, CAST(NULL AS INT), b, CAST(NULL AS INT), c, 3)) |
        | <ROW>\n    <c>3</c>\n</ROW> |

    Scenario: Integer zero is not treated as NULL
      When query
        """
        SELECT to_xml(named_struct('count', 0))
        """
      Then query result
        | to_xml(named_struct(count, 0)) |
        | <ROW>\n    <count>0</count>\n</ROW> |

    Scenario: Empty string is not treated as NULL
      When query
        """
        SELECT to_xml(named_struct('text', ''))
        """
      Then query result
        | to_xml(named_struct(text, '')) |
        | <ROW>\n    <text></text>\n</ROW> |

    Scenario: False boolean is not treated as NULL
      When query
        """
        SELECT to_xml(named_struct('flag', false))
        """
      Then query result
        | to_xml(named_struct(flag, false)) |
        | <ROW>\n    <flag>false</flag>\n</ROW> |

  Rule: Arrays

    Scenario: Primitive array becomes repeated elements
      When query
        """
        SELECT to_xml(named_struct('numbers', array(1,2,3)))
        """
      Then query result
        | to_xml(named_struct(numbers, array(1,2,3))) |
        | <ROW>\n    <numbers>1</numbers>\n    <numbers>2</numbers>\n    <numbers>3</numbers>\n</ROW> |

    Scenario: Empty array produces no elements
      When query
        """
        SELECT to_xml(named_struct('items', array()))
        """
      Then query result
        | to_xml(named_struct(items, array())) |
        | <ROW/> |

    Scenario: Array with single element
      When query
        """
        SELECT to_xml(named_struct('items', array(1)))
        """
      Then query result
        | to_xml(named_struct(items, array(1))) |
        | <ROW>\n    <items>1</items>\n</ROW> |

    Scenario: Array of structs repeats elements
      When query
        """
        SELECT to_xml(named_struct('items', array(named_struct('x',1), named_struct('x',2))))
        """
      Then query result
        | to_xml(named_struct(items, array(named_struct(x,1), named_struct(x,2)))) |
        | <ROW>\n    <items>\n        <x>1</x>\n    </items>\n    <items>\n        <x>2</x>\n    </items>\n</ROW> |

    Scenario: Array containing NULL elements
      When query
        """
        SELECT to_xml(named_struct('items', array(1, CAST(NULL AS INT), 3)))
        """
      Then query result
        | to_xml(named_struct(items, array(1, CAST(NULL AS INT), 3))) |
        | <ROW>\n    <items>1</items>\n    <items>3</items>\n</ROW> |

    Scenario: Array with boolean values preserves false
      When query
        """
        SELECT to_xml(named_struct('flags', array(true, false, true)))
        """
      Then query result
        | to_xml(named_struct(flags, array(true, false, true))) |
        | <ROW>\n    <flags>true</flags>\n    <flags>false</flags>\n    <flags>true</flags>\n</ROW> |

  Rule: Nested structures

    Scenario: Nested struct becomes nested XML elements
      When query
        """
        SELECT to_xml(named_struct('outer', named_struct('inner', 42)))
        """
      Then query result
        | to_xml(named_struct(outer, named_struct(inner, 42))) |
        | <ROW>\n    <outer>\n        <inner>42</inner>\n    </outer>\n</ROW> |

    Scenario: Deeply nested structures (3 levels)
      When query
        """
        SELECT to_xml(named_struct('a', named_struct('b', named_struct('c', 1))))
        """
      Then query result
        | to_xml(named_struct(a, named_struct(b, named_struct(c, 1)))) |
        | <ROW>\n    <a>\n        <b>\n            <c>1</c>\n        </b>\n    </a>\n</ROW> |

    Scenario: Nested struct with NULL inner value
      When query
        """
        SELECT to_xml(named_struct('outer', CAST(NULL AS STRUCT<inner:INT>)))
        """
      Then query result
        | to_xml(named_struct(outer, CAST(NULL AS STRUCT<INNER:INT>))) |
        | <ROW/> |

  Rule: XML escaping

    Scenario: Less-than character is escaped
      When query
        """
        SELECT to_xml(named_struct('msg', 'a < b'))
        """
      Then query result
        | to_xml(named_struct(msg, a < b)) |
        | <ROW>\n    <msg>a &lt; b</msg>\n</ROW> |

    Scenario: Greater-than character is escaped
      When query
        """
        SELECT to_xml(named_struct('msg', 'a > b'))
        """
      Then query result
        | to_xml(named_struct(msg, a > b)) |
        | <ROW>\n    <msg>a &gt; b</msg>\n</ROW> |

    Scenario: Ampersand character is escaped
      When query
        """
        SELECT to_xml(named_struct('msg', 'a & b'))
        """
      Then query result
        | to_xml(named_struct(msg, a & b)) |
        | <ROW>\n    <msg>a &amp; b</msg>\n</ROW> |

    Scenario: Multiple special characters together
      When query
        """
        SELECT to_xml(named_struct('msg', 'a < b & c > d'))
        """
      Then query result
        | to_xml(named_struct(msg, a < b & c > d)) |
        | <ROW>\n    <msg>a &lt; b &amp; c &gt; d</msg>\n</ROW> |

    Scenario: XSS prevention - script tag escaping
      When query
        """
        SELECT to_xml(named_struct('msg', '<script>alert("xss")</script>'))
        """
      Then query result
        | to_xml(named_struct(msg, <script>alert("xss")</script>)) |
        | <ROW>\n    <msg>&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;</msg>\n</ROW> |

  Rule: Timestamp and date formatting

    Scenario: Timestamp field uses ISO 8601 UTC format
      When query
        """
        SELECT to_xml(named_struct('ts', to_timestamp('2015-08-26', 'yyyy-MM-dd')))
        """
      Then query result
        | to_xml(named_struct(ts, to_timestamp(2015-08-26, yyyy-MM-dd))) |
        | <ROW>\n    <ts>2015-08-26T00:00:00.000Z</ts>\n</ROW> |

    Scenario: Pre-epoch timestamp is formatted correctly
      When query
        """
        SELECT to_xml(named_struct('ts', to_timestamp('1969-12-31', 'yyyy-MM-dd')))
        """
      Then query result
        | to_xml(named_struct(ts, to_timestamp(1969-12-31, yyyy-MM-dd))) |
        | <ROW>\n    <ts>1969-12-31T00:00:00.000Z</ts>\n</ROW> |

    Scenario: NULL timestamp is omitted
      When query
        """
        SELECT to_xml(named_struct('ts', CAST(NULL AS TIMESTAMP)))
        """
      Then query result
        | to_xml(named_struct(ts, CAST(NULL AS TIMESTAMP))) |
        | <ROW/> |

    Scenario: Date field uses default yyyy-MM-dd format
      When query
        """
        SELECT to_xml(named_struct('d', DATE '2015-08-26'))
        """
      Then query result
        | to_xml(named_struct(d, DATE '2015-08-26')) |
        | <ROW>\n    <d>2015-08-26</d>\n</ROW> |

  Rule: Decimal and special values

    Scenario: Decimal field is formatted as fixed-point string
      When query
        """
        SELECT to_xml(named_struct('price', CAST(9.99 AS DECIMAL(5,2))))
        """
      Then query result
        | to_xml(named_struct(price, CAST(9.99 AS DECIMAL(5,2)))) |
        | <ROW>\n    <price>9.99</price>\n</ROW> |

    Scenario: Negative decimal preserves sign
      When query
        """
        SELECT to_xml(named_struct('price', CAST(-0.99 AS DECIMAL(5,2))))
        """
      Then query result
        | to_xml(named_struct(price, CAST((- 0.99) AS DECIMAL(5,2)))) |
        | <ROW>\n    <price>-0.99</price>\n</ROW> |

    Scenario: Very large decimal value
      When query
        """
        SELECT to_xml(named_struct('big', CAST(999999.99 AS DECIMAL(10,2))))
        """
      Then query result
        | to_xml(named_struct(big, CAST(999999.99 AS DECIMAL(10,2)))) |
        | <ROW>\n    <big>999999.99</big>\n</ROW> |

    Scenario: Very small decimal value
      When query
        """
        SELECT to_xml(named_struct('tiny', CAST(0.0001 AS DECIMAL(5,4))))
        """
      Then query result
        | to_xml(named_struct(tiny, CAST(0.0001 AS DECIMAL(5,4)))) |
        | <ROW>\n    <tiny>0.0001</tiny>\n</ROW> |

    Scenario: Floating-point NaN value
      When query
        """
        SELECT to_xml(named_struct('nan', CAST('NaN' AS DOUBLE)))
        """
      Then query result
        | to_xml(named_struct(nan, CAST(NaN AS DOUBLE))) |
        | <ROW>\n    <nan>NaN</nan>\n</ROW> |

    Scenario: Floating-point positive infinity
      When query
        """
        SELECT to_xml(named_struct('pos', CAST('Infinity' AS DOUBLE)))
        """
      Then query result
        | to_xml(named_struct(pos, CAST(Infinity AS DOUBLE))) |
        | <ROW>\n    <pos>Infinity</pos>\n</ROW> |

    Scenario: Floating-point negative infinity
      When query
        """
        SELECT to_xml(named_struct('neg', CAST('-Infinity' AS DOUBLE)))
        """
      Then query result
        | to_xml(named_struct(neg, CAST((- Infinity) AS DOUBLE))) |
        | <ROW>\n    <neg>-Infinity</neg>\n</ROW> |

  Rule: Complex structures

    Scenario: Multiple data types in single struct
      When query
        """
        SELECT to_xml(named_struct('int_val', 42, 'str_val', 'hello', 'bool_val', true, 'float_val', 3.14))
        """
      Then query result
        | to_xml(named_struct(int_val, 42, str_val, hello, bool_val, true, float_val, 3.14)) |
        | <ROW>\n    <int_val>42</int_val>\n    <str_val>hello</str_val>\n    <bool_val>true</bool_val>\n    <float_val>3.14</float_val>\n</ROW> |

    Scenario: Boolean true value
      When query
        """
        SELECT to_xml(named_struct('flag', true))
        """
      Then query result
        | to_xml(named_struct(flag, true)) |
        | <ROW>\n    <flag>true</flag>\n</ROW> |

    Scenario: Boolean false value
      When query
        """
        SELECT to_xml(named_struct('flag', false))
        """
      Then query result
        | to_xml(named_struct(flag, false)) |
        | <ROW>\n    <flag>false</flag>\n</ROW> |

    Scenario: Struct with array and nested struct
      When query
        """
        SELECT to_xml(named_struct('items', array(1, 2), 'nested', named_struct('value', 42)))
        """
      Then query result
        | to_xml(named_struct(items, array(1, 2), nested, named_struct(value, 42))) |
        | <ROW>\n    <items>1</items>\n    <items>2</items>\n    <nested>\n        <value>42</value>\n    </nested>\n</ROW> |

    Scenario: Very large struct with 10 fields
      When query
        """
        SELECT to_xml(named_struct('a', 1, 'b', 2, 'c', 3, 'd', 4, 'e', 5, 'f', 6, 'g', 7, 'h', 8, 'i', 9, 'j', 10))
        """
      Then query result contains all fields from a to j
