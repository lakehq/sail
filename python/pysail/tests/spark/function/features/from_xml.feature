Feature: from_xml parses an XML string into a struct value

  Rule: Primitive types

    Scenario: Parse INT and DOUBLE fields
      When query
        """
        SELECT from_xml('<p><a>1</a><b>0.4</b></p>', 'a INT, b DOUBLE') AS r
        """
      Then query result
        | r          |
        | {1, 0.4}   |

    Scenario: Parse STRING field
      When query
        """
        SELECT from_xml('<p><s>sunset</s></p>', 's STRING').s AS result
        """
      Then query result
        | result |
        | sunset  |

    Scenario: Parse BOOLEAN true
      When query
        """
        SELECT from_xml('<p><flag>true</flag></p>', 'flag BOOLEAN').flag AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: Parse BOOLEAN false
      When query
        """
        SELECT from_xml('<p><flag>false</flag></p>', 'flag BOOLEAN').flag AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: Parse LONG field
      When query
        """
        SELECT from_xml('<p><x>43</x></p>', 'x LONG').x AS result
        """
      Then query result
        | result |
        | 43     |

    Scenario: Parse FLOAT field
      When query
        """
        SELECT from_xml('<p><x>3.5</x></p>', 'x FLOAT').x AS result
        """
      Then query result
        | result |
        | 3.5    |    

  Rule: Root tag is ignored

    Scenario: Root tag name does not affect parsing
      When query
        """
        SELECT
          from_xml('<ROW><a>4</a></ROW>',      'a INT').a AS r1,
          from_xml('<record><a>4</a></record>', 'a INT').a AS r2,
          from_xml('<p><a>4</a></p>',           'a INT').a AS r3
        """
      Then query result
        | r1 | r2 | r3 |
        | 4  | 4  | 4  |

  Rule: NULL handling

    Scenario: NULL input string returns NULL struct
      When query
        """
        SELECT from_xml(CAST(NULL AS STRING), 'a INT') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Missing tag returns NULL field
      When query
        """
        SELECT from_xml('<p><b>1</b></p>', 'a INT, b INT') AS r
        """
      Then query result
        | r         |
        | {NULL, 1} |

    Scenario: Empty tag with INT schema returns NULL
      When query
        """
        SELECT from_xml('<p><a></a></p>', 'a INT').a AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Empty tag with STRING schema returns empty string
      When query
        """
        SELECT from_xml('<p><a></a></p>', 'a STRING').a AS result
        """
      Then query result
        | result |
        |        |

    Scenario: Multi-row with missing field and NULL row
      When query
        """
        SELECT from_xml(xml_col, 'a INT, b STRING') AS r
        FROM VALUES
          ('<p><a>1</a><b>x</b></p>'),
          ('<p><a>2</a><b>y</b></p>'),
          ('<p><a>3</a></p>'),
          (CAST(NULL AS STRING))
        AS t(xml_col)
        """
      Then query result
        | r         |
        | {1, x}    |
        | {2, y}    |
        | {3, NULL} |
        | NULL      |

  Rule: nullValue option

    Scenario: nullValue option maps matching string to NULL for INT
      When query
        """
        SELECT from_xml('<p><a>N/A</a></p>', 'a INT', map('nullValue', 'N/A')).a AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: nullValue option maps matching string to NULL for STRING
      When query
        """
        SELECT from_xml('<p><a>N/A</a></p>', 'a STRING', map('nullValue', 'N/A')).a AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: nullValue empty string turns empty tag into NULL for STRING
      When query
        """
        SELECT from_xml('<p><a></a></p>', 'a STRING', map('nullValue', '')).a AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Nested structures

    Scenario: Nested struct becomes nested field access
      When query
        """
        SELECT from_xml(
          '<p><teacher>Pablo</teacher><student><name>Ana</name><rank>1</rank></student></p>',
          'teacher STRING, student STRUCT<name: STRING, rank: INT>'
        ) AS r
        """
      Then query result
        | r                           |
        | {Pablo, {Ana, 1}}           |

    Scenario: Three levels of nesting
      When query
        """
        SELECT from_xml('<p><a><b><c>45</c></b></a></p>', 'a STRUCT<b: STRUCT<c: INT>>').a.b.c AS result
        """
      Then query result
        | result |
        | 45     |

  Rule: Arrays

    Scenario: Repeated tags produce array of primitives
      When query
        """
        SELECT from_xml('<p><a>1</a><a>2</a><a>3</a></p>', 'a ARRAY<INT>').a AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |

    Scenario: Repeated struct tags produce array of structs
      When query
        """
        SELECT from_xml(
          '<p><student><name>Bob</name><rank>1</rank></student><student><name>Charlie</name><rank>2</rank></student></p>',
          'student ARRAY<STRUCT<name: STRING, rank: INT>>'
        ).student AS result
        """
      Then query result
        | result                              |
        | [{Bob, 1}, {Charlie, 2}]            |

  Rule: Date and timestamp types

    Scenario: Parse DATE with default format
      When query
        """
        SELECT from_xml('<p><d>2026-06-17</d></p>', 'd DATE').d AS result
        """
      Then query result
        | result     |
        | 2026-06-17 |

    Scenario: Parse TIMESTAMP with default format
      When query
        """
        SELECT from_xml('<p><ts>2026-06-17T18:00:00.000</ts></p>', 'ts TIMESTAMP').ts AS result
        """
      Then query result
        | result              |
        | 2026-06-17 18:00:00 |

    Scenario: Custom timestampFormat option
      When query
        """
        SELECT from_xml(
          '<p><ts>17/06/2026</ts></p>',
          'ts TIMESTAMP',
          map('timestampFormat', 'dd/MM/yyyy')
        ).ts AS result
        """
      Then query result
        | result              |
        | 2026-06-17 00:00:00 |

    Scenario: Parse TIMESTAMP_NTZ with default format
      When query
        """
        SELECT from_xml('<p><ts>2026-06-17T18:00:00.000</ts></p>', 'ts TIMESTAMP_NTZ').ts AS result
        """
      Then query result
        | result                  |
        | 2026-06-17 18:00:00 |

    Scenario: Custom timestampNTZFormat option
      When query
        """
        SELECT from_xml(
          '<p><ts>17/06/2026 18:00</ts></p>',
          'ts TIMESTAMP_NTZ',
          map('timestampNTZFormat', 'dd/MM/yyyy HH:mm')
        ).ts AS result
        """
      Then query result
        | result                  |
        | 2026-06-17 18:00:00 |

  Rule: XML entity unescaping

    Scenario: Less-than entity is unescaped
      When query
        """
        SELECT from_xml('<p><s>a &lt; b</s></p>', 's STRING').s AS result
        """
      Then query result
        | result  |
        | a < b   |

    Scenario: Ampersand entity is unescaped
      When query
        """
        SELECT from_xml('<p><s>a &amp; b</s></p>', 's STRING').s AS result
        """
      Then query result
        | result  |
        | a & b   |

    Scenario: Greater-than entity is unescaped
      When query
        """
        SELECT from_xml('<p><s>a &gt; b</s></p>', 's STRING').s AS result
        """
      Then query result
        | result  |
        | a > b   |

    Scenario: Quote entity is unescaped
      When query
        """
        SELECT from_xml('<p><s>say &quot;hi&quot;</s></p>', 's STRING').s AS result
        """
      Then query result
        | result    |
        | say "hi"  |

  Rule: Attributes

    Scenario: Attributes are parsed with default underscore prefix
      When query
        """
        SELECT from_xml('<p id="99"><name>Agua</name></p>', '_id STRING, name STRING') AS r
        """
      Then query result
        | r             |
        | {99, Agua}   |

    Scenario: Custom attributePrefix option
      When query
        """
        SELECT from_xml('<p id="99"><name>Amber</name></p>', 'id STRING, name STRING', map('attributePrefix', '')) AS r
        """
      Then query result
        | r             |
        | {99, Amber}   |

  Rule: valueTag option

    Scenario: Text content of element is captured via default valueTag field
      When query
        """
        SELECT from_xml('<p id="7">colors</p>', '_id STRING, _VALUE STRING') AS r
        """
      Then query result
        | r          |
        | {7, colors} |

  Rule: Parse mode

    Scenario: PERMISSIVE mode returns NULL field for bad cast
      When query
        """
        SELECT from_xml('<p><a>not_a_number</a></p>', 'a INT').a AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: Explicit PERMISSIVE mode returns NULL field for bad cast
      When query
        """
        SELECT from_xml('<p><a>not_a_number</a></p>', 'a INT', map('mode', 'PERMISSIVE')).a AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Decimal type

    Scenario: Parse DECIMAL field
      When query
        """
        SELECT from_xml('<p><a>2.12</a></p>', 'a DECIMAL(10,2)').a AS result
        """
      Then query result
        | result |
        | 2.12   |

    Scenario: DECIMAL truncates to scale
      When query
        """
        SELECT from_xml('<p><a>2.12159</a></p>', 'a DECIMAL(10,2)').a AS result
        """
      Then query result
        | result |
        | 2.12   |

    Scenario: DECIMAL overflow returns NULL
      When query
        """
        SELECT from_xml('<p><a>99999999999.99</a></p>', 'a DECIMAL(10,2)').a AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: DECIMAL bad value returns NULL
      When query
        """
        SELECT from_xml('<p><a>bad</a></p>', 'a DECIMAL(10,2)').a AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: DECIMAL in array
      When query
        """
        SELECT from_xml('<p><a>1.10</a><a>2.20</a></p>', 'a ARRAY<DECIMAL(10,2)>').a AS result
        """
      Then query result
        | result         |
        | [1.10, 2.20]   |