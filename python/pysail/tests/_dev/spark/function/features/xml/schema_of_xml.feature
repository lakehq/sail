@spark-4.0
Feature: schema_of_xml infers a DDL schema from a literal XML document

  Scenario Outline: schema_of_xml inference: <case>
    When query
      """
      SELECT schema_of_xml(<args>) AS result
      """
    Then query result
      | result   |
      | <result> |

    Examples:
      | case                     | args                                                                         | result                                                |
      | scalar types             | '<p><b>true</b><n>1</n><d>1.5</d><s>hello</s></p>'                           | STRUCT<b: BOOLEAN, d: DOUBLE, n: BIGINT, s: STRING>   |
      | nested elements          | '<p><a><b>1</b></a></p>'                                                     | STRUCT<a: STRUCT<b: BIGINT>>                          |
      | repeated elements        | '<p><a>1</a><a>2.5</a><a>3</a></p>'                                          | STRUCT<a: ARRAY<DOUBLE>>                              |
      | repeated structs         | '<p><a><b>1</b></a><a><c>true</c></a></p>'                                   | STRUCT<a: ARRAY<STRUCT<b: BIGINT, c: BOOLEAN>>>       |
      | root attributes          | '<p id="2"><a>1</a></p>'                                                     | STRUCT<_id: BIGINT, a: BIGINT>                        |
      | attributes and values    | '<p><a id="2">1</a><a>3</a></p>'                                             | STRUCT<a: ARRAY<STRUCT<_VALUE: BIGINT, _id: BIGINT>>> |
      | empty attributes         | '<p><a id="2"/></p>'                                                         | STRUCT<a: STRUCT<_id: BIGINT>>                        |
      | exclude attributes       | '<p><a attr="2">1</a><a>3</a></p>', map('excludeAttribute', 'true')          | STRUCT<a: ARRAY<BIGINT>>                              |
      | custom attribute prefix  | '<p><a id="2">1</a></p>', map('attributePrefix', '@', 'valueTag', 'text')    | STRUCT<a: STRUCT<`@id`: BIGINT, text: BIGINT>>        |
      | empty element            | '<p><a/></p>'                                                                | STRUCT<a: STRING>                                     |
      | empty root               | '<p/>'                                                                       | STRUCT<>                                              |
      | text root                | '<p>hello</p>'                                                               | STRUCT<_VALUE: STRING>                                |
      | mixed text               | '<p><a>hello<b>1</b>world</a></p>'                                           | STRUCT<a: STRUCT<_VALUE: ARRAY<STRING>, b: BIGINT>>   |
      | XML entities             | '<p><a>&amp;</a></p>'                                                        | STRUCT<a: STRING>                                     |
      | CDATA                    | '<p><a><![CDATA[42]]></a></p>'                                               | STRUCT<a: BIGINT>                                     |
      | quoted field name        | '<p><a-b>1</a-b></p>'                                                        | STRUCT<`a-b`: BIGINT>                                 |
      | namespace                | '<p xmlns:n="urn:test"><n:a>1</n:a></p>'                                     | STRUCT<`_xmlns:n`: STRING, `n:a`: BIGINT>             |
      | ignore namespace         | '<p xmlns:n="urn:test"><n:a>1</n:a></p>', map('ignoreNamespace', 'true')     | STRUCT<_n: STRING, a: BIGINT>                         |
      | leading zeros            | '<p><a>01</a></p>'                                                           | STRUCT<a: BIGINT>                                     |
      | whitespace               | '<p><a> 1 </a></p>'                                                          | STRUCT<a: BIGINT>                                     |
      | preserve whitespace      | '<p><a> true </a></p>', map('ignoreSurroundingSpaces', 'false')              | STRUCT<a: STRING>                                     |
      | disable inference        | '<p><a>1</a><b>true</b></p>', map('inferSchema', 'false')                    | STRUCT<a: STRING, b: STRING>                          |
      | decimal                  | '<p><a>1.50</a></p>', map('prefersDecimal', 'true')                          | STRUCT<a: DECIMAL(3,2)>                               |
      | merge decimals           | '<p><a>1.50</a><a>12.345</a></p>', map('prefersDecimal', 'true')             | STRUCT<a: ARRAY<DECIMAL(5,3)>>                        |
      | date and timestamp       | '<p><d>2024-01-02</d><t>2024-01-02T03:04:05</t></p>'                         | STRUCT<d: DATE, t: TIMESTAMP>                         |
      | custom date format       | '<p><d>02/01/2024</d></p>', map('dateFormat', 'dd/MM/yyyy')                  | STRUCT<d: DATE>                                       |
      | custom timestamp format  | '<p><t>02/01/2024 03:04</t></p>', map('timestampFormat', 'dd/MM/yyyy HH:mm') | STRUCT<t: TIMESTAMP>                                  |
      | case insensitive options | '<p><a id="2">1</a></p>', map('EXCLUDEATTRIBUTE', 'TrUe')                    | STRUCT<a: BIGINT>                                     |
      | malformed XML            | '<p><a>1</p>'                                                                | STRUCT<_corrupt_record: STRING>                       |
      | custom corrupt field     | '<p>', map('columnNameOfCorruptRecord', 'bad-record')                        | STRUCT<`bad-record`: STRING>                          |
      | null option value        | '<p><a id="2">1</a></p>', map('valueTag', CAST(NULL AS STRING))              | STRUCT<a: STRUCT<_id: BIGINT, null: BIGINT>>          |
      | foldable input           | concat('<p><a>', '1</a></p>')                                                | STRUCT<a: BIGINT>                                     |

  Scenario: schema_of_xml retains repeated text values as a struct
    When query
      """
      SELECT schema_of_xml('<p><a>hello<!-- comment -->world</a></p>') AS result
      """
    Then query result
      | result                                   |
      | STRUCT<a: STRUCT<_VALUE: ARRAY<STRING>>> |

  Scenario: schema_of_xml merges dates and timestamps
    When query
      """
      SELECT schema_of_xml('<p><a>2024-01-02</a><a>2024-01-02T03:04:05</a></p>') AS result
      """
    Then query result
      | result                      |
      | STRUCT<a: ARRAY<TIMESTAMP>> |

  Scenario Outline: schema_of_xml respects case sensitivity: <case_sensitive>
    Given config spark.sql.caseSensitive = <case_sensitive>
    When query
      """
      SELECT schema_of_xml('<p><a>1</a><A>2</A></p>') AS result
      """
    Then query result
      | result   |
      | <result> |

    Examples:
      | case_sensitive | result                       |
      | false          | STRUCT<a: ARRAY<BIGINT>>     |
      | true           | STRUCT<A: BIGINT, a: BIGINT> |

  Scenario: schema_of_xml infers timestamps without time zones
    Given config spark.sql.timestampType = TIMESTAMP_NTZ
    When query
      """
      SELECT schema_of_xml('<p><a>2024-01-02T03:04:05</a><b>2024-01-02T03:04:05Z</b></p>') AS result
      """
    Then query result
      | result                                 |
      | STRUCT<a: TIMESTAMP_NTZ, b: TIMESTAMP> |

  Scenario: schema_of_xml result can be used by from_xml
    When query
      """
      SELECT from_xml('<p><a>42</a></p>', schema_of_xml('<p><a>1</a></p>')).a AS result
      """
    Then query result
      | result |
      | 42     |

  Scenario Outline: schema_of_xml rejects invalid arguments: <case>
    When query
      """
      SELECT schema_of_xml(<args>) AS result
      """
    Then query error .*

    Examples:
      | case                | args                                 |
      | null input          | CAST(NULL AS STRING)                 |
      | numeric input       | 42                                   |
      | invalid options     | '<p/>', 42                           |
      | invalid boolean     | '<p/>', map('inferSchema', 'yes')    |
      | empty value tag     | '<p/>', map('valueTag', '')          |
      | malformed failfast  | '<p>', map('mode', 'FAILFAST')       |
      | drop malformed mode | '<p/>', map('mode', 'DROPMALFORMED') |

  Scenario: schema_of_xml rejects column input
    When query
      """
      SELECT schema_of_xml(CAST(id AS STRING)) FROM range(2)
      """
    Then query error .*
