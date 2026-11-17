@spark-4.0
Feature: Inferred XML schemas preserve scalar and text values when parsed

  Scenario: XML round trip applies the default whitespace policy
    When query
      """
      SELECT r._id AS id, r.a AS number, r.b AS boolean, r.s AS text
      FROM (
        SELECT from_xml(
          '<p id=" 2 "><a> 1 </a><b> true </b><s> text </s></p>',
          schema_of_xml('<p id=" 2 "><a> 1 </a><b> true </b><s> text </s></p>')
        ) AS r
      )
      """
    Then query result
      | id | number | boolean | text |
      | 2  | 1      | true    | text |

  Scenario Outline: XML round trip respects whitespace and null options: <case>
    When query
      """
      SELECT concat('[', CAST(from_xml(
        '<xml>', schema_of_xml('<xml>', map(<options>)), map(<options>)
      ).<field> AS STRING), ']') AS result
      """
    Then query result
      | result   |
      | <result> |

    Examples:
      | case                    | xml                  | options                            | field | result   |
      | trimmed element         | <p><a> 1 </a></p>    | 'ignoreSurroundingSpaces', 'true'  | a     | [1]      |
      | trimmed attribute       | <p id=" true "/>     | 'ignoreSurroundingSpaces', 'true'  | _id   | [true]   |
      | preserved element       | <p><a> text </a></p> | 'ignoreSurroundingSpaces', 'false' | a     | [ text ] |
      | preserved attribute     | <p id=" text "/>     | 'ignoreSurroundingSpaces', 'false' | _id   | [ text ] |
      | null element            | <p><a> N/A </a></p>  | 'nullValue', 'N/A'                 | a     | NULL     |
      | null attribute          | <p id=" N/A "/>      | 'nullValue', 'N/A'                 | _id   | NULL     |
      | case insensitive option | <p><a> text </a></p> | 'IGNORESURROUNDINGSPACES', 'FaLsE' | a     | [ text ] |

  Scenario Outline: XML round trip preserves text arrays: <case>
    When query
      """
      SELECT from_xml('<xml>', schema_of_xml('<xml>', map(<options>)), map(<options>)).<field> AS result
      """
    Then query result
      | result   |
      | <result> |

    Examples:
      | case            | xml                                              | options              | field    | result         |
      | comments        | <p><a>hello<!-- c -->world</a></p>               | 'valueTag', '_VALUE' | a._VALUE | [hello, world] |
      | mixed content   | <p><a>hello<b>1</b>world</a></p>                 | 'valueTag', '_VALUE' | a._VALUE | [hello, world] |
      | custom tag      | <p><a>1<!-- c -->2</a></p>                       | 'valueTag', 'text'   | a.text   | [1, 2]         |
      | root text       | <p>hello<!-- c -->world</p>                      | 'valueTag', '_VALUE' | _VALUE   | [hello, world] |
      | null segment    | <p><a>1<!-- c -->N/A<!-- c -->2</a></p>          | 'nullValue', 'N/A'   | a._VALUE | [1, NULL, 2]   |
      | whitespace      | <p><a> <!-- c --> hello <!-- c -->world </a></p> | 'valueTag', '_VALUE' | a._VALUE | [hello, world] |
      | adjacent CDATA  | <p><a>he<![CDATA[llo]]><!-- c -->world</a></p>   | 'valueTag', '_VALUE' | a._VALUE | [hello, world] |
      | processing node | <p><a>hello<?test value?>world</a></p>           | 'valueTag', '_VALUE' | a._VALUE | [hello, world] |

  Scenario: XML text arrays preserve missing values across rows
    When query
      """
      SELECT from_xml(xml, 'a STRUCT<_VALUE: ARRAY<STRING>>').a._VALUE AS result
      FROM VALUES
        ('<p><a>one<!-- c -->two</a></p>'),
        ('<p><a/></p>'),
        ('<p><a>three</a></p>') AS t(xml)
      """
    Then query result
      | result     |
      | [one, two] |
      | NULL       |
      | [three]    |

  Scenario Outline: XML round trip preserves inferred decimals: <value>
    When query
      """
      SELECT from_xml(
        '<p><a><value></a></p>',
        schema_of_xml('<p><a><value></a></p>', map('prefersDecimal', 'true'))
      ).a AS result
      """
    Then query result
      | result   |
      | <result> |

    Examples:
      | value       | result   |
      | 1.5e-2      | 0.015    |
      | +1.5e-2     | 0.015    |
      | -1.5e-2     | -0.015   |
      | 1,234.50    | 1234.50  |
      | -1,234.50   | -1234.50 |
      | 1,234.50e-2 | 12.3450  |
      | 1.234E+2    | 123.4    |

  Scenario Outline: XML round trip validates timestamp zones: <zone>
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT
        schema_of_xml('<p><a>2024-01-02T03:04:05<zone></a></p>') AS xml_schema,
        schema_of_json('{"a":"2024-01-02T03:04:05<zone>"}', map('inferTimestamp', 'true')) AS json_schema,
        CAST(from_xml(
          '<p><a>2024-01-02T03:04:05<zone></a></p>',
          schema_of_xml('<p><a>2024-01-02T03:04:05<zone></a></p>')
        ).a AS STRING) AS result
      """
    Then query result
      | xml_schema | json_schema | result   |
      | <schema>   | <schema>    | <result> |

    Examples:
      | zone      | schema               | result                       |
      | +18:00    | STRUCT<a: TIMESTAMP> | 2024-01-01 09:04:05          |
      | -18:00    | STRUCT<a: TIMESTAMP> | 2024-01-02 21:04:05          |
      | +00:00:59 | STRUCT<a: TIMESTAMP> | 2024-01-02 03:03:06          |
      | GMT+08:00 | STRUCT<a: TIMESTAMP> | 2024-01-01 19:04:05          |
      | UTC-08:00 | STRUCT<a: TIMESTAMP> | 2024-01-02 11:04:05          |
      | UT        | STRUCT<a: TIMESTAMP> | 2024-01-02 03:04:05          |
      | +18:30    | STRUCT<a: STRING>    | 2024-01-02T03:04:05+18:30    |
      | -18:00:01 | STRUCT<a: STRING>    | 2024-01-02T03:04:05-18:00:01 |
      | +00:00:99 | STRUCT<a: STRING>    | 2024-01-02T03:04:05+00:00:99 |

  Scenario: XML time-only timestamps use the current date
    Given config spark.sql.session.timeZone = UTC
    When query
      """
      SELECT CAST(t AS DATE) = current_date() AS today, date_format(t, 'HH:mm:ss') AS time
      FROM (
        SELECT from_xml('<p><t>03:04:05</t></p>', 't TIMESTAMP').t AS t
      )
      """
    Then query result
      | today | time     |
      | true  | 03:04:05 |
