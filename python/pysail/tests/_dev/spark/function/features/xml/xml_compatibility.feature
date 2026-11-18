@spark-4.0
Feature: XML functions follow Spark lexical names and XPath semantics

  Rule: Struct readers preserve lexical names

    Scenario: XML compatibility: XML namespace declarations and undeclared prefixes are ordinary names
      When query
        """
        SELECT from_xml(
          '<r xmlns="urn:default" xmlns:p="urn:same"><p:a p:id="7">12</p:a><q:a>34</q:a></r>',
          '`p:a` STRUCT<`_p:id`: INT, _VALUE: INT>, `q:a` INT, _xmlns STRING, `_xmlns:p` STRING'
        ) AS result
        """
      Then query result
        | result                              |
        | {{7, 12}, 34, urn:default, urn:same} |

    Scenario: XML compatibility: Namespace aliases remain distinct in inferred schemas
      When query
        """
        SELECT schema_of_xml('<r xmlns:p="urn:same" xmlns:q="urn:same"><p:a>1</p:a><q:a>2</q:a></r>') AS result
        """
      Then query result
        | result                                                                              |
        | STRUCT<`_xmlns:p`: STRING, `_xmlns:q`: STRING, `p:a`: BIGINT, `q:a`: BIGINT>           |

    Scenario: XML compatibility: Ignoring namespaces strips attribute and element prefixes
      When query
        """
        SELECT from_xml(
          '<r xmlns:p="urn:same"><p:a p:id="7">12</p:a><q:a q:id="8">34</q:a></r>',
          'a ARRAY<STRUCT<_id: INT, _VALUE: INT>>, _p STRING',
          map('ignoreNamespace', 'true')
        ) AS result
        """
      Then query result
        | result                           |
        | {[{7, 12}, {8, 34}], urn:same}   |

    Scenario: XML compatibility: Excluding attributes also excludes namespace declarations
      When query
        """
        SELECT from_xml('<r xmlns:p="urn:p" id="7"><p:a>12</p:a></r>',
          '_id INT, `_xmlns:p` STRING, `p:a` INT', map('excludeAttribute', 'true')) AS result
        """
      Then query result
        | result           |
        | {NULL, NULL, 12} |

    Scenario: XML compatibility: Scalar fields use the last repeated element
      When query
        """
        SELECT from_xml('<r><p:a>1</p:a><q:a>2</q:a></r>', 'a INT', map('ignoreNamespace', 'true')) AS result
        """
      Then query result
        | result |
        | {2}    |

    Scenario: XML compatibility: String fields retain inner markup with lexical names
      When query
        """
        SELECT from_xml('<r><a>x<p:b k="v"/>y &amp; z</a></r>', 'a STRING').a AS result
        """
      Then query result
        | result                       |
        | x<p:b k="v"></p:b>y & z      |

    Scenario: XML compatibility: Missing value text is null and repeated value text uses the last event
      When query
        """
        SELECT from_xml('<r><a>1</a></r>', '_VALUE STRING')._VALUE AS missing,
          from_xml('<r>first<a>1</a>last</r>', '_VALUE STRING')._VALUE AS last
        """
      Then query result
        | missing | last |
        | NULL    | last |

    Scenario: XML compatibility: Struct reader normalizes literal attribute whitespace before references
      When query
        """
        SELECT from_xml(concat('<r a="x', chr(13), chr(10), chr(9), 'y&#x9;z"/>'), '_a STRING')._a = concat('x  y', chr(9), 'z') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: XML compatibility: Ignored attribute prefixes overwrite colliding names
      When query
        """
        SELECT schema_of_xml('<r p:id="1" q:id="true"/>', map('ignoreNamespace', 'true')) AS schema,
          from_xml('<r p:id="1" q:id="true"/>', '_id BOOLEAN', map('ignoreNamespace', 'true'))._id AS value
        """
      Then query result
        | schema                | value |
        | STRUCT<_id: BOOLEAN> | true  |

  Rule: XPath uses local names without resolving namespace URIs

    Scenario Outline: XML compatibility: XPath name tests: <case>
      When query
        """
        SELECT xpath('<r xmlns="urn:default" xmlns:p="urn:p"><p:a p:id="7">12</p:a><q:a>34</q:a></r>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                    | path           | result       |
        | unprefixed elements     | r/a/text()     | [12, 34]     |
        | prefixed element tests  | r/p:a/text()   | []           |
        | unprefixed attributes   | r/a/@id        | [7]          |
        | attribute wildcard      | r/a/@*         | [7]          |
        | declarations hidden     | r/@*           | []           |
        | prefix namespace        | r/namespace::p | [urn:p]      |

    Scenario Outline: XML compatibility: XPath node names: <case>
      When query
        """
        SELECT xpath_string('<r xmlns:p="urn:p"><p:a p:id="7">12</p:a><a>34</a></r>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                 | path                        | result |
        | qualified name       | name(r/a[1])                   | p:a    |
        | local name           | local-name(r/a[1])             | a      |
        | empty namespace URI  | namespace-uri(r/a[1])          |        |
        | qualified attribute  | name(r/a/@id)               | p:id   |
        | attribute local name | local-name(r/a/@id)          | id     |
        | namespace name       | name(r/namespace::p)        | p      |
        | literal is unchanged | concat("a/b", name(r/a[1]))    | a/bp:a |

    Scenario: XML compatibility: XPath exposes and inherits default namespace declarations
      When query
        """
        SELECT xpath('<r xmlns="urn:default"><a/></r>', 'r/a/namespace::*') AS namespaces,
          xpath_string('<r xmlns="urn:default"/>', 'name(r/namespace::*[1])') AS name,
          xpath('<r xmlns="urn:default"/>', 'r/@*') AS attributes
        """
      Then query result
        | namespaces                                             | name | attributes |
        | [urn:default, http://www.w3.org/XML/1998/namespace]       |      | []         |

    Scenario: XML compatibility: XPath namespace bindings inherit and rebind independently of element matching
      When query
        """
        SELECT xpath_string('<r xmlns:p="urn:outer"><a xmlns:p="urn:inner"><p:b>1</p:b></a></r>', 'r/a/b/namespace::p') AS result
        """
      Then query result
        | result    |
        | urn:inner |

    Scenario: XML compatibility: XPath predicate name tests preserve position and axis semantics
      When query
        """
        SELECT xpath('<r><p:a id="1">x</p:a><q:a id="2">y</q:a><a id="3">z</a></r>', 'r/a[@id="2"]/following-sibling::a/text()') AS result
        """
      Then query result
        | result |
        | [z]    |

    Scenario: XML compatibility: XPath retains comments and processing instructions
      When query
        """
        SELECT xpath('<r><!--comment--><?target data?><p:a>x<![CDATA[y]]>&amp;z</p:a></r>', 'r/comment() | r/processing-instruction() | r/a/text()') AS result
        """
      Then query result
        | result                 |
        | [comment, data, x]   |

    Scenario: XML compatibility: XPath and struct readers split multiple colons differently
      When query
        """
        SELECT xpath_string('<r><p:q:a>2</p:q:a></r>', 'local-name(r/*)') AS name,
          xpath('<r><p:q:a>2</p:q:a></r>', 'r/a') AS matches,
          from_xml('<r><p:q:a>2</p:q:a></r>', 'a INT', map('ignoreNamespace', 'true')).a AS value
        """
      Then query result
        | name | matches | value |
        | q:a  | []      | 2     |

  Rule: Typed XPath conversion compatibility

    # XPath 3.1 numeric conversion requires at most one item.
    @sail-bug
    Scenario Outline: XML compatibility: Numeric conversion takes the first node for <function>
      When query
        """
        SELECT <function>('<r><p:a>12</p:a><q:a>34</q:a></r>', 'r/a') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | function     | result |
        | xpath_double | 12.0   |
        | xpath_float  | 12.0   |
        | xpath_int    | 12     |
        | xpath_long   | 12     |
        | xpath_number | 12.0   |
        | xpath_short  | 12     |

    # XPath 3.1 string conversion requires at most one item.
    @sail-bug
    Scenario: XML compatibility: Boolean and string conversions accept multiple nodes
      When query
        """
        SELECT xpath_boolean('<r><p:a>12</p:a><q:a>34</q:a></r>', 'r/a') AS present,
          xpath_string('<r><p:a>12</p:a><q:a>34</q:a></r>', 'r/a') AS first
        """
      Then query result
        | present | first |
        | true    | 12    |

    # XPath 3.1 sum rejects nonnumeric node values.
    @sail-bug
    Scenario: XML compatibility: Sum of nonnumeric nodes produces NaN
      When query
        """
        SELECT isnan(xpath_double('<r><a>12</a><a>bad</a></r>', 'sum(r/a)')) AS result
        """
      Then query result
        | result |
        | true   |

    # xee accepts XPath 3.1 numeric strings that XPath 1.0 rejects.
    @sail-bug
    Scenario Outline: XML compatibility: XPath numeric strings: <value>
      When query
        """
        SELECT isnan(xpath_double('<r><a><value></a></r>', 'r/a')) AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | value |
        | +12   |
        | 1e2   |
        | INF   |

    Scenario: XML compatibility: Short conversion narrows the converted Java integer
      When query
        """
        SELECT xpath_short('<r>65535</r>', 'r') AS wrapped,
          xpath_short('<r>999999999999</r>', 'r') AS overflow
        """
      Then query result
        | wrapped | overflow |
        | -1      | -1       |

  Rule: XML writer preserves lexical names and characters

    Scenario: XML compatibility: XML names round trip when writer name validation is disabled
      When query
        """
        SELECT from_xml(to_xml(named_struct('_p:id', 7, 'p:a', 12), map('rowTag', 'q:r', 'validateName', 'false')),
          '`_p:id` INT, `p:a` INT') AS result
        """
      Then query result
        | result  |
        | {7, 12} |

    Scenario: XML compatibility: XML writer escapes the CDATA terminator
      When query
        """
        SELECT to_xml(named_struct('_VALUE', ']]>')) AS result
        """
      Then query result
        | result               |
        | <ROW>]]&gt;</ROW>     |

    Scenario: XML compatibility: XML writer preserves carriage returns and attribute whitespace
      When query
        """
        SELECT to_xml(named_struct('_a', concat(chr(13), chr(10), chr(9)), '_VALUE', chr(13))) AS result
        """
      Then query result
        | result                                    |
        | <ROW a="&#xd;&#xa;&#x9;">&#xd;</ROW>       |

    Scenario: XML compatibility: XML writer validates qualified names by default
      When query
        """
        SELECT to_xml(named_struct('p:a', 1))
        """
      Then query error (?s).*Illegal name character.*

    Scenario: XML compatibility: XML writer supports compact output
      When query
        """
        SELECT to_xml(named_struct('a', 1, 'b', named_struct('c', 2)), map('indent', '')) = concat('<ROW><a>1</a><b><c>2</c></b></ROW>', chr(10)) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: XML compatibility: XML writer supports custom indentation
      When query
        """
        SELECT to_xml(named_struct('a', 1), map('indent', '--')) = concat('<ROW>', chr(10), '--<a>1</a>', chr(10), '</ROW>') AS result
        """
      Then query result
        | result |
        | true   |

  Rule: XPath DOM details

    Scenario: XML compatibility: XPath text values differ from DOM text node values around CDATA
      When query
        """
        SELECT xpath('<r>a<![CDATA[b]]>c</r>', 'r/text()') AS nodes,
          xpath_string('<r>a<![CDATA[b]]>c</r>', 'r/text()') AS value
        """
      Then query result
        | nodes | value |
        | [a]   | abc   |

    Scenario: XML compatibility: XPath exposes the last namespace declaration in DOM order
      When query
        """
        SELECT xpath('<r xmlns:q="urn:q" xmlns:p="urn:p"/>', 'r/namespace::*') AS result
        """
      Then query result
        | result                                               |
        | [urn:q, http://www.w3.org/XML/1998/namespace]         |

    Scenario: XML compatibility: XPath namespace declarations stay hidden from attribute node tests
      When query
        """
        SELECT xpath('<r xmlns:p="urn:p" a="1"/>', 'r/attribute::node()') AS result
        """
      Then query result
        | result |
        | [1]    |

    Scenario: XML compatibility: XPath expands internal entities containing text and markup
      When query
        """
        SELECT xpath_string('<!DOCTYPE r [<!ENTITY text "hello"><!ENTITY nested "<a>&text;</a>">]><r>&nested;</r>', 'r/a') AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: XML compatibility: XPath expands entities in attributes
      When query
        """
        SELECT xpath('<!DOCTYPE r [<!ENTITY value "a&amp;b">]><r a="&value;"/>', 'r/@a') AS result
        """
      Then query result
        | result |
        | [a&b]  |

    Scenario: XML compatibility: XPath does not resolve external entities
      When query
        """
        SELECT xpath_string('<!DOCTYPE r [<!ENTITY external SYSTEM "file:///nonexistent-sail-xml-test">]><r>before&external;after</r>', 'r') AS result
        """
      Then query result
        | result      |
        | beforeafter |

    Scenario: XML compatibility: XPath rejects recursive entities
      When query
        """
        SELECT xpath_string('<!DOCTYPE r [<!ENTITY loop "&loop;">]><r>&loop;</r>', 'r') AS result
        """
      Then query error (?s).*(entity|Entity|recursive|Recursive).*

    # XPath 3.1 function arguments do not implicitly select the first node.
    @sail-bug
    Scenario: XML compatibility: XPath string and rounding functions use the first node
      When query
        """
        SELECT xpath_string('<r><a>abcd</a><a>other</a></r>', 'substring(r/a, 2, 2)') AS text,
          xpath_double('<r><a>1.9</a><a>3.4</a></r>', 'floor(r/a)') AS number
        """
      Then query result
        | text | number |
        | bc   | 1.0    |

  Rule: XPath grammar preserves expression structure

    Scenario: XML compatibility: Nested calls preserve commas in string literals
      When query
        """
        SELECT xpath_string('<r><a>abcd</a><a>efgh</a></r>', 'concat(substring(r/a[1], 2, 2), ",", string(count(r/a)))') AS result
        """
      Then query result
        | result |
        | bc,2   |

    Scenario: XML compatibility: Reverse axis predicates differ from grouped filters
      When query
        """
        SELECT xpath_string('<r><a>1</a><a>2</a><a>3</a></r>', 'r/a[3]/preceding-sibling::a[1]') AS nearest,
          xpath_string('<r><a>1</a><a>2</a><a>3</a></r>', '(r/a[3]/preceding-sibling::a)[1]') AS first,
          xpath_string('<r><a>1</a><a>2</a></r>', '(r/a)[2]/text()') AS filtered
        """
      Then query result
        | nearest | first | filtered |
        | 2       | 1     | 2        |

    Scenario: XML compatibility: Operator names can also be XML element names
      When query
        """
        SELECT xpath('<r><and>yes</and><div>four</div><a id="x,y">one</a></r>', 'r/a[@id="x,y" and string-length(.)=3]/text()') AS predicate,
          xpath('<r><and>yes</and><div>four</div></r>', 'r/and/text() | r/div/text()') AS names
        """
      Then query result
        | predicate | names       |
        | [one]     | [yes, four] |

    Scenario: XML compatibility: XPath arithmetic and boolean precedence
      When query
        """
        SELECT xpath_double('<r/>', '2 + 3 * 4') AS number,
          xpath_boolean('<r/>', 'true() or false() and false()') AS value
        """
      Then query result
        | number | value |
        | 14.0   | true  |

    Scenario Outline: XML compatibility: XPath rejects mismatched delimiters: <path>
      When query
        """
        SELECT xpath_string('<r><a>1</a></r>', '<path>')
        """
      Then query error (?s).*Invalid XPath.*

      Examples:
        | path   |
        | (r/a]  |
        | r/a[1) |

    Scenario: XML compatibility: Unused parameter entity literals do not declare general entities
      When query
        """
        SELECT xpath_string(concat('<!DOCTYPE r [<!ENTITY % unused "<!ENTITY fake ', chr(39), 'bad', chr(39), '>">]><r>&fake;</r>'), 'r')
        """
      Then query error (?s).*(entity|Entity).*

  Rule: Entity expansion preserves XML context and enforces document limits

    Scenario Outline: XML compatibility: Entity references stay literal inside <context>
      When query
        """
        SELECT xpath_string('<xml>', '<path>') AS result
        """
      Then query result
        | result |
        | &x;    |

      Examples:
        | context                 | xml                                                                     | path                       |
        | CDATA                   | <!DOCTYPE r [<!ENTITY x "hi"><!ENTITY e "<![CDATA[&x;]]>">]><r>&e;</r>    | r                          |
        | comments                | <!DOCTYPE r [<!ENTITY x "hi"><!ENTITY e "<!--&x;-->">]><r>&e;</r>         | r/comment()                |
        | processing instructions | <!DOCTYPE r [<!ENTITY x "hi"><!ENTITY e "<?target &x;?>">]><r>&e;</r>     | r/processing-instruction() |

    Scenario: XML compatibility: Character references in declarations can produce markup
      When query
        """
        SELECT xpath_string('<!DOCTYPE r [<!ENTITY e "&#60;a>hi&#x3c;/a>">]><r>&e;</r>', 'r/a') AS result
        """
      Then query result
        | result |
        | hi     |

    Scenario: XML compatibility: Predefined entity references in declarations remain escaped text
      When query
        """
        SELECT xpath_string('<!DOCTYPE r [<!ENTITY e "&lt;a>hi&lt;/a>">]><r>&e;</r>', 'r') AS result
        """
      Then query result
        | result    |
        | <a>hi</a> |

    Scenario Outline: XML compatibility: Top-level entity expansion limits include <context>
      # Use a row value to avoid repeatedly expanding entities during constant folding.
      When query
        """
        SELECT xpath_string(concat('<!DOCTYPE r [<!ENTITY e "x">]><prefix>', id, repeat('&e;', 64001), '<suffix>'), 'r')
        FROM range(1)
        """
      Then query error (?s).*(entity|Entity).*

      Examples:
        | context    | prefix | suffix |
        | text       | <r>    | </r>   |
        | attributes | <r a=" | "/>    |

    Scenario: XML compatibility: Entity expansion permits the count limit
      When query
        """
        SELECT length(xpath_string(concat('<!DOCTYPE r [<!ENTITY e "x">]><r>', repeat('&e;', 64000), '</r>'), 'r')) AS result
        """
      Then query result
        | result |
        | 64000  |

    @sail-only
    Scenario Outline: XML compatibility: Cumulative entity bytes are bounded in <context>
      # Use a row value to avoid repeatedly expanding entities during constant folding.
      When query
        """
        SELECT xpath_string(concat('<!DOCTYPE r [<!ENTITY e "', repeat('x', 10000), '">]><prefix>', id, repeat('&e;', 1001), '<suffix>'), 'r')
        FROM range(1)
        """
      Then query error (?s).*XML entity size limit.*

      Examples:
        | context    | prefix | suffix |
        | text       | <r>    | </r>   |
        | attributes | <r a=" | "/>    |

  Rule: Ordinary text cannot contain a CDATA terminator

    Scenario: XML compatibility: Struct readers return null fields for literal CDATA terminators
      When query
        """
        SELECT from_xml('<r><a>abc]]>def</a></r>', 'a STRING').a AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: XML compatibility: Struct readers reject literal CDATA terminators in fail-fast mode
      When query
        """
        SELECT from_xml('<r><a>abc]]>def</a></r>', 'a STRING', map('mode', 'FAILFAST'))
        """
      Then query error (?s).*(XML|xml|MALFORMED).*

    Scenario Outline: XML compatibility: <function> rejects literal CDATA terminators
      When query
        """
        SELECT <function>('<r>abc]]>def</r>', 'r')
        """
      Then query error (?s).*(XML|xml|character data).*

      Examples:
        | function     |
        | xpath        |
        | xpath_string |

    Scenario: XML compatibility: Escaped CDATA terminators remain valid text
      When query
        """
        SELECT from_xml('<r><a>abc]]&gt;def</a></r>', 'a STRING').a AS value,
          xpath_string('<r>abc]]&gt;def</r>', 'r') AS xpath_value
        """
      Then query result
        | value      | xpath_value |
        | abc]]>def  | abc]]>def   |

    Scenario: XML compatibility: CDATA terminators formed across entity boundaries are invalid
      When query
        """
        SELECT xpath_string('<!DOCTYPE r [<!ENTITY e "]]">]><r>&e;></r>', 'r')
        """
      Then query error (?s).*(XML|xml|character data).*

    Scenario: XML compatibility: Literal CDATA terminators inside entities are invalid
      When query
        """
        SELECT xpath_string('<!DOCTYPE r [<!ENTITY e "]]>">]><r>&e;</r>', 'r')
        """
      Then query error (?s).*(XML|xml|character data).*

  Rule: Primitive fields require scalar content

    Scenario Outline: XML compatibility: Fail-fast mode rejects nested content in primitive fields: <content>
      When query
        """
        SELECT from_xml('<r><a><content></a></r>', 'a INT', map('mode', 'FAILFAST'))
        """
      Then query error (?s).*(XML|xml|MALFORMED).*

      Examples:
        | content  |
        | 1<b/>2   |
        | <b/>1    |
        | 1<b/>    |

    @sail-only
    Scenario: XML compatibility: Permissive mode keeps rows aligned after malformed nested array elements
      When query
        """
        SELECT id, from_xml(xml, 'b INT, a ARRAY<STRUCT<v: INT>>') AS result
        FROM VALUES
          (1, '<r><b>1</b><a><v>2</v></a></r>'),
          (2, '<r><b>3</b><a><v>4</v></a><a><v>5<x/>6</v></a></r>'),
          (3, '<r><b>7</b><a><v>8</v></a></r>') AS input(id, xml)
        ORDER BY id
        """
      Then query result
        | id | result       |
        | 1  | {1, [{2}]}   |
        | 2  | {NULL, NULL} |
        | 3  | {7, [{8}]}   |

  Rule: String field serialization does not depend on the thread stack size

    # The JVM StAX parser limits element depth to 1000.
    @sail-only
    Scenario: XML compatibility: String fields retain deeply nested markup
      When query
        """
        SELECT from_xml(concat('<r><a>', repeat('<b>', 10000), 'value', repeat('</b>', 10000), '</a></r>'),
          'a STRING', map('mode', 'FAILFAST')).a = concat(repeat('<b>', 10000), 'value', repeat('</b>', 10000)) AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Entity replacement text follows JVM line ending normalization

    Scenario: XML compatibility: Entity carriage returns normalize in text and attributes
      When query
        """
        SELECT xpath_string('<!DOCTYPE r [<!ENTITY e "&#xD;"><!ENTITY nested "&e;">]><r>&nested;</r>', 'r') = chr(10) AS text_value,
          xpath_string('<!DOCTYPE r [<!ENTITY e "&#xD;">]><r a="&e;"/>', 'r/@a') = ' ' AS attribute_value,
          xpath_string('<r>&#xD;</r>', 'r') = chr(13) AS direct_reference
        """
      Then query result
        | text_value | attribute_value | direct_reference |
        | true       | true            | true             |

  Rule: XML declarations and document types occur only once in their proper positions

    Scenario Outline: XML compatibility: Struct readers return null for malformed prologs: <case>
      When query
        """
        SELECT from_xml(concat(<prolog>, '<r><a>1</a></r>'), 'a INT').a AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                      | prolog                                       |
        | repeated declaration      | '<?xml version="1.0"?><?xml version="1.0"?>' |
        | repeated document type    | '<!DOCTYPE r><!DOCTYPE r>'                   |
        | declaration after DTD     | '<!DOCTYPE r><?xml version="1.0"?>'          |
        | declaration after comment | '<!--comment--><?xml version="1.0"?>'        |
        | declaration after PI      | '<?target data?><?xml version="1.0"?>'       |
        | declaration after space   | ' <?xml version="1.0"?>'                     |

    Scenario Outline: XML compatibility: Struct readers reject malformed prologs in fail-fast mode: <case>
      When query
        """
        SELECT from_xml(<xml>, 'a INT', map('mode', 'FAILFAST'))
        """
      Then query error (?s).*(XML|xml|MALFORMED).*

      Examples:
        | case                      | xml                                                         |
        | repeated declaration      | '<?xml version="1.0"?><?xml version="1.0"?><r><a>1</a></r>' |
        | repeated document type    | '<!DOCTYPE r><!DOCTYPE r><r><a>1</a></r>'                   |
        | declaration after DTD     | '<!DOCTYPE r><?xml version="1.0"?><r><a>1</a></r>'          |
        | declaration after comment | '<!--comment--><?xml version="1.0"?><r><a>1</a></r>'        |
        | declaration after PI      | '<?target data?><?xml version="1.0"?><r><a>1</a></r>'       |
        | declaration after space   | ' <?xml version="1.0"?><r><a>1</a></r>'                     |

    Scenario Outline: XML compatibility: XPath rejects malformed prologs: <case>
      When query
        """
        SELECT xpath_string(<xml>, 'r/a')
        """
      Then query error (?s).*(XML|xml|Error loading expression).*

      Examples:
        | case                       | xml                                                           |
        | repeated declaration       | '<?xml version="1.0"?><?xml version="1.0"?><r><a>1</a></r>'   |
        | repeated document type     | '<!DOCTYPE r><!DOCTYPE r><r><a>1</a></r>'                     |
        | declaration after DTD      | '<!DOCTYPE r><?xml version="1.0"?><r><a>1</a></r>'            |
        | declaration after comment  | '<!--comment--><?xml version="1.0"?><r><a>1</a></r>'          |
        | declaration after PI       | '<?target data?><?xml version="1.0"?><r><a>1</a></r>'         |
        | declaration after space    | ' <?xml version="1.0"?><r><a>1</a></r>'                       |
        | repeated DTD with entities | '<!DOCTYPE r [<!ENTITY e "1">]><!DOCTYPE r><r><a>&e;</a></r>' |

    Scenario Outline: XML compatibility: Readers accept valid prologs: <case>
      When query
        """
        SELECT from_xml(<xml>, 'a INT').a AS value,
          xpath_string(<xml>, 'r/a') AS xpath_value
        """
      Then query result
        | value | xpath_value |
        | 1     | 1           |

      Examples:
        | case                | xml                                                                                        |
        | declaration and DTD | '<?xml version="1.0"?><!DOCTYPE r><r><a>1</a></r>'                                         |
        | comments and PI     | '<?xml version="1.0"?><!--before--><?target data?><!DOCTYPE r><!--after--><r><a>1</a></r>' |
        | no declaration      | ' <!--comment--><!DOCTYPE r><r><a>1</a></r>'                                               |

  Rule: XPath arithmetic conversion compatibility

    Scenario Outline: XML compatibility: Arithmetic operand conversion: <path>
      When query
        """
        SELECT xpath_double('<r><a>1</a><a>9</a><b>2</b><b>8</b><bad>1e2</bad></r>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | path            | result    |
        | r/missing + 1   | NaN       |
        | -r/missing      | NaN       |

    # XPath 3.1 uses strict operand types and integer/decimal arithmetic.
    @sail-bug
    Scenario Outline: XML compatibility: Arithmetic operand conversion differs from XPath 3.1: <path>
      When query
        """
        SELECT xpath_double('<r><a>1</a><a>9</a><b>2</b><b>8</b><bad>1e2</bad></r>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | path            | result    |
        | r/a + 2         | 3.0       |
        | 2 + r/a         | 3.0       |
        | r/b - r/a       | 1.0       |
        | r/a * r/b       | 2.0       |
        | r/a div r/b     | 0.5       |
        | r/b mod r/a     | 0.0       |
        | -r/a            | -1.0      |
        | -(-r/a)         | 1.0       |
        | r/a + r/b * 3   | 7.0       |
        | (r/a + r/b) * 3 | 9.0       |
        | (r/a div 0) + 1 | Infinity  |
        | true() + 2      | 3.0       |
        | " 2 " + 1       | 3.0       |
        | "Infinity" + 1  | NaN       |
        | 1 div 0         | Infinity  |
        | -1 div 0        | -Infinity |
        | 0 div 0         | NaN       |
        | 5 mod 0         | NaN       |

    # Implicit numeric conversions accept XPath 3.1 numeric strings.
    @sail-bug
    Scenario Outline: XML compatibility: Arithmetic rejects XPath 3.1 numeric strings: <path>
      When query
        """
        SELECT xpath_double('<r><bad>1e2</bad></r>', '<path>') AS result
        """
      Then query result
        | result |
        | NaN    |

      Examples:
        | path         |
        | -(r/bad + 1) |
        | r/bad + 1    |
        | "+2" + 1     |

    # XPath 3.1 arithmetic rejects multiple operands in a sequence.
    @sail-bug
    Scenario: XML compatibility: Arithmetic conversions preserve predicate context
      When query
        """
        SELECT xpath('<r><a>1</a><a>9</a></r>', 'r/a[position() = ../a + 1]/text()') AS result
        """
      Then query result
        | result |
        | [9]    |

  Rule: XML declarations require version as the first attribute

    Scenario Outline: XML compatibility: Struct readers return null for missing or misplaced declaration versions: <declaration>
      When query
        """
        SELECT from_xml('<declaration><r><a>1</a></r>', 'a INT').a AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | declaration                            |
        | <?xml?>                                |
        | <?xml encoding="UTF-8"?>               |
        | <?xml encoding="UTF-8" version="1.0"?> |

    Scenario Outline: XML compatibility: Struct readers reject missing or misplaced declaration versions in fail-fast mode: <declaration>
      When query
        """
        SELECT from_xml('<declaration><r><a>1</a></r>', 'a INT', map('mode', 'FAILFAST'))
        """
      Then query error (?s).*(XML|xml|MALFORMED).*

      Examples:
        | declaration                            |
        | <?xml?>                                |
        | <?xml encoding="UTF-8"?>               |
        | <?xml encoding="UTF-8" version="1.0"?> |

    Scenario Outline: XML compatibility: XPath rejects missing or misplaced declaration versions: <declaration>
      When query
        """
        SELECT xpath_string('<declaration><r><a>1</a></r>', 'r/a')
        """
      Then query error (?s).*(XML|xml|Error loading expression).*

      Examples:
        | declaration                            |
        | <?xml?>                                |
        | <?xml encoding="UTF-8"?>               |
        | <?xml encoding="UTF-8" version="1.0"?> |

  Rule: Referenced internal parameter entities contribute declarations

    Scenario Outline: XML compatibility: Parameter entity declarations: <case>
      When query
        """
        SELECT xpath_string(<xml>, 'r') AS result
        """
      Then query result
        | result |
        | hello  |

      Examples:
        | case                         | xml                                                                                                                           |
        | internal declaration         | concat('<!DOCTYPE r [<!ENTITY % defs ', chr(39), '<!ENTITY e "hello">', chr(39), '>%defs;]><r>&e;</r>')                       |
        | nested parameter references  | "<!DOCTYPE r [<!ENTITY % a '&#37;b;'><!ENTITY % b '<!ENTITY e &#34;hello&#34;>'>%a;]><r>&e;</r>"                              |
        | first declaration wins       | "<!DOCTYPE r [<!ENTITY e 'hello'><!ENTITY % defs '<!ENTITY e &#34;other&#34;>'>%defs;]><r>&e;</r>"                            |
        | first parameter wins         | "<!DOCTYPE r [<!ENTITY % defs '<!ENTITY e &#34;hello&#34;>'><!ENTITY % defs '<!ENTITY e &#34;other&#34;>'>%defs;]><r>&e;</r>" |
        | separate parameter names     | "<!DOCTYPE r [<!ENTITY defs 'hello'><!ENTITY % defs '<!ENTITY e &#34;&defs;&#34;>'>%defs;]><r>&e;</r>"                        |
        | external parameter ignored   | "<!DOCTYPE r [<!ENTITY % ext SYSTEM 'file:///nonexistent-sail-xml-test'>%ext;]><r>hello</r>"                                  |
        | undeclared parameter ignored | "<!DOCTYPE r [%missing;]><r>hello</r>"                                                                                        |
        | references inside comments   | "<!DOCTYPE r [<!ENTITY % defs '<!--&#37;missing;--><!ENTITY e &#34;hello&#34;>'>%defs;]><r>&e;</r>"                               |

    Scenario: XML compatibility: Recursive parameter entities are rejected
      When query
        """
        SELECT xpath_string('<!DOCTYPE r [<!ENTITY % loop "&#37;loop;">%loop;]><r/>', 'r')
        """
      Then query error (?s).*(entity|Entity|recursive|Recursive).*

    Scenario: XML compatibility: Parameter entity references share the document count limit
      # Use a row value to avoid repeatedly expanding entities during constant folding.
      When query
        """
        SELECT xpath_string(concat('<!DOCTYPE r [<!ENTITY % p ""><!ENTITY e "x">', repeat('%p;', 64000), ']><r>', id, '&e;</r>'), 'r')
        FROM range(1)
        """
      Then query error (?s).*(entity|Entity).*

    @sail-only
    Scenario: XML compatibility: Parameter entity replacement bytes are bounded
      # Use a row value to avoid repeatedly expanding entities during constant folding.
      When query
        """
        SELECT xpath_string(concat('<!DOCTYPE r [<!ENTITY % p "<!--', repeat('x', 10000), '-->">', repeat('%p;', 1000), ']><r>', id, '</r>'), 'r')
        FROM range(1)
        """
      Then query error (?s).*XML entity size limit.*

    @sail-only
    Scenario: XML compatibility: Parameter entity nesting is bounded
      When query
        """
        SELECT xpath_string(concat('<!DOCTYPE r [',
          concat_ws('', transform(sequence(0, 100), i -> concat('<!ENTITY % p', i, ' "&#37;p', i + 1, ';">'))),
          '%p0;]><r/>'), 'r')
        """
      Then query error (?s).*XML entity expansion limit.*

    Scenario: XML compatibility: Parameter entity expansion permits the count limit
      When query
        """
        SELECT xpath_string(concat('<!DOCTYPE r [<!ENTITY % p "">', repeat('%p;', 64000), ']><r>ok</r>'), 'r') AS result
        """
      Then query result
        | result |
        | ok     |

  Rule: XPath numeric string conversion compatibility

    Scenario Outline: XML compatibility: Numeric string formatting: <path>
      When query
        """
        SELECT xpath_string('<r/>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | path                                  | result                                           |
        | 1000000 + 1                           | 1000001                                          |
        | -1000000 - 1                          | -1000001                                         |
        | 1 div 10000000                        | 0.0000001                                        |
        | -1 div 10000000                       | -0.0000001                                       |
        | -0                                    | 0                                                |
        | 0 div -1                              | 0                                                |

    # XPath 3.1 preserves integer/decimal precision and rejects decimal division by zero.
    @sail-bug
    Scenario Outline: XML compatibility: Numeric string formatting differs from XPath 3.1: <path>
      When query
        """
        SELECT xpath_string('<r/>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | path                                  | result                                           |
        | 1000000000000000000000000000000 + 1   | 1000000000000000000000000000000                  |
        | 1 div 1000000000000000000000000000000 | 0.0000000000000000000000000000009999999999999999 |
        | 1 div 0                               | Infinity                                         |
        | -1 div 0                              | -Infinity                                        |

    Scenario Outline: XML compatibility: Implicit numeric string formatting: <path>
      When query
        """
        SELECT xpath_string('<r/>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | path                                 | result             |
        | concat(1000000 + 1, ":", -0)         | 1000001:0          |
        | string(1000000 + 1)                  | 1000001            |

    # XPath 3.1 does not implicitly convert numeric arguments to strings.
    @sail-bug
    Scenario Outline: XML compatibility: Implicit numeric string formatting differs from XPath 3.1: <path>
      When query
        """
        SELECT xpath_string('<r/>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | path                                 | result             |
        | concat(1 div 10000000, ":", 1 div 0) | 0.0000001:Infinity |
        | starts-with(1000000 + 1, "1000")     | true               |
        | substring-before(1000000 + 1, "01")  | 10000              |
        | substring-after(1000000 + 1, "100")  | 0001               |
        | substring(1000000 + 1, 2, 3)         | 000                |
        | string-length(1000000 + 1)           | 7                  |
        | normalize-space(1000000 + 1)         | 1000001            |
        | translate(1000000 + 1, 0, 2)         | 1222221            |

    Scenario Outline: XML compatibility: Numeric string formatting retains compatible conversions: <path>
      When query
        """
        SELECT xpath_string('<r/>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | path                               | result |
        | true()                             | true   |
        | "1E7"                              | 1E7    |
        | r/missing                          |        |

    # XPath 3.1 rejects decimal division by zero and numeric arguments to contains.
    @sail-bug
    Scenario Outline: XML compatibility: Numeric string formatting retains compatible conversions differs from XPath 3.1: <path>
      When query
        """
        SELECT xpath_string('<r/>', '<path>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | path                               | result |
        | 0 div 0                            | NaN    |
        | contains(1000000 + 1, 1000000 + 1) | true   |

  Rule: Entity boundaries preserve independently normalized line endings

    Scenario Outline: XML compatibility: Content line endings at entity boundaries: <case>
      When query
        """
        SELECT xpath_string(<xml>, 'r') = concat(chr(10), chr(10)) AS result
        """
      Then query result
        | result |
        | true   |

      Examples:
        | case                     | xml                                                                                             |
        | entity before literal LF | concat('<!DOCTYPE r [<!ENTITY e "&#xD;">]><r>&e;', chr(10), '</r>')                             |
        | literal CR before entity | concat('<!DOCTYPE r [<!ENTITY e "&#xA;">]><r>', chr(13), '&e;</r>')                             |
        | adjacent entities        | '<!DOCTYPE r [<!ENTITY cr "&#xD;"><!ENTITY lf "&#xA;">]><r>&cr;&lf;</r>'                        |
        | nested entity boundary   | '<!DOCTYPE r [<!ENTITY cr "&#xD;"><!ENTITY lf "&#xA;"><!ENTITY both "&cr;&lf;">]><r>&both;</r>' |
        | declaration literal CR   | concat('<!DOCTYPE r [<!ENTITY e "', chr(13), '&#xA;">]><r>&e;</r>')                             |

    Scenario: XML compatibility: Direct character references retain carriage returns beside entities
      When query
        """
        SELECT xpath_string('<!DOCTYPE r [<!ENTITY e "&#xA;">]><r>&#xD;&e;</r>', 'r') = concat(chr(13), chr(10)) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: XML compatibility: Attribute line endings at entity boundaries remain separate spaces
      When query
        """
        SELECT xpath_string(concat('<!DOCTYPE r [<!ENTITY e "&#xD;">]><r a="&e;', chr(10), '"/>'), 'r/@a') = '  ' AS result,
          xpath_string('<!DOCTYPE r [<!ENTITY e "&#xD;">]><r a="&e;&#xA;"/>', 'r/@a') = concat(' ', chr(10)) AS reference
        """
      Then query result
        | result | reference |
        | true   | true      |

  Rule: XML declarations validate versions and all pseudo-attributes

    Scenario Outline: XML compatibility: Invalid declaration contents return null fields: <declaration>
      When query
        """
        SELECT from_xml('<declaration><r><a>1</a></r>', 'a INT').a AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | declaration                                             |
        | <?xml version="1.0" encoding=""?>                       |
        | <?xml version="2.0"?>                                   |
        | <?xml version="1.2"?>                                   |
        | <?xml version=""?>                                      |
        | <?xml version="1.0" bogus="x"?>                         |
        | <?xml version="1.0" standalone="true"?>                 |
        | <?xml version="1.0" standalone="YES"?>                  |
        | <?xml version="1.0" standalone="yes" encoding="UTF-8"?> |
        | <?xml version="1.0" version="1.0"?>                     |
        | <?xml version="1.0" encoding="UTF-8" encoding="UTF-8"?> |
        | <?xml version="1.0" standalone="yes" standalone="yes"?> |
        | <?xml version="1.0"encoding="UTF-8"?>                   |
        | <?xml version=1.0?>                                     |

    Scenario Outline: XML compatibility: Invalid declaration contents fail in fail-fast mode: <declaration>
      When query
        """
        SELECT from_xml('<declaration><r><a>1</a></r>', 'a INT', map('mode', 'FAILFAST'))
        """
      Then query error (?s).*(XML|xml|MALFORMED).*

      Examples:
        | declaration                                             |
        | <?xml version="1.0" encoding=""?>                       |
        | <?xml version="2.0"?>                                   |
        | <?xml version="1.2"?>                                   |
        | <?xml version=""?>                                      |
        | <?xml version="1.0" bogus="x"?>                         |
        | <?xml version="1.0" standalone="true"?>                 |
        | <?xml version="1.0" standalone="YES"?>                  |
        | <?xml version="1.0" standalone="yes" encoding="UTF-8"?> |
        | <?xml version="1.0" version="1.0"?>                     |
        | <?xml version="1.0" encoding="UTF-8" encoding="UTF-8"?> |
        | <?xml version="1.0" standalone="yes" standalone="yes"?> |
        | <?xml version="1.0"encoding="UTF-8"?>                   |
        | <?xml version=1.0?>                                     |

    Scenario Outline: XML compatibility: XPath rejects invalid declaration contents: <declaration>
      When query
        """
        SELECT xpath_string('<declaration><r><a>1</a></r>', 'r/a')
        """
      Then query error (?s).*(XML|xml|Error loading expression).*

      Examples:
        | declaration                                             |
        | <?xml version="2.0"?>                                   |
        | <?xml version="1.2"?>                                   |
        | <?xml version=""?>                                      |
        | <?xml version="1.0" bogus="x"?>                         |
        | <?xml version="1.0" standalone="true"?>                 |
        | <?xml version="1.0" standalone="YES"?>                  |
        | <?xml version="1.0" standalone="yes" encoding="UTF-8"?> |
        | <?xml version="1.0" version="1.0"?>                     |
        | <?xml version="1.0" encoding="UTF-8" encoding="UTF-8"?> |
        | <?xml version="1.0" standalone="yes" standalone="yes"?> |
        | <?xml version="1.0"encoding="UTF-8"?>                   |
        | <?xml version=1.0?>                                     |

    Scenario Outline: XML compatibility: Valid declaration contents remain accepted: <declaration>
      When query
        """
        SELECT from_xml('<declaration><r><a>1</a></r>', 'a INT', map('mode', 'FAILFAST')).a AS value,
          xpath_string('<declaration><r><a>1</a></r>', 'r/a') AS xpath_value
        """
      Then query result
        | value | xpath_value |
        | 1     | 1           |

      Examples:
        | declaration                                                    |
        | <?xml version="1.1"?>                                          |
        | <?xml version = "1.0" encoding = "UTF-8" standalone = "yes" ?> |
        | <?xml version="1.0" standalone="no"?>                          |
        | <?xml version="1.0" encoding="made-up"?>                       |
        | <?xml version="1.0" encoding="1bad"?>                          |

    Scenario: XML compatibility: Valid declaration contents allow empty encoding for XPath
      When query
        """
        SELECT xpath_string('<?xml version="1.0" encoding=""?><r><a>1</a></r>', 'r/a') AS result
        """
      Then query result
        | result |
        | 1      |
