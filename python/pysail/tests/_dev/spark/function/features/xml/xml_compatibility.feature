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
        | qualified name       | name(r/a)                   | p:a    |
        | local name           | local-name(r/a)             | a      |
        | empty namespace URI  | namespace-uri(r/a)          |        |
        | qualified attribute  | name(r/a/@id)               | p:id   |
        | attribute local name | local-name(r/a/@id)          | id     |
        | namespace name       | name(r/namespace::p)        | p      |
        | literal is unchanged | concat("a/b", name(r/a))    | a/bp:a |

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

  Rule: Typed XPath conversions follow XPath 1.0

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

    Scenario: XML compatibility: Boolean and string conversions accept multiple nodes
      When query
        """
        SELECT xpath_boolean('<r><p:a>12</p:a><q:a>34</q:a></r>', 'r/a') AS present,
          xpath_string('<r><p:a>12</p:a><q:a>34</q:a></r>', 'r/a') AS first
        """
      Then query result
        | present | first |
        | true    | 12    |

    Scenario: XML compatibility: Sum of nonnumeric nodes produces NaN
      When query
        """
        SELECT isnan(xpath_double('<r><a>12</a><a>bad</a></r>', 'sum(r/a)')) AS result
        """
      Then query result
        | result |
        | true   |

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

    Scenario Outline: XML compatibility: XPath rejects XPath 3.1 extensions: <path>
      When query
        """
        SELECT xpath_string('<r><a>1</a></r>', '<path>')
        """
      Then query error (?s).*(Invalid XPath|Could not find function|Extra illegal tokens).*

      Examples:
        | path      |
        | head(r/a) |
        | r/a, r/a  |

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
        SELECT xpath_string('<r><a>abcd</a><a>efgh</a></r>', 'concat(substring(r/a, 2, 2), ",", string(count(r/a)))') AS result
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
