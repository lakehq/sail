# SPDX-License-Identifier: Apache-2.0

@sail-only
Feature: XPath uses the engine version with Spark XML DOM behavior

  Scenario Outline: XPath 3.1 expressions preserve lexical XML name matching: <path>
    When query
      """
      SELECT xpath_string('<r><p:a>1</p:a><q:a>2</q:a></r>', '<path>') AS result
      """
    Then query result
      | result   |
      | <result> |

    Examples:
      | path                                                | result  |
      | head(r/a)                                           | 1       |
      | string-join(r/a, ",")                               | 1,2     |
      | let $x := r/a return string-join($x, ":")           | 1:2     |
      | string-join((for $x in r/a return string($x)), ",") | 1,2     |
      | if (exists(r/a)) then "yes" else "no"               | yes     |
      | some $x in r/a satisfies $x = 2                     | true    |
      | map {"value": r/a[2]}?value                         | 2       |
      | [r/a[1], r/a[2]]?2                                  | 2       |
      | array {r/a}?2                                       | 2       |
      | r/a => head()                                       | 1       |
      | (function($nodes) { head($nodes) })(r/a)            | 1       |
      | (head#1)(r/a)                                       | 1       |
      | name(head(r/a))                                     | p:a     |
      | (local-name#1)(r/a[2])                              | a       |
      | r/a[1] ! (name#0)()                                | p:a     |
      | r/a[2] ! (local-name#0)()                          | a       |
      | r/a[1] ! (let $f := name#0 return (../a[2] ! $f())) | p:a     |
      | string-join(r/a ! name(.), ",")                     | p:a,q:a |
      | Q{http://www.w3.org/2005/xpath-functions}head(r/a)  | 1       |
      | head(r/*:a)                                         | 1       |

  Scenario: XPath 3.1 variables with different unbound prefixes are distinct
    When query
      """
      SELECT xpath_string('<r/>',
        'let $p:x := 7, $q:x := 9 return concat($p:x, ":", $q:x)') AS result
      """
    Then query result
      | result |
      | 7:9    |

  Scenario: XPath 3.1 rejects an undeclared variable with a different unbound prefix
    When query
      """
      SELECT xpath_string('<r/>', 'let $p:x := 7 return $q:x')
      """
    Then query error (?s).*XPST0008.*

  Scenario: XPath 3.1 variables with unbound prefixes are distinct from explicit EQNames
    When query
      """
      SELECT xpath_string('<r/>',
        'let $p:x := 7, $Q{urn:sail:unbound-xpath-prefix:0}x := 9
         return concat($p:x, ":", $Q{urn:sail:unbound-xpath-prefix:0}x)') AS result
      """
    Then query result
      | result |
      | 7:9    |

  Scenario Outline: XPath 3.1 node tests keep lexical XML name matching: <test>
    When query
      """
      SELECT xpath_int('<r xmlns:p="urn:p"><p:a/></r>', 'count(r/<test>)') AS result
      """
    Then query result
      | result   |
      | <result> |

    Examples:
      | test         | result |
      | p:a          | 0      |
      | p:*          | 0      |
      | element(p:a) | 0      |
      | *:a          | 1      |

  Scenario: XPath 3.1 sequences retain Spark node values
    When query
      """
      SELECT xpath('<r><a>1</a><a>2</a></r>', '(r/a[2]/text(), r/a[1]/text())') AS result
      """
    Then query result
      | result |
      | [2, 1] |

  Scenario Outline: XPath 3.1 numeric conversions: <path>
    When query
      """
      SELECT xpath_double('<r><a>1e2</a><b>+2</b></r>', '<path>') AS result
      """
    Then query result
      | result   |
      | <result> |

    Examples:
      | path        | result   |
      | number(r/a) | 100.0    |
      | number(r/b) | 2.0      |
      | 1e2 + 2     | 102.0    |
      | r/a + 1     | 101.0    |
      | 1e0 div 0   | Infinity |

  Scenario Outline: XPath 3.1 rejects implicit XPath 1.0 conversions: <path>
    When query
      """
      SELECT xpath_string('<r><a>1</a><a>2</a></r>', '<path>')
      """
    Then query error (?s).*Error evaluating XPath.*

    Examples:
      | path            |
      | string(r/a)     |
      | number(r/a)     |
      | r/a + 1         |
      | true() + 2      |
      | substring(1, 1) |
      | 1 div 0         |
      | 5 mod 0         |
