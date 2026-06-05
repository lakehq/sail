Feature: xpath() extracts XML nodes with Spark-compatible semantics

  Rule: Node selection returns arrays of string-like values

    Scenario: xpath returns text nodes in document order
      When query
        """
        SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b></a>', 'a/b/text()') AS result
        """
      Then query result
        | result        |
        | [b1, b2, b3]  |

    Scenario: xpath returns NULL entries for element nodes
      When query
        """
        SELECT xpath('<a><b>b1</b><b>b2</b></a>', 'a/b') AS result
        """
      Then query result
        | result        |
        | [NULL, NULL]  |

    Scenario: xpath returns an empty list when no nodes match
      When query
        """
        SELECT xpath('<a><b>1</b></a>', 'a/c') AS result
        """
      Then query result
        | result |
        | []     |

  Rule: Empty or null inputs return NULL

    Scenario: xpath returns NULL for empty xml
      When query
        """
        SELECT xpath('', 'a/b') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: xpath returns NULL for empty path
      When query
        """
        SELECT xpath('<a><b>1</b></a>', '') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: xpath returns NULL for null xml or path
      When query
        """
        SELECT
          xpath(CAST(NULL AS STRING), 'a/b') AS null_xml,
          xpath('<a><b>1</b></a>', CAST(NULL AS STRING)) AS null_path
        """
      Then query result
        | null_xml | null_path |
        | NULL     | NULL      |

  Rule: Non-node XPath results fail

    Scenario: xpath rejects expressions that do not return a node list
      When query
        """
        SELECT xpath('<a><b>1</b></a>', 'sum(a/b)') AS result
        """
      Then query error (?s).*NodeList.*
