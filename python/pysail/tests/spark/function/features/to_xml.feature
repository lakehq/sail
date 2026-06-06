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

    Scenario: XML declaration is present by default
      When query
        """
        SELECT to_xml(named_struct('a', 1)) LIKE '<?xml%' AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Arrays

    Scenario: Primitive array becomes repeated elements
      When query
        """
        SELECT xpath(to_xml(named_struct('numbers', array(1, 2, 3))), '/ROW/numbers/item/text()') AS result
        """
      Then query result
        | result    |
        | [1, 2, 3] |