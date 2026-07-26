@like_escape
Feature: LIKE and ILIKE with ESCAPE clause

  Rule: Custom ESCAPE character

    Scenario Outline: Custom ESCAPE: <case>
      When query
        """
        SELECT <value> <op> <pattern> ESCAPE <escape> AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                                  | value                      | op    | pattern                   | escape | result |
        | ilike with '/' as escape character                                    | '%SystemDrive%/Users/John' | ilike | '/%SYSTEMDrive/%//Users%' | '/'    | true   |
        | like with '/' as escape character                                     | '%SystemDrive%/Users/John' | like  | '/%SystemDrive/%//Users%' | '/'    | true   |
        | ilike with '/' as escape character and lowercase pattern              | '%SystemDrive%/Users/John' | ilike | '/%SystemDrive/%//users%' | '/'    | true   |
        | like with '!' escape before a non-special char (lenient pass-through) | 'a!b'                      | LIKE  | 'a!b'                     | '!'    | true   |
        | like with '!' escape and literal '!' in value but not pattern         | 'a!b'                      | LIKE  | 'ab'                      | '!'    | false  |

    Scenario: like with '!' escape leaves backslash as a literal
      When query
        """
        SELECT 'a\\xy' LIKE 'a\\%' ESCAPE '!' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: like with '!' escape adjacent to a literal backslash (lenient)
      When query
        """
        SELECT '!\\xy' LIKE '!\\%' ESCAPE '!' AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Default backslash escape

    Scenario: ilike with default backslash escape
      When query
        """
        SELECT '%SystemDrive%\\Users\\John' ilike '\\%SystemDrive\\%\\\\users%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: like with default backslash escape
      When query
        """
        SELECT '%SystemDrive%\\Users\\John' like '\\%SystemDrive\\%\\\\Users%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: ilike with raw string value and raw string pattern
      When query
        """
        SELECT r'%SystemDrive%\users\John' ilike r'\%SystemDrive\%\\Users%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: ilike with raw string value and escaped backslash pattern
      When query
        """
        SELECT r'%SystemDrive%\users\John' ilike '\%SystemDrive\%\\\\Users%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: like with raw string value and escaped backslash pattern
      When query
        """
        SELECT r'%SystemDrive%\Users\John' like '%SystemDrive%\\\\Users%' AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: like with raw string value and raw string pattern
      When query
        """
        SELECT r'%SystemDrive%\Users\John' like r'%SystemDrive%\\Users%' AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Case sensitivity

    Scenario Outline: Case sensitivity: <case>
      When query
        """
        SELECT <fn>(<value>, <pattern>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                         | fn    | value   | pattern | result |
        | ilike as function is case-insensitive with uppercase pattern | ilike | 'Spark' | '_PARK' | true   |
        | like as function matches same-case pattern                   | like  | 'Spark' | '_park' | true   |
        | like as function returns false on value mismatch             | like  | 'Spock' | '_park' | false  |
        | like as function is case-sensitive on pattern                | like  | 'Spock' | '_pArk' | false  |
