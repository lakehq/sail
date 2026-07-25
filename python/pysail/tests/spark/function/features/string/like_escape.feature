Feature: LIKE and ILIKE with ESCAPE clause

  Rule: Custom ESCAPE character

    Scenario: ilike with '/' as escape character
      When query
      """
      SELECT '%SystemDrive%/Users/John' ilike '/%SYSTEMDrive/%//Users%' ESCAPE '/' AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: like with '/' as escape character
      When query
      """
      SELECT '%SystemDrive%/Users/John' like '/%SystemDrive/%//Users%' ESCAPE '/' AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: ilike with '/' as escape character and lowercase pattern
      When query
      """
      SELECT '%SystemDrive%/Users/John' ilike '/%SystemDrive/%//users%' ESCAPE '/' AS result
      """
      Then query result
      | result |
      | true   |

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

    Scenario: like with '!' escape before a non-special char (lenient pass-through)
      When query
      """
      SELECT 'a!b' LIKE 'a!b' ESCAPE '!' AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: like with '!' escape and literal '!' in value but not pattern
      When query
      """
      SELECT 'a!b' LIKE 'ab' ESCAPE '!' AS result
      """
      Then query result
      | result |
      | false  |

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

    Scenario: ilike as function is case-insensitive with uppercase pattern
      When query
      """
      SELECT ilike('Spark', '_PARK') AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: like as function matches same-case pattern
      When query
      """
      SELECT like('Spark', '_park') AS result
      """
      Then query result
      | result |
      | true   |

    Scenario: like as function returns false on value mismatch
      When query
      """
      SELECT like('Spock', '_park') AS result
      """
      Then query result
      | result |
      | false  |

    Scenario: like as function is case-sensitive on pattern
      When query
      """
      SELECT like('Spock', '_pArk') AS result
      """
      Then query result
      | result |
      | false  |
