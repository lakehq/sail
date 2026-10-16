Feature: like() and ilike() pattern matching with optional escape char

  Rule: like 2-arg basic matching

    Scenario: empty string matches empty pattern
      When query
        """
        SELECT like('', '') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: empty string does not match non-empty pattern
      When query
        """
        SELECT like('', 'a') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: percent matches anything
      When query
        """
        SELECT like('anything', '%') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: underscore matches single character
      When query
        """
        SELECT like('a', '_') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: two underscores match two characters
      When query
        """
        SELECT like('ab', '__') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: two underscores do not match three characters
      When query
        """
        SELECT like('abc', '__') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: mixed percent and underscore
      When query
        """
        SELECT like('hello world', '_ello%') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: percent in middle
      When query
        """
        SELECT like('hello world end', 'hello%end') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: like is case sensitive
      When query
        """
        SELECT like('Spark', '_PARK') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: exact match
      When query
        """
        SELECT like('Spark', 'Spark') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: no match
      When query
        """
        SELECT like('Spark', 'xyz') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: percent at start
      When query
        """
        SELECT like('hello', '%llo') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: percent at end
      When query
        """
        SELECT like('hello', 'hel%') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: backslash escapes percent
      When query
        """
        SELECT like('a%b', 'a\\%b') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: unicode with underscore
      When query
        """
        SELECT like('café', 'caf_') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: unicode with percent
      When query
        """
        SELECT like('日本語テスト', '日本%') AS result
        """
      Then query result
        | result |
        | true   |

  Rule: like NULL handling

    Scenario: null string returns null
      When query
        """
        SELECT like(CAST(NULL AS STRING), '%') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: null pattern returns null
      When query
        """
        SELECT like('Spark', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: both null returns null
      When query
        """
        SELECT like(CAST(NULL AS STRING), CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: like 3-arg with custom escape character

    Scenario: escape percent with hash
      When query
        """
        SELECT like('hello%world', 'hello#%world', '#') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: escaped percent does not match other characters
      When query
        """
        SELECT like('helloXworld', 'hello#%world', '#') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: escape underscore with hash
      When query
        """
        SELECT like('a_b', 'a#_b', '#') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: escaped underscore does not match other characters
      When query
        """
        SELECT like('aXb', 'a#_b', '#') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: double escape matches literal escape char
      When query
        """
        SELECT like('a#b', 'a##b', '#') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: percent still works as wildcard with custom escape
      When query
        """
        SELECT like('anything', '%', '#') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: underscore still works with custom escape
      When query
        """
        SELECT like('ab', 'a_', '#') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: null string with 3-arg returns null
      When query
        """
        SELECT like(CAST(NULL AS STRING), 'hello#%world', '#') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: null pattern with 3-arg returns null
      When query
        """
        SELECT like('hello', CAST(NULL AS STRING), '#') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: forward slash as escape char
      When query
        """
        SELECT like('%SystemDrive%/Users/John', '/%SystemDrive/%//Users%', '/') AS result
        """
      Then query result
        | result |
        | true   |

  Rule: ilike 2-arg case-insensitive matching

    Scenario: ilike is case insensitive
      When query
        """
        SELECT ilike('Spark', 'SPARK') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: ilike percent case insensitive
      When query
        """
        SELECT ilike('Spark SQL', '%sql') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: ilike underscore case insensitive
      When query
        """
        SELECT ilike('Spark', '_PARK') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: ilike no match
      When query
        """
        SELECT ilike('Spark', 'xyz') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: ilike null returns null
      When query
        """
        SELECT ilike(CAST(NULL AS STRING), '%') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: ilike unicode case insensitive
      When query
        """
        SELECT ilike('CAFÉ', 'caf_') AS result
        """
      Then query result
        | result |
        | true   |

  Rule: ilike 3-arg with custom escape character

    Scenario: ilike escape percent case insensitive
      When query
        """
        SELECT ilike('Hello%World', 'hello#%world', '#') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: ilike escaped percent no match
      When query
        """
        SELECT ilike('HelloXWorld', 'hello#%world', '#') AS result
        """
      Then query result
        | result |
        | false  |

    Scenario: ilike double escape case insensitive
      When query
        """
        SELECT ilike('A#B', 'a##b', '#') AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Multi-row column expressions

    Scenario: like on column values
      When query
        """
        SELECT name, like(name, 'S%') AS starts_s
        FROM VALUES ('Spark'), ('SQL'), ('Python'), (CAST(NULL AS STRING)) AS t(name)
        """
      Then query result
        | name   | starts_s |
        | Spark  | true     |
        | SQL    | true     |
        | Python | false    |
        | NULL   | NULL     |
