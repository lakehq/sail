@try_parse_json @spark-4
Feature: try_parse_json (safe version of parse_json)

  Rule: Valid JSON parsing

    Scenario: try_parse_json valid JSON integer
      When query
        """
        SELECT variant_get(try_parse_json('42'), '$', 'int') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: try_parse_json valid JSON string
      When query
        """
        SELECT variant_get(try_parse_json('"hello"'), '$', 'string') AS result
        """
      Then query result
        | result |
        | hello  |

    Scenario: try_parse_json valid JSON object
      When query
        """
        SELECT variant_get(try_parse_json('{"a":1}'), '$.a', 'int') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: try_parse_json valid JSON null
      When query
        """
        SELECT is_variant_null(try_parse_json('null')) AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: try_parse_json valid JSON array
      When query
        """
        SELECT variant_get(try_parse_json('[1,2,3]'), '$[0]', 'int') AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: Invalid JSON returns NULL

    Scenario: try_parse_json invalid JSON returns NULL
      When query
        """
        SELECT try_parse_json('not json') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_parse_json empty string returns NULL
      When query
        """
        SELECT try_parse_json('') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: try_parse_json NULL input returns NULL
      When query
        """
        SELECT try_parse_json(NULL) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Trailing content and multi-row

    Scenario: try_parse_json trailing garbage parses valid prefix
      When query
        """
        SELECT try_parse_json('42 extra') AS result
        """
      Then query result
        | result |
        | 42     |

    Scenario: try_parse_json multi-row with invalid
      When query
        """
        SELECT try_parse_json(v) AS result
        FROM VALUES ('42'), ('bad json'), ('null'), ('{"a":1}') AS t(v)
        """
      Then query result
        | result  |
        | 42      |
        | NULL    |
        | null    |
        | {"a":1} |
