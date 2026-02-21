Feature: parse_url() extracts URL component

  Rule: Basic usage

    Scenario: parse_url host
      When query
        """
        SELECT parse_url('https://example.com:8080/path?q=1', 'HOST') AS result
        """
      Then query result
        | result      |
        | example.com |

    Scenario: parse_url path
      When query
        """
        SELECT parse_url('https://example.com/path', 'PATH') AS result
        """
      Then query result
        | result |
        | /path  |

    Scenario: parse_url query
      When query
        """
        SELECT parse_url('https://example.com?a=1&b=2', 'QUERY') AS result
        """
      Then query result
        | result  |
        | a=1&b=2 |

    Scenario: parse_url query param
      When query
        """
        SELECT parse_url('https://example.com?a=1&b=2', 'QUERY', 'b') AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: parse_url protocol
      When query
        """
        SELECT parse_url('https://example.com/path', 'PROTOCOL') AS result
        """
      Then query result
        | result |
        | https  |

  Rule: Null handling

    Scenario: parse_url null url
      When query
        """
        SELECT parse_url(CAST(NULL AS STRING), 'HOST') AS result
        """
      Then query result
        | result |
        | NULL   |
