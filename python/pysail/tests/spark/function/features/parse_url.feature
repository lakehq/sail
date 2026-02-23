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

  Rule: Additional parts

    Scenario: parse_url file
      When query
        """
        SELECT parse_url('https://example.com/path?q=1', 'FILE') AS result
        """
      Then query result
        | result     |
        | /path?q=1  |

    Scenario: parse_url authority
      When query
        """
        SELECT parse_url('https://user:pass@example.com:8080/path', 'AUTHORITY') AS result
        """
      Then query result
        | result                     |
        | user:pass@example.com:8080 |

    Scenario: parse_url userinfo
      When query
        """
        SELECT parse_url('https://user:pass@example.com/path', 'USERINFO') AS result
        """
      Then query result
        | result    |
        | user:pass |

    Scenario: parse_url ref
      When query
        """
        SELECT parse_url('https://example.com/path#frag', 'REF') AS result
        """
      Then query result
        | result |
        | frag   |

  Rule: Malformed URLs

    Scenario: parse_url with URL without scheme returns NULL
      When query
        """
        SELECT parse_url('notaurl', 'HOST') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url with URL without scheme PATH returns the string
      When query
        """
        SELECT parse_url('notaurl', 'PATH') AS result
        """
      Then query result
        | result  |
        | notaurl |

    Scenario: parse_url with URL without scheme FILE returns the string
      When query
        """
        SELECT parse_url('notaurl', 'FILE') AS result
        """
      Then query result
        | result  |
        | notaurl |

    Scenario: parse_url with URL without scheme PROTOCOL returns NULL
      When query
        """
        SELECT parse_url('notaurl', 'PROTOCOL') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url with empty string returns NULL
      When query
        """
        SELECT parse_url('', 'HOST') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url with invalid part returns NULL
      When query
        """
        SELECT parse_url('https://example.com', 'INVALID') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Null handling

    Scenario: parse_url null url
      When query
        """
        SELECT parse_url(CAST(NULL AS STRING), 'HOST') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url null part
      When query
        """
        SELECT parse_url('https://example.com', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |
