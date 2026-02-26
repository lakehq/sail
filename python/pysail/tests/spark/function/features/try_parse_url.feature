Feature: try_parse_url migration tests
  Tests exposing differences between Sail and DataFusion fork implementations.
  Fork inherits parse_url limitations: fewer string type combinations (3 vs 27).

  Rule: try_parse_url basic URL parts extraction

    Scenario: try_parse_url extracts HOST
      When query
      """
      SELECT try_parse_url('https://spark.apache.org/path?query=1', 'HOST') AS result
      """
      Then query result
      | result             |
      | spark.apache.org   |

    Scenario: try_parse_url extracts PATH
      When query
      """
      SELECT try_parse_url('https://spark.apache.org/path?query=1', 'PATH') AS result
      """
      Then query result
      | result |
      | /path  |

    Scenario: try_parse_url extracts QUERY
      When query
      """
      SELECT try_parse_url('https://spark.apache.org/path?query=1', 'QUERY') AS result
      """
      Then query result
      | result  |
      | query=1 |

    Scenario: try_parse_url extracts PROTOCOL
      When query
      """
      SELECT try_parse_url('https://spark.apache.org/path?query=1', 'PROTOCOL') AS result
      """
      Then query result
      | result |
      | https  |

  Rule: try_parse_url with QUERY and specific key

    Scenario: try_parse_url extracts specific query parameter
      When query
      """
      SELECT try_parse_url('https://spark.apache.org/path?key1=val1&key2=val2', 'QUERY', 'key2') AS result
      """
      Then query result
      | result |
      | val2   |

    Scenario: try_parse_url with missing query key returns NULL
      When query
      """
      SELECT try_parse_url('https://spark.apache.org/path?key1=val1', 'QUERY', 'missing') AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: try_parse_url error handling (returns NULL instead of error)

    Scenario: try_parse_url with invalid URL returns NULL
      When query
      """
      SELECT try_parse_url('not_a_url', 'HOST') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: try_parse_url with invalid part name returns NULL
      When query
      """
      SELECT try_parse_url('https://spark.apache.org', 'INVALID') AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: try_parse_url with NULL inputs

    Scenario: try_parse_url with NULL URL
      When query
      """
      SELECT try_parse_url(NULL, 'HOST') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: try_parse_url with NULL part
      When query
      """
      SELECT try_parse_url('https://spark.apache.org', NULL) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: try_parse_url extracts additional URL components

    Scenario: try_parse_url extracts AUTHORITY
      When query
      """
      SELECT try_parse_url('https://user:pass@spark.apache.org:8080/path', 'AUTHORITY') AS result
      """
      Then query result
      | result                        |
      | user:pass@spark.apache.org:8080 |

    Scenario: try_parse_url extracts USERINFO
      When query
      """
      SELECT try_parse_url('https://user:pass@spark.apache.org/path', 'USERINFO') AS result
      """
      Then query result
      | result    |
      | user:pass |

    Scenario: try_parse_url extracts REF (fragment)
      When query
      """
      SELECT try_parse_url('https://spark.apache.org/path#section1', 'REF') AS result
      """
      Then query result
      | result   |
      | section1 |

    Scenario: try_parse_url extracts FILE
      When query
      """
      SELECT try_parse_url('https://spark.apache.org/path?query=1', 'FILE') AS result
      """
      Then query result
      | result        |
      | /path?query=1 |
