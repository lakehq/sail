@try_parse_url
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

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to try_parse_url yields the schema Spark declares
      When query
        """
        SELECT try_parse_url('http://spark.apache.org/path?query=1', 'HOST') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null column input to try_parse_url yields the schema Spark declares
      When query
        """
        SELECT try_parse_url(CAST(id AS STRING), 'HOST') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable column input to try_parse_url stays nullable
      When query
        """
        SELECT try_parse_url(c, 'HOST') AS result FROM VALUES ('http://spark.apache.org/path?query=1'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Result values (migrated from test_try_parse_url.txt doctests)

    Scenario: try_parse_url doctest #1 (result)
      When query
        """
        SELECT try_parse_url('https://example.com/a?x=1', 'QUERY', 'x') AS result, typeof(try_parse_url('https://example.com/a?x=1', 'QUERY', 'x')) AS type
        """
      Then query result
        | result | type |
        | 1 | string |

    Scenario: try_parse_url doctest #2 (result)
      When query
        """
        SELECT try_parse_url('www.example.com/path?x=1', 'HOST') AS result, typeof(try_parse_url('www.example.com/path?x=1', 'HOST')) AS type
        """
      Then query result
        | result | type |
        | NULL | string |

    Scenario: try_parse_url doctest #3 (result)
      When query
        """
        SELECT try_parse_url('https://example.com/?a=1', 'QUERY', 'b') AS result, typeof(try_parse_url('https://example.com/?a=1', 'QUERY', 'b')) AS type
        """
      Then query result
        | result | type |
        | NULL | string |

    Scenario: try_parse_url doctest #4 (result)
      When query
        """
        SELECT try_parse_url('https://example.com/path#frag', 'REF') AS result, typeof(try_parse_url('https://example.com/path#frag', 'REF')) AS type
        """
      Then query result
        | result | type |
        | frag | string |

    Scenario: try_parse_url doctest #5 (result)
      When query
        """
        SELECT try_parse_url('ftp://user:pwd@ftp.example.com:21/files', 'USERINFO') AS result, typeof(try_parse_url('ftp://user:pwd@ftp.example.com:21/files', 'USERINFO')) AS type
        """
      Then query result
        | result | type |
        | user:pwd | string |

    Scenario: try_parse_url doctest #6 (result)
      When query
        """
        SELECT try_parse_url('http://[2001:db8::2]:8080/index.html?ok=1', 'HOST') AS result, typeof(try_parse_url('http://[2001:db8::2]:8080/index.html?ok=1', 'HOST')) AS type
        """
      Then query result
        | result | type |
        | [2001:db8::2] | string |

    Scenario: try_parse_url doctest #7 (result)
      When query
        """
        SELECT try_parse_url('notaurl', 'HOST') AS result, typeof(try_parse_url('notaurl', 'HOST')) AS type
        """
      Then query result
        | result | type |
        | NULL | string |

    Scenario: try_parse_url doctest #8 (result)
      When query
        """
        SELECT try_parse_url('https://example.com', 'PATH') AS result, typeof(try_parse_url('https://example.com', 'PATH')) AS type
        """
      Then query result
        | result | type |
        |  | string |

    Scenario: try_parse_url doctest #9 (result)
      When query
        """
        SELECT try_parse_url('https://example.com/a/b?x=1&y=2#frag', 'PROTOCOL') AS result, typeof(try_parse_url('https://example.com/a/b?x=1&y=2#frag', 'PROTOCOL')) AS type
        """
      Then query result
        | result | type |
        | https | string |

    Scenario: try_parse_url doctest #10 (result)
      When query
        """
        SELECT try_parse_url('https://ex.com/?Tag=ok', 'QUERY', 'tag') AS result, typeof(try_parse_url('https://ex.com/?Tag=ok', 'QUERY', 'tag')) AS type
        """
      Then query result
        | result | type |
        | NULL | string |

