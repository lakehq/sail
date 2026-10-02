Feature: try_parse_url migration tests
  Tests exposing differences between Sail and DataFusion fork implementations.
  Fork inherits parse_url limitations: fewer string type combinations (3 vs 27).

  Rule: try_parse_url basic URL parts extraction

    Scenario Outline: try_parse_url extracts <part>
      When query
        """
        SELECT try_parse_url('https://spark.apache.org/path?query=1', '<part>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | part     | result           |
        | HOST     | spark.apache.org |
        | PATH     | /path            |
        | QUERY    | query=1          |
        | PROTOCOL | https            |

  Rule: try_parse_url with QUERY and specific key

    Scenario Outline: Query key: <case>
      When query
        """
        SELECT try_parse_url('<url>', 'QUERY', '<key>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                              | url                                               | key     | result |
        | try_parse_url extracts specific query parameter   | https://spark.apache.org/path?key1=val1&key2=val2 | key2    | val2   |
        | try_parse_url with missing query key returns NULL | https://spark.apache.org/path?key1=val1           | missing | NULL   |

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

    Scenario Outline: NULL input: <case>
      When query
        """
        SELECT try_parse_url(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                         | args                             |
        | try_parse_url with NULL URL  | NULL, 'HOST'                     |
        | try_parse_url with NULL part | 'https://spark.apache.org', NULL |

  Rule: try_parse_url extracts additional URL components

    Scenario Outline: Component: <case>
      When query
        """
        SELECT try_parse_url('<url>', '<part>') AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                  | url                                          | part      | result                          |
        | try_parse_url extracts AUTHORITY      | https://user:pass@spark.apache.org:8080/path | AUTHORITY | user:pass@spark.apache.org:8080 |
        | try_parse_url extracts USERINFO       | https://user:pass@spark.apache.org/path      | USERINFO  | user:pass                       |
        | try_parse_url extracts REF (fragment) | https://spark.apache.org/path#section1       | REF       | section1                        |
        | try_parse_url extracts FILE           | https://spark.apache.org/path?query=1        | FILE      | /path?query=1                   |

  @function(nullability)
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

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT try_parse_url(<args>) AS result, typeof(try_parse_url(<args>)) AS type
        """
      Then query result
        | result   | type   |
        | <result> | string |

      Examples:
        | case                               | args                                                  | result        |
        | try_parse_url doctest #1 (result)  | 'https://example.com/a?x=1', 'QUERY', 'x'             | 1             |
        | try_parse_url doctest #2 (result)  | 'www.example.com/path?x=1', 'HOST'                    | NULL          |
        | try_parse_url doctest #3 (result)  | 'https://example.com/?a=1', 'QUERY', 'b'              | NULL          |
        | try_parse_url doctest #4 (result)  | 'https://example.com/path#frag', 'REF'                | frag          |
        | try_parse_url doctest #5 (result)  | 'ftp://user:pwd@ftp.example.com:21/files', 'USERINFO' | user:pwd      |
        | try_parse_url doctest #6 (result)  | 'http://[2001:db8::2]:8080/index.html?ok=1', 'HOST'   | [2001:db8::2] |
        | try_parse_url doctest #7 (result)  | 'notaurl', 'HOST'                                     | NULL          |
        | try_parse_url doctest #8 (result)  | 'https://example.com', 'PATH'                         |               |
        | try_parse_url doctest #9 (result)  | 'https://example.com/a/b?x=1&y=2#frag', 'PROTOCOL'    | https         |
        | try_parse_url doctest #10 (result) | 'https://ex.com/?Tag=ok', 'QUERY', 'tag'              | NULL          |

  Rule: Argument count validation

    Scenario: try_parse_url zero arguments errors
      When query
      """
      SELECT try_parse_url() AS result
      """
      Then query error .*

    Scenario: try_parse_url one argument errors
      When query
      """
      SELECT try_parse_url('https://x.com') AS result
      """
      Then query error .*

    Scenario: try_parse_url four arguments errors
      When query
      """
      SELECT try_parse_url('https://x.com', 'HOST', 'a', 'extra') AS result
      """
      Then query error .*

  Rule: NULL combinatorial - 2 args

    Scenario: try_parse_url NULL url 2-arg
      When query
      """
      SELECT try_parse_url(CAST(NULL AS STRING), 'HOST') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: try_parse_url NULL part 2-arg
      When query
      """
      SELECT try_parse_url('https://x.com', CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: try_parse_url both NULL 2-arg
      When query
      """
      SELECT try_parse_url(CAST(NULL AS STRING), CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: NULL combinatorial - 3 args

    Scenario: try_parse_url NULL url 3-arg
      When query
      """
      SELECT try_parse_url(CAST(NULL AS STRING), 'QUERY', 'a') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: try_parse_url NULL part 3-arg
      When query
      """
      SELECT try_parse_url('https://x.com?a=1', CAST(NULL AS STRING), 'a') AS result
      """
      Then query result
      | result |
      | NULL   |

    @sail-bug
    Scenario: try_parse_url NULL key 3-arg
      When query
      """
      SELECT try_parse_url('https://x.com?a=1', 'QUERY', CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: try_parse_url all NULL 3-arg
      When query
      """
      SELECT try_parse_url(CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING)) AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Invalid URLs return NULL (not error)

    Scenario: try_parse_url just scheme returns NULL
      When query
      """
      SELECT try_parse_url('https://', 'HOST') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: try_parse_url space in URL returns NULL
      When query
      """
      SELECT try_parse_url('https://exam ple.com', 'HOST') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: try_parse_url empty URL returns NULL
      When query
      """
      SELECT try_parse_url('', 'HOST') AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: Edge cases matching parse_url

    @sail-bug
    Scenario: try_parse_url root path returns slash
      When query
      """
      SELECT try_parse_url('https://example.com/', 'PATH') AS result
      """
      Then query result
      | result |
      | /      |

    Scenario: try_parse_url no path returns empty
      When query
      """
      SELECT try_parse_url('https://example.com', 'PATH') AS result
      """
      Then query result
      | result |
      |        |

    @sail-bug
    Scenario: try_parse_url port in authority preserved
      When query
      """
      SELECT try_parse_url('https://example.com:443/path', 'AUTHORITY') AS result
      """
      Then query result
      | result          |
      | example.com:443 |

    @sail-bug
    Scenario: try_parse_url encoded query not decoded
      When query
      """
      SELECT try_parse_url('https://x.com?q=hello%20world', 'QUERY', 'q') AS result
      """
      Then query result
      | result        |
      | hello%20world |

    Scenario: try_parse_url duplicate keys returns first
      When query
      """
      SELECT try_parse_url('https://x.com?a=1&b=2&a=3', 'QUERY', 'a') AS result
      """
      Then query result
      | result |
      | 1      |

    Scenario: try_parse_url empty query value
      When query
      """
      SELECT try_parse_url('https://x.com?a=&b=2', 'QUERY', 'a') AS result
      """
      Then query result
      | result |
      |        |

    @sail-bug
    Scenario: try_parse_url key without value returns NULL
      When query
      """
      SELECT try_parse_url('https://x.com?a&b=2', 'QUERY', 'a') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: try_parse_url invalid part returns NULL
      When query
      """
      SELECT try_parse_url('https://x.com', 'INVALID') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: try_parse_url empty part returns NULL
      When query
      """
      SELECT try_parse_url('https://x.com', '') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: try_parse_url lowercase part returns NULL
      When query
      """
      SELECT try_parse_url('https://x.com', 'host') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: try_parse_url IPv4 host
      When query
      """
      SELECT try_parse_url('http://192.168.1.1:80/path', 'HOST') AS result
      """
      Then query result
      | result      |
      | 192.168.1.1 |

    Scenario: try_parse_url FTP scheme
      When query
      """
      SELECT try_parse_url('ftp://files.example.com/file.txt', 'PROTOCOL') AS result
      """
      Then query result
      | result |
      | ftp    |

  Rule: Multi-row

    Scenario: try_parse_url multi-row HOST extraction
      When query
      """
      SELECT try_parse_url(url, 'HOST') AS result FROM VALUES ('https://a.com'), ('https://b.com'), (NULL), ('notaurl'), ('https://'), ('') AS t(url)
      """
      Then query result
      | result |
      | a.com  |
      | b.com  |
      | NULL   |
      | NULL   |
      | NULL   |
      | NULL   |

  Rule: Error conditions

    Scenario: try_parse_url integer input errors
      When query
      """
      SELECT try_parse_url(42, 'HOST') AS result
      """
      Then query error .*

    Scenario: try_parse_url boolean part errors
      When query
      """
      SELECT try_parse_url('https://x.com', true) AS result
      """
      Then query error .*
