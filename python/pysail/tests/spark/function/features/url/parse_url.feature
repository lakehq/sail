@parse_url
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
        | result    |
        | /path?q=1 |

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

    Scenario: parse_url schemeless URL with query extracts PATH without query
      When query
        """
        SELECT parse_url('notaurl?key=value', 'PATH') AS result
        """
      Then query result
        | result  |
        | notaurl |

    Scenario: parse_url schemeless URL with query extracts FILE with query
      When query
        """
        SELECT parse_url('notaurl?key=value', 'FILE') AS result
        """
      Then query result
        | result            |
        | notaurl?key=value |

    Scenario: parse_url schemeless URL extracts QUERY
      When query
        """
        SELECT parse_url('notaurl?key=value', 'QUERY') AS result
        """
      Then query result
        | result    |
        | key=value |

    Scenario: parse_url schemeless URL extracts QUERY with key
      When query
        """
        SELECT parse_url('notaurl?a=1&b=2', 'QUERY', 'b') AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: parse_url schemeless URL extracts REF
      When query
        """
        SELECT parse_url('notaurl#reference', 'REF') AS result
        """
      Then query result
        | result    |
        | reference |

    Scenario: parse_url schemeless URL with query and fragment
      When query
        """
        SELECT parse_url('page?q=1#frag', 'PATH') AS r1,
               parse_url('page?q=1#frag', 'QUERY') AS r2,
               parse_url('page?q=1#frag', 'REF') AS r3,
               parse_url('page?q=1#frag', 'FILE') AS r4
        """
      Then query result
        | r1   | r2  | r3   | r4       |
        | page | q=1 | frag | page?q=1 |

    Scenario: parse_url schemeless URL with path segments
      When query
        """
        SELECT parse_url('a/b/c?q=1', 'PATH') AS r1,
               parse_url('a/b/c?q=1', 'QUERY') AS r2,
               parse_url('a/b/c?q=1', 'FILE') AS r3
        """
      Then query result
        | r1    | r2  | r3        |
        | a/b/c | q=1 | a/b/c?q=1 |

    Scenario: parse_url schemeless URL HOST AUTHORITY USERINFO are NULL
      When query
        """
        SELECT parse_url('notaurl?q=1', 'HOST') AS r1,
               parse_url('notaurl?q=1', 'AUTHORITY') AS r2,
               parse_url('notaurl?q=1', 'USERINFO') AS r3,
               parse_url('notaurl?q=1', 'PROTOCOL') AS r4
        """
      Then query result
        | r1   | r2   | r3   | r4   |
        | NULL | NULL | NULL | NULL |

    Scenario: parse_url schemeless URL with only query
      When query
        """
        SELECT parse_url('?key=value', 'QUERY') AS r1,
               parse_url('?key=value', 'QUERY', 'key') AS r2,
               parse_url('?key=value', 'PATH') AS r3
        """
      Then query result
        | r1        | r2    | r3 |
        | key=value | value |    |

    Scenario: parse_url schemeless URL with only fragment
      When query
        """
        SELECT parse_url('#frag', 'REF') AS r1,
               parse_url('#frag', 'PATH') AS r2
        """
      Then query result
        | r1   | r2 |
        | frag |    |

    Scenario: parse_url schemeless URL multiple query params
      When query
        """
        SELECT parse_url('page?a=1&b=2&c=3', 'QUERY') AS r1,
               parse_url('page?a=1&b=2&c=3', 'QUERY', 'a') AS r2,
               parse_url('page?a=1&b=2&c=3', 'QUERY', 'c') AS r3,
               parse_url('page?a=1&b=2&c=3', 'QUERY', 'missing') AS r4
        """
      Then query result
        | r1          | r2 | r3 | r4   |
        | a=1&b=2&c=3 | 1  | 3  | NULL |

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

  @spark_null
  Rule: Output schema

    Scenario: a non-null url literal yields a string
      When query
        """
        SELECT parse_url('http://a.com/p', 'HOST') AS result
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a non-null url column yields a string
      When query
        """
        SELECT parse_url(CONCAT('http://a', CAST(id AS STRING), '.com'), 'HOST') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

    Scenario: a nullable url column stays nullable
      When query
        """
        SELECT parse_url(c, 'HOST') AS result FROM VALUES ('http://a.com'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: string (nullable = true)
        """

  Rule: Result values (migrated from test_parse_url.txt doctests)

    Scenario: parse_url doctest #1 (result)
      When query
        """
        SELECT parse_url('https://example.com/a?x=1', 'QUERY', 'x') AS result, typeof(parse_url('https://example.com/a?x=1', 'QUERY', 'x')) AS type
        """
      Then query result
        | result | type   |
        | 1      | string |

    Scenario: parse_url doctest #2 (result)
      When query
        """
        SELECT parse_url('https://example.com/path#frag', 'REF') AS result, typeof(parse_url('https://example.com/path#frag', 'REF')) AS type
        """
      Then query result
        | result | type   |
        | frag   | string |

    Scenario: parse_url doctest #3 (result)
      When query
        """
        SELECT parse_url('ftp://user:pwd@ftp.example.com:21/files', 'USERINFO') AS result, typeof(parse_url('ftp://user:pwd@ftp.example.com:21/files', 'USERINFO')) AS type
        """
      Then query result
        | result   | type   |
        | user:pwd | string |

    Scenario: parse_url doctest #4 (result)
      When query
        """
        SELECT parse_url('http://[2001:db8::2]:8080/index.html?ok=1', 'HOST') AS result, typeof(parse_url('http://[2001:db8::2]:8080/index.html?ok=1', 'HOST')) AS type
        """
      Then query result
        | result        | type   |
        | [2001:db8::2] | string |

    Scenario: parse_url doctest #5 (result)
      When query
        """
        SELECT parse_url('https://example.com', 'PATH') AS result, typeof(parse_url('https://example.com', 'PATH')) AS type
        """
      Then query result
        | result | type   |
        |        | string |

    Scenario: parse_url doctest #6 (result)
      When query
        """
        SELECT parse_url('https://example.com/a/b?x=1&y=2#frag', 'PROTOCOL') AS result, typeof(parse_url('https://example.com/a/b?x=1&y=2#frag', 'PROTOCOL')) AS type
        """
      Then query result
        | result | type   |
        | https  | string |

    Scenario: parse_url doctest #7 (result)
      When query
        """
        select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'HOST')
        """
      Then query result
        | parse_url(http://userinfo@spark.apache.org/path?query=1#Ref, HOST) |
        | spark.apache.org                                                   |

    Scenario: parse_url doctest #8 (result)
      When query
        """
        select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'PATH')
        """
      Then query result
        | parse_url(http://userinfo@spark.apache.org/path?query=1#Ref, PATH) |
        | /path                                                              |

    Scenario: parse_url doctest #9 (result)
      When query
        """
        select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'QUERY')
        """
      Then query result
        | parse_url(http://userinfo@spark.apache.org/path?query=1#Ref, QUERY) |
        | query=1                                                             |

    Scenario: parse_url doctest #10 (result)
      When query
        """
        select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'REF')
        """
      Then query result
        | parse_url(http://userinfo@spark.apache.org/path?query=1#Ref, REF) |
        | Ref                                                               |

    Scenario: parse_url doctest #11 (result)
      When query
        """
        select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'PROTOCOL')
        """
      Then query result
        | parse_url(http://userinfo@spark.apache.org/path?query=1#Ref, PROTOCOL) |
        | http                                                                   |

    Scenario: parse_url doctest #12 (result)
      When query
        """
        select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'FILE')
        """
      Then query result
        | parse_url(http://userinfo@spark.apache.org/path?query=1#Ref, FILE) |
        | /path?query=1                                                      |

    Scenario: parse_url doctest #13 (result)
      When query
        """
        select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'AUTHORITY')
        """
      Then query result
        | parse_url(http://userinfo@spark.apache.org/path?query=1#Ref, AUTHORITY) |
        | userinfo@spark.apache.org                                               |

    Scenario: parse_url doctest #14 (result)
      When query
        """
        select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', 'USERINFO')
        """
      Then query result
        | parse_url(http://userinfo@spark.apache.org/path?query=1#Ref, USERINFO) |
        | userinfo                                                               |
