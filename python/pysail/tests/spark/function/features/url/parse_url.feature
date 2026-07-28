@parse_url
Feature: parse_url() extracts URL component

  Rule: Basic usage

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT parse_url(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | args                                        | result      |
        | parse_url host        | 'https://example.com:8080/path?q=1', 'HOST' | example.com |
        | parse_url path        | 'https://example.com/path', 'PATH'          | /path       |
        | parse_url query       | 'https://example.com?a=1&b=2', 'QUERY'      | a=1&b=2     |
        | parse_url query param | 'https://example.com?a=1&b=2', 'QUERY', 'b' | 2           |
        | parse_url protocol    | 'https://example.com/path', 'PROTOCOL'      | https       |

  Rule: Additional parts

    Scenario Outline: Additional part: <case>
      When query
        """
        SELECT parse_url(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                | args                                                   | result                     |
        | parse_url file      | 'https://example.com/path?q=1', 'FILE'                 | /path?q=1                  |
        | parse_url authority | 'https://user:pass@example.com:8080/path', 'AUTHORITY' | user:pass@example.com:8080 |
        | parse_url userinfo  | 'https://user:pass@example.com/path', 'USERINFO'       | user:pass                  |
        | parse_url ref       | 'https://example.com/path#frag', 'REF'                 | frag                       |

  Rule: Malformed URLs

    Scenario Outline: Schemeless: <case>
      When query
        """
        SELECT parse_url(<args>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                                                            | args                             | result            |
        | parse_url with URL without scheme returns NULL                  | 'notaurl', 'HOST'                | NULL              |
        | parse_url with URL without scheme PATH returns the string       | 'notaurl', 'PATH'                | notaurl           |
        | parse_url with URL without scheme FILE returns the string       | 'notaurl', 'FILE'                | notaurl           |
        | parse_url schemeless URL with query extracts PATH without query | 'notaurl?key=value', 'PATH'      | notaurl           |
        | parse_url schemeless URL with query extracts FILE with query    | 'notaurl?key=value', 'FILE'      | notaurl?key=value |
        | parse_url schemeless URL extracts QUERY                         | 'notaurl?key=value', 'QUERY'     | key=value         |
        | parse_url schemeless URL extracts QUERY with key                | 'notaurl?a=1&b=2', 'QUERY', 'b'  | 2                 |
        | parse_url schemeless URL extracts REF                           | 'notaurl#reference', 'REF'       | reference         |
        | parse_url with URL without scheme PROTOCOL returns NULL         | 'notaurl', 'PROTOCOL'            | NULL              |
        | parse_url with empty string returns NULL                        | '', 'HOST'                       | NULL              |
        | parse_url with invalid part returns NULL                        | 'https://example.com', 'INVALID' | NULL              |

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

  Rule: Null handling

    Scenario Outline: NULL argument: <case>
      When query
        """
        SELECT parse_url(<args>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                | args                                        |
        | parse_url null url  | CAST(NULL AS STRING), 'HOST'                |
        | parse_url null part | 'https://example.com', CAST(NULL AS STRING) |

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

    Scenario Outline: Doctest: <case>
      When query
        """
        SELECT parse_url(<args>) AS result, typeof(parse_url(<args>)) AS type
        """
      Then query result
        | result   | type   |
        | <result> | string |

      Examples:
        | case                          | args                                                  | result        |
        | parse_url doctest #1 (result) | 'https://example.com/a?x=1', 'QUERY', 'x'             | 1             |
        | parse_url doctest #2 (result) | 'https://example.com/path#frag', 'REF'                | frag          |
        | parse_url doctest #3 (result) | 'ftp://user:pwd@ftp.example.com:21/files', 'USERINFO' | user:pwd      |
        | parse_url doctest #4 (result) | 'http://[2001:db8::2]:8080/index.html?ok=1', 'HOST'   | [2001:db8::2] |
        | parse_url doctest #5 (result) | 'https://example.com', 'PATH'                         |               |
        | parse_url doctest #6 (result) | 'https://example.com/a/b?x=1&y=2#frag', 'PROTOCOL'    | https         |

    Scenario Outline: Doctest (derived column name): <case>
      When query
        """
        select parse_url('http://userinfo@spark.apache.org/path?query=1#Ref', <part>)
        """
      Then query result
        | parse_url(http://userinfo@spark.apache.org/path?query=1#Ref, <name>) |
        | <result>                                                             |

      Examples:
        | case                           | part        | name      | result                    |
        | parse_url doctest #7 (result)  | 'HOST'      | HOST      | spark.apache.org          |
        | parse_url doctest #8 (result)  | 'PATH'      | PATH      | /path                     |
        | parse_url doctest #9 (result)  | 'QUERY'     | QUERY     | query=1                   |
        | parse_url doctest #10 (result) | 'REF'       | REF       | Ref                       |
        | parse_url doctest #11 (result) | 'PROTOCOL'  | PROTOCOL  | http                      |
        | parse_url doctest #12 (result) | 'FILE'      | FILE      | /path?query=1             |
        | parse_url doctest #13 (result) | 'AUTHORITY' | AUTHORITY | userinfo@spark.apache.org |
        | parse_url doctest #14 (result) | 'USERINFO'  | USERINFO  | userinfo                  |
