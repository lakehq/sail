Feature: parse_url() extracts URL component

  Rule: Argument count validation

    Scenario: parse_url zero arguments errors
      When query
        """
        SELECT parse_url() AS result
        """
      Then query error .*

    Scenario: parse_url one argument errors
      When query
        """
        SELECT parse_url('https://x.com') AS result
        """
      Then query error .*

    Scenario: parse_url four arguments errors
      When query
        """
        SELECT parse_url('https://x.com', 'HOST', 'a', 'extra') AS result
        """
      Then query error .*

  Rule: NULL combinatorial - 2 args

    Scenario: parse_url NULL url and valid part
      When query
        """
        SELECT parse_url(CAST(NULL AS STRING), 'HOST') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url valid url and NULL part
      When query
        """
        SELECT parse_url('https://example.com', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url NULL url and NULL part
      When query
        """
        SELECT parse_url(CAST(NULL AS STRING), CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: NULL combinatorial - 3 args

    Scenario: parse_url NULL url with valid part and key
      When query
        """
        SELECT parse_url(CAST(NULL AS STRING), 'QUERY', 'a') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url valid url with NULL part and key
      When query
        """
        SELECT parse_url('https://x.com?a=1', CAST(NULL AS STRING), 'a') AS result
        """
      Then query result
        | result |
        | NULL   |

    @sail-bug
    # Sail doesn't handle NULL key correctly in 3-arg form
    Scenario: parse_url valid url and part with NULL key
      When query
        """
        SELECT parse_url('https://x.com?a=1', 'QUERY', CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url all three args NULL
      When query
        """
        SELECT parse_url(CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING)) AS result
        """
      Then query result
        | result |
        | NULL   |

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
        | result             |
        | notaurl?key=value  |

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

  Rule: All URL parts from full URL

    Scenario: parse_url extract HOST from full URL
      When query
        """
        SELECT parse_url('https://user:pw@example.com:8080/path?q=1#frag', 'HOST') AS result
        """
      Then query result
        | result      |
        | example.com |

    Scenario: parse_url extract PATH from full URL
      When query
        """
        SELECT parse_url('https://user:pw@example.com:8080/path?q=1#frag', 'PATH') AS result
        """
      Then query result
        | result |
        | /path  |

    Scenario: parse_url extract QUERY from full URL
      When query
        """
        SELECT parse_url('https://user:pw@example.com:8080/path?q=1#frag', 'QUERY') AS result
        """
      Then query result
        | result |
        | q=1    |

    Scenario: parse_url extract REF from full URL
      When query
        """
        SELECT parse_url('https://user:pw@example.com:8080/path?q=1#frag', 'REF') AS result
        """
      Then query result
        | result |
        | frag   |

    Scenario: parse_url extract PROTOCOL from full URL
      When query
        """
        SELECT parse_url('https://user:pw@example.com:8080/path?q=1#frag', 'PROTOCOL') AS result
        """
      Then query result
        | result |
        | https  |

    Scenario: parse_url extract FILE from full URL
      When query
        """
        SELECT parse_url('https://user:pw@example.com:8080/path?q=1#frag', 'FILE') AS result
        """
      Then query result
        | result    |
        | /path?q=1 |

    Scenario: parse_url extract AUTHORITY from full URL
      When query
        """
        SELECT parse_url('https://user:pw@example.com:8080/path?q=1#frag', 'AUTHORITY') AS result
        """
      Then query result
        | result                   |
        | user:pw@example.com:8080 |

    Scenario: parse_url extract USERINFO from full URL
      When query
        """
        SELECT parse_url('https://user:pw@example.com:8080/path?q=1#frag', 'USERINFO') AS result
        """
      Then query result
        | result  |
        | user:pw |

  Rule: Missing URL parts

    Scenario: parse_url no query returns NULL
      When query
        """
        SELECT parse_url('https://example.com/path', 'QUERY') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url no ref returns NULL
      When query
        """
        SELECT parse_url('https://example.com/path', 'REF') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url no userinfo returns NULL
      When query
        """
        SELECT parse_url('https://example.com/path', 'USERINFO') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url no path returns empty string
      When query
        """
        SELECT parse_url('https://example.com', 'PATH') AS result
        """
      Then query result
        | result |
        |        |

    @sail-bug
    # Sail returns empty string instead of "/" for root path
    Scenario: parse_url root path returns slash
      When query
        """
        SELECT parse_url('https://example.com/', 'PATH') AS result
        """
      Then query result
        | result |
        | /      |

  Rule: Special URLs

    Scenario: parse_url IPv4 host
      When query
        """
        SELECT parse_url('http://192.168.1.1:80/path', 'HOST') AS result
        """
      Then query result
        | result      |
        | 192.168.1.1 |

    @sail-bug
    # Sail returns different authority format with port
    Scenario: parse_url port in authority
      When query
        """
        SELECT parse_url('https://example.com:443/path', 'AUTHORITY') AS result
        """
      Then query result
        | result          |
        | example.com:443 |

    @sail-bug
    # Sail decodes %20 to space but Spark returns raw encoded value
    Scenario: parse_url encoded query value not decoded
      When query
        """
        SELECT parse_url('https://x.com?q=hello%20world', 'QUERY', 'q') AS result
        """
      Then query result
        | result       |
        | hello%20world |

    Scenario: parse_url duplicate query keys returns first value
      When query
        """
        SELECT parse_url('https://x.com?a=1&b=2&a=3', 'QUERY', 'a') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: parse_url empty query value
      When query
        """
        SELECT parse_url('https://x.com?a=&b=2', 'QUERY', 'a') AS result
        """
      Then query result
        | result |
        |        |

    @sail-bug
    # Sail returns empty string instead of NULL for key without = sign
    Scenario: parse_url query key without value returns NULL
      When query
        """
        SELECT parse_url('https://x.com?a&b=2', 'QUERY', 'a') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url FTP scheme
      When query
        """
        SELECT parse_url('ftp://files.example.com/file.txt', 'PROTOCOL') AS result
        """
      Then query result
        | result |
        | ftp    |

    Scenario: parse_url just scheme errors (invalid URL)
      When query
        """
        SELECT parse_url('https://', 'HOST') AS result
        """
      Then query error .*

  Rule: Part name edge cases

    Scenario: parse_url invalid part returns NULL
      When query
        """
        SELECT parse_url('https://example.com', 'INVALID') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url empty part returns NULL
      When query
        """
        SELECT parse_url('https://example.com', '') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url lowercase part returns NULL (case sensitive)
      When query
        """
        SELECT parse_url('https://example.com', 'host') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: Multi-row

    Scenario: parse_url multi-row HOST extraction
      When query
        """
        SELECT parse_url(url, 'HOST') AS result FROM VALUES ('https://a.com'), ('https://b.com'), (NULL), ('notaurl') AS t(url)
        """
      Then query result
        | result |
        | a.com  |
        | b.com  |
        | NULL   |
        | NULL   |

  Rule: Error conditions

    Scenario: parse_url integer URL errors
      When query
        """
        SELECT parse_url(42, 'HOST') AS result
        """
      Then query error .*

    Scenario: parse_url boolean part errors
      When query
        """
        SELECT parse_url('https://example.com', true) AS result
        """
      Then query error .*
