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

    Scenario: parse_url port in authority
      When query
        """
        SELECT parse_url('https://example.com:443/path', 'AUTHORITY') AS result
        """
      Then query result
        | result          |
        | example.com:443 |

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

  Rule: QUERY key edge cases

    Scenario: parse_url QUERY key-only param without equals returns NULL
      When query
        """
        SELECT parse_url('http://ex.com?keyonly', 'QUERY', 'keyonly') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url QUERY key with empty value returns empty string
      When query
        """
        SELECT parse_url('http://ex.com?key=', 'QUERY', 'key') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: parse_url QUERY key search is literal not percent-decoded
      When query
        """
        SELECT parse_url('http://ex.com?a%20b=1', 'QUERY', 'a b') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url QUERY key search matches encoded form exactly
      When query
        """
        SELECT parse_url('http://ex.com?a%20b=1', 'QUERY', 'a%20b') AS result
        """
      Then query result
        | result |
        | 1      |

    Scenario: parse_url QUERY duplicate keys returns first occurrence
      When query
        """
        SELECT parse_url('http://ex.com?a=1&a=2', 'QUERY', 'a') AS result
        """
      Then query result
        | result |
        | 1      |

  Rule: FILE edge cases

    Scenario: parse_url FILE no path only query
      When query
        """
        SELECT parse_url('http://example.com?foo=bar', 'FILE') AS result
        """
      Then query result
        | result   |
        | ?foo=bar |

    Scenario: parse_url FILE no path no query with fragment returns empty string
      When query
        """
        SELECT parse_url('http://example.com#frag', 'FILE') AS result
        """
      Then query result
        | result |
        |        |

    Scenario: parse_url FILE trailing question mark root path
      When query
        """
        SELECT parse_url('http://ex.com/?', 'FILE') AS result
        """
      Then query result
        | result |
        | /?     |

    Scenario: parse_url FILE trailing question mark no path
      When query
        """
        SELECT parse_url('http://ex.com?', 'FILE') AS result
        """
      Then query result
        | result |
        | ?      |

    Scenario: parse_url FILE no path no query returns empty string
      When query
        """
        SELECT parse_url('http://example.com', 'FILE') AS result
        """
      Then query result
        | result |
        |        |

  Rule: PATH percent-encoding preserved

    Scenario: parse_url PATH preserves percent-encoding
      When query
        """
        SELECT parse_url('https://ex.com/dir%20/pa%20th.HTML', 'PATH') AS result
        """
      Then query result
        | result                  |
        | /dir%20/pa%20th.HTML   |

  Rule: AUTHORITY port handling

    Scenario: parse_url AUTHORITY includes explicit port even if default for scheme
      When query
        """
        SELECT parse_url('https://example.com:443/path', 'AUTHORITY') AS result,
               parse_url('http://example.com:80/path', 'AUTHORITY') AS result2
        """
      Then query result
        | result              | result2           |
        | example.com:443     | example.com:80    |

    Scenario: parse_url AUTHORITY includes userinfo and non-default port
      When query
        """
        SELECT parse_url('ftp://user:pwd@ftp.example.com:21/files?x=1#frag', 'AUTHORITY') AS result
        """
      Then query result
        | result                        |
        | user:pwd@ftp.example.com:21   |

  Rule: Authority-less URLs

    Scenario: parse_url file colon slash PATH returns slash
      When query
        """
        SELECT parse_url('file:/', 'PATH') AS result
        """
      Then query result
        | result |
        | /      |

    Scenario: parse_url file triple slash PATH returns path
      When query
        """
        SELECT parse_url('file:///etc/hosts', 'PATH') AS result
        """
      Then query result
        | result     |
        | /etc/hosts |

    Scenario: parse_url file colon slash HOST returns NULL
      When query
        """
        SELECT parse_url('file:/', 'HOST') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url custom authority-less scheme PATH
      When query
        """
        SELECT parse_url('custom:/root/path', 'PATH') AS result
        """
      Then query result
        | result     |
        | /root/path |

    Scenario: parse_url file triple slash HOST returns NULL
      When query
        """
        SELECT parse_url('file:///etc/hosts', 'HOST') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url file triple slash AUTHORITY returns NULL
      When query
        """
        SELECT parse_url('file:///etc/hosts', 'AUTHORITY') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url custom authority-less scheme QUERY
      When query
        """
        SELECT parse_url('custom:/root/path?q=1', 'QUERY') AS result
        """
      Then query result
        | result |
        | q=1    |

    Scenario: parse_url custom authority-less scheme REF
      When query
        """
        SELECT parse_url('custom:/root/path#frag', 'REF') AS result
        """
      Then query result
        | result |
        | frag   |

  Rule: Opaque URIs

    Scenario: parse_url opaque mailto PATH returns NULL
      When query
        """
        SELECT parse_url('mailto:user@example.com', 'PATH') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url opaque mailto HOST returns NULL
      When query
        """
        SELECT parse_url('mailto:user@example.com', 'HOST') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url opaque mailto PROTOCOL returns scheme
      When query
        """
        SELECT parse_url('mailto:user@example.com', 'PROTOCOL') AS result
        """
      Then query result
        | result |
        | mailto |

    Scenario: parse_url opaque tel PATH returns NULL
      When query
        """
        SELECT parse_url('tel:+1-816-555-1212', 'PATH') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url opaque urn PATH returns NULL
      When query
        """
        SELECT parse_url('urn:isbn:0451450523', 'PATH') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url opaque mailto FILE returns NULL
      When query
        """
        SELECT parse_url('mailto:user@example.com', 'FILE') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url opaque tel FILE returns NULL
      When query
        """
        SELECT parse_url('tel:+1-816-555-1212', 'FILE') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url opaque mailto QUERY returns NULL
      When query
        """
        SELECT parse_url('mailto:user@example.com?subject=hello', 'QUERY') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url opaque mailto QUERY with key returns NULL
      When query
        """
        SELECT parse_url('mailto:user@example.com?subject=hello', 'QUERY', 'subject') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url opaque urn REF returns fragment
      When query
        """
        SELECT parse_url('urn:isbn:0451450523#section', 'REF') AS result
        """
      Then query result
        | result  |
        | section |

  Rule: Percent-encoded host

    Scenario: parse_url percent-encoded host returns NULL
      When query
        """
        SELECT parse_url('http://ex%61mple.com/path', 'HOST') AS result
        """
      Then query result
        | result |
        | NULL   |

  Rule: IPv6

    Scenario: parse_url IPv6 HOST returns bracketed form
      When query
        """
        SELECT parse_url('http://[::1]/path', 'HOST') AS result
        """
      Then query result
        | result |
        | [::1]  |

    Scenario: parse_url IPv6 with port AUTHORITY
      When query
        """
        SELECT parse_url('http://[::1]:8080/path', 'AUTHORITY') AS result
        """
      Then query result
        | result       |
        | [::1]:8080   |

  Rule: Non-ASCII characters

    Scenario: parse_url PATH preserves non-ASCII unicode characters
      When query
        """
        SELECT parse_url('http://example.com/über/path', 'PATH') AS result
        """
      Then query result
        | result      |
        | /über/path  |

    Scenario: parse_url PATH preserves non-ASCII accented characters
      When query
        """
        SELECT parse_url('http://example.com/café', 'PATH') AS result
        """
      Then query result
        | result  |
        | /café   |

    Scenario: parse_url PATH preserves non-ASCII CJK characters
      When query
        """
        SELECT parse_url('http://example.com/中文/path', 'PATH') AS result
        """
      Then query result
        | result     |
        | /中文/path  |

    Scenario: parse_url PATH preserves mix of non-ASCII and percent-encoded
      When query
        """
        SELECT parse_url('http://example.com/über%20path', 'PATH') AS result
        """
      Then query result
        | result         |
        | /über%20path   |

    Scenario: parse_url PATH preserves percent-encoded bytes without decoding
      When query
        """
        SELECT parse_url('http://example.com/%C3%BCber/path', 'PATH') AS result
        """
      Then query result
        | result            |
        | /%C3%BCber/path   |

    Scenario: parse_url HOST returns NULL for non-ASCII internationalized domain
      When query
        """
        SELECT parse_url('http://münchen.de/path', 'HOST') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url AUTHORITY returns non-ASCII internationalized domain as-is
      When query
        """
        SELECT parse_url('http://münchen.de/path', 'AUTHORITY') AS result
        """
      Then query result
        | result     |
        | münchen.de |

    Scenario: parse_url non-ASCII host PATH and other parts still extractable
      When query
        """
        SELECT parse_url('http://münchen.de/path', 'PROTOCOL') AS r1,
               parse_url('http://münchen.de/path', 'PATH') AS r2,
               parse_url('http://münchen.de/path?q=1', 'QUERY') AS r3,
               parse_url('http://münchen.de/path', 'HOST') AS r4
        """
      Then query result
        | r1   | r2    | r3  | r4   |
        | http | /path | q=1 | NULL |

    Scenario: parse_url HOST returns NULL for Japanese internationalized domain
      When query
        """
        SELECT parse_url('http://例え.jp/path', 'HOST') AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: parse_url AUTHORITY returns Japanese IDN as-is
      When query
        """
        SELECT parse_url('http://例え.jp/path', 'AUTHORITY') AS result
        """
      Then query result
        | result  |
        | 例え.jp  |

    Scenario: parse_url QUERY preserves non-ASCII value
      When query
        """
        SELECT parse_url('http://example.com/path?q=über', 'QUERY') AS result
        """
      Then query result
        | result  |
        | q=über  |

    Scenario: parse_url QUERY key lookup returns non-ASCII value
      When query
        """
        SELECT parse_url('http://example.com/path?q=über', 'QUERY', 'q') AS result
        """
      Then query result
        | result |
        | über   |

    Scenario: parse_url QUERY key lookup returns CJK value
      When query
        """
        SELECT parse_url('http://example.com/path?q=中文', 'QUERY', 'q') AS result
        """
      Then query result
        | result |
        | 中文   |

    Scenario: parse_url QUERY key lookup with non-ASCII key
      When query
        """
        SELECT parse_url('http://example.com/?über=value', 'QUERY', 'über') AS result
        """
      Then query result
        | result |
        | value  |

    Scenario: parse_url REF preserves non-ASCII fragment
      When query
        """
        SELECT parse_url('http://example.com/path#über', 'REF') AS result
        """
      Then query result
        | result |
        | über   |

    Scenario: parse_url USERINFO preserves non-ASCII username
      When query
        """
        SELECT parse_url('http://über:pass@example.com/', 'USERINFO') AS result
        """
      Then query result
        | result     |
        | über:pass  |

    Scenario: parse_url AUTHORITY includes non-ASCII userinfo
      When query
        """
        SELECT parse_url('http://über:pass@example.com/', 'AUTHORITY') AS result
        """
      Then query result
        | result                  |
        | über:pass@example.com   |
