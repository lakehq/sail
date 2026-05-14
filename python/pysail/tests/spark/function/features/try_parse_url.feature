@try_parse_url
Feature: try_parse_url function

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

  Rule: Basic URL parts extraction

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

    Scenario: try_parse_url extracts AUTHORITY
      When query
      """
      SELECT try_parse_url('https://user:pass@spark.apache.org:8080/path', 'AUTHORITY') AS result
      """
      Then query result
      | result                          |
      | user:pass@spark.apache.org:8080 |

    Scenario: try_parse_url extracts USERINFO
      When query
      """
      SELECT try_parse_url('https://user:pass@spark.apache.org/path', 'USERINFO') AS result
      """
      Then query result
      | result    |
      | user:pass |

    Scenario: try_parse_url extracts REF
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

  Rule: QUERY with specific key

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

  Rule: Invalid URLs return NULL (not error)

    Scenario: try_parse_url with invalid URL returns NULL
      When query
      """
      SELECT try_parse_url('not_a_url', 'HOST') AS result
      """
      Then query result
      | result |
      | NULL   |

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

    Scenario: try_parse_url port in authority preserved
      When query
      """
      SELECT try_parse_url('https://example.com:443/path', 'AUTHORITY') AS result
      """
      Then query result
      | result          |
      | example.com:443 |

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
