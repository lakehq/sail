Feature: regexp_extract_all() extracts all regex capture group matches from strings

  Rule: Basic extraction with group index

    Scenario: regexp_extract_all with group index 0 returns entire matches
      When query
      """
      SELECT regexp_extract_all('100-200,300-400,500-600', r'(\d+)-(\d+)', 0) AS result
      """
      Then query result
      | result                      |
      | [100-200, 300-400, 500-600] |

    Scenario: regexp_extract_all with group index 1 returns first capture group
      When query
      """
      SELECT regexp_extract_all('100-200,300-400,500-600', r'(\d+)-(\d+)', 1) AS result
      """
      Then query result
      | result          |
      | [100, 300, 500] |

    Scenario: regexp_extract_all with group index 2 returns second capture group
      When query
      """
      SELECT regexp_extract_all('100-200,300-400,500-600', r'(\d+)-(\d+)', 2) AS result
      """
      Then query result
      | result          |
      | [200, 400, 600] |

  Rule: Default group index

    Scenario: regexp_extract_all defaults to group index 1
      When query
      """
      SELECT regexp_extract_all('1a 2b 14m', r'(\d+)([a-z]+)') AS result
      """
      Then query result
      | result     |
      | [1, 2, 14] |

    Scenario: regexp_extract_all without idx renders the synthesized idx=1 in the column name
      When query
      """
      SELECT regexp_extract_all('1a 2b 14m', r'([0-9]+)([a-z]+)')
      """
      Then query result
      | regexp_extract_all(1a 2b 14m, ([0-9]+)([a-z]+), 1) |
      | [1, 2, 14]                                         |

  Rule: No match and edge cases

    Scenario: regexp_extract_all returns empty array when no match
      When query
      """
      SELECT regexp_extract_all('foo', r'(\d+)', 1) AS result
      """
      Then query result
      | result |
      | []     |

    Scenario: regexp_extract_all returns empty strings for unmatched optional groups
      When query
      """
      SELECT to_json(regexp_extract_all('aaaac aaabc', r'(a+)(b)?(c)', 2)) AS result
      """
      Then query result
      | result   |
      | ["","b"] |

  Rule: NULL handling

    Scenario: regexp_extract_all returns NULL when input is NULL
      When query
      """
      SELECT regexp_extract_all(NULL, r'(\d+)', 1) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: regexp_extract_all returns NULL when pattern is NULL
      When query
      """
      SELECT regexp_extract_all('abc', NULL, 1) AS result
      """
      Then query result
      | result |
      | NULL   |
