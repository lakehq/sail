Feature: regexp_replace() replaces regular expression matches

  Rule: Capture replacement

    Scenario: extract a URL domain with an anchored capture
      When query
      """
      SELECT regexp_replace(
        'https://www.example.com/path/to/page',
        r'^https?://(?:www\.)?([^/]+)/.*$',
        '$1'
      ) AS result
      """
      Then query result
      | result      |
      | example.com |

  Rule: Global replacement

    Scenario: replace every unanchored match
      When query
      """
      SELECT regexp_replace('100-200', r'(\d+)', 'num') AS result
      """
      Then query result
      | result  |
      | num-num |
