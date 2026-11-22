# SPDX-License-Identifier: Apache-2.0

Feature: jaro_winkler_similarity() returns the Jaro-Winkler similarity of two strings

  Rule: Basic usage

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT jaro_winkler_similarity(<left>, <right>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                         | left      | right     | result             |
        | no characters in common      | 'ABC'     | 'XYZ'     | 0.0                |
        | transposed characters        | 'MARTHA'  | 'MARHTA'  | 0.9611111111111111 |
        | no common prefix, no boost   | 'kitten'  | 'sitting' | 0.746031746031746  |
        | identical strings            | 'Spark'   | 'Spark'   | 1.0                |
        | both strings empty           | ''        | ''        | 1.0                |
        | one string empty             | 'abc'     | ''        | 0.0                |
        | comparison is case sensitive | 'abc'     | 'ABC'     | 0.0                |

  Rule: Null propagation

    Scenario Outline: Null propagation: <case>
      When query
        """
        SELECT jaro_winkler_similarity(<left>, <right>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                | left                 | right                |
        | null on the left    | CAST(NULL AS STRING) | 'abc'                |
        | null on the right   | 'abc'                | CAST(NULL AS STRING) |
        | null on both sides  | CAST(NULL AS STRING) | CAST(NULL AS STRING) |

  Rule: Column expressions

    Scenario: jaro_winkler_similarity on column values
      When query
        """
        SELECT jaro_winkler_similarity(a, b) AS result
        FROM VALUES ('MARTHA', 'MARHTA'), ('ABC', 'XYZ'), ('abc', CAST(NULL AS STRING)) AS t(a, b)
        ORDER BY a DESC
        """
      Then query result
        | result             |
        | NULL               |
        | 0.9611111111111111 |
        | 0.0                |
