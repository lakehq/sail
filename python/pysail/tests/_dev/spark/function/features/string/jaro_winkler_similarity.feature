# SPDX-License-Identifier: Apache-2.0

@spark-4.3
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

  Rule: Spark-specific behavior
    # Values recorded from Spark 4.3.0. Spark applies the Winkler prefix boost to every non-zero
    # Jaro score (Apache Commons Text only does above 0.7) and compares UTF-16 code units, so a
    # supplementary character such as an emoji counts as two units.

    Scenario Outline: Spark-specific: <case>
      When query
        """
        SELECT jaro_winkler_similarity(<left>, <right>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                              | left       | right      | result             |
        | boost applied below 0.7 threshold | 'ab'       | 'ac'       | 0.7                |
        | shared prefix of three            | 'abc'      | 'abd'      | 0.8222222222222222 |
        | prefix capped at four             | 'abcd'     | 'abxy'     | 0.7333333333333333 |
        | different lengths                 | 'DIXON'    | 'DICKSONX' | 0.8133333333333332 |
        | different lengths swapped         | 'DICKSONX' | 'DIXON'    | 0.8133333333333332 |
        | only transposed characters        | 'ab'       | 'ba'       | 0.0                |
        | accented character                | 'café'     | 'cafe'     | 0.8833333333333334 |
        | emoji counts as two code units    | '😀a'      | '😀b'      | 0.8222222222222222 |
        | different emoji                   | '😀'       | '😁'       | 0.7                |
        | emoji in the middle               | 'a😀b'     | 'a😁b'     | 0.8666666666666667 |

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
