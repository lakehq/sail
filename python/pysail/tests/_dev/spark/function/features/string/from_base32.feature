# SPDX-License-Identifier: Apache-2.0

@spark-4.3
Feature: from_base32() decodes an RFC 4648 base32 string into binary

  Rule: Basic usage

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT hex(from_base32(<input>)) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                   | input              | result       |
        | padded input           | 'MZXW6YTBOI======' | 666F6F626172 |
        | one byte               | 'MY======'         | 66           |
        | two bytes              | 'MZXQ===='         | 666F         |
        | three bytes            | 'MZXW6==='         | 666F6F       |
        | four bytes             | 'MZXW6YQ='         | 666F6F62     |
        | five bytes             | 'MZXW6YTB'         | 666F6F6261   |
        | empty string           | ''                 |              |
        | lowercase input        | 'mzxw6ytboi======' | 666F6F626172 |
        | missing padding        | 'MZXW6YTBOI'       | 666F6F626172 |

  Rule: Null propagation

    Scenario Outline: Null propagation: <case>
      When query
        """
        SELECT from_base32(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                 | input                |
        | null string          | CAST(NULL AS STRING) |
        | untyped null literal | NULL                 |

  Rule: Column expressions

    Scenario: from_base32 on column values keeps nulls
      When query
        """
        SELECT decode(from_base32(value), 'UTF-8') AS result
        FROM VALUES ('MY======'), (CAST(NULL AS STRING)), ('MZXW6YTB') AS data(value)
        ORDER BY value IS NULL, value
        """
      Then query result
        | result |
        | f      |
        | fooba  |
        | NULL   |
