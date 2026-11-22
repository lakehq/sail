# SPDX-License-Identifier: Apache-2.0

Feature: to_base32() encodes a binary value as an RFC 4648 base32 string

  Rule: Basic usage

    Scenario Outline: Basic: <case>
      When query
        """
        SELECT to_base32(<input>) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                   | input           | result           |
        | string input           | 'foobar'        | MZXW6YTBOI====== |
        | binary input           | x'666f6f626172' | MZXW6YTBOI====== |
        | empty string           | ''              |                  |
        | one byte is padded     | 'f'             | MY======         |
        | two bytes are padded   | 'fo'            | MZXQ====         |
        | three bytes are padded | 'foo'           | MZXW6===         |
        | four bytes are padded  | 'foob'          | MZXW6YQ=         |
        | five bytes need none   | 'fooba'         | MZXW6YTB         |

  Rule: Null propagation

    Scenario Outline: Null propagation: <case>
      When query
        """
        SELECT to_base32(<input>) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                 | input                |
        | null string          | CAST(NULL AS STRING) |
        | null binary          | CAST(NULL AS BINARY) |
        | untyped null literal | NULL                 |

  Rule: Column expressions

    Scenario: to_base32 on column values keeps nulls
      When query
        """
        SELECT to_base32(value) AS result
        FROM VALUES ('f'), (CAST(NULL AS STRING)), ('fooba') AS data(value)
        ORDER BY value IS NULL, value
        """
      Then query result
        | result   |
        | MY====== |
        | MZXW6YTB |
        | NULL     |

  Rule: Round trip

    Scenario: from_base32 reverses to_base32
      When query
        """
        SELECT decode(from_base32(to_base32('Spark SQL')), 'UTF-8') AS result
        """
      Then query result
        | result    |
        | Spark SQL |
