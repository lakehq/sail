Feature: bitmap_construct_agg builds a bitmap from bit positions

  Rule: bitmap_construct_agg sets bits from bitmap_bit_position

    Scenario: bitmap_construct_agg with identical input values
      When query
        """
        SELECT substring(hex(bitmap_construct_agg(bitmap_bit_position(col))), 0, 6) AS result
        FROM VALUES (1), (1), (1) AS tab(col)
        """
      Then query result
        | result |
        | 010000 |

    Scenario: bitmap_construct_agg with distinct input values
      When query
        """
        SELECT substring(hex(bitmap_construct_agg(bitmap_bit_position(col))), 0, 6) AS result
        FROM VALUES (1), (2), (3) AS tab(col)
        """
      Then query result
        | result |
        | 070000 |

    Scenario: bitmap_construct_agg with larger bit positions
      When query
        """
        SELECT substring(hex(bitmap_construct_agg(bitmap_bit_position(col))), 0, 6) AS result
        FROM VALUES (1), (9), (17) AS tab(col)
        """
      Then query result
        | result |
        | 010101 |

  Rule: bitmap_construct_agg returns a binary value

    Scenario: bitmap_construct_agg returns binary type
      When query
        """
        SELECT typeof(bitmap_construct_agg(bitmap_bit_position(col))) AS result
        FROM VALUES (1) AS tab(col)
        """
      Then query result
        | result |
        | binary |

  Rule: bitmap_construct_agg handles null values

    Scenario: bitmap_construct_agg ignores null input values
      When query
        """
        SELECT substring(hex(bitmap_construct_agg(bitmap_bit_position(col))), 0, 6) AS result
        FROM VALUES (1), (CAST(NULL AS BIGINT)), (3) AS tab(col)
        """
      Then query result
        | result |
        | 050000 |

    Scenario: bitmap_construct_agg with all null inputs
      When query
        """
        SELECT substring(hex(bitmap_construct_agg(bitmap_bit_position(col))), 0, 6) AS result
        FROM VALUES (CAST(NULL AS BIGINT)), (CAST(NULL AS BIGINT)) AS tab(col)
        """
      Then query result
        | result |
        | 000000 |

    Scenario: bitmap_construct_agg on empty input returns an empty bitmap
      When query
        """
        SELECT substring(hex(bitmap_construct_agg(bitmap_bit_position(col))), 0, 6) AS result
        FROM (SELECT CAST(1 AS BIGINT) AS col WHERE false) AS tab
        """
      Then query result
        | result |
        | 000000 |

    Scenario: bitmap_construct_agg supports negative input values
      When query
        """
        SELECT substring(hex(bitmap_construct_agg(bitmap_bit_position(col))), 0, 6) AS result
        FROM VALUES (-1), (-2), (-3) AS tab(col)
        """
      Then query result
        | result |
        | 0E0000 |

  Rule: bitmap_count can count bits in bitmap_construct_agg output

    Scenario: bitmap_count of bitmap_construct_agg output
      When query
        """
        SELECT bitmap_count(bitmap_construct_agg(bitmap_bit_position(col))) AS result
        FROM VALUES (1), (2), (3) AS tab(col)
        """
      Then query result
        | result |
        | 3      |

  Rule: bitmap_construct_agg as a window function

    Scenario: bitmap_construct_agg over window
      When query
        """
        SELECT
          id,
          substring(
            hex(bitmap_construct_agg(bitmap_bit_position(col)) OVER (
              ORDER BY id
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )),
            0,
            6
          ) AS result
        FROM VALUES (1, 1), (2, 2), (3, 3) AS tab(id, col)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 010000 |
        | 2  | 030000 |
        | 3  | 070000 |
