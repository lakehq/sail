Feature: bitmap_construct_agg builds a bitmap from bit positions

  Rule: bitmap_construct_agg sets bits from bitmap_bit_position

    Scenario Outline: bitmap_construct_agg with <case>
      When query
        """
        SELECT substring(hex(bitmap_construct_agg(bitmap_bit_position(col))), 0, 6) AS result
        FROM VALUES <values> AS tab(col)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                   | values         | result |
        | identical input values | (1), (1), (1)  | 010000 |
        | distinct input values  | (1), (2), (3)  | 070000 |
        | larger bit positions   | (1), (9), (17) | 010101 |

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

    Scenario Outline: bitmap_construct_agg <case>
      When query
        """
        SELECT substring(hex(bitmap_construct_agg(bitmap_bit_position(col))), 0, 6) AS result
        FROM VALUES <values> AS tab(col)
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                      | values                                         | result |
        | ignores null input values | (1), (CAST(NULL AS BIGINT)), (3)               | 050000 |
        | with all null inputs      | (CAST(NULL AS BIGINT)), (CAST(NULL AS BIGINT)) | 000000 |
        | supports negative values  | (-1), (-2), (-3)                               | 0E0000 |

    Scenario: bitmap_construct_agg on empty input returns an empty bitmap
      When query
        """
        SELECT substring(hex(bitmap_construct_agg(bitmap_bit_position(col))), 0, 6) AS result
        FROM (SELECT CAST(1 AS BIGINT) AS col WHERE false) AS tab
        """
      Then query result
        | result |
        | 000000 |

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
