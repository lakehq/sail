Feature: bitmap_or_agg returns the bitwise OR of all input bitmaps

  Rule: bitmap_or_agg ORs identical bitmaps

    Scenario: bitmap_or_agg with identical input values
      When query
        """
        SELECT substring(hex(bitmap_or_agg(col)), 0, 6) AS result
        FROM VALUES (X'10'), (X'10'), (X'10') AS tab(col)
        """
      Then query result
        | result |
        | 100000 |

  Rule: bitmap_or_agg ORs distinct bitmaps

    Scenario: bitmap_or_agg with distinct input values
      When query
        """
        SELECT substring(hex(bitmap_or_agg(col)), 0, 6) AS result
        FROM VALUES (X'10'), (X'20'), (X'40') AS tab(col)
        """
      Then query result
        | result |
        | 700000 |

    Scenario: bitmap_or_agg with multi-byte bitmaps
      When query
        """
        SELECT substring(hex(bitmap_or_agg(col)), 0, 8) AS result
        FROM VALUES (X'0100'), (X'0002') AS tab(col)
        """
      Then query result
        | result   |
        | 01020000 |

  Rule: bitmap_or_agg returns a binary value

    Scenario: bitmap_or_agg returns binary type
      When query
        """
        SELECT typeof(bitmap_or_agg(col)) AS result
        FROM VALUES (X'10') AS tab(col)
        """
      Then query result
        | result |
        | binary |

  Rule: bitmap_or_agg handles null values

    Scenario: bitmap_or_agg ignores null input values
      When query
        """
        SELECT substring(hex(bitmap_or_agg(col)), 0, 6) AS result
        FROM VALUES (X'10'), (CAST(NULL AS BINARY)), (X'40') AS tab(col)
        """
      Then query result
        | result |
        | 500000 |

    Scenario: bitmap_or_agg with all null inputs
      When query
        """
        SELECT substring(hex(bitmap_or_agg(col)), 0, 6) AS result
        FROM VALUES (CAST(NULL AS BINARY)), (CAST(NULL AS BINARY)) AS tab(col)
        """
      Then query result
        | result |
        | 000000 |

    Scenario: bitmap_or_agg on empty input returns an empty bitmap
      When query
        """
        SELECT substring(hex(bitmap_or_agg(col)), 0, 6) AS result
        FROM (SELECT CAST(X'00' AS BINARY) AS col WHERE false) AS tab
        """
      Then query result
        | result |
        | 000000 |

  Rule: bitmap_or_agg rejects invalid oversized inputs

    Scenario: bitmap_or_agg with oversized bitmap input
      When query
        """
        SELECT bitmap_or_agg(to_binary(repeat('00', 4097), 'hex')) AS result
        """
      Then query error bitmap_or_agg input length 4097 exceeds maximum 4096

  Rule: bitmap_or_agg works with bitmap_construct_agg output

    Scenario: bitmap_or_agg of bitmap_construct_agg bitmaps
      When query
        """
        SELECT bitmap_count(bitmap_or_agg(bm)) AS result
        FROM (
          SELECT bitmap_construct_agg(bitmap_bit_position(col)) AS bm
          FROM VALUES (1), (2), (3) AS tab(col)
          UNION ALL
          SELECT bitmap_construct_agg(bitmap_bit_position(col)) AS bm
          FROM VALUES (4), (5), (6) AS tab(col)
        ) AS combined
        """
      Then query result
        | result |
        | 6      |

  Rule: bitmap_or_agg as a window function

    Scenario: bitmap_or_agg over window
      When query
        """
        SELECT
          id,
          substring(
            hex(bitmap_or_agg(col) OVER (
              ORDER BY id
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )),
            0,
            6
          ) AS result
        FROM VALUES (1, X'10'), (2, X'20'), (3, X'40') AS tab(id, col)
        ORDER BY id
        """
      Then query result ordered
        | id | result |
        | 1  | 100000 |
        | 2  | 300000 |
        | 3  | 700000 |
