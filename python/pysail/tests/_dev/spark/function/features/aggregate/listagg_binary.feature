Feature: listagg of binary values

  # Spark joins binary values as bytes (ByteArray.concatWS), so the values do not have to be
  # valid UTF-8, and the result is binary. A NULL delimiter joins the values without a delimiter.
  Rule: Binary values are joined as bytes

    Scenario: listagg of binary values that are not valid UTF-8
      When query
        """
        SELECT
          hex(listagg(b) WITHIN GROUP (ORDER BY id)) AS a,
          hex(listagg(b, X'00') WITHIN GROUP (ORDER BY id)) AS b,
          hex(listagg(b, X'80') WITHIN GROUP (ORDER BY id)) AS c,
          hex(listagg(b, NULL) WITHIN GROUP (ORDER BY id)) AS d,
          hex(string_agg(b, X'') WITHIN GROUP (ORDER BY id)) AS e
        FROM VALUES (1, X'80'), (2, X'C3'), (3, NULL), (4, X'41FF') AS t(id, b)
        """
      Then query result
        | a        | b            | c            | d        | e        |
        | 80C341FF | 8000C30041FF | 8080C38041FF | 80C341FF | 80C341FF |

    Scenario: listagg of binary values is binary
      When query
        """
        SELECT listagg(b, X'2C') WITHIN GROUP (ORDER BY id) AS l
        FROM VALUES (1, X'80'), (2, X'C3'), (3, NULL), (4, X'41FF') AS t(id, b)
        """
      Then query schema
        """
        root
         |-- l: binary (nullable = true)
        """

    Scenario: DISTINCT listagg of binary values
      When query
        """
        SELECT hex(listagg(DISTINCT b) WITHIN GROUP (ORDER BY b)) AS a
        FROM VALUES (1, X'80'), (2, X'C3'), (3, NULL), (4, X'41FF') AS t(id, b)
        """
      Then query result
        | a        |
        | 41FF80C3 |

    Scenario: listagg of binary values per group
      When query
        """
        SELECT g, hex(listagg(b, X'2C') WITHIN GROUP (ORDER BY b)) AS l
        FROM VALUES (1, X'FF'), (1, X'00'), (2, NULL), (3, X'') AS t(g, b)
        GROUP BY g
        ORDER BY g
        """
      Then query result ordered
        | g | l      |
        | 1 | 002CFF |
        | 2 | NULL   |
        | 3 |        |

    Scenario: listagg of valid UTF-8 binary values
      When query
        """
        SELECT hex(listagg(CAST(s AS BINARY), X'2C') WITHIN GROUP (ORDER BY s)) AS l
        FROM VALUES ('a'), ('b') AS t(s)
        """
      Then query result
        | l      |
        | 612C62 |

  Rule: Binary values are joined as bytes over a window

    Scenario: listagg of binary values WITHIN GROUP over the whole input
      When query
        """
        SELECT id, hex(listagg(b, X'7C') WITHIN GROUP (ORDER BY id) OVER ()) AS l
        FROM VALUES (1, X'80'), (2, X'C3'), (3, NULL), (4, X'41FF') AS t(id, b)
        ORDER BY id
        """
      Then query result ordered
        | id | l            |
        | 1  | 807CC37C41FF |
        | 2  | 807CC37C41FF |
        | 3  | 807CC37C41FF |
        | 4  | 807CC37C41FF |

    Scenario: listagg of binary values over a sliding window frame
      When query
        """
        SELECT id, hex(listagg(b) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) AS l
        FROM VALUES (1, X'80'), (2, X'C3'), (3, NULL), (4, X'41FF') AS t(id, b)
        ORDER BY id
        """
      Then query result ordered
        | id | l    |
        | 1  | 80   |
        | 2  | 80C3 |
        | 3  | C3   |
        | 4  | 41FF |
