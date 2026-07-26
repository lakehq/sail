@try_aes_decrypt
Feature: try_aes_decrypt with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: try_aes_decrypt — the argument is resolved per row, not taken from the first row

    @column_args
    Scenario: try_aes_decrypt with the argument as a literal
      When query
        """
        SELECT hex(try_aes_decrypt(unhex('6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210'), '0000111122223333', 'GCM')) AS result
        """
      Then query result ordered
        | result             |
        | 537061726B2053514C |

    # Sail returns the wrong value on the column path: Sail returns NULL for every row.
    @column_args @sail-bug
    Scenario Outline: Try_aes_decrypt: <case>
      When query
        """
        SELECT hex(try_aes_decrypt(unhex('6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210'), <args>)) AS result FROM VALUES (1, <v1>), (2, <v2>) AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result             |
        | 537061726B2053514C |
        | <r2>               |

      Examples:
        | case                                                           | args                  | v1                 | v2                 | r2                 |
        | try_aes_decrypt takes argument 2 from a column containing NULL | c, 'GCM'              | '0000111122223333' | NULL               | NULL               |
        | try_aes_decrypt takes argument 2 from a column                 | c, 'GCM'              | '0000111122223333' | '0000111122223333' | 537061726B2053514C |
        | try_aes_decrypt takes argument 3 from a column containing NULL | '0000111122223333', c | 'GCM'              | NULL               | NULL               |
        | try_aes_decrypt takes argument 3 from a column                 | '0000111122223333', c | 'GCM'              | 'GCM'              | 537061726B2053514C |

  @spark_null
  Rule: Output schema

    Scenario: a non-null literal input to try_aes_decrypt yields the schema Spark declares
      When query
        """
        SELECT try_aes_decrypt(unhex('6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210'), '0000111122223333', 'GCM') AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a nullable column input to try_aes_decrypt stays nullable
      When query
        """
        SELECT try_aes_decrypt(c, '0000111122223333', 'GCM') AS result FROM VALUES (unhex('6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210')), (CAST(NULL AS BINARY)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """
