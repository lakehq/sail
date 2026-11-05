Feature: aes_decrypt with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: aes_decrypt — the argument may come from a column

    @function(columnargs)
    Scenario: aes_decrypt with the argument as a literal
      When query
        """
        SELECT hex(aes_decrypt(unbase64('2NYmDCjgXTbbxGA3/SnJEfFC/JQ7olk2VQWReIAAFKo='), '1234567890abcdef', 'CBC')) AS result
        """
      Then query result ordered
        | result                   |
        | 41706163686520537061726B |

    @function(columnargs)
    Scenario: aes_decrypt takes argument 2 from a column
      When query
        """
        SELECT hex(aes_decrypt(unbase64('2NYmDCjgXTbbxGA3/SnJEfFC/JQ7olk2VQWReIAAFKo='), c, 'CBC')) AS result FROM VALUES (1, '1234567890abcdef'), (2, '1234567890abcdef') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                   |
        | 41706163686520537061726B |
        | 41706163686520537061726B |

    @function(columnargs)
    Scenario: aes_decrypt takes argument 3 from a column
      When query
        """
        SELECT hex(aes_decrypt(unbase64('2NYmDCjgXTbbxGA3/SnJEfFC/JQ7olk2VQWReIAAFKo='), '1234567890abcdef', c)) AS result FROM VALUES (1, 'CBC'), (2, 'CBC') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                   |
        | 41706163686520537061726B |
        | 41706163686520537061726B |

    @function(columnargs)
    Scenario: aes_decrypt takes argument 5 from a column
      When query
        """
        SELECT hex(aes_decrypt(unbase64('AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4'), 'abcdefghijklmnop12345678ABCDEFGH', 'GCM', 'DEFAULT', c)) AS result FROM VALUES (1, 'This is an AAD mixed into the input'), (2, 'This is an AAD mixed into the input') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result     |
        | 537061726B |
        | 537061726B |

    @function(columnargs)
    Scenario: all five aes_decrypt arguments vary by row
      When query
        """
        SELECT CAST(aes_decrypt(unhex(input), key, mode, padding, aad) AS STRING) AS decrypted
        FROM VALUES
          (1, '0805FFCC36FF55DBBFD3ADDAEDF13EA9', 'abcdefghijklmnop', 'ECB', 'PKCS', ''),
          (2, '313233343536373839303132333435364983708A8C04F73C2795EBDE50CE5B9C', '1234567890abcdef', 'CBC', 'DEFAULT', ''),
          (3, '313233343536373839303132DF04DA52F624B2D606D361386DFEE900858833', 'fedcba0987654321', 'GCM', 'NONE', 'tag-a'),
          (4, '6162636465666768696A6B6C87B30EF120A58CAAF19575DB6C31F2FAAA60C9', '0123456789abcdef', 'GCM', 'DEFAULT', 'tag-b')
        AS t(i, input, key, mode, padding, aad)
        ORDER BY i
        """
      Then query result ordered
        | decrypted |
        | hello     |
        | world     |
        | foo       |
        | bar       |

    @function(columnargs)
    Scenario Outline: aes_decrypt matches Spark for every mode and AES key size
      When query
        """
        SELECT CAST(aes_decrypt(unhex(input), key, mode, padding) AS STRING) AS decrypted
        FROM VALUES (<input>, <key>, <mode>, <padding>) AS t(input, key, mode, padding)
        """
      Then query result ordered
        | decrypted                       |
        | 0123456789abcdef0123456789abcdef |

      Examples:
        | input | key | mode | padding |
        | '747F22502381A3FB7EB0CB42CB5F6612747F22502381A3FB7EB0CB42CB5F66128E64CE873F174DBB2423FCD814580E15' | 'abcdefghijklmnop' | 'ecb' | 'PKCS' |
        | '2AB1BF730E222B8F5869D05BAA8BD89D2AB1BF730E222B8F5869D05BAA8BD89DF510CAEF4B07373A801517297C633981' | 'abcdefghijklmnop12345678' | 'ecb' | 'DEFAULT' |
        | '4756B1BEA026AF221C1EDF32E82392484756B1BEA026AF221C1EDF32E82392488FD203B02BE5617B5C54951FE0502341' | 'abcdefghijklmnop12345678ABCDEFGH' | 'ecb' | 'pkcs' |
        | '3132333435363738393031323334353686FF54939F267491DCC0704B40BF610F68CC3F9FFA45BAA552ED1FE0DCD79441C390B98CC28B989483D498E359564D46' | 'abcdefghijklmnop' | 'cbc' | 'DEFAULT' |
        | '31323334353637383930313233343536397A194A858E67B9BCD2F07EBFC1F4EFCFD339A8920378DB8CBB93E0E852FF99951325FC795FF8B4E334C7C6BD803ADA' | 'abcdefghijklmnop12345678' | 'cbc' | 'PKCS' |
        | '31323334353637383930313233343536C269387905EA0EFF7FE94070F750DD9D419114C660CA949D72387CB04240C38EB92B271771F965FF8C1A5C40AEBE7480' | 'abcdefghijklmnop12345678ABCDEFGH' | 'cbc' | 'default' |
        | '313233343536373839303132BB08D1825210AAFAB030000DDD7C5B084711D4C552CE0EF68C6919E5F5E9565FDF381CC99B10F1B1C1A74E6F162AD798' | 'abcdefghijklmnop' | 'gcm' | 'NONE' |
        | '313233343536373839303132560D3E6275A105F36DAF4CE21E0271181B043E6ED01555BAF36C7A2DC00B8C2C8DDB56E5F2BE868F17F6ECE770AB7B03' | 'abcdefghijklmnop12345678' | 'gcm' | 'DEFAULT' |
        | '313233343536373839303132D066234A9C3D08164FC352FF7C70064F488528FB6F67C656B99D3A156DC6FA627FBB1432549AFC1F7CD49B9A32ED0F42' | 'abcdefghijklmnop12345678ABCDEFGH' | 'gcm' | 'none' |

    @function(columnargs)
    Scenario Outline: aes_decrypt rejects invalid padding per row
      When query
        """
        SELECT aes_decrypt(unhex(input), 'abcdefghijklmnop', mode, padding)
        FROM VALUES ('0805FFCC36FF55DBBFD3ADDAEDF13EA9', 'ECB', 'PKCS'),
                    ('0805FFCC36FF55DBBFD3ADDAEDF13EA9', <mode>, <padding>) AS t(input, mode, padding)
        """
      Then query error (?i)padding

      Examples:
        | mode  | padding   |
        | 'ECB' | 'NONE'    |
        | 'CBC' | 'NONE'    |
        | 'GCM' | 'PKCS'    |
        | 'GCM' | 'UNKNOWN' |

    @function(columnargs)
    Scenario Outline: aes_decrypt preserves Spark empty-input and ECB AAD behavior
      When query
        """
        SELECT hex(aes_decrypt(unhex(input), 'abcdefghijklmnop', mode, 'DEFAULT', aad)) AS decrypted
        FROM VALUES (<input>, <mode>, <aad>) AS t(input, mode, aad)
        """
      Then query result ordered
        | decrypted   |
        | <decrypted> |

      Examples:
        | input                              | mode  | aad       | decrypted  |
        | ''                                 | 'ECB' | ''        |            |
        | '31323334353637383930313233343536' | 'CBC' | ''        |            |
        | '0805FFCC36FF55DBBFD3ADDAEDF13EA9' | 'ECB' | 'ignored' | 68656C6C6F |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal yields binary
      When query
        """
        SELECT aes_decrypt(aes_encrypt('hello', '1234567890123456'), '1234567890123456') AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a nullable column stays nullable
      When query
        """
        SELECT aes_decrypt(c, '1234567890123456') AS result FROM VALUES (aes_encrypt('hi','1234567890123456')), (CAST(NULL AS BINARY)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a non-null column yields binary
      When query
        """
        SELECT aes_decrypt(aes_encrypt(CAST(id AS STRING), '1234567890123456'), '1234567890123456') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """
