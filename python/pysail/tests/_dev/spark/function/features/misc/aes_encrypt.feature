Feature: aes_encrypt with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: aes_encrypt — the argument may come from a column

    @function(columnargs)
    Scenario: aes_encrypt with the argument as a literal
      When query
        """
        SELECT base64(aes_encrypt('Spark', 'abcdefghijklmnop12345678ABCDEFGH', 'CBC', 'DEFAULT', unhex('00000000000000000000000000000000'))) AS result
        """
      Then query result ordered
        | result                                       |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |

    @function(columnargs)
    Scenario: aes_encrypt takes argument 1 from a column holding two different values
      When query
        """
        SELECT base64(aes_encrypt(c, 'abcdefghijklmnop12345678ABCDEFGH', 'CBC', 'DEFAULT', unhex('00000000000000000000000000000000'))) AS result FROM VALUES (1, 'Spark'), (2, 'Spark SQL') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                       |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |
        | AAAAAAAAAAAAAAAAAAAAAFfH3r/2mb/RDzBWeYjUD7c= |

    @function(columnargs)
    Scenario: aes_encrypt takes argument 1 from a column
      When query
        """
        SELECT base64(aes_encrypt(c, 'abcdefghijklmnop12345678ABCDEFGH', 'CBC', 'DEFAULT', unhex('00000000000000000000000000000000'))) AS result FROM VALUES (1, 'Spark'), (2, 'Spark') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                       |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |

    @function(columnargs)
    Scenario: aes_encrypt takes argument 2 from a column holding two different values
      When query
        """
        SELECT base64(aes_encrypt('Spark', c, 'CBC', 'DEFAULT', unhex('00000000000000000000000000000000'))) AS result FROM VALUES (1, 'abcdefghijklmnop12345678ABCDEFGH'), (2, '0000111122223333') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                       |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |
        | AAAAAAAAAAAAAAAAAAAAADSP83694Ft4gLozkGj72l4= |

    @function(columnargs)
    Scenario: aes_encrypt takes argument 2 from a column
      When query
        """
        SELECT base64(aes_encrypt('Spark', c, 'CBC', 'DEFAULT', unhex('00000000000000000000000000000000'))) AS result FROM VALUES (1, 'abcdefghijklmnop12345678ABCDEFGH'), (2, 'abcdefghijklmnop12345678ABCDEFGH') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                       |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |

    @function(columnargs)
    Scenario: aes_encrypt takes argument 3 from a column
      When query
        """
        SELECT base64(aes_encrypt('Spark', 'abcdefghijklmnop12345678ABCDEFGH', c, 'DEFAULT', unhex('00000000000000000000000000000000'))) AS result FROM VALUES (1, 'CBC'), (2, 'CBC') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                       |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |

    @function(columnargs)
    Scenario: aes_encrypt takes argument 6 from a column
      When query
        """
        SELECT base64(aes_encrypt('Spark', 'abcdefghijklmnop12345678ABCDEFGH', 'GCM', 'DEFAULT', unhex('000000000000000000000000'), c)) AS result FROM VALUES (1, 'This is an AAD mixed into the input'), (2, 'This is an AAD mixed into the input') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                       |
        | AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4 |
        | AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4 |

    @function(columnargs)
    Scenario: all six aes_encrypt arguments vary by row
      When query
        """
        SELECT input, key, mode, padding, iv, aad,
               hex(aes_encrypt(input, key, mode, padding, iv, aad)) AS encrypted
        FROM VALUES
          (1, 'hello', 'abcdefghijklmnop', 'ECB', 'PKCS', '', ''),
          (2, 'world', '1234567890abcdef', 'CBC', 'DEFAULT', '1234567890123456', ''),
          (3, 'foo', 'fedcba0987654321', 'GCM', 'NONE', '123456789012', 'tag-a'),
          (4, 'bar', '0123456789abcdef', 'GCM', 'DEFAULT', 'abcdefghijkl', 'tag-b')
        AS t(i, input, key, mode, padding, iv, aad)
        ORDER BY i
        """
      Then query result ordered
        | input | key              | mode | padding | iv               | aad   | encrypted                                                        |
        | hello | abcdefghijklmnop | ECB  | PKCS    |                  |       | 0805FFCC36FF55DBBFD3ADDAEDF13EA9                                 |
        | world | 1234567890abcdef | CBC  | DEFAULT | 1234567890123456 |       | 313233343536373839303132333435364983708A8C04F73C2795EBDE50CE5B9C |
        | foo   | fedcba0987654321 | GCM  | NONE    | 123456789012     | tag-a | 313233343536373839303132DF04DA52F624B2D606D361386DFEE900858833   |
        | bar   | 0123456789abcdef | GCM  | DEFAULT | abcdefghijkl     | tag-b | 6162636465666768696A6B6C87B30EF120A58CAAF19575DB6C31F2FAAA60C9   |

    @function(columnargs)
    Scenario Outline: a NULL aes_encrypt <argument> makes only that row NULL
      When query
        """
        SELECT aes_encrypt(<args>) IS NULL AS result
        FROM VALUES (1, <value>), (2, CAST(NULL AS STRING)) AS t(i, c)
        ORDER BY i
        """
      Then query result ordered
        | result |
        | false  |
        | true   |

      Examples:
        | argument | value              | args                                                                       |
        | input    | 'hello'            | c, 'abcdefghijklmnop', 'GCM', 'DEFAULT', '123456789012', 'tag-a'           |
        | key      | 'abcdefghijklmnop' | 'hello', c, 'GCM', 'DEFAULT', '123456789012', 'tag-a'                       |
        | mode     | 'GCM'              | 'hello', 'abcdefghijklmnop', c, 'DEFAULT', '123456789012', 'tag-a'         |
        | padding  | 'DEFAULT'          | 'hello', 'abcdefghijklmnop', 'GCM', c, '123456789012', 'tag-a'             |
        | iv       | '123456789012'     | 'hello', 'abcdefghijklmnop', 'GCM', 'DEFAULT', c, 'tag-a'                  |
        | aad      | 'tag-a'            | 'hello', 'abcdefghijklmnop', 'GCM', 'DEFAULT', '123456789012', c           |

    @function(columnargs)
    Scenario Outline: aes_encrypt rejects invalid encryption parameters per row
      When query
        """
        SELECT aes_encrypt('hello', 'abcdefghijklmnop', mode, padding, iv, aad)
        FROM VALUES ('GCM', 'DEFAULT', '123456789012', ''),
                    (<mode>, <padding>, <iv>, <aad>) AS t(mode, padding, iv, aad)
        """
      Then query error (?i)<error>

      Examples:
        | mode    | padding   | iv                 | aad     | error   |
        | 'ECB'   | 'NONE'    | ''                 | ''      | padding |
        | 'CBC'   | 'NONE'    | '1234567890123456' | ''      | padding |
        | 'GCM'   | 'PKCS'    | '123456789012'     | ''      | padding |
        | 'GCM'   | 'UNKNOWN' | '123456789012'     | ''      | padding |
        | ' GCM ' | 'DEFAULT' | '123456789012'     | ''      | mode    |
        | ''      | 'DEFAULT' | ''                 | ''      | mode    |
        | 'ECB'   | 'DEFAULT' | '1234567890123456' | ''      | iv      |
        | 'CBC'   | 'DEFAULT' | '123456789012'     | ''      | iv      |
        | 'GCM'   | 'DEFAULT' | '1234567890123456' | ''      | iv      |
        | 'CBC'   | 'DEFAULT' | '1234567890123456' | 'tag-a' | aad     |
        | 'ECB'   | 'DEFAULT' | ''                 | 'tag-a' | aad     |

    @function(columnargs)
    Scenario: ECB encrypts each block independently and includes a padding block
      When query
        """
        SELECT hex(aes_encrypt(input, 'abcdefghijklmnop', 'ecb', 'pkcs')) AS encrypted
        FROM VALUES ('0123456789abcdef0123456789abcdef') AS t(input)
        """
      Then query result ordered
        | encrypted                                                                                        |
        | 747F22502381A3FB7EB0CB42CB5F6612747F22502381A3FB7EB0CB42CB5F66128E64CE873F174DBB2423FCD814580E15 |

    @function(columnargs)
    Scenario: ECB decrypts Spark ciphertext without an IV prefix
      When query
        """
        SELECT CAST(aes_decrypt(
          unhex('747F22502381A3FB7EB0CB42CB5F6612747F22502381A3FB7EB0CB42CB5F66128E64CE873F174DBB2423FCD814580E15'),
          'abcdefghijklmnop', 'ECB', 'DEFAULT') AS STRING) AS result
        """
      Then query result ordered
        | result                           |
        | 0123456789abcdef0123456789abcdef |

    @function(columnargs)
    Scenario Outline: aes_encrypt matches Spark for every mode and AES key size
      When query
        """
        SELECT hex(aes_encrypt(input, key, mode, padding, iv)) AS encrypted
        FROM VALUES ('0123456789abcdef0123456789abcdef', <key>, <mode>, <padding>, <iv>)
        AS t(input, key, mode, padding, iv)
        """
      Then query result ordered
        | encrypted   |
        | <encrypted> |

      Examples:
        | key | mode | padding | iv | encrypted |
        | 'abcdefghijklmnop' | 'ecb' | 'PKCſ' | '' | 747F22502381A3FB7EB0CB42CB5F6612747F22502381A3FB7EB0CB42CB5F66128E64CE873F174DBB2423FCD814580E15 |
        | 'abcdefghijklmnop12345678' | 'ecb' | 'DEFAULT' | '' | 2AB1BF730E222B8F5869D05BAA8BD89D2AB1BF730E222B8F5869D05BAA8BD89DF510CAEF4B07373A801517297C633981 |
        | 'abcdefghijklmnop12345678ABCDEFGH' | 'ecb' | 'pkcs' | '' | 4756B1BEA026AF221C1EDF32E82392484756B1BEA026AF221C1EDF32E82392488FD203B02BE5617B5C54951FE0502341 |
        | 'abcdefghijklmnop' | 'cbc' | 'DEFAULT' | '1234567890123456' | 3132333435363738393031323334353686FF54939F267491DCC0704B40BF610F68CC3F9FFA45BAA552ED1FE0DCD79441C390B98CC28B989483D498E359564D46 |
        | 'abcdefghijklmnop12345678' | 'cbc' | 'PKCS' | '1234567890123456' | 31323334353637383930313233343536397A194A858E67B9BCD2F07EBFC1F4EFCFD339A8920378DB8CBB93E0E852FF99951325FC795FF8B4E334C7C6BD803ADA |
        | 'abcdefghijklmnop12345678ABCDEFGH' | 'cbc' | 'default' | '1234567890123456' | 31323334353637383930313233343536C269387905EA0EFF7FE94070F750DD9D419114C660CA949D72387CB04240C38EB92B271771F965FF8C1A5C40AEBE7480 |
        | 'abcdefghijklmnop' | 'gcm' | 'NONE' | '123456789012' | 313233343536373839303132BB08D1825210AAFAB030000DDD7C5B084711D4C552CE0EF68C6919E5F5E9565FDF381CC99B10F1B1C1A74E6F162AD798 |
        | 'abcdefghijklmnop12345678' | 'gcm' | 'DEFAULT' | '123456789012' | 313233343536373839303132560D3E6275A105F36DAF4CE21E0271181B043E6ED01555BAF36C7A2DC00B8C2C8DDB56E5F2BE868F17F6ECE770AB7B03 |
        | 'abcdefghijklmnop12345678ABCDEFGH' | 'gcm' | 'none' | '123456789012' | 313233343536373839303132D066234A9C3D08164FC352FF7C70064F488528FB6F67C656B99D3A156DC6FA627FBB1432549AFC1F7CD49B9A32ED0F42 |

    @function(columnargs)
    Scenario: Spark folds aes_encrypt with only literal arguments
      When query
        """
        SELECT count(DISTINCT aes_encrypt('hello', 'abcdefghijklmnop')) AS result FROM range(4)
        """
      Then query result ordered
        | result |
        | 1      |

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal yields binary
      When query
        """
        SELECT aes_encrypt('hello', '1234567890123456') AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a non-null column yields binary
      When query
        """
        SELECT aes_encrypt(CAST(id AS STRING), '1234567890123456') AS result FROM range(3)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    Scenario: a nullable column stays nullable
      When query
        """
        SELECT aes_encrypt(c, '1234567890123456') AS result FROM VALUES ('hello'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """
