@sail-only
Feature: try_aes_encrypt with arguments resolved per row
  # Sail's extension uses Spark aes_encrypt behavior for valid rows and returns
  # NULL for encryption errors. Spark does not register try_aes_encrypt.

  @function(columnargs)
  Scenario: all six try_aes_encrypt arguments vary by row
    When query
      """
      SELECT hex(try_aes_encrypt(input, key, mode, padding, iv, aad)) AS encrypted
      FROM VALUES
        (1, 'hello', 'abcdefghijklmnop', 'ECB', 'PKCS', '', ''),
        (2, 'world', '1234567890abcdef', 'CBC', 'DEFAULT', '1234567890123456', ''),
        (3, 'foo', 'fedcba0987654321', 'GCM', 'NONE', '123456789012', 'tag-a'),
        (4, 'bar', '0123456789abcdef', 'GCM', 'DEFAULT', 'abcdefghijkl', 'tag-b')
      AS t(i, input, key, mode, padding, iv, aad)
      ORDER BY i
      """
    Then query result ordered
      | encrypted                                                      |
      | 0805FFCC36FF55DBBFD3ADDAEDF13EA9                               |
      | 313233343536373839303132333435364983708A8C04F73C2795EBDE50CE5B9C |
      | 313233343536373839303132DF04DA52F624B2D606D361386DFEE900858833 |
      | 6162636465666768696A6B6C87B30EF120A58CAAF19575DB6C31F2FAAA60C9 |

  @function(columnargs)
  Scenario Outline: try_aes_encrypt preserves valid rows around an invalid <argument>
    When query
      """
      SELECT hex(try_aes_encrypt(<args>)) AS encrypted
      FROM VALUES (1, <valid>), (2, <invalid>), (3, <valid>) AS t(i, c)
      ORDER BY i
      """
    Then query result ordered
      | encrypted                                                      |
      | 6162636465666768696A6B6C87B30EF120A58CAAF19575DB6C31F2FAAA60C9 |
      | NULL                                                           |
      | 6162636465666768696A6B6C87B30EF120A58CAAF19575DB6C31F2FAAA60C9 |

    Examples:
      | argument | valid              | invalid   | args                                                                |
      | key      | '0123456789abcdef' | 'short'   | 'bar', c, 'GCM', 'DEFAULT', 'abcdefghijkl', 'tag-b'                  |
      | mode     | 'GCM'              | 'UNKNOWN' | 'bar', '0123456789abcdef', c, 'DEFAULT', 'abcdefghijkl', 'tag-b'    |
      | padding  | 'DEFAULT'          | 'PKCS'    | 'bar', '0123456789abcdef', 'GCM', c, 'abcdefghijkl', 'tag-b'        |
      | iv       | 'abcdefghijkl'     | 'short'   | 'bar', '0123456789abcdef', 'GCM', 'DEFAULT', c, 'tag-b'             |

  @function(columnargs)
  Scenario Outline: try_aes_encrypt returns NULL for unsupported <mode> parameters
    When query
      """
      SELECT try_aes_encrypt(input, 'abcdefghijklmnop', <mode>, <padding>, <iv>, <aad>) AS encrypted
      FROM VALUES ('hello'), ('world') AS t(input)
      """
    Then query result ordered
      | encrypted |
      | NULL      |
      | NULL      |

    Examples:
      | mode  | padding   | iv                 | aad     |
      | 'ECB' | 'NONE'    | ''                 | ''      |
      | 'CBC' | 'NONE'    | '1234567890123456' | ''      |
      | 'ECB' | 'DEFAULT' | '1234567890123456' | ''      |
      | 'CBC' | 'DEFAULT' | '1234567890123456' | 'tag-a' |
      | 'ECB' | 'DEFAULT' | ''                 | 'tag-a' |

  @function(columnargs)
  Scenario Outline: a NULL try_aes_encrypt <argument> affects only that row
    When query
      """
      SELECT try_aes_encrypt(<args>) IS NULL AS result
      FROM VALUES (1, <value>), (2, CAST(NULL AS STRING)), (3, <value>) AS t(i, c)
      ORDER BY i
      """
    Then query result ordered
      | result |
      | false  |
      | true   |
      | false  |

    Examples:
      | argument | value              | args                                                                |
      | input    | 'bar'              | c, '0123456789abcdef', 'GCM', 'DEFAULT', 'abcdefghijkl', 'tag-b'    |
      | key      | '0123456789abcdef' | 'bar', c, 'GCM', 'DEFAULT', 'abcdefghijkl', 'tag-b'                  |
      | mode     | 'GCM'              | 'bar', '0123456789abcdef', c, 'DEFAULT', 'abcdefghijkl', 'tag-b'    |
      | padding  | 'DEFAULT'          | 'bar', '0123456789abcdef', 'GCM', c, 'abcdefghijkl', 'tag-b'        |
      | iv       | 'abcdefghijkl'     | 'bar', '0123456789abcdef', 'GCM', 'DEFAULT', c, 'tag-b'             |
      | aad      | 'tag-b'            | 'bar', '0123456789abcdef', 'GCM', 'DEFAULT', 'abcdefghijkl', c      |

  @function(columnargs)
  Scenario Outline: try_aes_encrypt applies defaults for <mode>
    When query
      """
      SELECT CAST(try_aes_decrypt(try_aes_encrypt(<args>), key, <mode>) AS STRING) AS decrypted
      FROM VALUES (1, 'hello', 'abcdefghijklmnop'), (2, 'world', '1234567890abcdef') AS t(i, input, key)
      ORDER BY i
      """
    Then query result ordered
      | decrypted |
      | hello     |
      | world     |

    Examples:
      | mode  | args              |
      | 'GCM' | input, key        |
      | 'CBC' | input, key, 'CBC' |
      | 'ECB' | input, key, 'ECB' |

  @function(columnargs)
  Scenario: try_aes_encrypt handles scalar results and errors
    When query
      """
      SELECT hex(try_aes_encrypt('hello', 'abcdefghijklmnop', 'ECB')) AS encrypted,
             try_aes_encrypt('hello', 'short') AS invalid,
             try_aes_encrypt(CAST(NULL AS STRING), 'abcdefghijklmnop') AS missing
      """
    Then query result ordered
      | encrypted                        | invalid | missing |
      | 0805FFCC36FF55DBBFD3ADDAEDF13EA9 | NULL    | NULL    |
