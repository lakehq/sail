Feature: try_aes_encrypt
  # `try_aes_encrypt` is a Sail-only spelling. Spark 4.2.0 has no `TryAesEncrypt` expression and
  # does not register the name -- the JVM answers UNRESOLVED_ROUTINE -- so every scenario here is
  # @sail-only.
  #
  # The ciphertexts are still anchored to Spark: with an explicit IV the CBC output is
  # deterministic, and the values below are byte-for-byte the ones Spark's strict `aes_encrypt`
  # produces for the same input, key and IV (see misc/aes_encrypt.feature, whose expectations were
  # captured on the Spark JVM).

  Rule: With an explicit IV the ciphertext is deterministic

    @sail-only
    Scenario Outline: try_aes_encrypt CBC: <case>
      When query
        """
        SELECT base64(try_aes_encrypt(<expr>, <key>, 'CBC', 'DEFAULT', unhex('00000000000000000000000000000000'))) AS result
        """
      Then query result
        | result   |
        | <result> |

      Examples:
        | case                  | expr        | key                                | result                                       |
        | AES-128 (16-byte key) | 'Spark'     | '1234567890123456'                 | AAAAAAAAAAAAAAAAAAAAABXRHaovyd1h+/SQgYlovNA= |
        | AES-256 (32-byte key) | 'Spark'     | 'abcdefghijklmnop12345678ABCDEFGH' | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |
        | a longer plaintext    | 'Spark SQL' | 'abcdefghijklmnop12345678ABCDEFGH' | AAAAAAAAAAAAAAAAAAAAAFfH3r/2mb/RDzBWeYjUD7c= |

  Rule: A failure yields NULL instead of raising

    @sail-only
    Scenario Outline: try_aes_encrypt yields NULL: <case>
      When query
        """
        SELECT base64(try_aes_encrypt(<args>)) AS result
        """
      Then query result
        | result |
        | NULL   |

      Examples:
        | case                              | args                                     |
        | the key is not 16, 24 or 32 bytes | 'Spark', 'short'                         |
        | the mode is not a supported one   | 'Spark', '1234567890123456', 'NOPE'      |
        | the input is NULL                 | CAST(NULL AS STRING), '1234567890123456' |

  Rule: A structurally invalid call still raises

    # The two-sided `try_*` contract: bad DATA becomes NULL (rule above), but a call Spark would
    # reject at analysis must still raise. `invoke_with_args` maps every error to NULL, so the
    # arity is gated in `return_field_from_args` instead -- without it a one-argument call was
    # silently NULL.
    @sail-only
    Scenario: try_aes_encrypt rejects a single argument
      When query
        """
        SELECT try_aes_encrypt('Spark') AS result
        """
      Then query error try_aes_encrypt.*requires

  Rule: The result decrypts back to the input

    @sail-only
    Scenario: a GCM round trip through try_aes_decrypt returns the plaintext
      When query
        """
        SELECT CAST(try_aes_decrypt(try_aes_encrypt('Spark', '1234567890123456'), '1234567890123456') AS STRING) AS result
        """
      Then query result
        | result |
        | Spark  |

  @function(nullability)
  Rule: Output schema

    @sail-only
    Scenario: a non-null literal input to try_aes_encrypt still yields a nullable binary
      When query
        """
        SELECT try_aes_encrypt('Spark', '1234567890123456', 'CBC', 'DEFAULT', unhex('00000000000000000000000000000000')) AS result
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """

    @sail-only
    Scenario: a nullable column input to try_aes_encrypt stays nullable
      When query
        """
        SELECT try_aes_encrypt(c, '1234567890123456', 'CBC', 'DEFAULT', unhex('00000000000000000000000000000000')) AS result FROM VALUES ('Spark'), (CAST(NULL AS STRING)) AS t(c)
        """
      Then query schema
        """
        root
         |-- result: binary (nullable = true)
        """
