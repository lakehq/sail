Feature: aes_encrypt with an argument coming from a column
  # A behaviour-governing argument given as a literal is constant-folded, so the literal
  # scenarios never exercise the columnar kernel. These scenarios pass the same argument
  # through a column. All expected values were captured on Spark JVM 4.x.

  Rule: aes_encrypt — the argument may come from a column

    @column_args
    Scenario: aes_encrypt with the argument as a literal
      When query
        """
        SELECT base64(aes_encrypt('Spark', 'abcdefghijklmnop12345678ABCDEFGH', 'CBC', 'DEFAULT', unhex('00000000000000000000000000000000'))) AS result
        """
      Then query result ordered
        | result                                       |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |

    # Sail rejects the column: Sail errors: Spark `aes_encrypt`: Expr requires a single value, got StringArray [ "Spark", "Spark SQL",...
    @column_args @sail-bug
    Scenario: aes_encrypt takes argument 1 from a column holding two different values
      When query
        """
        SELECT base64(aes_encrypt(c, 'abcdefghijklmnop12345678ABCDEFGH', 'CBC', 'DEFAULT', unhex('00000000000000000000000000000000'))) AS result FROM VALUES (1, 'Spark'), (2, 'Spark SQL') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                       |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |
        | AAAAAAAAAAAAAAAAAAAAAFfH3r/2mb/RDzBWeYjUD7c= |

    # Sail rejects the column: Sail errors: Spark `aes_encrypt`: Expr requires a single value, got StringArray [ "Spark", "Spark", ]
    @column_args @sail-bug
    Scenario: aes_encrypt takes argument 1 from a column
      When query
        """
        SELECT base64(aes_encrypt(c, 'abcdefghijklmnop12345678ABCDEFGH', 'CBC', 'DEFAULT', unhex('00000000000000000000000000000000'))) AS result FROM VALUES (1, 'Spark'), (2, 'Spark') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                       |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |

    # Sail rejects the column: Sail errors: Spark `aes_encrypt`: Key requires a single value, got StringArray [ "abcdefghijklmnop12345...
    @column_args @sail-bug
    Scenario: aes_encrypt takes argument 2 from a column holding two different values
      When query
        """
        SELECT base64(aes_encrypt('Spark', c, 'CBC', 'DEFAULT', unhex('00000000000000000000000000000000'))) AS result FROM VALUES (1, 'abcdefghijklmnop12345678ABCDEFGH'), (2, '0000111122223333') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                       |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |
        | AAAAAAAAAAAAAAAAAAAAADSP83694Ft4gLozkGj72l4= |

    # Sail rejects the column: Sail errors: Spark `aes_encrypt`: Key requires a single value, got StringArray [ "abcdefghijklmnop12345...
    @column_args @sail-bug
    Scenario: aes_encrypt takes argument 2 from a column
      When query
        """
        SELECT base64(aes_encrypt('Spark', c, 'CBC', 'DEFAULT', unhex('00000000000000000000000000000000'))) AS result FROM VALUES (1, 'abcdefghijklmnop12345678ABCDEFGH'), (2, 'abcdefghijklmnop12345678ABCDEFGH') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                       |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |

    # Sail rejects the column: Sail errors: Spark `aes_encrypt`: Mode requires a single value, got StringArray [ "CBC", "CBC", ]
    @column_args @sail-bug
    Scenario: aes_encrypt takes argument 3 from a column
      When query
        """
        SELECT base64(aes_encrypt('Spark', 'abcdefghijklmnop12345678ABCDEFGH', c, 'DEFAULT', unhex('00000000000000000000000000000000'))) AS result FROM VALUES (1, 'CBC'), (2, 'CBC') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                       |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |
        | AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg= |

    # Sail rejects the column: Sail errors: Spark `aes_encrypt`: AAD requires a single value, got StringArray [ "This is an AAD mixed...
    @column_args @sail-bug
    Scenario: aes_encrypt takes argument 6 from a column
      When query
        """
        SELECT base64(aes_encrypt('Spark', 'abcdefghijklmnop12345678ABCDEFGH', 'GCM', 'DEFAULT', unhex('000000000000000000000000'), c)) AS result FROM VALUES (1, 'This is an AAD mixed into the input'), (2, 'This is an AAD mixed into the input') AS t(i, c) ORDER BY i
        """
      Then query result ordered
        | result                                       |
        | AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4 |
        | AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4 |
