Feature: error messages do not leak Rust internals
  # Sail formats some error messages with Rust's `Debug` (`{x:?}`), which dumps the internal
  # value — an Arrow array, a `ColumnarValue`, a parser AST node — into text the user reads.
  # Spark never exposes its internals this way. Each scenario asserts both that the query is
  # rejected for the expected reason and that the message carries no Rust `Debug` output.

  Rule: an argument-count error reports the count, not the arguments

    @sail-bug
    Scenario: xpath called with one argument
      When query
        """
        SELECT xpath('<a><b>1</b></a>')
        """
      Then query fails without internal details (?i)(requires 2 parameters|expected 2 values)

    @sail-bug
    Scenario: xpath called with three arguments
      When query
        """
        SELECT xpath('<a/>', 'a', 'b')
        """
      Then query fails without internal details (?i)(requires 2 parameters|expected 2 values)

  Rule: an argument-type error reports the type, not the value

    @sail-bug
    Scenario: aes_encrypt with a non-binary input
      When query
        """
        SELECT aes_encrypt(1, '0000000000000000')
        """
      Then query fails without internal details (?i)binary

    @sail-bug
    Scenario: aes_encrypt with a non-binary key
      When query
        """
        SELECT aes_encrypt('abc', 1)
        """
      Then query fails without internal details (?i)binary

    @sail-bug
    Scenario: aes_encrypt with a non-string mode
      When query
        """
        SELECT aes_encrypt('abc', '0000000000000000', 1)
        """
      Then query fails without internal details (?i)string

    @sail-bug
    Scenario: aes_encrypt with a non-binary initialization vector
      When query
        """
        SELECT aes_encrypt('abc', '0000000000000000', 'GCM', 'DEFAULT', 1)
        """
      Then query fails without internal details (?i)binary

    @sail-bug
    Scenario: aes_encrypt with non-string additional authenticated data
      When query
        """
        SELECT aes_encrypt('abc', '0000000000000000', 'GCM', 'DEFAULT', '000000000000', 1)
        """
      Then query fails without internal details DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE

    @sail-bug
    Scenario: aes_decrypt with a non-binary input
      When query
        """
        SELECT aes_decrypt(1, '0000000000000000')
        """
      Then query fails without internal details (?i)binary

    @sail-bug
    Scenario: aes_decrypt with a non-binary key
      When query
        """
        SELECT aes_decrypt(x'00', 1)
        """
      Then query fails without internal details (?i)binary

  Rule: an unresolved name error reports the name, not the parser node

    @sail-bug
    Scenario: table reference with too many name parts
      When query
        """
        SELECT * FROM a.b.c.d.e
        """
      Then query fails without internal details REQUIRES_SINGLE_PART_NAMESPACE

    @sail-bug
    Scenario: qualified star for an unknown relation
      When query
        """
        SELECT nosuchtable.* FROM (SELECT 1)
        """
      Then query fails without internal details nosuchtable
