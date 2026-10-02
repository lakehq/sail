@spark-4
Feature: the collation a string carries

  # Spark 4 puts the collation INSIDE the type: `collate('a', 'UTF8_LCASE')` is not a string with a
  # flag beside it, it is a `string collate UTF8_LCASE`, and that type is what makes `=`, `GROUP BY`,
  # `DISTINCT` and ordering fold case. Sail registers `collate` as `F::unknown`
  # (`sail-plan/src/function/scalar/string.rs`), so the whole family is unimplemented today and this
  # file only pins the default: without a collation the comparison is exact.
  #
  # TODO(parity): when `collate` and `collation` land, this file is not to be extended a scenario at
  # a time -- the whole matrix has to be measured again on the JVM, because a collation is a
  # property of the TYPE and so it rides through everything:
  #   * every builtin collation: UTF8_BINARY (the default), UTF8_LCASE, UNICODE, UNICODE_CI,
  #     UNICODE_AI, UNICODE_CI_AI, and the same names with the `SYSTEM.BUILTIN.` prefix;
  #   * every operator the type reaches: `=`, `<`, `IN`, `BETWEEN`, `LIKE`, `RLIKE`, `startswith`,
  #     `contains`, `instr`, `replace`, `split`, `array_contains`, `array_distinct`;
  #   * every place a type is compared rather than a value: `GROUP BY`, `DISTINCT`, `ORDER BY`,
  #     `JOIN` keys, window `PARTITION BY`, a map KEY, `UNION` between two different collations;
  #   * propagation through functions -- Spark's `Hex` keeps the collation of its input
  #     (`mathExpressions.scala`), and so do `upper`, `lower`, `concat`, `substring`;
  #   * the type as declared in DDL (`CREATE TABLE t (c STRING COLLATE UTF8_LCASE)`), what
  #     `collation()` returns (fully qualified: `SYSTEM.BUILTIN.UTF8_LCASE`), and what survives a
  #     write to Parquet and back;
  #   * `spark.sql.session.collation.default`, which moves the default under every one of the above.
  #
  # Measured on 2026-09-23: today every door is SHUT, and shut with a parser error rather than
  # silently ignored, so no query can come back quietly wrong because of a collation. Verified
  # rejected: `'A' COLLATE UTF8_LCASE`, `CAST(... AS STRING COLLATE ...)`, a collated GROUP BY,
  # DISTINCT, JOIN key and ORDER BY, `SHOW COLLATIONS`, a collation on a map key, and -- the one
  # that does not go through SQL text -- a DDL schema handed to `createDataFrame`
  # ("c STRING COLLATE UTF8_LCASE"). Those nine are the doors to re-measure when the feature lands.
  # Not verified: a table whose STORED schema already declares a collation (Iceberg or Delta
  # metadata written by Spark), since Sail cannot create one today.
  # `CREATE SCHEMA ... DEFAULT COLLATION` is refused by Spark 4.2 itself, behind
  # `spark.sql.collation.schemaLevel.enabled`; `spark.sql.session.collation.default` does not exist.
  #
  # And a trap to write down before anyone starts: `Then query schema` CANNOT SEE a collation. It
  # compares the tree string, which prints `string collate UTF8_LCASE` as plain `string` at every
  # depth -- top level, inside an array, inside a struct. A scenario that asserts a collation there
  # goes GREEN with the collation lost. Only `schema.simpleString()` carries it, and that rendering
  # drops the nullability in exchange, so the two are complementary and a new step is needed.
  # Measured on the Spark 4.2 JVM over Spark Connect, 2026-09-23.

  Rule: without a collation a comparison is exact

    Scenario: two strings that differ only in case are not equal
      When query
        """
        SELECT 'A' = 'a' AS result
        """
      Then query result collected
        | result |
        | false  |

    Scenario: the default collation sorts uppercase before lowercase
      When query
        """
        SELECT concat_ws(',', collect_list(c)) AS result
        FROM (SELECT c FROM VALUES ('b'), ('A'), ('a') AS t(c) ORDER BY c)
        """
      Then query result collected
        | result |
        | A,a,b  |
