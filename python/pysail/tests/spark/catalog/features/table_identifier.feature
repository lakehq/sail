@table_identifier
Feature: Table names with digit-leading identifiers

  # `1m` is a single identifier token in both engines: Spark lexes IDENTIFIER
  # as (LETTER | DIGIT | '_')+ and `m` is not a numeric literal suffix (unlike
  # `y`, `s`, `l`, `f`, `d`, `bd`), so `foo.1m` is a valid qualified table name.

  Background:
    Given final statement
      """
      DROP DATABASE IF EXISTS foo CASCADE
      """
    Given statement
      """
      CREATE DATABASE IF NOT EXISTS foo
      """

  Scenario: Create a table with a digit-leading name and an explicit data source
    Given final statement
      """
      DROP TABLE IF EXISTS foo.1m
      """
    Given statement
      """
      CREATE TABLE foo.1m(a INT) USING parquet
      """
    Given statement
      """
      INSERT INTO foo.1m VALUES (1), (2)
      """
    When query
      """
      SELECT a FROM foo.1m ORDER BY a
      """
    Then query result ordered
      | a |
      | 1 |
      | 2 |

  # Spark also parses this form, but the JVM outcome depends on the version:
  # Spark 3.5 with the in-memory catalog rejects it at analysis time
  # (NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT), while Spark 4.x creates a
  # data source table with the default format. Sail always uses the default
  # table format, so the scenario is pinned to Sail only.
  @sail-only
  Scenario: Create a table with a digit-leading name without an explicit data source
    Given final statement
      """
      DROP TABLE IF EXISTS foo.1m
      """
    Given statement
      """
      CREATE TABLE foo.1m(a INT)
      """
    Given statement
      """
      INSERT INTO foo.1m VALUES (1), (2)
      """
    When query
      """
      SELECT a FROM foo.1m ORDER BY a
      """
    Then query result ordered
      | a |
      | 1 |
      | 2 |
