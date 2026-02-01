Feature: Catalog operations return correct field names
  # PySpark maps tableName from the server to name in the client Table namedtuple.
  # The camelCase fields (tableType, isTemporary) are passed through directly.

  Rule: listTables returns correct field values

    Scenario: listTables returns table name
      Given a temporary view "test_view" from query "SELECT 1 AS col"
      When I call spark.catalog.listTables()
      Then the result contains a table with name "test_view"

    Scenario: listTables returns tableType in camelCase
      Given a temporary view "test_view" from query "SELECT 1 AS col"
      When I call spark.catalog.listTables()
      Then the result contains a table with tableType "TEMPORARY"

    Scenario: listTables returns isTemporary in camelCase
      Given a temporary view "test_view" from query "SELECT 1 AS col"
      When I call spark.catalog.listTables()
      Then the result contains a table with isTemporary True

  Rule: listDatabases returns correct field values

    Scenario: listDatabases returns database name
      When I call spark.catalog.listDatabases()
      Then the result contains a database with name "default"
