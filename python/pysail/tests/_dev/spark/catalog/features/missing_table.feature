Feature: Reads of a missing table

  # When a table is not found, the read resolver tries the name as a lake
  # metadata relation such as `t.snapshots` and looks the rest of the name up
  # as the base table. For `catalog.database.table` that rest is
  # `catalog.database`, which names no table and is refused as an empty
  # namespace. The read must still report the table it could not find.
  # The catalog is named `sail` only on Sail.
  @sail-only
  Scenario: A catalog-qualified read of a missing table reports the table as not found
    When query
      """
      SELECT * FROM sail.default.missing_table_for_lookup
      """
    Then query error \[TABLE_OR_VIEW_NOT_FOUND\]
