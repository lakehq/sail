Feature: from_csv column display name matches Spark

  Spark renders `from_csv` as a UnaryExpression: only the input column
  appears in the column display name, and the schema and options arguments
  are dropped. The same convention applies to `any_value`, `first_value`,
  and `last_value`, which share Sail's unary display-name match arm.

  Scenario: from_csv with a schema string
    When query
      """
      SELECT from_csv(value, 'a INT, b INT, c INT')
      FROM VALUES ('1,2,3') AS t(value)
      """
    Then query result
      | from_csv(value) |
      | {1, 2, 3}       |

  Scenario: from_csv with a schema string and an options map
    When query
      """
      SELECT from_csv(value, 'a INT, b INT, c INT', map('sep', ','))
      FROM VALUES ('1,2,3') AS t(value)
      """
    Then query result
      | from_csv(value) |
      | {1, 2, 3}       |

  Scenario: any_value drops the ignoreNulls argument from its display name
    When query
      """
      SELECT any_value(x, true)
      FROM VALUES (1) AS t(x)
      """
    Then query result
      | any_value(x) |
      | 1            |

  Scenario: first_value drops the ignoreNulls argument from its display name
    When query
      """
      SELECT first_value(x, true)
      FROM VALUES (1) AS t(x)
      """
    Then query result
      | first_value(x) |
      | 1              |

  Scenario: last_value drops the ignoreNulls argument from its display name
    When query
      """
      SELECT last_value(x, true)
      FROM VALUES (1) AS t(x)
      """
    Then query result
      | last_value(x) |
      | 1             |
