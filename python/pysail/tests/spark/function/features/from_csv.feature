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

  Scenario: from_csv with schema_of_csv as the schema argument
    When query
      """
      SELECT from_csv(value, schema_of_csv('1,abc')) AS result
      FROM VALUES ('42,hello') AS t(value)
      """
    Then query result
      | result       |
      | {42, hello}  |

  Scenario: from_csv with schema_of_csv handles multiple rows
    When query
      """
      SELECT from_csv(value, schema_of_csv('0,x')) AS result
      FROM VALUES ('1,a'), ('2,b'), ('3,c') AS t(value)
      ORDER BY result._c0
      """
    Then query result ordered
      | result  |
      | {1, a}  |
      | {2, b}  |
      | {3, c}  |

  Scenario: from_csv with schema_of_csv options as the schema argument
    When query
      """
      SELECT from_csv(value, schema_of_csv('1|abc', map('sep', '|')), map('sep', '|')) AS result
      FROM VALUES ('42|hello') AS t(value)
      """
    Then query result
      | result       |
      | {42, hello}  |

  Scenario: from_csv with schema_of_csv delimiter option as the schema argument
    When query
      """
      SELECT from_csv(value, schema_of_csv('1|abc', map('delimiter', '|')), map('delimiter', '|')) AS result
      FROM VALUES ('42|hello') AS t(value)
      """
    Then query result
      | result       |
      | {42, hello}  |

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
