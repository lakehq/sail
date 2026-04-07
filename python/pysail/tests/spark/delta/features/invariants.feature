Feature: Delta Lake Column Invariants

  Rule: Top-level invariants from Delta metadata are enforced on writes
    Background:
      Given variable location for temporary directory invariants_top_level
      Given final statement
        """
        DROP TABLE IF EXISTS delta_invariants_top_level
        """
      Given statement template
        """
        CREATE TABLE delta_invariants_top_level (
          value INT,
          label STRING
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_invariants_top_level VALUES (4, 'seed')
        """
      Given delta log first commit column value has invariant value > 3

    Scenario: Valid rows are accepted
      Given statement
        """
        INSERT INTO delta_invariants_top_level VALUES (5, 'ok'), (8, 'great')
        """
      When query
        """
        SELECT value, label
        FROM delta_invariants_top_level
        ORDER BY value
        """
      Then query result ordered
        | value | label |
        | 4     | seed  |
        | 5     | ok    |
        | 8     | great |

    Scenario: Rows that violate the invariant are rejected
      Given statement with error (?s).*Delta invariant violated for field 'value': value > 3.*
        """
        INSERT INTO delta_invariants_top_level VALUES (3, 'bad')
        """
      When query
        """
        SELECT COUNT(*) AS count
        FROM delta_invariants_top_level
        """
      Then query result
        | count |
        | 1     |

  Rule: Nested struct invariants from Delta metadata are enforced on writes
    Background:
      Given variable location for temporary directory invariants_nested
      Given final statement
        """
        DROP TABLE IF EXISTS delta_invariants_nested
        """
      Given statement template
        """
        CREATE TABLE delta_invariants_nested (
          id INT,
          person STRUCT<name: STRING, age: INT>
        )
        USING DELTA
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_invariants_nested
        VALUES (1, named_struct('name', 'seed', 'age', 18))
        """
      Given delta log first commit column person.age has invariant person.age >= 18

    Scenario: Valid nested rows are accepted
      Given statement
        """
        INSERT INTO delta_invariants_nested
        VALUES (2, named_struct('name', 'alice', 'age', 21))
        """
      When query
        """
        SELECT id, person.name AS name, person.age AS age
        FROM delta_invariants_nested
        ORDER BY id
        """
      Then query result ordered
        | id | name  | age |
        | 1  | seed  | 18  |
        | 2  | alice | 21  |

    Scenario: Invalid nested rows are rejected
      Given statement with error (?s).*Delta invariant violated for field 'person.age': person.age >= 18.*
        """
        INSERT INTO delta_invariants_nested
        VALUES (3, named_struct('name', 'bob', 'age', 17))
        """
      When query
        """
        SELECT COUNT(*) AS count
        FROM delta_invariants_nested
        """
      Then query result
        | count |
        | 1     |
