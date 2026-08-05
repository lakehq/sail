Feature: Delta Lake Update

  Rule: Expanded row updates
    Background:
      Given variable location for temporary directory delta_update
      Given final statement
        """
        DROP TABLE IF EXISTS delta_update_basic
        """
      Given statement template
        """
        CREATE TABLE delta_update_basic (
          id INT,
          value INT,
          previous_value INT,
          label STRING
        )
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_update_basic
        SELECT * FROM VALUES
          (1, 10, 100, 'keep'),
          (2, 20, 200, 'change'),
          (3, 30, 300, 'change')
        """

    Scenario: Conditional UPDATE rewrites changed rows and copies the rest of each touched file
      Given statement
        """
        UPDATE delta_update_basic AS target
        SET value = target.value + 5,
            label = concat(target.label, '-updated')
        WHERE target.id >= 2
        """
      Then delta log latest commit info contains
        | path                              | value               |
        | operation                         | "UPDATE"            |
        | operationParameters.predicate | "target . id >= 2 " |
        | operationMetrics.numUpdatedRows   | 2                   |
        | operationMetrics.numCopiedRows    | 1                   |
      When query
        """
        SELECT id, value, previous_value, label
        FROM delta_update_basic
        ORDER BY id
        """
      Then query result ordered
        | id | value | previous_value | label          |
        | 1  | 10    | 100            | keep           |
        | 2  | 25    | 200            | change-updated |
        | 3  | 35    | 300            | change-updated |

    Scenario: UPDATE assignments use the original row values
      Given statement
        """
        UPDATE delta_update_basic
        SET value = previous_value,
            previous_value = value
        WHERE id = 1
        """
      When query
        """
        SELECT id, value, previous_value, label
        FROM delta_update_basic
        ORDER BY id
        """
      Then query result ordered
        | id | value | previous_value | label  |
        | 1  | 100   | 10             | keep   |
        | 2  | 20    | 200            | change |
        | 3  | 30    | 300            | change |

    Scenario: UPDATE without a predicate changes every row
      Given statement
        """
        UPDATE delta_update_basic
        SET value = value * 2
        """
      When query
        """
        SELECT id, value, previous_value, label
        FROM delta_update_basic
        ORDER BY id
        """
      Then query result ordered
        | id | value | previous_value | label  |
        | 1  | 20    | 100            | keep   |
        | 2  | 40    | 200            | change |
        | 3  | 60    | 300            | change |

    Scenario: EXPLAIN EXTENDED shows UPDATE row actions before the Delta rewrite
      When query
        """
        EXPLAIN EXTENDED
        UPDATE delta_update_basic AS target
        SET value = target.value + 5
        WHERE target.id = 2
        """
      Then query plan matches snapshot
