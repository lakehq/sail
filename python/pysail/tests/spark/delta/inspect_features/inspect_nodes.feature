Feature: Delta Lake Inspect Node Outputs

  Background:
    Given variable location for temporary directory delta_inspect_nodes
    Given final statement
      """
      DROP TABLE IF EXISTS delta_inspect_nodes
      """
    Given statement template
      """
      CREATE TABLE delta_inspect_nodes (
      id INT,
      value STRING
      )
      USING DELTA LOCATION {{ location.sql }}
      """
    Given statement
      """
      INSERT INTO delta_inspect_nodes SELECT * FROM VALUES (1, 'v1'), (2, 'v2')
      """

  Scenario: INSPECT NODE_OUTPUT captures DeltaCommitExec output
    When query
      """
      INSPECT NODE_OUTPUT 'DeltaCommitExec' FOR
      INSERT INTO delta_inspect_nodes SELECT * FROM VALUES (3, 'v3')
      """
    Then query result
      | count |
      | 1     |

  Scenario: INSPECT NODE_OUTPUT captures DeltaScanByAddsExec output
    When query
      """
      INSPECT NODE_OUTPUT 'DeltaScanByAddsExec' FOR
      DELETE FROM delta_inspect_nodes WHERE id = 2
      """
    Then query result
      | id | value |
      | 1  | v1    |
      | 2  | v2    |

  Scenario: INSPECT NODE_OUTPUT captures DeltaLogReplayExec columns
    When query
      """
      INSPECT NODE_OUTPUT 'DeltaLogReplayExec' FOR
      DELETE FROM delta_inspect_nodes WHERE id = 2
      AS PRETTY
      """
    Then query output matches snapshot

  Scenario: INSPECT NODE_OUTPUT captures DeltaDiscoveryExec columns
    When query
      """
      INSPECT NODE_OUTPUT 'DeltaDiscoveryExec' FOR
      DELETE FROM delta_inspect_nodes WHERE id = 2
      AS PRETTY
      """
    Then query output matches snapshot

  Scenario: INSPECT NODE_OUTPUT captures DeltaRemoveActionsExec columns
    When query
      """
      INSPECT NODE_OUTPUT 'DeltaRemoveActionsExec' FOR
      DELETE FROM delta_inspect_nodes WHERE id = 2
      AS PRETTY
      """
    Then query output matches snapshot

  Scenario: INSPECT NODE_OUTPUT captures DeltaWriterExec columns
    When query
      """
      INSPECT NODE_OUTPUT 'DeltaWriterExec' FOR
      INSERT INTO delta_inspect_nodes SELECT * FROM VALUES (3, 'v3')
      AS PRETTY
      """
    Then query output matches snapshot
