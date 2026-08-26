@spark-4.2
Feature: Clone an isolated Spark Connect session
  A client can fork session state without sharing later mutations or runtime ownership.

  # Runner: python/pysail/tests/spark/session/test_clone_session.py.

  Scenario: Clone inherited state and mutate both sessions independently
    Given a source session with configuration and a temporary view
    When the client clones the source session
    Then the clone has a different valid session UUID
    And the clone inherits the configuration and temporary view
    And later configuration and temporary-view changes remain isolated

  Scenario: Clone with an explicit target UUID
    Given a running source session
    When the client clones it with a valid target UUID
    Then the clone uses that target UUID

  Scenario: Reject an invalid target UUID
    Given a running source session
    When the client clones it with an invalid target UUID
    Then the clone request fails and the source remains usable

  Scenario Outline: Releasing either session preserves the other
    Given a source session and its clone
    When the client releases the <released> session
    Then the <remaining> session can execute a query

    Examples:
      | released | remaining |
      | source   | clone     |
      | clone    | source    |
