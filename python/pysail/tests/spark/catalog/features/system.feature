Feature: System catalog queries

  Scenario: Filter and limit pushdown for system tables
    When query
      """
      EXPLAIN SELECT * FROM system.session.sessions
      WHERE session_id = ''
      LIMIT 1
      """
    Then query plan matches snapshot

    When query
      """
      EXPLAIN SELECT * FROM system.session.sessions
      WHERE session_id != '' AND session_id != 'x'
      LIMIT 1
      """
    Then query plan matches snapshot

    When query
      """
      EXPLAIN SELECT * FROM system.execution.jobs
      WHERE session_id = '' AND job_id = 0
      LIMIT 1
      """
    Then query plan matches snapshot

    When query
      """
      EXPLAIN SELECT * FROM system.execution.stages
      WHERE session_id = '' AND job_id = 0
      LIMIT 1
      """
    Then query plan matches snapshot

    When query
      """
      EXPLAIN SELECT * FROM system.execution.tasks
      WHERE session_id = '' AND job_id = 0
      LIMIT 1
      """
    Then query plan matches snapshot

    When query
      """
      EXPLAIN SELECT * FROM system.cluster.workers
      WHERE session_id = '' AND worker_id = 0
      LIMIT 1
      """
    Then query plan matches snapshot

  Scenario: Partial filter pushdown for system tables
    When query
      """
      EXPLAIN SELECT * FROM system.execution.jobs
      WHERE concat(session_id, cast(job_id AS string)) = '0' AND job_id + 1 = 1
      """
    Then query plan matches snapshot

  Scenario: No filter pushdown for system tables
    When query
      """
      EXPLAIN SELECT * FROM system.execution.jobs
      WHERE session_id = '' OR job_id = 0
      """
    Then query plan matches snapshot

  Scenario: Projection for system tables
    When query
      """
      EXPLAIN SELECT session_id, status FROM system.session.sessions
      """
    Then query plan matches snapshot

  Scenario: Session table queries
    When query
      """
      SELECT count(*) AS count FROM (
        SELECT session_id FROM system.session.sessions
        WHERE user_id = current_user()
        LIMIT 1
      )
      """
    Then query result
      | count |
      | 1     |
