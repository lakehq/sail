Feature: TIME functions (make_time, time_diff, time_trunc)

  Rule: make_time

    Scenario: basic make_time
      When query
      """
      SELECT make_time(6, 30, 45.887) AS result
      """
      Then query result
      | result       |
      | 06:30:45.887 |

    Scenario: make_time midnight
      When query
      """
      SELECT make_time(0, 0, 0) AS result
      """
      Then query result
      | result   |
      | 00:00:00 |

    Scenario: make_time max precision
      When query
      """
      SELECT make_time(23, 59, 59.999999) AS result
      """
      Then query result
      | result          |
      | 23:59:59.999999 |

    Scenario: make_time integer seconds
      When query
      """
      SELECT make_time(12, 0, 30) AS result
      """
      Then query result
      | result   |
      | 12:00:30 |

    Scenario: make_time NULL propagation
      When query
      """
      SELECT make_time(NULL, 30, 0) AS result
      """
      Then query result
      | result |
      | NULL   |


  Rule: time_diff

    Scenario: time_diff hours exact
      When query
      """
      SELECT time_diff('HOUR', TIME '20:30:29', TIME '21:30:29') AS result
      """
      Then query result
      | result |
      | 1      |

    Scenario: time_diff hours truncation
      When query
      """
      SELECT time_diff('HOUR', TIME '20:30:29', TIME '21:30:28') AS result
      """
      Then query result
      | result |
      | 0      |

    Scenario: time_diff negative
      When query
      """
      SELECT time_diff('HOUR', TIME '20:30:29', TIME '12:00:00') AS result
      """
      Then query result
      | result |
      | -8     |

    Scenario: time_diff minutes
      When query
      """
      SELECT time_diff('MINUTE', TIME '10:00:00', TIME '10:45:30') AS result
      """
      Then query result
      | result |
      | 45     |

    Scenario: time_diff seconds
      When query
      """
      SELECT time_diff('SECOND', TIME '10:00:00', TIME '10:00:30') AS result
      """
      Then query result
      | result |
      | 30     |

    Scenario: time_diff microseconds
      When query
      """
      SELECT time_diff('MICROSECOND', TIME '00:00:00', TIME '00:00:01') AS result
      """
      Then query result
      | result  |
      | 1000000 |

    Scenario: time_diff milliseconds
      When query
      """
      SELECT time_diff('MILLISECOND', TIME '00:00:00', TIME '00:00:01.500') AS result
      """
      Then query result
      | result |
      | 1500   |

    Scenario: time_diff NULL start propagates to NULL
      When query
      """
      SELECT time_diff('HOUR', NULL, TIME '01:00:00') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: time_diff NULL end propagates to NULL
      When query
      """
      SELECT time_diff('MINUTE', TIME '10:00:00', NULL) AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: time_diff NULL unit propagates to NULL
      When query
      """
      SELECT time_diff(NULL, TIME '10:00:00', TIME '11:00:00') AS result
      """
      Then query result
      | result |
      | NULL   |

  Rule: time_trunc

    Scenario: time_trunc hour
      When query
      """
      SELECT time_trunc('HOUR', TIME '09:32:05.359') AS result
      """
      Then query result
      | result   |
      | 09:00:00 |

    Scenario: time_trunc minute
      When query
      """
      SELECT time_trunc('MINUTE', TIME '09:32:05.359') AS result
      """
      Then query result
      | result   |
      | 09:32:00 |

    Scenario: time_trunc second
      When query
      """
      SELECT time_trunc('SECOND', TIME '09:32:05.359') AS result
      """
      Then query result
      | result   |
      | 09:32:05 |

    Scenario: time_trunc millisecond
      When query
      """
      SELECT time_trunc('MILLISECOND', TIME '09:32:05.123456') AS result
      """
      Then query result
      | result       |
      | 09:32:05.123 |

    Scenario: time_trunc microsecond passthrough
      When query
      """
      SELECT time_trunc('MICROSECOND', TIME '09:32:05.123456') AS result
      """
      Then query result
      | result          |
      | 09:32:05.123456 |

    Scenario: time_trunc NULL unit propagates to NULL
      When query
      """
      SELECT time_trunc(NULL, TIME '09:32:05.123456') AS result
      """
      Then query result
      | result |
      | NULL   |

    Scenario: time_trunc NULL time propagates to NULL
      When query
      """
      SELECT time_trunc('HOUR', NULL) AS result
      """
      Then query result
      | result |
      | NULL   |
