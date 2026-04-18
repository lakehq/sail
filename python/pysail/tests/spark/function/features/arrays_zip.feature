@arrays_zip
Feature: arrays_zip comprehensive tests

  Rule: Basic usage

    Scenario: arrays_zip two arrays same length
      When query
        """
        SELECT arrays_zip(array(1,2,3), array('a','b','c')) AS result
        """
      Then query result
        | result                   |
        | [{1, a}, {2, b}, {3, c}] |

    Scenario: arrays_zip three arrays
      When query
        """
        SELECT arrays_zip(array(1,2), array('a','b'), array(true,false)) AS result
        """
      Then query result
        | result                        |
        | [{1, a, true}, {2, b, false}] |

    Scenario: arrays_zip single array
      When query
        """
        SELECT arrays_zip(array(1,2,3)) AS result
        """
      Then query result
        | result          |
        | [{1}, {2}, {3}] |

    @sail-bug
    # Sail rejects 0 args but Spark returns empty array
    Scenario: arrays_zip zero args returns empty array
      When query
        """
        SELECT arrays_zip() AS result
        """
      Then query result
        | result |
        | []     |

  Rule: Different array lengths

    Scenario: arrays_zip first longer pads NULL
      When query
        """
        SELECT arrays_zip(array(1,2,3), array('a','b')) AS result
        """
      Then query result
        | result                      |
        | [{1, a}, {2, b}, {3, NULL}] |

    Scenario: arrays_zip second longer pads NULL
      When query
        """
        SELECT arrays_zip(array(1,2), array('a','b','c')) AS result
        """
      Then query result
        | result                      |
        | [{1, a}, {2, b}, {NULL, c}] |

    Scenario: arrays_zip one empty array
      When query
        """
        SELECT arrays_zip(array(1,2), array()) AS result
        """
      Then query result
        | result                 |
        | [{1, NULL}, {2, NULL}] |

    Scenario: arrays_zip both empty arrays
      When query
        """
        SELECT arrays_zip(array(), array()) AS result
        """
      Then query result
        | result |
        | []     |

  Rule: NULL handling

    @sail-bug
    # Sail errors on untyped NULL but Spark returns NULL
    Scenario: arrays_zip untyped NULL array returns NULL
      When query
        """
        SELECT arrays_zip(NULL, array(1,2)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: arrays_zip typed NULL array returns NULL
      When query
        """
        SELECT arrays_zip(CAST(NULL AS ARRAY<INT>), array(1,2)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: arrays_zip with NULL elements
      When query
        """
        SELECT arrays_zip(array(1,NULL,3), array('a','b','c')) AS result
        """
      Then query result
        | result                      |
        | [{1, a}, {NULL, b}, {3, c}] |

    Scenario: arrays_zip all NULL elements
      When query
        """
        SELECT arrays_zip(array(NULL,NULL), array(NULL,NULL)) AS result
        """
      Then query result
        | result                       |
        | [{NULL, NULL}, {NULL, NULL}] |

  Rule: Type variety

    Scenario: arrays_zip int and double
      When query
        """
        SELECT arrays_zip(array(1,2), array(1.5,2.5)) AS result
        """
      Then query result
        | result                 |
        | [{1, 1.5}, {2, 2.5}]  |

    Scenario: arrays_zip nested arrays
      When query
        """
        SELECT arrays_zip(array(array(1,2),array(3,4)), array('a','b')) AS result
        """
      Then query result
        | result                     |
        | [{[1, 2], a}, {[3, 4], b}] |

    Scenario: arrays_zip boolean and int
      When query
        """
        SELECT arrays_zip(array(true,false), array(1,2)) AS result
        """
      Then query result
        | result                  |
        | [{true, 1}, {false, 2}] |

  Rule: Multi-row

    Scenario: arrays_zip multi-row
      When query
        """
        SELECT arrays_zip(a, b) AS result FROM VALUES (array(1,2), array('x','y')), (array(3), array('z','w')), (CAST(NULL AS ARRAY<INT>), array('a')) AS t(a, b)
        """
      Then query result
        | result              |
        | [{1, x}, {2, y}]   |
        | [{3, z}, {NULL, w}] |
        | NULL                |

  Rule: Error conditions

    Scenario: arrays_zip non-array input errors
      When query
        """
        SELECT arrays_zip(1, 2) AS result
        """
      Then query error .*

    Scenario: arrays_zip mixed array and non-array errors
      When query
        """
        SELECT arrays_zip(array(1,2), 'hello') AS result
        """
      Then query error .*
