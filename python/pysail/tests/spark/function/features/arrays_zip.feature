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

    Scenario: arrays_zip four args asymmetric
      When query
        """
        SELECT arrays_zip(array(1), array('a','b'), array(true, false, NULL), array(1.0)) AS result
        """
      Then query result
        | result                                                              |
        | [{1, a, true, 1.0}, {NULL, b, false, NULL}, {NULL, NULL, NULL, NULL}] |

    Scenario: arrays_zip self-zip same column
      When query
        """
        SELECT arrays_zip(a, a) AS result FROM VALUES (array(1,2,3)) AS t(a)
        """
      Then query result
        | result                   |
        | [{1, 1}, {2, 2}, {3, 3}] |

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

    Scenario: arrays_zip four args all empty returns empty
      When query
        """
        SELECT arrays_zip(
          CAST(array() AS ARRAY<INT>),
          CAST(array() AS ARRAY<INT>),
          CAST(array() AS ARRAY<INT>),
          CAST(array() AS ARRAY<INT>)
        ) AS result
        """
      Then query result
        | result |
        | []     |

    Scenario: arrays_zip very asymmetric 1 vs 5
      When query
        """
        SELECT arrays_zip(array(1), array('a','b','c','d','e')) AS result
        """
      Then query result
        | result                                               |
        | [{1, a}, {NULL, b}, {NULL, c}, {NULL, d}, {NULL, e}] |

  Rule: NULL handling

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

    Scenario: arrays_zip both args NULL returns NULL
      When query
        """
        SELECT arrays_zip(CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<STRING>)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: arrays_zip four NULL args returns NULL
      When query
        """
        SELECT arrays_zip(
          CAST(NULL AS ARRAY<INT>),
          CAST(NULL AS ARRAY<STRING>),
          CAST(NULL AS ARRAY<BOOLEAN>),
          CAST(NULL AS ARRAY<DOUBLE>)
        ) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: arrays_zip NULL and empty returns NULL
      When query
        """
        SELECT arrays_zip(CAST(NULL AS ARRAY<INT>), CAST(array() AS ARRAY<STRING>)) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: arrays_zip untyped empty array() pads first as NULL
      When query
        """
        SELECT arrays_zip(array(), array(1,2)) AS result
        """
      Then query result
        | result                 |
        | [{NULL, 1}, {NULL, 2}] |

  Rule: Type variety

    Scenario: arrays_zip int and double
      When query
        """
        SELECT arrays_zip(array(1,2), array(1.5,2.5)) AS result
        """
      Then query result
        | result                |
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

    Scenario: arrays_zip struct elements
      When query
        """
        SELECT arrays_zip(array(struct(1 AS a)), array(struct('x' AS b))) AS result
        """
      Then query result
        | result       |
        | [{{1}, {x}}] |

    Scenario: arrays_zip map elements
      When query
        """
        SELECT arrays_zip(array(map(1,'a')), array(map(2,'b'))) AS result
        """
      Then query result
        | result                 |
        | [{{1 -> a}, {2 -> b}}] |

    Scenario: arrays_zip binary elements
      When query
        """
        SELECT arrays_zip(array(X'01', X'02'), array(X'0A', X'0B')) AS result
        """
      Then query result
        | result                       |
        | [{[01], [0A]}, {[02], [0B]}] |

    Scenario: arrays_zip date elements
      When query
        """
        SELECT arrays_zip(array(DATE'2024-01-01'), array(DATE'2024-12-31')) AS result
        """
      Then query result
        | result                     |
        | [{2024-01-01, 2024-12-31}] |

    Scenario: arrays_zip timestamp elements
      When query
        """
        SELECT arrays_zip(array(TIMESTAMP'2024-01-01 00:00:00'), array(TIMESTAMP'2024-12-31 23:59:59')) AS result
        """
      Then query result
        | result                                       |
        | [{2024-01-01 00:00:00, 2024-12-31 23:59:59}] |

    Scenario: arrays_zip decimal elements
      When query
        """
        SELECT arrays_zip(
          array(CAST(1.5 AS DECIMAL(10,2)), CAST(2.5 AS DECIMAL(10,2))),
          array(1, 2)
        ) AS result
        """
      Then query result
        | result                 |
        | [{1.50, 1}, {2.50, 2}] |

    Scenario: arrays_zip float32 elements
      When query
        """
        SELECT arrays_zip(
          array(CAST(1.5 AS FLOAT)),
          array(CAST(2.5 AS FLOAT))
        ) AS result
        """
      Then query result
        | result       |
        | [{1.5, 2.5}] |

    Scenario: arrays_zip deeply nested arrays
      When query
        """
        SELECT arrays_zip(array(array(1,2)), array(array(3,4))) AS result
        """
      Then query result
        | result             |
        | [{[1, 2], [3, 4]}] |

    Scenario: arrays_zip sequence results
      When query
        """
        SELECT arrays_zip(sequence(1, 3), sequence(10, 12)) AS result
        """
      Then query result
        | result                      |
        | [{1, 10}, {2, 11}, {3, 12}] |

  Rule: Composition

    Scenario: arrays_zip nested in arrays_zip
      When query
        """
        SELECT arrays_zip(arrays_zip(array(1,2), array('a','b')), array(true, false)) AS result
        """
      Then query result
        | result                            |
        | [{{1, a}, true}, {{2, b}, false}] |

    Scenario: arrays_zip element field access by index-name
      When query
        """
        SELECT arrays_zip(array(1,2,3), array('x','y','z'))[1].`0` AS result
        """
      Then query result
        | result |
        | 2      |

    Scenario: arrays_zip element field access second position
      When query
        """
        SELECT arrays_zip(array(1,2,3), array('x','y','z'))[1].`1` AS result
        """
      Then query result
        | result |
        | y      |

    Scenario: arrays_zip to_json roundtrip
      When query
        """
        SELECT to_json(arrays_zip(array(1,2), array('a','b'))) AS result
        """
      Then query result
        | result                            |
        | [{"0":1,"1":"a"},{"0":2,"1":"b"}] |

    Scenario: arrays_zip LATERAL VIEW explode flatten
      When query
        """
        SELECT e.`0` AS x, e.`1` AS y FROM
          (SELECT arrays_zip(array(1,2,3), array('a','b','c')) AS z) LATERAL VIEW explode(z) AS e
        """
      Then query result ordered
        | x | y |
        | 1 | a |
        | 2 | b |
        | 3 | c |

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

    Scenario: arrays_zip multi-row uneven lengths per row
      When query
        """
        SELECT arrays_zip(a, b) AS result FROM VALUES
          (array(1,2,3), array('x','y')),
          (array(1), array('a','b','c','d','e')),
          (array(), array('z'))
        AS t(a, b)
        """
      Then query result
        | result                                                |
        | [{1, x}, {2, y}, {3, NULL}]                          |
        | [{1, a}, {NULL, b}, {NULL, c}, {NULL, d}, {NULL, e}] |
        | [{NULL, z}]                                           |

    Scenario: arrays_zip multi-row empty row mixed with NULL
      When query
        """
        SELECT arrays_zip(a, b) AS result FROM VALUES
          (array(), CAST(array() AS ARRAY<STRING>)),
          (array(1), array('x')),
          (CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<STRING>))
        AS t(a, b)
        """
      Then query result
        | result   |
        | []       |
        | [{1, x}] |
        | NULL     |

    Scenario: arrays_zip multi-row all-null column returns all NULL
      When query
        """
        SELECT arrays_zip(a, b) AS result FROM VALUES
          (CAST(NULL AS ARRAY<INT>), array(1, 2)),
          (CAST(NULL AS ARRAY<INT>), array(3, 4)),
          (CAST(NULL AS ARRAY<INT>), array(5, 6))
        AS t(a, b)
        """
      Then query result
        | result |
        | NULL   |
        | NULL   |
        | NULL   |

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
