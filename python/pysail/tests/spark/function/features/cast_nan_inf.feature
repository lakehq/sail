@cast_nan_inf
Feature: CAST and type constructors with NaN and Infinity (issue #630)

  Rule: FLOAT type constructor

    Scenario: FLOAT NaN
      When query
        """
        SELECT FLOAT('NAN') AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: FLOAT NaN lowercase
      When query
        """
        SELECT FLOAT('nan') AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: FLOAT NaN mixed case
      When query
        """
        SELECT FLOAT('Nan') AS result
        """
      Then query result
        | result |
        | NaN    |

    @sail-bug
    # Sail does not trim spaces before parsing NaN
    Scenario: FLOAT NaN with spaces
      When query
        """
        SELECT FLOAT(' NaN ') AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: FLOAT negative NaN
      When query
        """
        SELECT FLOAT('-NaN') AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: FLOAT Infinity
      When query
        """
        SELECT FLOAT('Infinity') AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: FLOAT negative Infinity
      When query
        """
        SELECT FLOAT('-Infinity') AS result
        """
      Then query result
        | result    |
        | -Infinity |

    Scenario: FLOAT Infinity lowercase
      When query
        """
        SELECT FLOAT('infinity') AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: FLOAT normal value
      When query
        """
        SELECT FLOAT('42') AS result
        """
      Then query result
        | result |
        | 42.0   |

  Rule: DOUBLE type constructor

    Scenario: DOUBLE NaN
      When query
        """
        SELECT DOUBLE('NAN') AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: DOUBLE Infinity uppercase
      When query
        """
        SELECT DOUBLE('INFINITY') AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: DOUBLE negative Infinity uppercase
      When query
        """
        SELECT DOUBLE('-INFINITY') AS result
        """
      Then query result
        | result    |
        | -Infinity |

    @sail-bug
    # Sail does not trim spaces before parsing Infinity
    Scenario: DOUBLE Infinity with spaces
      When query
        """
        SELECT DOUBLE(' Infinity ') AS result
        """
      Then query result
        | result   |
        | Infinity |

    Scenario: DOUBLE normal value
      When query
        """
        SELECT DOUBLE('3.14') AS result
        """
      Then query result
        | result |
        | 3.14   |

  Rule: CAST to FLOAT/DOUBLE

    Scenario: CAST NaN to FLOAT
      When query
        """
        SELECT CAST('NaN' AS FLOAT) AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: CAST NaN to DOUBLE
      When query
        """
        SELECT CAST('NaN' AS DOUBLE) AS result
        """
      Then query result
        | result |
        | NaN    |

  Rule: Integer types reject NaN and Infinity

    Scenario: INT NaN errors
      When query
        """
        SELECT INT('NAN') AS result
        """
      Then query error .*

    Scenario: CAST NaN to INT errors
      When query
        """
        SELECT CAST('NaN' AS INT) AS result
        """
      Then query error .*

    Scenario: INT Infinity errors
      When query
        """
        SELECT INT('Infinity') AS result
        """
      Then query error .*

    Scenario: BIGINT NaN errors
      When query
        """
        SELECT BIGINT('NaN') AS result
        """
      Then query error .*

    Scenario: SMALLINT NaN errors
      When query
        """
        SELECT SMALLINT('NaN') AS result
        """
      Then query error .*

    Scenario: TINYINT NaN errors
      When query
        """
        SELECT TINYINT('NaN') AS result
        """
      Then query error .*

    Scenario: DECIMAL NaN errors
      When query
        """
        SELECT CAST('NaN' AS DECIMAL(10,2)) AS result
        """
      Then query error .*

    Scenario: INT invalid string errors
      When query
        """
        SELECT INT('hello') AS result
        """
      Then query error .*

  Rule: TRY_CAST with NaN

    Scenario: TRY_CAST NaN to INT returns NULL
      When query
        """
        SELECT TRY_CAST('NaN' AS INT) AS result
        """
      Then query result
        | result |
        | NULL   |

    Scenario: TRY_CAST NaN to FLOAT returns NaN
      When query
        """
        SELECT TRY_CAST('NaN' AS FLOAT) AS result
        """
      Then query result
        | result |
        | NaN    |

  Rule: NaN arithmetic

    Scenario: NaN plus number is NaN
      When query
        """
        SELECT FLOAT('NaN') + 1 AS result
        """
      Then query result
        | result |
        | NaN    |

    Scenario: NaN equals NaN is true in Spark
      When query
        """
        SELECT FLOAT('NaN') = FLOAT('NaN') AS result
        """
      Then query result
        | result |
        | true   |

    Scenario: NaN greater than zero is true in Spark
      When query
        """
        SELECT FLOAT('NaN') > 0 AS result
        """
      Then query result
        | result |
        | true   |

  Rule: Multi-row with NaN and Infinity

    Scenario: VALUES with FLOAT NaN Infinity and NULL
      When query
        """
        SELECT * FROM VALUES (FLOAT('NaN')), (FLOAT('Infinity')), (FLOAT('-Infinity')), (NULL), (0.0), (1.5) AS t(v)
        """
      Then query result
        | v         |
        | NaN       |
        | Infinity  |
        | -Infinity |
        | NULL      |
        | 0.0       |
        | 1.5       |

    Scenario: VALUES with DOUBLE NaN Infinity and NULL
      When query
        """
        SELECT * FROM VALUES (DOUBLE('NaN')), (DOUBLE('Infinity')), (DOUBLE('-Infinity')), (NULL), (0.0) AS t(v)
        """
      Then query result
        | v         |
        | NaN       |
        | Infinity  |
        | -Infinity |
        | NULL      |
        | 0.0       |
