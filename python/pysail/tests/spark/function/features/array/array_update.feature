Feature: array update functions

  Rule: array_append

    Scenario: array_append preserves widened element type
      When query
        """
        SELECT array_append(array(1, 2), 3D) AS value, typeof(array_append(array(1, 2), 3D)) AS data_type
        """
      Then query result
        | value           | data_type     |
        | [1.0, 2.0, 3.0] | array<double> |

  Rule: array_insert

    Scenario: array_insert preserves widened element type
      When query
        """
        SELECT array_insert(array(1, 2), 2, 3D) AS value, typeof(array_insert(array(1, 2), 2, 3D)) AS data_type
        """
      Then query result
        | value           | data_type     |
        | [1.0, 3.0, 2.0] | array<double> |
