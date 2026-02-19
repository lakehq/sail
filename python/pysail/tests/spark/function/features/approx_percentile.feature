Feature: approx_percentile() and percentile_approx() aggregate functions

  Rule: Scalar percentile

    Scenario: approx_percentile with scalar percentile
      When query
      """
      SELECT approx_percentile(x, 0.5) AS median FROM (VALUES (0), (1), (2), (3), (4)) AS t(x)
      """
      Then query result
      | median |
      | 2.0    |

    Scenario: percentile_approx with scalar percentile
      When query
      """
      SELECT percentile_approx(x, 0.5) AS median FROM (VALUES (0), (1), (2), (3), (4)) AS t(x)
      """
      Then query result
      | median |
      | 2.0    |

  Rule: Array of percentiles

    Scenario: approx_percentile with array of percentiles
      When query
      """
      SELECT approx_percentile(col, array(0.25, 0.5, 0.75)) AS percentiles FROM (VALUES (0), (1), (2), (3), (4)) AS t(col)
      """
      Then query result
      | percentiles     |
      | [1.0, 2.0, 3.0] |

    Scenario: approx_percentile with single-element array
      When query
      """
      SELECT approx_percentile(col, array(0.5)) AS percentiles FROM (VALUES (0), (1), (2), (3), (4)) AS t(col)
      """
      Then query result
      | percentiles |
      | [2.0]       |

  Rule: With accuracy argument (3rd arg is dropped)

    Scenario: approx_percentile with accuracy argument
      When query
      """
      SELECT approx_percentile(x, 0.5, 100) AS median FROM (VALUES (0), (1), (2), (3), (4)) AS t(x)
      """
      Then query result
      | median |
      | 2.0    |

    Scenario: approx_percentile with array percentiles and accuracy
      When query
      """
      SELECT approx_percentile(x, array(0.25, 0.75), 100) AS percentiles FROM (VALUES (0), (1), (2), (3), (4)) AS t(x)
      """
      Then query result
      | percentiles     |
      | [1.0, 3.0]      |

  Rule: NULL handling

    Scenario: approx_percentile ignores NULLs
      When query
      """
      SELECT approx_percentile(x, 0.5) AS median FROM (VALUES (NULL), (1), (2), (3), (NULL)) AS t(x)
      """
      Then query result
      | median |
      | 2.0    |

    Scenario: approx_percentile with all NULLs returns NULL
      When query
      """
      SELECT approx_percentile(x, 0.5) AS median FROM (VALUES (CAST(NULL AS INT)), (NULL), (NULL)) AS t(x)
      """
      Then query result
      | median |
      | NULL   |
