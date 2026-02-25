Feature: concat_ws function

  Rule: concat_ws with scalar arguments

    Scenario: concat_ws with multiple string arguments
      When query
        """
        SELECT concat_ws(',', 'a', 'b', 'c') AS result
        """
      Then query result
        | result  |
        | a,b,c   |

    Scenario: concat_ws with null arguments
      When query
        """
        SELECT concat_ws(',', 'a', NULL, 'c') AS result
        """
      Then query result
        | result  |
        | a,c     |

    Scenario: concat_ws with single argument
      When query
        """
        SELECT concat_ws(',', 'a') AS result
        """
      Then query result
        | result  |
        | a       |

    Scenario: concat_ws with no arguments after separator
      When query
        """
        SELECT concat_ws(',') AS result
        """
      Then query result
        | result  |
        |         |

    Scenario: concat_ws with null separator returns null
      When query
        """
        SELECT concat_ws(NULL, 'a', 'b', 'c') AS result
        """
      Then query result
        | result  |
        | NULL    |

  Rule: concat_ws with array arguments

    Scenario: concat_ws with array argument
      When query
        """
        SELECT concat_ws(',', array('a', 'b', 'c')) AS result
        """
      Then query result
        | result  |
        | a,b,c   |

    Scenario: concat_ws with array containing nulls
      When query
        """
        SELECT concat_ws(',', array('a', NULL, 'c')) AS result
        """
      Then query result
        | result  |
        | a,c     |

    Scenario: concat_ws with multiple arrays
      When query
        """
        SELECT concat_ws(',', array('a', 'b'), array('c', 'd')) AS result
        """
      Then query result
        | result    |
        | a,b,c,d   |

    Scenario: concat_ws with mixed scalar and array arguments
      When query
        """
        SELECT concat_ws(',', 'x', array('a', 'b'), 'y') AS result
        """
      Then query result
        | result    |
        | x,a,b,y   |
