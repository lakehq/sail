Feature: DataFrame joins

  # Migrated from `python/pysail/tests/spark/dataframe/test_join.txt`, which was itself adapted
  # from the doctests of `pyspark.sql.dataframe.DataFrame.join`. These cases stay on the
  # DataFrame API because a column object such as `df1.name` resolves through the plan id of
  # the frame it came from, which a SQL qualifier does not go through.

  Rule: A join on a name keeps one copy of the key

    Scenario: join on name
      When dataframe for join on name
      Then dataframe result
        | name | age | height |
        | Bob  | 5   | 85     |

    Scenario: join on name selecting a column of each side
      When dataframe for join on name selecting a column of each side
      Then dataframe result
        | name | height |
        | Bob  | 85     |

    Scenario: join on name selecting columns by name
      When dataframe for join on name selecting columns by name
      Then dataframe result
        | name | height |
        | Bob  | 85     |

    Scenario: join on two names
      When dataframe for join on two names
      Then dataframe result
        | name | age | height |
        | Bob  | 5   | NULL   |

    Scenario: join on two names selecting the left side
      When dataframe for join on two names selecting the left side
      Then dataframe result
        | name | age |
        | Bob  | 5   |

  Rule: A shared name is ambiguous when the join keeps both copies

    Scenario: join on a name equality
      When dataframe for join on a name equality
      Then dataframe result
        | name | age | name | height |
        | Bob  | 5   | Bob  | 85     |

    Scenario: join on a name equality selecting the duplicated name
      When dataframe for join on a name equality selecting the duplicated name
      Then dataframe error AMBIGUOUS_REFERENCE

    Scenario: outer self join selecting the ambiguous name
      When dataframe for outer self join selecting the ambiguous name
      Then dataframe error AMBIGUOUS_COLUMN_REFERENCE

    Scenario: outer self join of two aliases
      When dataframe for outer self join of two aliases
      Then dataframe result ordered
        | name  | age |
        | Bob   | 5   |
        | Alice | 2   |

  Rule: An outer join on a condition resolves each side through its own frame

    Scenario: outer join on a name equality
      When dataframe for outer join on a name equality
      Then dataframe result ordered
        | name  | age  | name | height |
        | Bob   | 5    | Bob  | 85     |
        | Alice | 2    | NULL | NULL   |
        | NULL  | NULL | Tom  | 80     |

    Scenario: outer join on a name equality selecting a column of each side
      When dataframe for outer join on a name equality selecting a column of each side
      Then dataframe result ordered
        | name  | height |
        | Bob   | 85     |
        | Alice | NULL   |
        | NULL  | 80     |

    Scenario: outer join on a name equality sorted after the projection
      When dataframe for outer join on a name equality sorted after the projection
      Then dataframe result ordered
        | name  | height |
        | Bob   | 85     |
        | Alice | NULL   |
        | NULL  | 80     |

    Scenario: outer join on two equalities
      When dataframe for outer join on two equalities
      Then dataframe result ordered
        | name  | age  |
        | NULL  | NULL |
        | NULL  | NULL |
        | NULL  | 10   |
        | Alice | NULL |
        | Bob   | 5    |

  Rule: An outer join on a name resolves the key from either side

    Scenario: outer join on name
      When dataframe for outer join on name
      Then dataframe result ordered
        | name  | age  | height |
        | Tom   | NULL | 80     |
        | Bob   | 5    | 85     |
        | Alice | 2    | NULL   |

    Scenario: outer join on name sorted by the left side
      When dataframe for outer join on name sorted by the left side
      Then dataframe result ordered
        | name  | age  | height |
        | Bob   | 5    | 85     |
        | Alice | 2    | NULL   |
        | Tom   | NULL | 80     |

    Scenario: outer join on name sorted by the right side
      When dataframe for outer join on name sorted by the right side
      Then dataframe result ordered
        | name  | age  | height |
        | Tom   | NULL | 80     |
        | Bob   | 5    | 85     |
        | Alice | 2    | NULL   |

    Scenario: outer join on name selecting columns by name
      When dataframe for outer join on name selecting columns by name
      Then dataframe result ordered
        | name  | height |
        | Tom   | 80     |
        | Bob   | 85     |
        | Alice | NULL   |

    Scenario: outer join on name selecting the left name
      When dataframe for outer join on name selecting the left name
      Then dataframe result ordered
        | name  | height |
        | Bob   | 85     |
        | Alice | NULL   |
        | NULL  | 80     |

    Scenario: outer join on name selecting the right name
      When dataframe for outer join on name selecting the right name
      Then dataframe result ordered
        | name | height |
        | Tom  | 80     |
        | Bob  | 85     |
        | NULL | NULL   |

    Scenario: outer join on two names
      When dataframe for outer join on two names
      Then dataframe result ordered
        | name  | age  | height |
        | NULL  | NULL | NULL   |
        | Alice | 2    | NULL   |
        | Alice | 10   | 80     |
        | Bob   | 5    | NULL   |
        | Tom   | NULL | NULL   |

  Rule: Every join type is available through the DataFrame API

    Scenario: left outer join on name
      When dataframe for left outer join on name
      Then dataframe result ordered
        | name  | age | height |
        | Alice | 2   | NULL   |
        | Bob   | 5   | 85     |

    Scenario: right outer join on name
      When dataframe for right outer join on name
      Then dataframe result ordered
        | name | age  | height |
        | Bob  | 5    | 85     |
        | Tom  | NULL | 80     |

    Scenario: left semi join on name
      When dataframe for left semi join on name
      Then dataframe result
        | name | age |
        | Bob  | 5   |

    Scenario: left anti join on name
      When dataframe for left anti join on name
      Then dataframe result
        | name  | age |
        | Alice | 2   |
