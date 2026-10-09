Feature: Persistent views with legacy current configurations

  Rule: Captured configuration is used by default

    Scenario: an ANSI view keeps its ANSI shift count cast when read with ANSI disabled
      Given config spark.sql.ansi.enabled = true
      And final statement
        """
        DROP VIEW IF EXISTS view_captured_ansi_shift
        """
      And statement
        """
        CREATE OR REPLACE VIEW view_captured_ansi_shift AS
        SELECT shiftrightunsigned(id + 8, 4294967297L) AS v FROM range(2)
        """
      And config spark.sql.ansi.enabled = false
      When query
        """
        SELECT v FROM view_captured_ansi_shift
        """
      Then query error 4294967297

  Rule: Legacy current configurations

    Scenario: an ANSI view uses the current non-ANSI shift count cast
      Given config spark.sql.ansi.enabled = true
      And final statement
        """
        DROP VIEW IF EXISTS view_current_configs_shift
        """
      And statement
        """
        CREATE OR REPLACE VIEW view_current_configs_shift AS
        SELECT shiftrightunsigned(id + 8, 4294967297L) AS v FROM range(2)
        """
      And config spark.sql.ansi.enabled = false
      And config spark.sql.legacy.useCurrentConfigsForView = true
      When query
        """
        SELECT v FROM view_current_configs_shift
        """
      Then query result
        | v |
        | 4 |
        | 4 |

    Scenario: a non-ANSI view uses the current ANSI string-to-number coercion
      Given config spark.sql.ansi.enabled = false
      And final statement
        """
        DROP VIEW IF EXISTS view_current_configs_string
        """
      And statement
        """
        CREATE OR REPLACE VIEW view_current_configs_string AS
        SELECT IF(id > 0, 1, '2.5') AS v FROM range(2)
        """
      And config spark.sql.ansi.enabled = true
      And config spark.sql.legacy.useCurrentConfigsForView = true
      When query
        """
        SELECT v FROM view_current_configs_string
        """
      Then query error 2\.5

    # Spark casts the view output to the stored view schema (`SessionCatalog.castColToType`),
    # so the current non-ANSI FLOAT common type is read as the stored DOUBLE column.
    Scenario: a view read with current configurations keeps its stored column type
      Given config spark.sql.ansi.enabled = true
      And final statement
        """
        DROP VIEW IF EXISTS view_current_configs_float
        """
      And statement
        """
        CREATE OR REPLACE VIEW view_current_configs_float AS
        SELECT IF(id = 0, CAST(1.5 AS FLOAT), CAST(id AS BIGINT)) AS x FROM range(2)
        """
      And config spark.sql.ansi.enabled = false
      And config spark.sql.legacy.useCurrentConfigsForView = true
      When query
        """
        SELECT x, typeof(x) AS t FROM view_current_configs_float
        """
      Then query result
        | x   | t      |
        | 1.0 | double |
        | 1.5 | double |

    # TODO: Defer overflowing DECIMAL casts of constants like Spark. `typeof` does not
    #  evaluate its argument in Spark, but Sail folds the overflowing constant cast.
    @sail-bug
    Scenario: a view read with current legacy decimal configurations keeps its stored DECIMAL type
      Given final statement
        """
        DROP VIEW IF EXISTS view_current_configs_decimal
        """
      And statement
        """
        CREATE OR REPLACE VIEW view_current_configs_decimal AS
        SELECT IF(id = 0, CAST(1 AS DECIMAL(38,0)), CAST(0.1 AS DECIMAL(38,38))) AS d FROM range(1)
        """
      And config spark.sql.legacy.decimal.retainFractionDigitsOnTruncate = true
      And config spark.sql.legacy.useCurrentConfigsForView = true
      When query
        """
        SELECT typeof(d) AS t FROM view_current_configs_decimal
        """
      Then query result
        | t             |
        | decimal(38,0) |
