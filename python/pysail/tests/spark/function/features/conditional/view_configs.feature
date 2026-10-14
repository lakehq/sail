Feature: Persistent views with captured configurations

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
