"""BDD tests for Arrow Flight SQL using pytest-bdd.

This module uses pytest-bdd to run Gherkin feature files that test
the Sail Arrow Flight SQL server functionality.
"""

import pytest
from pytest_bdd import scenarios

# Load all scenarios from the features directory
scenarios("features/")


# Mark all tests as requiring Flight SQL server
pytestmark = pytest.mark.flight_sql
