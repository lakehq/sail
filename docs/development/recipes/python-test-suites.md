---
title: Python Test Suites
rank: 33
---

# Python Test Suites

We organize Python tests into suites so that developers can select the areas relevant to a change.
The Build workflow uses these suites to control which tests run in CI, and you can use the same suites when testing locally.

Every test belongs to one suite, based on its test area or markers.
For example, `udf`, `dataframe`, `sql`, and `benchmark` cover their respective areas.
Integration tests belong to <code><SyntaxText raw="'integration-'<group>" /></code> suites, and grouped function tests belong to <code><SyntaxText raw="'function-'<group>" /></code> suites.
The `default` suite contains the remaining tests.

## Selecting Suites in CI

The Build workflow enables all suites except `slow` and `integration-*` by default for pull requests and pushes to the `main` branch.
Scheduled builds enable every suite.
For a pull request, maintainers can change the selection to include integration tests or focus on particular suites.

Python test discovery and selection run only when the workflow builds a Python package.
Documentation-only pull requests that reuse a baseline package skip Python tests and do not publish a Python test selection checklist.
Re-running these builds does not enable Python tests.

After the Build workflow completes, open the **Python Test Selection** comment and expand **Test Suites** to see the available suites and test counts.
Check or uncheck the suites you want to run, then **re-run all jobs** or push another commit to apply the selection.
Editing the comment does not start a build, and rerunning only failed jobs may reuse the previous selection.

Please wait for the current build to finish before editing the checklist.
When the build publishes its report, it replaces the checklist comment, so edits made while the build is running will be lost.

Your choices are retained as new suites are discovered, and new suites use the defaults.
If the checklist is missing or a suite has no checkbox, the defaults also apply.
Removed suites disappear from the checklist.

## Testing Across PySpark Versions

CI distributes selected tests across compatible PySpark versions.
An affinity identifies tests that share a preference order over PySpark versions.
By default, BDD scenarios use feature-file affinity, while other tests use module affinity.
Assignment stays consistent within a pull request.
Suites control test selection, independently of version assignment.
Tests requiring coverage across versions, such as UDF tests, run on every compatible version.

### Shared Resources

Fixtures that start expensive shared services can declare a resource to keep their consumers together across modules.
For example:

```python
import pytest

from pysail.testing.resources import resource


@pytest.fixture(scope="session")
@resource(name="catalog-service", weight=10)
def catalog_service():
    ...
```

Place `@resource` below `@pytest.fixture`.
The name is a nonempty string without surrounding whitespace, and the weight is a positive finite number representing priority when choosing an affinity.
The weight does not estimate runtime or determine execution order.
Tests inherit resources from their static fixture dependencies, including indirect dependencies, autouse fixtures, and `usefixtures` markers.
Fixture overrides replace the resource declaration unless they depend on the overridden fixture.
For dependencies requested dynamically with `request.getfixturevalue()`, declare the resource on the requesting fixture as well.

Each BDD scenario has feature-file affinity with weight 1, including all examples of a scenario outline.
The feature-file path identifies the affinity independently of the Python loader module.
Other tests, including doctests, have module affinity with weight 1.
The highest-weight resource provides resource affinity only when its weight exceeds 1.
Otherwise, the test keeps its default feature-file or module affinity.
Different tests in a module can therefore have different affinities.
Multiple fixtures can declare the same resource name and weight.
If a test depends on different resource names with the same weight, collection fails, even if another resource has a higher weight.
Collection also fails if a test depends on declarations of the same resource name with different weights.
These checks apply to local runs and discovery as well as matrix runs.

Each affinity has a deterministic preference order over PySpark versions, derived from the shared CI seed.
In the code, an affinity is a `(kind, identifier)` pair, such as `("resource", "catalog-service")` or `("feature", "_dev/spark/example.feature")`.
The kind distinguishes resource names from feature-file and module paths.
A test runs on the first version in that order satisfying all its `spark(version=...)` requirements.
Tests with `spark(cross_version=True)` still run on every compatible version.
Collection order and suite, keyword, or path filters do not change the preference order.

Resources express preferred placement and do not change fixture scope or lifecycle.
For ordinary tests, module-scoped fixtures are already kept with their module by default, so grouping their consumers across modules does not share fixture instances.
BDD feature files loaded by one module can be assigned to different versions, repeating module-scoped setup in those jobs.
Avoid assigning a resource to ubiquitous fixtures such as the default Spark session, which would concentrate many unrelated tests on one version.

### Coverage

The counts in the checklist describe the discovered tests.
The number executed depends on suite selection and version requirements.
Pull request builds and builds on the `main` branch report partial coverage, while scheduled builds provide a full-suite coverage baseline.
These coverage results are tracked separately so that partial runs do not replace the full baseline.

## Running Suites Locally

Use the pytest marker filter to select suites when working on a change.
For example, the following command runs the `udf` and `delta` suites.

```bash
hatch run pytest --pyargs pysail.tests pysail.tests._dev -m "suite(name='udf') or suite(name='delta')"
```

Local pytest runs include integration tests by default.
Use `-m 'not integration'` to exclude them, or `-m integration` to run only integration tests.
You can combine suite selection with `-k` and path filters to narrow the test run further.
Add `--collect-only` to inspect the selected tests without running them.

<script setup>
import SyntaxText from "@theme/components/SyntaxText.vue";
</script>
