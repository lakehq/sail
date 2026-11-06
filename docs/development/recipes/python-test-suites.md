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
Tests with the same suite and version requirements run together, and their assignment stays consistent within a pull request.
Tests requiring coverage across versions, such as UDF tests, run on every compatible version.

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
