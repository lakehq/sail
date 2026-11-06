const fs = require("node:fs");

const {
  findReportComment,
  renderReport,
  writeReport,
} = require("./pull-request-report.cjs");

const REPORT_NAME = "python-test-selection";

function resolveSuiteChoices(suites, event, overrides = {}) {
  if (!["pull_request", "push", "schedule"].includes(event)) {
    throw new Error(`Unsupported test event: ${event}`);
  }
  return suites
    .filter(({ name }) => {
      if (event === "pull_request" && Object.hasOwn(overrides, name)) {
        return overrides[name];
      }
      return (
        event === "schedule" ||
        (name !== "slow" && !name.startsWith("integration-"))
      );
    })
    .map(({ name }) => name);
}

function parseSuiteChoices(body = "") {
  const choices = {};
  const checkboxes = /^- \[([ xX])\] `([a-z0-9][a-z0-9-]*)`(?:\s|$)/gm;
  for (const [, checked, name] of body.matchAll(checkboxes)) {
    choices[name] = checked.toLowerCase() === "x";
  }
  return choices;
}

function validateInventory(inventory) {
  if (!Array.isArray(inventory)) {
    throw new Error("Expected a suite inventory array");
  }
  if (!inventory.length || inventory.length > 200)
    throw new Error("Invalid suite inventory size");
  const names = new Set();
  for (const entry of inventory) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry))
      throw new Error("Invalid suite inventory entry");
    const { name, test_count } = entry;
    if (
      typeof name !== "string" ||
      !/^[a-z0-9][a-z0-9-]{0,79}$/.test(name) ||
      names.has(name) ||
      !Number.isSafeInteger(test_count) ||
      test_count <= 0
    ) {
      throw new Error("Invalid suite inventory entry");
    }
    names.add(name);
  }
  return [...inventory].sort((a, b) => a.name.localeCompare(b.name));
}

async function readSuiteChoices({ github, context }) {
  const pr = context.payload.pull_request;
  if (!pr) return {};
  const comment = await findReportComment(
    github,
    context.repo,
    pr.number,
    REPORT_NAME,
  );
  return parseSuiteChoices(comment?.body);
}

function writeSuiteReport({
  context,
  runAttempt,
  suites,
  choices,
  reportPath = "python-test-selection-report",
}) {
  const pr = context.payload.pull_request;
  if (!pr) return;
  const report = renderSuiteChecklist(
    suites,
    {
      id: context.runId,
      run_number: context.runNumber,
      run_attempt: runAttempt,
      repository: { full_name: `${context.repo.owner}/${context.repo.repo}` },
    },
    choices,
  );
  writeReport(reportPath, pr.number, report);
}

function renderSuiteChecklist(suites, run, selectedSuites = []) {
  const enabled = new Set(selectedSuites);
  const lines = suites.map(
    ({ name, test_count }) =>
      `- [${enabled.has(name) ? "x" : " "}] \`${name}\` (${test_count} ${test_count === 1 ? "test" : "tests"})`,
  );
  const body = [
    "### Python Test Selection",
    "",
    "Please expand the list to view or select test suites.",
    "",
    "<details>",
    "<summary>Test Suites</summary>",
    "",
    [
      "Maintainers can edit the checkboxes below, then **re-run all jobs** or push a commit to apply their selection.",
      "The build reads these choices during test discovery and keeps them when new test suites are discovered.",
      "Editing this comment does not start a build.",
    ].join(" "),
    "",
    [
      ":warning: Wait for the build to finish before editing this comment.",
      "When the build publishes its report, it overwrites any edits made while it was running.",
    ].join(" "),
    "",
    ...lines,
    "",
    "</details>",
    "",
    `The test suites were discovered by [Build #${run.run_number}](https://github.com/${run.repository.full_name}/actions/runs/${run.id}).`,
  ].join("\n");
  return renderReport(REPORT_NAME, run, body);
}

async function preparePythonTestSuites({
  github,
  context,
  runAttempt,
  inventoryPath = "python-test-suites.json",
  reportPath = "python-test-selection-report",
}) {
  const suites = validateInventory(
    JSON.parse(fs.readFileSync(inventoryPath, "utf8")),
  );
  const overrides = await readSuiteChoices({ github, context });
  const choices = resolveSuiteChoices(suites, context.eventName, overrides);
  writeSuiteReport({ context, runAttempt, suites, choices, reportPath });
  return choices;
}

module.exports = {
  resolveSuiteChoices,
  validateInventory,
  parseSuiteChoices,
  renderSuiteChecklist,
  readSuiteChoices,
  writeSuiteReport,
  preparePythonTestSuites,
};
