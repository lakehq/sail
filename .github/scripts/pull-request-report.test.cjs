const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { test } = require("node:test");

const { validateReport } = require("./pull-request-report.cjs");
const { renderSuiteChecklist } = require("./python-test-suites.cjs");

function temporaryDirectory(t) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "sail-report-"));
  t.after(() => fs.rmSync(directory, { recursive: true }));
  return directory;
}

function pythonReport(t) {
  const reportPath = temporaryDirectory(t);
  const run = {
    id: 123,
    run_attempt: 2,
    run_number: 42,
    head_sha: "abc",
    pull_requests: [], // Fork workflow_run payloads can omit this association.
    repository: { full_name: "lakehq/sail" },
  };
  const body = renderSuiteChecklist([], run);
  fs.writeFileSync(path.join(reportPath, "pull-request.txt"), "1\n");
  fs.writeFileSync(path.join(reportPath, "report.md"), body);
  const pr = {
    state: "open",
    head: { sha: "abc" },
    base: { repo: { full_name: "lakehq/sail" } },
  };
  const comments = [{ user: { login: "github-actions[bot]" }, body }];
  const github = {
    paginate: async () => comments,
    rest: {
      pulls: { get: async () => ({ data: pr }) },
      issues: { listComments() {} },
    },
  };
  const options = {
    github,
    context: {
      repo: { owner: "lakehq", repo: "sail" },
      payload: { workflow_run: run },
    },
    core: { info() {} },
    reportPath,
    reportName: "sail-python-test-suites",
  };
  return { options, body, run, pr, comments };
}

test("other reports need no checklist metadata or API lookups", async (t) => {
  const reportPath = temporaryDirectory(t);
  fs.writeFileSync(path.join(reportPath, "pull-request.txt"), "1\n");
  for (const reportName of ["spark-3.5.9-test", "ibis-test", "gold-data"]) {
    assert.equal(await validateReport({ reportPath, reportName }), 1);
  }
});

test("all report types reject invalid names and pull request numbers", async (t) => {
  const reportPath = temporaryDirectory(t);
  for (const reportName of ["gold-data", "sail-python-test-suites"]) {
    for (const number of ["0", "-1", "1.5", "1 extra", "9007199254740992"]) {
      fs.writeFileSync(path.join(reportPath, "pull-request.txt"), number);
      await assert.rejects(
        validateReport({ reportPath, reportName }),
        /Invalid pull request number/,
      );
    }
  }
  for (const reportName of ["invalid report name", "../report", undefined]) {
    await assert.rejects(
      validateReport({ reportPath, reportName }),
      /Invalid report name/,
    );
  }
});

test("all report types skip missing or empty pull request numbers", async (t) => {
  const reportPath = temporaryDirectory(t);
  for (const reportName of ["gold-data", "sail-python-test-suites"]) {
    assert.equal(await validateReport({ reportPath, reportName }), "");
    fs.writeFileSync(path.join(reportPath, "pull-request.txt"), "\n");
    assert.equal(await validateReport({ reportPath, reportName }), "");
    fs.unlinkSync(path.join(reportPath, "pull-request.txt"));
  }
});

test("Python report validation preserves the artifact and accepts missing bot comments", async (t) => {
  const { options, body, comments, run } = pythonReport(t);
  assert.equal(await validateReport(options), 1);
  assert.equal(
    fs.readFileSync(path.join(options.reportPath, "report.md"), "utf8"),
    body,
  );
  comments.length = 0;
  assert.equal(await validateReport(options), 1);
  run.pull_requests = [{ number: 1 }];
  assert.equal(await validateReport(options), 1);
});

test("Python reports reject metadata from another run or attempt", async (t) => {
  const { options, run } = pythonReport(t);
  const invalidReports = [
    renderSuiteChecklist([], { ...run, id: run.id + 1 }),
    renderSuiteChecklist([], { ...run, run_attempt: run.run_attempt - 1 }),
    "<!-- sail-python-test-suites -->\nMissing run metadata",
    `<!-- sail-python-test-suites-run:${run.id}:${run.run_attempt} -->`,
  ];
  for (const body of invalidReports) {
    fs.writeFileSync(path.join(options.reportPath, "report.md"), body);
    await assert.rejects(validateReport(options), /completed workflow attempt/);
  }
});

test("Python reports skip closed, obsolete, or unrelated pull requests", async (t) => {
  const { options, run, pr } = pythonReport(t);
  pr.state = "closed";
  assert.equal(await validateReport(options), "");
  pr.state = "open";
  pr.head.sha = "new-commit";
  assert.equal(await validateReport(options), "");
  pr.head.sha = run.head_sha;
  pr.base.repo.full_name = "another/repository";
  assert.equal(await validateReport(options), "");
  pr.base.repo.full_name = "lakehq/sail";
  run.pull_requests = [{ number: 2 }];
  assert.equal(await validateReport(options), "");
});

test("Python reports cannot overwrite a newer run or attempt", async (t) => {
  const { options, run, comments } = pythonReport(t);
  for (const newer of [
    { ...run, id: run.id + 1 },
    { ...run, run_attempt: run.run_attempt + 1 },
  ]) {
    comments[0].body = renderSuiteChecklist([], newer);
    assert.equal(await validateReport(options), "");
  }
  for (const older of [
    { ...run, id: run.id - 1 },
    { ...run, run_attempt: run.run_attempt - 1 },
  ]) {
    comments[0].body = renderSuiteChecklist([], older);
    assert.equal(await validateReport(options), 1);
  }
});
