const fs = require("node:fs");
const path = require("node:path");

function reportHeader(reportName) {
  return `<!-- ${reportName} -->`;
}

function renderReport(reportName, run, body) {
  return [
    reportHeader(reportName),
    `<!-- ${reportName}-run:${run.id}:${run.run_attempt} -->`,
    body,
  ].join("\n");
}

function writeReport(reportPath, number, body) {
  fs.mkdirSync(reportPath, { recursive: true });
  fs.writeFileSync(path.join(reportPath, "pull-request.txt"), `${number}\n`);
  fs.writeFileSync(path.join(reportPath, "report.md"), body);
}

async function findReportComment(github, repo, number, reportName) {
  const header = reportHeader(reportName);
  const comments = await github.paginate(github.rest.issues.listComments, {
    ...repo,
    issue_number: number,
    per_page: 100,
  });
  return comments.find(
    ({ body, user }) =>
      user?.login === "github-actions[bot]" && body?.startsWith(header),
  );
}

function readPullRequestNumber(reportPath) {
  const filename = path.join(reportPath, "pull-request.txt");
  if (!fs.existsSync(filename)) return undefined;
  const text = fs.readFileSync(filename, "utf8").trim();
  if (!text) return undefined;
  const number = Number(text);
  if (!/^[1-9][0-9]*$/.test(text) || !Number.isSafeInteger(number)) {
    throw new Error("Invalid pull request number");
  }
  return number;
}

function parseReportRun(body, reportName) {
  const marker = new RegExp(`<!-- ${reportName}-run:(\\d+):(\\d+) -->`);
  const match = body.match(marker);
  if (!match) return undefined;
  return { id: Number(match[1]), run_attempt: Number(match[2]) };
}

function validateReportRun(body, reportName, run) {
  const generated = parseReportRun(body, reportName);
  if (
    !body.startsWith(reportHeader(reportName)) ||
    !generated ||
    generated.id !== run.id ||
    generated.run_attempt !== run.run_attempt
  ) {
    throw new Error("Report does not belong to the completed workflow attempt");
  }
}

function matchesPullRequest(pr, run, repo, number) {
  return (
    pr.state === "open" &&
    pr.head.sha === run.head_sha &&
    pr.base.repo.full_name === `${repo.owner}/${repo.repo}` &&
    (!run.pull_requests.length ||
      run.pull_requests.some((pullRequest) => pullRequest.number === number))
  );
}

function isNewerRun(previous, run) {
  return (
    previous !== undefined &&
    (previous.id > run.id ||
      (previous.id === run.id && previous.run_attempt > run.run_attempt))
  );
}

async function isCurrentReport({ github, context, core, reportName, number }) {
  const run = context.payload.workflow_run;
  // Artifacts are untrusted data. Bind the destination to the completed run,
  // and never execute code or generate report content from a PR in this workflow.
  const { data: pr } = await github.rest.pulls.get({
    ...context.repo,
    pull_number: number,
  });
  if (!matchesPullRequest(pr, run, context.repo, number)) {
    core.info("Ignoring an obsolete or unrelated report");
    return false;
  }

  const comment = await findReportComment(
    github,
    context.repo,
    number,
    reportName,
  );
  const previous = comment
    ? parseReportRun(comment.body, reportName)
    : undefined;
  return !isNewerRun(previous, run);
}

async function validateReport(options) {
  const { reportPath, reportName } = options;
  if (typeof reportName !== "string" || !/^[a-z0-9_.-]+$/.test(reportName)) {
    throw new Error("Invalid report name");
  }
  const number = readPullRequestNumber(reportPath);
  if (number === undefined) return "";

  // Python test selection reports carry suite choices across builds, so only the current
  // workflow attempt may publish them. Other reports retain their existing behavior.
  if (reportName === "python-test-selection") {
    const body = fs.readFileSync(path.join(reportPath, "report.md"), "utf8");
    validateReportRun(body, reportName, options.context.payload.workflow_run);
    if (!(await isCurrentReport({ ...options, number }))) return "";
  }
  return number;
}

module.exports = {
  findReportComment,
  renderReport,
  writeReport,
  validateReport,
};
