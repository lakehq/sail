const fs = require("node:fs");

async function prepareReport({
  github,
  context,
  reportName,
  reportPath,
  runAttempt,
}) {
  const { data: run } = await github.rest.actions.getWorkflowRun({
    ...context.repo,
    run_id: context.runId,
    exclude_pull_requests: true,
  });
  const metadata = {
    report: reportName,
    pr: context.payload.pull_request.number,
    workflow_id: run.workflow_id,
    run_id: context.runId,
    run_number: context.runNumber,
    attempt: runAttempt,
  };
  const body = fs.readFileSync(reportPath, "utf8");
  fs.writeFileSync(
    reportPath,
    `<!-- sail-pull-request-report:v1 ${JSON.stringify(metadata)} -->\n${body}`,
  );
}

module.exports = { prepareReport };
