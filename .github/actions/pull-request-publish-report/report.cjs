const fs = require("node:fs");

function parseReportMetadata(body) {
  const [firstLine] = body.split(/\r?\n/, 1);
  const match = firstLine.match(/^<!-- sail-pull-request-report:v1 (.+) -->$/);
  if (!match) return undefined;

  let metadata;
  try {
    metadata = JSON.parse(match[1]);
  } catch {
    return undefined;
  }
  if (
    typeof metadata?.report !== "string" ||
    !/^[a-z0-9_.-]+$/.test(metadata.report) ||
    !["pr", "workflow_id", "run_id", "run_number", "attempt"].every(
      (key) => Number.isSafeInteger(metadata[key]) && metadata[key] > 0,
    )
  ) {
    return undefined;
  }
  return metadata;
}

function matchesPullRequest(pr, run, context, number) {
  const { owner, repo } = context.repo;
  if (
    pr.number !== number ||
    pr.state !== "open" ||
    pr.head.sha !== run.head_sha ||
    pr.base.repo.full_name !== `${owner}/${repo}`
  ) {
    return false;
  }
  if (run.pull_requests?.length) {
    return run.pull_requests.some(
      (pullRequest) => pullRequest.number === number,
    );
  }

  // Fork runs can omit the pull request list. Both report workflows target the default branch.
  return (
    pr.base.ref === context.payload.repository.default_branch &&
    run.head_repository?.id != null &&
    pr.head.repo?.id === run.head_repository.id &&
    Boolean(run.head_branch) &&
    pr.head.ref === run.head_branch
  );
}

async function validateReport({
  github,
  context,
  core,
  reportPath,
  reportName,
}) {
  const body = fs.readFileSync(reportPath, "utf8");
  const metadata = parseReportMetadata(body);
  const run = context.payload.workflow_run;
  if (!metadata) {
    core.warning(
      "Ignoring a report with missing or invalid versioned metadata",
    );
    return;
  }

  // The artifact is untrusted. Bind its identity to the completed workflow run.
  if (
    run?.event !== "pull_request" ||
    metadata.report !== reportName ||
    metadata.workflow_id !== run.workflow_id ||
    metadata.run_id !== run.id ||
    metadata.run_number !== run.run_number
  ) {
    throw new Error(
      "Report metadata does not match the completed workflow run",
    );
  }

  // A partial rerun can leave the artifact from a previous attempt in place.
  if (metadata.attempt !== run.run_attempt) {
    core.info("Ignoring a report from a different workflow attempt");
    return;
  }

  const { data: pr } = await github.rest.pulls.get({
    ...context.repo,
    pull_number: metadata.pr,
  });
  if (!matchesPullRequest(pr, run, context, metadata.pr)) {
    core.info("Ignoring a report for an obsolete or unrelated pull request");
    return;
  }

  const comments = await github.paginate(github.rest.issues.listComments, {
    ...context.repo,
    issue_number: metadata.pr,
    per_page: 100,
  });
  const alreadyPublished = comments.some(({ body, user }) => {
    if (user?.login !== "github-actions[bot]" || !body) return false;
    const previous = parseReportMetadata(body);
    return (
      previous?.report === metadata.report &&
      previous.pr === metadata.pr &&
      previous.workflow_id === metadata.workflow_id &&
      (previous.run_number > metadata.run_number ||
        (previous.run_number === metadata.run_number &&
          previous.attempt >= metadata.attempt))
    );
  });
  if (alreadyPublished) {
    core.info(
      "Ignoring a report that has already been published or superseded",
    );
    return;
  }

  return metadata.pr;
}

module.exports = { validateReport };
