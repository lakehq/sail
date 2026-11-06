const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { test } = require("node:test");
const {
  validateInventory,
  resolveSuiteChoices,
  parseSuiteChoices,
  renderSuiteChecklist,
  readSuiteChoices,
  writeSuiteReport,
  preparePythonTestSuites,
} = require("./python-test-suites.cjs");

const suite = (name, test_count = 1) => ({
  name,
  test_count,
});
const inventory = [
  suite("default", 100),
  suite("udf", 30),
  suite("integration-hms", 5),
  suite("slow", 49),
];
const run = {
  id: 123,
  run_attempt: 1,
  run_number: 42,
  head_sha: "abc",
  pull_requests: [], // Fork workflow_run payloads can omit this association.
  repository: { full_name: "lakehq/sail" },
};

const overrides = {
  default: true,
  udf: false,
  "integration-hms": true,
  slow: true,
};
const choices = ["default", "integration-hms", "slow"];

test("checklist renders the effective choices", () => {
  assert.deepEqual(
    parseSuiteChoices(renderSuiteChecklist(inventory, run, choices)),
    overrides,
  );
});

for (const event of ["pull_request", "push", "schedule"]) {
  test(`${event} defaults and checklist overrides`, () => {
    const defaults =
      event === "schedule"
        ? ["default", "udf", "integration-hms", "slow"]
        : ["default", "udf"];
    assert.deepEqual(resolveSuiteChoices(inventory, event), defaults);
    assert.deepEqual(
      resolveSuiteChoices(inventory, event, overrides),
      event === "pull_request" ? choices : defaults,
    );
  });
}

test("new suites use defaults and removed suites disappear", () => {
  const suites = [
    suite("new"),
    suite("integration-new"),
    suite("udf"),
    suite("constructor"),
  ];
  assert.deepEqual(resolveSuiteChoices(suites, "pull_request", overrides), [
    "new",
    "constructor",
  ]);
});

test("unsupported events are rejected", () => {
  for (const event of [undefined, "workflow_dispatch", "constructor"]) {
    assert.throws(
      () => resolveSuiteChoices(inventory, event),
      /Unsupported test event/,
    );
    assert.throws(
      () => resolveSuiteChoices([], event),
      /Unsupported test event/,
    );
  }
});

test("all suites can be disabled", () => {
  const disabled = Object.fromEntries(
    inventory.map(({ name }) => [name, false]),
  );
  assert.deepEqual(
    resolveSuiteChoices(inventory, "pull_request", disabled),
    [],
  );
});

test("missing PR comments use defaults", async () => {
  const github = {
    paginate: async () => [],
    rest: { issues: { listComments() {} } },
  };
  const context = {
    repo: { owner: "lakehq", repo: "sail" },
    payload: { pull_request: { number: 1 } },
  };
  const previous = await readSuiteChoices({ github, context });
  assert.deepEqual(resolveSuiteChoices(inventory, "pull_request", previous), [
    "default",
    "udf",
  ]);
});

test("untrusted suite names and invalid inventories are rejected", () => {
  for (const data of [
    {},
    [],
    [null],
    [suite("bad`name")],
    [suite("valid", "1")],
    [suite("valid", -1)],
    [suite("valid", 0)],
    [suite("duplicate"), suite("duplicate")],
    [{ name: "valid" }],
  ]) {
    assert.throws(() => validateInventory(data));
  }
  assert.deepEqual(
    validateInventory([...inventory].reverse()),
    [...inventory].sort((a, b) => a.name.localeCompare(b.name)),
  );
});

test("reads checklist choices only from the matching bot comment", async () => {
  const body = renderSuiteChecklist(inventory, run, choices);
  const comments = [
    { user: { login: "contributor" }, body: body.replace("[ ]", "[x]") },
    { user: { login: "github-actions[bot]" }, body: "<!-- other-report -->" },
    { user: { login: "github-actions[bot]" }, body },
  ];
  const github = {
    paginate: async () => comments,
    rest: { issues: { listComments() {} } },
  };
  const context = {
    repo: { owner: "lakehq", repo: "sail" },
    payload: { pull_request: { number: 1 } },
  };
  assert.deepEqual(await readSuiteChoices({ github, context }), {
    default: true,
    udf: false,
    "integration-hms": true,
    slow: true,
  });
  comments.length = 0;
  assert.deepEqual(await readSuiteChoices({ github, context }), {});
  assert.deepEqual(await readSuiteChoices({ context: { payload: {} } }), {});
});

test("checkbox parsing accepts uppercase checks and uses the last choice", () => {
  assert.deepEqual(
    parseSuiteChoices(
      [
        "- [X] `udf`",
        "- [x] `default`",
        "- [ ] `default`",
        "- [x] `invalid_name`",
        "- [x] `not-a-suite`trailing",
      ].join("\n"),
    ),
    { udf: true, default: false },
  );
});

test("discovery resolves the inventory and writes the effective checklist", async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "sail-selection-"));
  t.after(() => fs.rmSync(directory, { recursive: true }));
  const reportPath = path.join(directory, "python-test-selection-report");
  const inventoryPath = path.join(directory, "suites.json");
  fs.writeFileSync(inventoryPath, JSON.stringify(inventory));
  const github = {
    paginate: async () => [
      {
        user: { login: "github-actions[bot]" },
        body: renderSuiteChecklist(inventory, run, choices),
      },
    ],
    rest: { issues: { listComments() {} } },
  };
  const context = {
    eventName: "pull_request",
    repo: { owner: "lakehq", repo: "sail" },
    payload: { pull_request: { number: 1 } },
    runId: run.id,
    runNumber: run.run_number,
  };
  const selected = await preparePythonTestSuites({
    github,
    context,
    runAttempt: 2,
    inventoryPath,
    reportPath,
  });
  assert.deepEqual(selected, choices);
  assert.equal(
    fs.readFileSync(path.join(reportPath, "pull-request.txt"), "utf8"),
    "1\n",
  );
  const body = fs.readFileSync(path.join(reportPath, "report.md"), "utf8");
  assert.ok(body.includes("<!-- python-test-selection-run:123:2 -->"));
  assert.equal(parseSuiteChoices(body).udf, false);
  assert.equal(Object.keys(parseSuiteChoices(body)).length, inventory.length);
  assert.ok(body.includes("it overwrites any edits made while it was running"));
  assert.deepEqual(parseSuiteChoices(body), overrides);
});

test("does not write a pull request report for other build events", (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "sail-selection-"));
  t.after(() => fs.rmSync(directory, { recursive: true }));
  const reportPath = path.join(directory, "python-test-selection-report");
  writeSuiteReport({
    context: { payload: {} },
    suites: inventory,
    choices,
    reportPath,
  });
  assert.equal(fs.existsSync(reportPath), false);
});
