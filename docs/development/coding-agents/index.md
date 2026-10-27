---
title: Coding Agents
rank: 2
---

# Coding Agents

You can add `AGENTS.md` in the project root directory to provide instructions for coding agents.
This file is ignored by Git so that you can customize it for your needs.

Here is an example that instructs coding agents to read instructions in the `.github` directory when making changes or reviewing code.

::: warning
The example may change over time. You would need to maintain your local instructions to keep them up to date.
:::

```markdown
Read `.github/copilot-instructions.md` for an overview of the project.

Read `.github/instructions/dev.instructions.md` before making changes in this repository and follow it as the default project guidance.

When performing a code review on Rust changes, also read `.github/instructions/rust-review.instructions.md` and follow it for the review.

When performing a code review on Python changes, also read `.github/instructions/python-review.instructions.md` and follow it for the review.

If multiple instruction files apply, use the more specific file for the task in addition to the default project guidance.

You only need to read the instructions once per session. There is no need to read them upon every task.
```
