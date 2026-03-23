---
title: Agent Skills
rank: 2
---

# Agent Skills

The `sail spark run` command can be used as a CLI tool that enables your LLM agents to execute PySpark scripts via Sail's highly efficient compute engine.
By exposing this command as an agent skill, the agent can perform data processing tasks using the familiar PySpark API while enjoying lightning-fast performance.

Here is an example skill definition.
Refer to your LLM provider's documentation to see how to load the skill for your agents.

::: code-group

<!-- prettier-ignore -->
<<< ./_code/pyspark-skill.md [SKILL.md]

:::
