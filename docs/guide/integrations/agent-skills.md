---
title: Agent Skills
rank: 2
---

# Agent Skills

The `sail spark run` command can be used as a CLI tool that lets your LLM agents execute PySpark scripts via Sail's compute engine.
By exposing this command as an agent skill, the agent can perform data processing tasks using the familiar PySpark API.

Here is an example skill definition.
Refer to your LLM provider's documentation to see how to load the skill for your agents.

::: code-group

<!-- prettier-ignore -->
<<< ./_code/pyspark-skill.md [SKILL.md]

:::
