---
title: Configuration
rank: 50
---

# Configuration

Sail comes with a configuration system that affects its behavior.
The Sail configuration is complementary to the Spark configuration.

The complete list of configuration options can be found in the [Reference](/reference/configuration/).

## Concepts

Each configuration option is defined as a **key** with its associated **value**.

The key is a string that contains multiple names separated by dots (`.`).
Each name is a string containing lowercase letters, digits, and underscores (`_`).

The names form a path in the hierarchical configuration **namespace**.
For example, the key `foo.bar.baz` refers to the `baz` option in the `bar` namespace, which is, in turn, in the `foo` namespace.
We also say that `foo` and `foo.bar` are **prefixes** of the key.

The value has a **type** and comes with a sensible **default value**.
The value types are `string`, `number`, `boolean`, `array`, and `map`.

Some configuration options may further restrict the allowed values of a given type.
For example, an option may be defined to have `number` values but only accept unsigned integers, so `1.0` would be invalid for this option, even though `1.0` is a `number`.

As a Sail user, you usually care about the **string representation** of the value. This leads to the following section, which shows how to set configuration options via environment variables.

## Environment Variables

You can set configuration options via **environment variables** when running the Sail CLI or using the PySail library.

To map a configuration key to an environment variable, use the following steps:

1. Convert letters in the key to uppercase. Keep digits and underscores (`_`) unchanged.
2. Replace `.` in the key with two underscores (`__`) to separate hierarchy levels.
3. Prefix the key with `SAIL_`.

To specify a configuration value, use its **string representation** that follows TOML syntax, while the quotes around string values can be omitted.

::: info
The quotes around string values are useful to avoid ambiguity with value types.
For example, `"1.0"` is parsed as a `string`, while `1.0` is parsed as a `number`, so you must use quotes to distinguish between the two.
:::

You can set one configuration option at a time or a group of configuration options using the TOML inline table syntax.
When setting a group of options, the "key" is a common prefix for all the individual keys.

Here are some examples of setting configuration options via environment variables.
The examples are shown in a shell script.

```bash
# Set `mode` to "local".
# The quotes (`""`) are part of the shell syntax and are not seen by Sail.
export SAIL_MODE="local"

# Set `mode` to "local" using a quoted string.
# The outer quotes (`''`) are part of the shell syntax and are not seen by Sail.
# The inner quotes (`""`) are part of the string representation of the
# configuration value and are seen by Sail.
export SAIL_MODE='"local"'

# Set `cluster.worker_task_slots` to 16.
# The quotes (`""`) are part of the shell syntax and are not seen by Sail.
export SAIL_CLUSTER__WORKER_TASK_SLOTS="16"

# Set multiple configuration options at once.
# The quotes (`''`) are part of the shell syntax and are not seen by Sail.
export SAIL_CLUSTER='{ worker_task_slots = 16, worker_max_count = 2 }'
```
