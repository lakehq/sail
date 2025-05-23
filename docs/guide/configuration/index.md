---
title: Configuration
rank: 50
---

# Configuration

Sail comes with a configuration system that affects the behavior of Sail.
The configuration keys are hierarchical and have sensible defaults.
The Sail configuration is meant to be complementary to the Spark configuration.

You can refer to the [configuration definition file](https://github.com/lakehq/sail/blob/main/crates/sail-common/src/config/default.toml) for a complete list of configuration keys and their default values.

You can override configuration values by environment variables when running the Sail CLI or using the PySail library.
The environment variable names are prefixed with `SAIL_`.
Use upper case letters and underscores to refer to names in the configuration key hierarchy, and use two underscores (`__`) to separate hierarchy levels.
The environment variable values are parsed as TOML values, while the quotes around string values can be omitted.
You can override one configuration key that expects a single primitive value, or a group of configuration keys using the TOML inline table syntax.

Here are some examples of overriding configuration values via environment variables.
The examples are shown in a shell script.

```bash
# Set `mode` to "local".
export SAIL_MODE="local"

# Set `mode` to "local" using quoted string.
export SAIL_MODE='"local"'

# Set `cluster.worker_task_slots` to 16.
# Note that the quotes are part of the shell syntax and are not seen by Sail.
export SAIL_CLUSTER__WORKER_TASK_SLOTS="16"
```
