/// The environment variable that turns Sail CLI into a Python interpreter.
/// This allows the embedded Python interpreter to fork child Python processes.
/// This is used by the Sail CLI.
pub const INTERNAL_ENV_VAR_PYTHON_INTERPRETER: &str = "SAIL_INTERNAL__PYTHON_INTERPRETER";
