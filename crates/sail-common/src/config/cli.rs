use figment::providers::{Env, Serialized};
use figment::Figment;
use serde::{Deserialize, Serialize};

use crate::error::{CommonError, CommonResult};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CliConfig {
    pub run_python: bool,
}

impl CliConfig {
    pub fn load() -> CommonResult<Self> {
        Figment::from(Serialized::defaults(CliConfig::default()))
            .merge(Env::prefixed("SAIL_INTERNAL_").map(|p| p.as_str().replace("__", ".").into()))
            .extract()
            .map_err(|e| CommonError::InvalidArgument(e.to_string()))
    }
}

/// Environment variables for CLI configuration.
pub struct CliConfigEnv;

impl CliConfigEnv {
    /// Turn Sail CLI into a Python interpreter.
    /// This allows the embedded Python interpreter to fork child Python processes.
    pub const RUN_PYTHON: &'static str = "SAIL_INTERNAL_RUN_PYTHON";
}
