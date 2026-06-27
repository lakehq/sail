#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("environment variable error: {0}")]
    EnvVar(#[from] std::env::VarError),
    #[error("Rust syntax error: {0}")]
    Syn(#[from] syn::Error),
    #[error("invalid input: {0}")]
    InvalidInput(String),
}
