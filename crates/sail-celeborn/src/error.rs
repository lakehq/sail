use thiserror::Error;

pub type CelebornResult<T> = Result<T, CelebornError>;

#[derive(Debug, Error)]
pub enum CelebornError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protobuf error: {0}")]
    Protobuf(#[from] prost::DecodeError),
    #[error("request timed out")]
    Timeout,
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("invalid transport response: {0}")]
    Protocol(String),
    #[error("master error: status {status}")]
    Master { status: i32 },
    #[error("worker error: status {status}")]
    Worker { status: i32 },
    #[error("application error: {0}")]
    Application(String),
    #[error("actor has stopped")]
    ActorStopped,
}
