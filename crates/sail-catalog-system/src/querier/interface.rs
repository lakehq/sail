use async_trait::async_trait;
use datafusion::common::Result;

pub use crate::gen::catalog::SessionRow;

#[async_trait]
pub trait SessionQuerier: Send + Sync {
    async fn sessions(&self) -> Result<Vec<SessionRow>>;
}
