use std::collections::HashMap;

use datafusion::prelude::SessionContext;
use tonic::codegen::tokio_stream::Stream;
use tonic::Status;

use crate::error::{SparkError, SparkResult};
use crate::spark::connect::add_artifacts_request::Payload;
use crate::spark::connect::add_artifacts_response::ArtifactSummary;
use crate::spark::connect::artifact_statuses_response::ArtifactStatus;

pub(crate) async fn handle_add_artifacts(
    _ctx: &SessionContext,
    _stream: impl Stream<Item = Result<Payload, Status>>,
) -> SparkResult<Vec<ArtifactSummary>> {
    Err(SparkError::todo("handle add artifacts"))
}

pub(crate) async fn handle_artifact_statuses(
    _ctx: &SessionContext,
    _names: Vec<String>,
) -> SparkResult<HashMap<String, ArtifactStatus>> {
    Err(SparkError::todo("handle artifact statuses"))
}
