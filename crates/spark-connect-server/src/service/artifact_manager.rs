use std::collections::HashMap;
use std::sync::Arc;

use tonic::codegen::tokio_stream::Stream;
use tonic::Status;

use crate::session::Session;
use crate::spark::connect::add_artifacts_request::Payload;
use crate::spark::connect::add_artifacts_response::ArtifactSummary;
use crate::spark::connect::artifact_statuses_response::ArtifactStatus;

pub(crate) async fn handle_add_artifacts(
    _session: Arc<Session>,
    _stream: impl Stream<Item = Result<Payload, Status>>,
) -> Result<Vec<ArtifactSummary>, Status> {
    todo!()
}

pub(crate) async fn handle_artifact_statuses(
    _session: Arc<Session>,
    _names: Vec<String>,
) -> Result<HashMap<String, ArtifactStatus>, Status> {
    todo!()
}
