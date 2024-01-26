use crate::session::SessionManager;
use crate::spark::connect::config_request::operation::OpType;
use crate::spark::connect::config_request::{
    Get, GetAll, GetOption, GetWithDefault, IsModifiable, Operation, Set, Unset,
};
use crate::spark::connect::spark_connect_service_server::SparkConnectService;
use crate::spark::connect::{
    AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest, AnalyzePlanResponse,
    ArtifactStatusesRequest, ArtifactStatusesResponse, ConfigRequest, ConfigResponse,
    ExecutePlanRequest, ExecutePlanResponse, InterruptRequest, InterruptResponse, KeyValue,
    ReattachExecuteRequest, ReleaseExecuteRequest, ReleaseExecuteResponse,
};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

pub struct SparkConnectServer {
    session_manager: SessionManager,
}

impl Default for SparkConnectServer {
    fn default() -> Self {
        Self {
            session_manager: SessionManager::new(),
        }
    }
}

#[tonic::async_trait]
impl SparkConnectService for SparkConnectServer {
    type ExecutePlanStream = ReceiverStream<Result<ExecutePlanResponse, Status>>;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        println!("{:?}", request);
        todo!()
    }

    async fn analyze_plan(
        &self,
        request: Request<AnalyzePlanRequest>,
    ) -> Result<Response<AnalyzePlanResponse>, Status> {
        println!("{:?}", request);
        todo!()
    }

    async fn config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        println!("{:?}", request);
        let request = request.into_inner();
        let session_id = request.session_id;
        let session = self.session_manager.get_session(&session_id)?;
        let mut session = session
            .lock()
            .or_else(|_| Err(Status::internal("failed to lock session")))?;

        let mut pairs = vec![];
        let warnings = vec![];
        if let Some(Operation { op_type: Some(op) }) = request.operation {
            match op {
                OpType::Get(Get { keys }) => {
                    for key in keys {
                        pairs.push(KeyValue {
                            key: key.clone(),
                            value: session.get_config(&key).map(|v| v.clone()),
                        });
                    }
                }
                OpType::Set(Set { pairs: kv }) => {
                    for KeyValue { key, value } in kv {
                        if let Some(value) = value {
                            session.set_config(&key, &value);
                        } else {
                            session.unset_config(&key);
                        }
                    }
                }
                OpType::GetWithDefault(GetWithDefault { pairs: kv }) => {
                    for KeyValue { key, value } in kv {
                        pairs.push(KeyValue {
                            key: key.clone(),
                            value: session.get_config(&key).map(|v| v.clone()).or(value),
                        });
                    }
                }
                OpType::GetOption(GetOption { keys }) => {
                    for key in keys {
                        if let Some(value) = session.get_config(&key) {
                            pairs.push(KeyValue {
                                key: key.clone(),
                                value: Some(value.clone()),
                            });
                        }
                    }
                }
                OpType::GetAll(GetAll { prefix }) => {
                    for (k, v) in session.iter_config(&prefix) {
                        pairs.push(KeyValue {
                            key: k.clone(),
                            value: Some(v.clone()),
                        });
                    }
                }
                OpType::Unset(Unset { keys }) => {
                    for key in keys {
                        session.unset_config(&key);
                    }
                }
                OpType::IsModifiable(IsModifiable { keys }) => {
                    for key in keys {
                        pairs.push(KeyValue {
                            key: key.clone(),
                            value: Some("true".to_string()),
                        });
                    }
                }
            }
        }
        let response = ConfigResponse {
            session_id,
            pairs,
            warnings,
        };
        Ok(Response::new(response))
    }

    async fn add_artifacts(
        &self,
        request: Request<Streaming<AddArtifactsRequest>>,
    ) -> Result<Response<AddArtifactsResponse>, Status> {
        println!("{:?}", request);
        todo!()
    }

    async fn artifact_status(
        &self,
        request: Request<ArtifactStatusesRequest>,
    ) -> Result<Response<ArtifactStatusesResponse>, Status> {
        println!("{:?}", request);
        todo!()
    }

    async fn interrupt(
        &self,
        request: Request<InterruptRequest>,
    ) -> Result<Response<InterruptResponse>, Status> {
        println!("{:?}", request);
        todo!()
    }

    type ReattachExecuteStream = ReceiverStream<Result<ExecutePlanResponse, Status>>;

    async fn reattach_execute(
        &self,
        request: Request<ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        println!("{:?}", request);
        todo!()
    }

    async fn release_execute(
        &self,
        request: Request<ReleaseExecuteRequest>,
    ) -> Result<Response<ReleaseExecuteResponse>, Status> {
        println!("{:?}", request);
        todo!()
    }
}
