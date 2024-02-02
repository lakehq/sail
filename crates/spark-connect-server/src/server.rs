use crate::plan::{execute_plan, from_spark_relation, write_arrow_batches};
use crate::schema::to_spark_schema;
use crate::session::SessionManager;
use crate::spark::connect::config_request as cr;
use crate::spark::connect::execute_plan_response as epr;
use crate::spark::connect::plan;
use crate::spark::connect::spark_connect_service_server::SparkConnectService;
use crate::spark::connect::{
    AddArtifactsRequest, AddArtifactsResponse, AnalyzePlanRequest, AnalyzePlanResponse,
    ArtifactStatusesRequest, ArtifactStatusesResponse, ConfigRequest, ConfigResponse,
    ExecutePlanRequest, ExecutePlanResponse, InterruptRequest, InterruptResponse, KeyValue, Plan,
    ReattachExecuteRequest, ReleaseExecuteRequest, ReleaseExecuteResponse,
};
use datafusion::prelude::SessionContext;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

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
        let request = request.into_inner();
        let session_id = request.session_id;
        let operation_id = request
            .operation_id
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let mut responses = vec![];
        if let ExecutePlanRequest {
            plan:
                Some(Plan {
                    op_type: Some(plan::OpType::Root(relation)),
                }),
            ..
        } = request
        {
            let ctx = SessionContext::new();
            let plan = from_spark_relation(&ctx, &relation).await?;
            let batches = execute_plan(&ctx, &plan).await?;
            let schema = batches.schema();
            let output = write_arrow_batches(batches).await?;

            responses.push(ExecutePlanResponse {
                session_id: session_id.clone(),
                operation_id: operation_id.clone(),
                response_id: Uuid::new_v4().to_string(),
                metrics: None,
                observed_metrics: vec![],
                schema: Some(to_spark_schema(schema)?),
                response_type: Some(epr::ResponseType::ArrowBatch(output)),
            });
        }

        responses.push(ExecutePlanResponse {
            session_id: session_id.clone(),
            operation_id: operation_id.clone(),
            response_id: Uuid::new_v4().to_string(),
            metrics: None,
            observed_metrics: vec![],
            schema: None,
            response_type: Some(epr::ResponseType::ResultComplete(epr::ResultComplete {})),
        });
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            for response in responses {
                let _ = tx.send(Ok(response)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
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
        if let Some(cr::Operation { op_type: Some(op) }) = request.operation {
            match op {
                cr::operation::OpType::Get(cr::Get { keys }) => {
                    for key in keys {
                        pairs.push(KeyValue {
                            key: key.clone(),
                            value: session.get_config(&key).map(|v| v.clone()),
                        });
                    }
                }
                cr::operation::OpType::Set(cr::Set { pairs: kv }) => {
                    for KeyValue { key, value } in kv {
                        if let Some(value) = value {
                            session.set_config(&key, &value);
                        } else {
                            session.unset_config(&key);
                        }
                    }
                }
                cr::operation::OpType::GetWithDefault(cr::GetWithDefault { pairs: kv }) => {
                    for KeyValue { key, value } in kv {
                        pairs.push(KeyValue {
                            key: key.clone(),
                            value: session.get_config(&key).map(|v| v.clone()).or(value),
                        });
                    }
                }
                cr::operation::OpType::GetOption(cr::GetOption { keys }) => {
                    for key in keys {
                        if let Some(value) = session.get_config(&key) {
                            pairs.push(KeyValue {
                                key: key.clone(),
                                value: Some(value.clone()),
                            });
                        }
                    }
                }
                cr::operation::OpType::GetAll(cr::GetAll { prefix }) => {
                    for (k, v) in session.iter_config(&prefix) {
                        pairs.push(KeyValue {
                            key: k.clone(),
                            value: Some(v.clone()),
                        });
                    }
                }
                cr::operation::OpType::Unset(cr::Unset { keys }) => {
                    for key in keys {
                        session.unset_config(&key);
                    }
                }
                cr::operation::OpType::IsModifiable(cr::IsModifiable { keys }) => {
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
        let request = request.into_inner();
        let session_id = request.session_id;
        let operation_id = request.operation_id;
        let response = ReleaseExecuteResponse {
            session_id,
            operation_id: Some(operation_id),
        };
        Ok(Response::new(response))
    }
}
